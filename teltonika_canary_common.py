#!/usr/bin/env python3
"""
Спільна бібліотека для канареечних моніторів Teltonika M2M.
Містить:
  - формування пакетів Codec 8 із розрахунком реального курсу (bearing);
  - TrackPacer для точного таймінгу (щільні кроки, паузи CAN, розриви зв'язку);
  - парсер історії сповіщень і Звіту v2 з прив'язкою 1-в-1 по metadata.fuelStartTime;
  - класифікацію статусів OK / ПРОПУСК / ДУБЛЬ / ДУБЛЬ ЗІ ЗСУВОМ;
  - діагностику і Telegram-сповіщення.
"""

import json
import logging
import math
import re
import socket
import struct
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("teltonika_canary_common")

try:
    from zoneinfo import ZoneInfo

    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV_TZ = timezone(timedelta(hours=2))


def get_resilient_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """Створює HTTP-сесію з повторними спробами для стабільного опитування M2M API."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


GLOBAL_SESSION = get_resilient_session()


def crc16_arc(data: bytes) -> int:
    """Обчислює CRC-16/ARC для кадру Teltonika Codec 8."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Повертає відстань у метрах між двома WGS-84 точками."""
    radius = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Обчислює істинний азимут (0..359°) від попередньої точки до поточної."""
    if abs(lat2 - lat1) < 1e-7 and abs(lon2 - lon1) < 1e-7:
        return -1
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    deg = math.degrees(math.atan2(y, x))
    return int(round((deg + 360.0) % 360.0)) % 360


def send_teltonika_point(
    cfg: dict,
    level: float,
    lat: float,
    lon: float,
    speed: int = 0,
    bearing: int | None = None,
    ignition: int = 1,
    extra_2byte_ios: dict[int, float] | None = None,
) -> None:
    """Надсилає одну точку Teltonika Codec 8 через TCP з автоповторами (підтримує кілька 2-байтових IO)."""
    host, port, imei = cfg["ingest_host"], cfg["ingest_port"], cfg["tracker_imei"]
    timestamp_ms = int(time.time() * 1000)
    used_bearing = cfg.get("bearing", 0) if bearing is None else int(bearing) % 360

    avl_data = struct.pack(
        ">Q b i i h h B h",
        timestamp_ms,
        0,
        int(round(lon * 10_000_000)),
        int(round(lat * 10_000_000)),
        cfg["altitude"],
        used_bearing,
        cfg["sat"],
        int(round(speed)),
    )

    io_2byte: list[tuple[int, int]] = [(201, int(round(level)))]
    if extra_2byte_ios:
        for io_id in sorted(extra_2byte_ios):
            val = extra_2byte_ios[io_id]
            if val is not None and int(io_id) != 201:
                io_2byte.append((int(io_id), int(round(val))))

    io_data = bytearray()
    io_data.append(0)  # Event IO ID
    io_data.append(2 + len(io_2byte))  # Total IO elements

    # 1-byte IOs: Movement (240) та Ignition (239)
    io_data.append(2)
    io_data.append(240)
    io_data.append(1 if speed > 0 else 0)
    io_data.append(239)
    io_data.append(1 if ignition else 0)

    # 2-byte IOs: Fuel level (201) + опційно AdBlue (203) тощо
    io_data.append(len(io_2byte))
    for io_id, io_val in io_2byte:
        io_data.append(io_id)
        io_data.extend(struct.pack(">h", io_val))

    # 4-byte and 8-byte elements: 0
    io_data.append(0)
    io_data.append(0)

    data_part8 = bytearray([0x08, 1]) + avl_data + io_data + bytearray([1])
    crc8 = crc16_arc(data_part8)
    packet_c8 = (
        bytearray(b"\x00\x00\x00\x00")
        + struct.pack(">I", len(data_part8))
        + data_part8
        + struct.pack(">I", crc8)
    )

    extra_str = ""
    if len(io_2byte) > 1:
        extra_str = ", " + ", ".join(
            ("AdBlue=%d л" % v if k == 203 else f"io_{k}=%d" % v) for k, v in io_2byte[1:]
        )

    log.info(
        "Teltonika: Fuel=%d л%s, Speed=%d км/год, Course=%d°, Ignition=%d (lat=%.6f, lon=%.6f)",
        int(round(level)),
        extra_str,
        int(round(speed)),
        used_bearing,
        1 if ignition else 0,
        lat,
        lon,
    )

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            with socket.create_connection((host, port), timeout=cfg["http_timeout_sec"]) as sock:
                imei_b = imei.encode("ascii")
                sock.sendall(struct.pack(">H", len(imei_b)) + imei_b)
                resp = sock.recv(1)
                if not resp or resp[0] != 1:
                    log.warning("Handshake не підтверджено сервером: %s", resp)
                    return
                sock.sendall(packet_c8)
                sock.recv(4)
            return
        except (socket.error, OSError) as exc:
            if attempt == max_retries:
                log.error("TCP Error вичерпано всі спроби (%d): %s", max_retries, exc)
                raise
            delay = 3 * attempt
            log.warning(
                "Помилка підключення (спроба %d/%d). Повтор через %d с: %s",
                attempt,
                max_retries,
                delay,
                exc,
            )
            time.sleep(delay)


class TrackPacer:
    """Керує інтервалами відправки точок (щільні по 15 с, сон CAN по 10 хв, довільні гепи) без дрейфу."""

    def __init__(self, cfg: dict, step_sec: float = 15.0, initial_bearing: int = 90) -> None:
        self.cfg = cfg
        self.step_sec = step_sec
        self.next_deadline: float | None = None
        self.last_lat: float | None = None
        self.last_lon: float | None = None
        self.last_bearing: int = initial_bearing

    def advance(self, gap_sec: float) -> None:
        """Додає довільний розрив у часі без відправки точок (наприклад, розрив зв'язку або рідка точка)."""
        if self.next_deadline is None:
            self.next_deadline = time.monotonic() + gap_sec
        else:
            # Віднімаємо один базовий крок, якщо наступний emit сам додає step_sec
            self.next_deadline += gap_sec - self.step_sec

    def emit(
        self,
        level: float,
        lat: float,
        lon: float,
        speed: int = 0,
        step_override: float | None = None,
        extra_ios: dict[int, float] | None = None,
    ) -> None:
        now = time.monotonic()
        if self.next_deadline is None:
            self.next_deadline = now
        elif now < self.next_deadline:
            time.sleep(self.next_deadline - now)

        if self.last_lat is not None and self.last_lon is not None and speed > 0:
            computed = calculate_bearing(self.last_lat, self.last_lon, lat, lon)
            if computed >= 0:
                self.last_bearing = computed

        send_teltonika_point(
            self.cfg,
            level=level,
            lat=lat,
            lon=lon,
            speed=speed,
            bearing=self.last_bearing,
            extra_2byte_ios=extra_ios,
        )
        self.last_lat, self.last_lon = lat, lon
        dt = self.step_sec if step_override is None else step_override
        self.next_deadline = max(time.monotonic(), self.next_deadline + dt)

    def send_plateau(
        self,
        level: float,
        lat: float,
        lon: float,
        count: int,
        step_sec: float | None = None,
        extra_ios: dict[int, float] | None = None,
    ) -> None:
        for _ in range(count):
            self.emit(level=level, lat=lat, lon=lon, speed=0, step_override=step_sec, extra_ios=extra_ios)

    def send_curve(
        self,
        levels: list[float],
        lat: float,
        lon: float,
        step_sec: float | None = None,
        extra_ios: dict[int, float] | None = None,
    ) -> None:
        for lvl in levels:
            self.emit(level=lvl, lat=lat, lon=lon, speed=0, step_override=step_sec, extra_ios=extra_ios)

    def send_sparse_steps(
        self,
        steps: list[tuple[float, float]],
        lat: float,
        lon: float,
        extra_ios: dict[int, float] | None = None,
    ) -> None:
        """Відправляє рідкі точки [(gap_before_sec, level), ...], кожна з власним інтервалом."""
        for idx, (gap_s, lvl) in enumerate(steps):
            if idx > 0 and gap_s != self.step_sec:
                # Коригуємо deadline відносно попередньої точки
                self.next_deadline = (self.next_deadline or time.monotonic()) + (gap_s - self.step_sec)
            elif idx == 0 and gap_s > 0:
                self.next_deadline = (self.next_deadline or time.monotonic()) + (gap_s - self.step_sec)
            self.emit(level=lvl, lat=lat, lon=lon, speed=0, extra_ios=extra_ios)

    def send_trip(
        self,
        waypoints: list[tuple[float, float, int]],
        start_level: float,
        end_level: float,
        extra_levels: dict[int, tuple[float, float]] | None = None,
    ) -> None:
        n = len(waypoints)
        for idx, (lat, lon, spd) in enumerate(waypoints):
            frac = idx / max(1, n - 1)
            lvl = start_level + (end_level - start_level) * frac
            ex = (
                {k: s_v + (e_v - s_v) * frac for k, (s_v, e_v) in extra_levels.items()}
                if extra_levels
                else None
            )
            self.emit(level=lvl, lat=lat, lon=lon, speed=spd, extra_ios=ex)


# --------------------------------------------------------------------------
# M2M API & Event Extraction
# --------------------------------------------------------------------------


def refresh_api_token(cfg: dict) -> None:
    try:
        resp = GLOBAL_SESSION.post(
            f"{cfg['api_base_url']}{cfg['login_path']}",
            json={"email": cfg["api_email"], "password": cfg["api_password"]},
            timeout=cfg["http_timeout_sec"],
        )
        resp.raise_for_status()
        cfg["api_token"] = resp.json().get("token")
        log.info("Отримано новий токен M2M API")
    except Exception as exc:
        log.warning("Не вдалося оновити токен API: %s", exc)


def fetch_history(cfg: dict, page_size: int | None = None, page: int = 1) -> dict:
    if not cfg.get("api_token"):
        refresh_api_token(cfg)
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    params = dict(cfg["history_query"])
    params["page"] = page
    if page_size is not None:
        params["per_page"] = page_size
    try:
        resp = GLOBAL_SESSION.get(
            f"{cfg['api_base_url']}{cfg['history_path']}",
            headers=headers,
            params=params,
            timeout=cfg["http_timeout_sec"],
        )
        if resp.status_code in (401, 403):
            refresh_api_token(cfg)
            headers = {"Authorization": f"Bearer {cfg['api_token']}"}
            resp = GLOBAL_SESSION.get(
                f"{cfg['api_base_url']}{cfg['history_path']}",
                headers=headers,
                params=params,
                timeout=cfg["http_timeout_sec"],
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.warning("Помилка запиту історії сповіщень (page=%d): %s", page, exc)
        return {}


def fetch_history_since(cfg: dict, start_ts: float, max_pages: int = 6) -> list[dict]:
    """Вивантажує сторінки історії, поки записи не досягнуть start_ts - 180 с (гарантує повну вибірку)."""
    all_items: list[dict] = []
    seen_ids: set = set()
    per_page = int(cfg.get("history_query", {}).get("per_page", 100) or 100)
    cutoff = start_ts - 180.0

    for page in range(1, max_pages + 1):
        res = fetch_history(cfg, page_size=per_page, page=page)
        items = res.get("items") or res.get("data") or []
        if not items:
            break
        oldest_ts = None
        for it in items:
            if not isinstance(it, dict):
                continue
            eid = it.get("id")
            if eid is not None and eid in seen_ids:
                continue
            if eid is not None:
                seen_ids.add(eid)
            all_items.append(it)
            ts = parse_item_timestamp(it)
            if ts is not None and (oldest_ts is None or ts < oldest_ts):
                oldest_ts = ts
        if len(items) < per_page or (oldest_ts is not None and oldest_ts < cutoff):
            break
    return all_items


def resolve_device_id(cfg: dict) -> None:
    if cfg.get("device_id"):
        return
    if not cfg.get("api_token") and not (cfg.get("api_email") and cfg.get("api_password")):
        return
    try:
        if not cfg.get("api_token"):
            refresh_api_token(cfg)
        headers = {"Authorization": f"Bearer {cfg['api_token']}"}
        url = f"{cfg['api_base_url']}/devices"
        params = {"search": cfg["device_name"], "per_page": 20}
        resp = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
        if resp.status_code in (401, 403):
            refresh_api_token(cfg)
            headers = {"Authorization": f"Bearer {cfg['api_token']}"}
            resp = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items") or data.get("data") or []
            for dev in items:
                if dev.get("name") == cfg["device_name"] or str(dev.get("uniqueId")) == str(
                    cfg["tracker_imei"]
                ):
                    cfg["device_id"] = dev.get("id")
                    log.info("Автоматично визначено device_id=%s для %s", cfg["device_id"], cfg["device_name"])
                    return
    except Exception as exc:
        log.warning("Не вдалося автоматично визначити device_id: %s", exc)


def _normalize_iso_for_py310(txt: str) -> str:
    """Нормалізує ISO-рядок під Python 3.10 (дробові секунди 1..9 цифр -> 6, Z -> +00:00, +HHMM -> +HH:MM)."""
    s = txt.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    s = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s)
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    return s


def _parse_any_ts(val) -> float | None:
    if isinstance(val, dict):
        for key in ("time", "timestamp", "date", "dateTime", "startTime", "endTime", "value", "start", "end"):
            if val.get(key) is not None:
                return _parse_any_ts(val[key])
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val) / 1000.0 if val > 1e11 else float(val)
    if isinstance(val, str) and val.strip():
        txt = val.strip()
        try:
            dt = datetime.fromisoformat(_normalize_iso_for_py310(txt))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KYIV_TZ)
            return dt.timestamp()
        except Exception:
            pass
        m = re.search(r"(\d{2})[./](\d{2})[./](\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?", txt)
        if m:
            try:
                day, month, year, hour, minute = map(int, m.groups()[:5])
                sec = int(m.group(6)) if m.group(6) else 0
                return datetime(year, month, day, hour, minute, sec, tzinfo=KYIV_TZ).timestamp()
            except Exception:
                pass
    return None


def _parse_metadata(item: dict) -> dict:
    meta = item.get("metadata", {})
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return meta if isinstance(meta, dict) else {}


def parse_item_timestamp(item: dict) -> float | None:
    for field in ("createdAt", "eventTime", "serverTime", "time"):
        ts = _parse_any_ts(item.get(field))
        if ts is not None:
            return ts
    return _parse_any_ts(str(item.get("message") or ""))


def extract_event_start_ts(item: dict) -> float | None:
    """Повертає час початку паливної події (metadata.fuelStartTime / з тексту повідомлення)."""
    meta = _parse_metadata(item)
    for key in ("fuelStartTime", "startTime"):
        ts = _parse_any_ts(meta.get(key))
        if ts is not None:
            return ts
    msg = str(item.get("message") or "")
    m_start = re.search(
        r"початку[^\d]*(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?",
        msg,
        re.I,
    )
    if m_start:
        try:
            day, month, year, hour, minute = map(int, m_start.groups()[:5])
            sec = int(m_start.group(6)) if m_start.group(6) else 0
            return datetime(year, month, day, hour, minute, sec, tzinfo=KYIV_TZ).timestamp()
        except Exception:
            pass
    return parse_item_timestamp(item)


def extract_event_finish_ts(item: dict) -> float | None:
    meta = _parse_metadata(item)
    for key in ("fuelFinishTime", "finishTime", "endTime"):
        ts = _parse_any_ts(meta.get(key))
        if ts is not None:
            return ts
    return None


def extract_event_time(item: dict) -> str:
    ts = extract_event_start_ts(item)
    if ts:
        try:
            return datetime.fromtimestamp(ts, tz=KYIV_TZ).strftime("%H:%M:%S")
        except Exception:
            pass
    msg = str(item.get("message") or "")
    m_time = re.search(r"(\d{2}:\d{2}(?::\d{2})?)", msg)
    return m_time.group(1) if m_time else ""


def _format_duration_sec(sec: int) -> str:
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    parts = []
    if h > 0:
        parts.append(f"{h} год")
    if m > 0 or h > 0:
        parts.append(f"{m} хв")
    if s > 0 or not parts:
        parts.append(f"{s} с")
    return " ".join(parts)


def extract_event_duration(item: dict) -> str:
    msg = str(item.get("message") or "")
    m_dur = re.search(r"триваліст[^\:\d]*[:\s]+([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)", msg, re.I)
    if m_dur:
        parts = [int(p) for p in m_dur.group(1).split(":")]
        if len(parts) == 3:
            return _format_duration_sec(parts[0] * 3600 + parts[1] * 60 + parts[2])
        if len(parts) == 2:
            return _format_duration_sec(parts[0] * 60 + parts[1])
        return m_dur.group(1)

    meta = _parse_metadata(item)
    dur = meta.get("duration") or meta.get("eventDuration") or meta.get("fuelDuration")
    if dur is not None:
        if isinstance(dur, (int, float)):
            sec = int(dur)
            if sec > 360000:
                sec //= 1000
            return _format_duration_sec(sec)
        if isinstance(dur, str) and dur.strip():
            try:
                return _format_duration_sec(int(float(dur)))
            except Exception:
                return dur.strip()

    st = extract_event_start_ts(item)
    ft = extract_event_finish_ts(item)
    if st is not None and ft is not None:
        return _format_duration_sec(int(round(abs(ft - st))))
    return ""


def extract_event_volume(item: dict, item_str: str) -> float | None:
    meta = _parse_metadata(item)
    volume = meta.get("fuelVolume")
    if volume is None:
        finish = meta.get("fuelFinishValue")
        start_val = meta.get("fuelStartValue")
        if isinstance(finish, (int, float)) and isinstance(start_val, (int, float)):
            volume = round(abs(finish - start_val), 2)
    if isinstance(volume, (int, float)):
        return float(volume)
    m = re.search(r"об[\'’`]?ємом\s+([\d\.]+)\s*л", str(item.get("message", "")) + " " + item_str, re.I)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    return None


def get_existing_event_ids(cfg: dict) -> tuple[set, int]:
    try:
        res = fetch_history(cfg)
        items = res.get("items") or res.get("data") or []
        ids = set()
        max_id = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            ev_id = it.get("id")
            if ev_id is not None:
                try:
                    num_id = int(ev_id)
                    ids.add(num_id)
                    if num_id > max_id:
                        max_id = num_id
                except (ValueError, TypeError):
                    ids.add(ev_id)
        return ids, max_id
    except Exception:
        return set(), 0


def _extract_sensor_name(item: dict, meta: dict) -> str | None:
    for k in ("sensorName", "fuelSensorName", "sensor"):
        v = meta.get(k) or item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    msg = str(item.get("message") or "")
    m = re.search(r"по\s+датчику\s+([^\s,;]+)", msg, re.I)
    return m.group(1).strip() if m else None


def collect_history_events(
    cfg: dict,
    start_ts: float,
    initial_event_ids: set,
    max_initial_id: int,
) -> tuple[list[dict], list[dict]]:
    """Збирає нові REFILL та DRAIN сповіщення лише цільового об'єкта після start_ts (із пагінацією)."""
    items = fetch_history_since(cfg, start_ts)
    refills_found: list[dict] = []
    drains_found: list[dict] = []

    target_id = cfg.get("device_id")
    target_name = cfg.get("device_name", "")
    target_imei = str(cfg.get("tracker_imei", ""))
    target_refill = cfg.get("template_refill", "").lower().rstrip(":")
    target_drain = cfg.get("template_drain", "").lower().rstrip(":")
    scenario_mode = cfg.get("scenario_mode", "single")

    other_unit_names = {
        "refill_drain_unit",
        "refill_drain_unit_double",
        "refill_drain_unit_triple",
        "refill_drain_unit_quatro",
    } - {target_name.lower()}

    for item in items:
        if not isinstance(item, dict):
            continue
        event_id = item.get("id")
        try:
            num_id = int(event_id)
            if num_id in initial_event_ids or (max_initial_id > 0 and num_id <= max_initial_id):
                continue
        except (ValueError, TypeError):
            if event_id in initial_event_ids:
                continue

        ev_created_ts = parse_item_timestamp(item)
        if ev_created_ts is not None and ev_created_ts < (start_ts - 60):
            continue

        ev_start_ts = extract_event_start_ts(item)
        # Якщо fuelStartTime значно старіший за старт цього запуску (>2 хв до start_ts), це хвіст минулого прогону
        if ev_start_ts is not None and ev_start_ts < (start_ts - 120):
            continue

        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        dev_id = item.get("deviceId")
        dev_name = str(item.get("deviceName") or "").strip()
        dev_name_l = dev_name.lower()
        unique_id = str(item.get("uniqueId") or "").strip()

        raw_type = str(item.get("type", "")).upper()
        template_type = str(item.get("templateType", "")).upper()
        template_name = str(item.get("templateName", "")).lower()
        message = str(item.get("message", "")).lower()

        if "test_notification_o.rosenko" in dev_name_l:
            continue
        if dev_name_l in other_unit_names or any(other in message for other in other_unit_names):
            continue
        if scenario_mode == "single" and (
            "double" in template_name or "сценарій 2" in template_name
            or "triple" in template_name or "quatro" in template_name
        ):
            continue
        elif scenario_mode == "double" and (
            "single" in template_name or "сценарій 1" in template_name
            or "triple" in template_name or "quatro" in template_name
        ):
            continue
        elif scenario_mode == "triple" and (
            "single" in template_name or "сценарій 1" in template_name
            or "double" in template_name or "сценарій 2" in template_name
            or "quatro" in template_name
        ):
            continue
        elif scenario_mode == "quatro" and (
            "single" in template_name or "сценарій 1" in template_name
            or "double" in template_name or "сценарій 2" in template_name
            or "triple" in template_name
        ):
            continue

        matched = False
        if target_id and dev_id == target_id:
            matched = True
        elif target_imei and (unique_id == target_imei or target_imei in item_str):
            matched = True
        elif target_name and dev_name_l == target_name.lower():
            matched = True
        elif target_name and (
            f" {target_name.lower()} " in message
            or f" {target_name.lower()}." in message
            or f" {target_name.lower()}:" in message
            or f"'{target_name.lower()}'" in message
            or f'"{target_name.lower()}"' in message
        ):
            matched = True
        elif (target_refill and target_refill in template_name) or (target_drain and target_drain in template_name):
            if scenario_mode == "single" and not any(w in template_name for w in ("double", "triple", "quatro")):
                matched = True
            elif scenario_mode == "double" and not any(w in template_name for w in ("single", "triple", "quatro")):
                matched = True
            elif scenario_mode == "triple" and "triple" in template_name:
                matched = True
            elif scenario_mode == "quatro" and "quatro" in template_name:
                matched = True

        if not matched:
            continue

        meta = _parse_metadata(item)
        ev_obj = {
            "id": event_id,
            "volume": extract_event_volume(item, item_str),
            "time": extract_event_time(item),
            "start_ts": ev_start_ts,
            "finish_ts": extract_event_finish_ts(item),
            "created_ts": ev_created_ts,
            "sensor_id": meta.get("fuelSensorId"),
            "sensor_name": _extract_sensor_name(item, meta),
            "duration": extract_event_duration(item),
        }

        if (
            raw_type in ("REFILL", "REFIL")
            or template_type in ("REFILL", "REFIL")
            or (target_refill and (target_refill in template_name or target_refill in item_lower))
            or "заправк" in template_name
            or "заправк" in message
        ):
            if not any(r["id"] == event_id for r in refills_found):
                refills_found.append(ev_obj)
        elif (
            raw_type == "DRAIN"
            or template_type == "DRAIN"
            or (target_drain and (target_drain in template_name or target_drain in item_lower))
            or "злив" in template_name
            or "злив" in message
        ):
            if not any(d["id"] == event_id for d in drains_found):
                drains_found.append(ev_obj)

    refills_found.sort(key=lambda x: (x["start_ts"] or 0.0, str(x["id"])))
    drains_found.sort(key=lambda x: (x["start_ts"] or 0.0, str(x["id"])))
    return refills_found, drains_found


def _cluster_events(events: list[dict], time_gap_sec: float = 180.0, vol_tol: float = 35.0) -> list[list[dict]]:
    clusters: list[list[dict]] = []
    for ev in events:
        placed = False
        ev_t, ev_v = ev.get("start_ts"), ev.get("volume")
        ev_sid, ev_sname = ev.get("sensor_id"), (ev.get("sensor_name") or "").lower()
        for cl in clusters:
            ref = cl[0]
            ref_t, ref_v = ref.get("start_ts"), ref.get("volume")
            ref_sid, ref_sname = ref.get("sensor_id"), (ref.get("sensor_name") or "").lower()
            same_sid = ev_sid is None or ref_sid is None or ev_sid == ref_sid
            same_sname = not ev_sname or not ref_sname or ev_sname == ref_sname
            time_close = ev_t is not None and ref_t is not None and abs(ev_t - ref_t) <= time_gap_sec
            vol_close = ev_v is None or ref_v is None or abs(ev_v - ref_v) <= vol_tol
            if same_sid and same_sname and time_close and vol_close:
                cl.append(ev)
                placed = True
                break
        if not placed:
            clusters.append([ev])
    return clusters


# --------------------------------------------------------------------------
# Report v2 Parsing & 1-to-1 Audit (per Section 1.4)
# --------------------------------------------------------------------------


def _extract_float(val) -> float | None:
    if isinstance(val, dict):
        for key in ("value", "volume", "amount"):
            if val.get(key) is not None:
                return _extract_float(val[key])
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val)
    if isinstance(val, str):
        m = re.search(r"[-+]?\d+(?:[.,]\d+)?", val)
        if m:
            try:
                return float(m.group(0).replace(",", "."))
            except Exception:
                pass
    return None


def _normalize_report_row(row, headers: list[str] | None) -> dict:
    """Нормалізує рядок таблиці звіту M2M v2 (`ReportRow: {row: [...]}` або dict/list) у словник події."""
    if isinstance(row, list) and headers:
        mapped = {str(h): row[i] for i, h in enumerate(headers) if i < len(row)}
    elif isinstance(row, dict):
        arr = None
        for arr_key in ("row", "cells", "values", "data"):
            if isinstance(row.get(arr_key), list):
                arr = row[arr_key]
                break
        if arr is not None:
            if headers:
                mapped = {str(h): arr[i] for i, h in enumerate(headers) if i < len(arr)}
            else:
                mapped = {f"col_{i}": arr[i] for i in range(len(arr))}
        else:
            mapped = row
    else:
        return {}

    lower_map = {str(k).lower(): v for k, v in mapped.items()}
    start_ts = None
    finish_ts = None
    volume = None
    sensor_name = None

    # 1. Пріоритетне точне співставлення стандартних колонок M2M Report v2
    for exact_key in ("refill_start_time", "drain_start_time", "start_time", "start_interval"):
        if exact_key in lower_map and start_ts is None:
            start_ts = _parse_any_ts(lower_map[exact_key])
            if start_ts is None and lower_map[exact_key] not in (None, "", "-"):
                log.warning("Не вдалося розпарсити %s=%r у рядку звіту", exact_key, lower_map[exact_key])
    for exact_key in ("refill_end_time", "drain_end_time", "end_time", "end_interval"):
        if exact_key in lower_map and finish_ts is None:
            finish_ts = _parse_any_ts(lower_map[exact_key])
            if finish_ts is None and lower_map[exact_key] not in (None, "", "-"):
                log.warning("Не вдалося розпарсити %s=%r у рядку звіту", exact_key, lower_map[exact_key])
    for exact_key in ("refill_volume", "drain_volume", "volume", "refilled", "total_refill", "total_drain"):
        if exact_key in lower_map and volume is None:
            cand = _extract_float(lower_map[exact_key])
            if cand is not None:
                volume = abs(cand)
    if "sensor_name" in lower_map and lower_map["sensor_name"] is not None:
        sensor_name = str(lower_map["sensor_name"])

    # 2. Fallback для локалізованих/кастомних заголовків (виключаючи локації, лічильники й рівні)
    _non_vol_words = (
        "time", "date", "час", "тривал", "duration",
        "location", "address", "адрес", "місц", "coord", "коорд",
        "count", "кільк", "level", "рівен", "start", "end", "finish", "почат", "кінец",
    )
    for k, v in lower_map.items():
        if start_ts is None and any(w in k for w in ("start", "begin", "from", "почат")):
            if "level" not in k and "value" not in k and "рівен" not in k and "coord" not in k and "loc" not in k:
                start_ts = _parse_any_ts(v)
        if finish_ts is None and any(w in k for w in ("finish", "end", "to", "кінец", "заверш")):
            if "level" not in k and "value" not in k and "рівен" not in k and "coord" not in k and "loc" not in k:
                finish_ts = _parse_any_ts(v)
        if volume is None and any(w in k for w in ("volume", "vol", "об'єм", "обʼєм", "обєм", "refill", "drain", "заправ", "злив")):
            if not any(nw in k for nw in _non_vol_words):
                cand = _extract_float(v)
                if cand is not None:
                    volume = abs(cand)
        if sensor_name is None and any(w in k for w in ("sensor", "датчик")) and v is not None:
            sensor_name = str(v)

    return {
        "start_ts": start_ts,
        "finish_ts": finish_ts,
        "volume": volume,
        "sensor": sensor_name,
        "raw_keys": list(mapped.keys())[:12],
    }


def _flatten_report_rows(items) -> list:
    """Рекурсивно розгортає `groupedRows` / `nestedRows` таблиці звіту до листкових `ReportRow`."""
    leaf_rows: list = []
    if not items:
        return leaf_rows
    if isinstance(items, dict):
        for key in ("ungroupedRows", "nestedRows", "groupedRows", "rows", "items", "data"):
            if items.get(key):
                leaf_rows.extend(_flatten_report_rows(items[key]))
        if not leaf_rows and isinstance(items.get("row"), list):
            leaf_rows.append(items)
        return leaf_rows
    if not isinstance(items, list):
        return leaf_rows
    for item in items:
        if isinstance(item, dict) and ("nestedRows" in item or "groupedBy" in item or item.get("hasNested")):
            nested = item.get("nestedRows")
            if nested:
                leaf_rows.extend(_flatten_report_rows(nested))
        elif isinstance(item, (dict, list)):
            leaf_rows.append(item)
    return leaf_rows


def _parse_report_payload(data: dict, report_id: int | str) -> tuple[list[dict], list[dict], str]:
    """Витягує події REFILL/DRAIN із `ReportResultDto` і повертає діагностичний опис структури."""
    refills: list[dict] = []
    drains: list[dict] = []
    logged_schema = False
    table_summaries: list[str] = []

    tables = data.get("tables") or []
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        t_type = str(tbl.get("tableType") or "").upper()
        title = str(tbl.get("title") or "").lower()
        is_refill = "REFILL" in t_type or "заправ" in title or "refuel" in title
        is_drain = "DRAIN" in t_type or "злив" in title or "drain" in title

        ungrp = tbl.get("ungroupedRows") or []
        grp = tbl.get("groupedRows") or []
        tot = tbl.get("total") or []
        cols = tbl.get("columns") or tbl.get("labels") or tbl.get("headers") or []
        table_summaries.append(
            f"{t_type or title or '?'}(ungrp={len(ungrp)},grp={len(grp)},tot={len(tot)},cols={len(cols)})"
        )

        if not (is_refill or is_drain):
            continue

        col_names = [
            c.get("key") or c.get("field") or c.get("name") or c.get("title") or str(i)
            if isinstance(c, dict)
            else str(c)
            for i, c in enumerate(cols)
        ]

        rows = list(ungrp)
        if not rows and grp:
            rows = _flatten_report_rows(grp)

        for r in rows:
            norm = _normalize_report_row(r, col_names or None)
            if not logged_schema and norm.get("raw_keys"):
                log.info("Схема рядка звіту №%s (%s): keys=%s", report_id, t_type or title, norm["raw_keys"])
                logged_schema = True
            norm["type"] = "REFILL" if is_refill else "DRAIN"
            if is_refill:
                refills.append(norm)
            else:
                drains.append(norm)

    refills.sort(key=lambda x: x["start_ts"] if x["start_ts"] is not None else (x["finish_ts"] if x["finish_ts"] is not None else float("inf")))
    drains.sort(key=lambda x: x["start_ts"] if x["start_ts"] is not None else (x["finish_ts"] if x["finish_ts"] is not None else float("inf")))
    diag = (
        f"reportType={data.get('reportType')}, elementId={data.get('elementId')}, "
        f"tables=[{', '.join(table_summaries)}], charts={len(data.get('charts') or [])}"
    )
    return refills, drains, diag


def fetch_report_events(cfg: dict, start_ts: float) -> dict:
    """Виконує звіт v2/v1 (пробуючи UI-формат Київ+offset і UTC fallback) і повертає події REFILL/DRAIN."""
    report_id = cfg.get("report_id")
    device_id = cfg.get("device_id")
    if not report_id or not device_id:
        return {"checked": False, "reason": "report_id або device_id не налаштовано"}

    if not cfg.get("api_token"):
        refresh_api_token(cfg)

    t_from_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc) - timedelta(minutes=15)
    t_to_utc = datetime.now(timezone.utc) + timedelta(minutes=10)
    t_from_kyiv = t_from_utc.astimezone(KYIV_TZ)
    t_to_kyiv = t_to_utc.astimezone(KYIV_TZ)

    v2_url = f"{cfg['api_base_url']}/v2/report/template/{report_id}/execute"
    v1_url = f"{cfg['api_base_url']}/report/template/{report_id}/execute"

    # M2M UI (`Xo`) надсилає локальний час із зсувом (+02:00/+03:00); якщо 0 подій — пробуємо UTC Z, naive та v1.
    attempts = [
        (v2_url, "v2+kyiv_offset", t_from_kyiv.isoformat(timespec="seconds"), t_to_kyiv.isoformat(timespec="seconds")),
        (v2_url, "v2+utc_z", t_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), t_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")),
        (v2_url, "v2+kyiv_naive", t_from_kyiv.strftime("%Y-%m-%dT%H:%M:%S"), t_to_kyiv.strftime("%Y-%m-%dT%H:%M:%S")),
        (v1_url, "v1+kyiv_offset", t_from_kyiv.isoformat(timespec="seconds"), t_to_kyiv.isoformat(timespec="seconds")),
        (v1_url, "v1+utc_z", t_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), t_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]

    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    last_err = ""
    any_200 = False
    best_refills: list[dict] = []
    best_drains: list[dict] = []
    best_diag = ""
    best_mode = ""

    for url, mode_label, d_from, d_to in attempts:
        params = {"elementId": device_id, "dateFrom": d_from, "dateTo": d_to}
        try:
            log.info(
                "Запит звіту №%s (%s, elementId=%s, %s .. %s)...",
                report_id, mode_label, device_id, d_from, d_to,
            )
            r = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
            if r.status_code in (401, 403):
                refresh_api_token(cfg)
                headers = {"Authorization": f"Bearer {cfg['api_token']}"}
                r = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}: {r.text[:120]}"
                continue

            data = r.json()
            any_200 = True
            refills, drains, diag = _parse_report_payload(data, report_id)
            best_diag = diag
            best_mode = mode_label
            if refills or drains:
                min_event_ts = start_ts - 120.0
                r_in = [x for x in refills if x.get("start_ts") is None or x["start_ts"] >= min_event_ts]
                d_in = [x for x in drains if x.get("start_ts") is None or x["start_ts"] >= min_event_ts]
                skipped = (len(refills) - len(r_in)) + (len(drains) - len(d_in))
                if skipped > 0:
                    log.info(
                        "Відфільтровано передстартових подій звіту (< start_ts-120s): %d (залишилось R=%d, D=%d)",
                        skipped, len(r_in), len(d_in),
                    )
                best_refills, best_drains = r_in, d_in
                break
        except Exception as exc:
            last_err = str(exc)

    if not any_200:
        log.warning("Не вдалося отримати звіт №%s: %s", report_id, last_err)
        return {"checked": False, "reason": f"Помилка API ({last_err})"}

    if not best_refills and not best_drains and best_diag:
        log.warning("Звіт №%s повернув 0 заправок і 0 зливів у всіх форматах! Структура: %s", report_id, best_diag)

    log.info(
        "Результат звіту №%s [%s]: заправок=%d, зливів=%d",
        report_id, best_mode or "?", len(best_refills), len(best_drains),
    )
    return {
        "checked": True,
        "report_id": report_id,
        "format": best_mode,
        "diag": best_diag,
        "refills": best_refills,
        "drains": best_drains,
        "refill_count": len(best_refills),
        "drain_count": len(best_drains),
    }


def _fmt_ts_kyiv(ts: float | None) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts, tz=KYIV_TZ).strftime("%H:%M:%S")


def _fmt_delay(finish_ts: float | None, created_ts: float | None) -> str:
    if finish_ts is None or created_ts is None:
        return "—"
    diff = int(round(created_ts - finish_ts))
    if diff < 0:
        return f"{diff} с"
    return f"{diff // 60} хв {diff % 60} с" if diff >= 60 else f"{diff} с"


def audit_notifications_vs_report(
    cfg: dict,
    report: dict,
    refills_found: list[dict],
    drains_found: list[dict],
) -> dict:
    """
    Виконує звірку 1-в-1 сповіщень з подіями звіту (по fuelStartTime і типу).
    Повертає класифікацію: OK / ПРОПУСК / ДУБЛЬ / ДУБЛЬ ЗІ ЗСУВОМ.
    """
    exp_r = cfg.get("expected_refills", 2)
    exp_d = cfg.get("expected_drains", 1)
    lines: list[str] = []
    has_missing = False
    has_duplicates = False

    if not report.get("checked"):
        # Fallback: кластерний аналіз, якщо звіт тимчасово недоступний
        r_cl = _cluster_events(refills_found)
        d_cl = _cluster_events(drains_found)
        has_missing = (len(r_cl) < exp_r) or (len(d_cl) < exp_d)
        has_duplicates = (len(refills_found) > exp_r) or (len(drains_found) > exp_d)
        for i in range(max(exp_r, len(r_cl))):
            if i < len(r_cl):
                ev = r_cl[i][0]
                lines.append(f"{'✅' if len(r_cl[i])==1 else '⚠️ ДУБЛЬ'} REFILL #{i+1}: +{ev.get('volume')} л ({ev.get('time')}) x{len(r_cl[i])}")
            else:
                lines.append(f"❌ REFILL #{i+1}: ПРОПУСК")
        for i in range(max(exp_d, len(d_cl))):
            if i < len(d_cl):
                ev = d_cl[i][0]
                lines.append(f"{'✅' if len(d_cl[i])==1 else '⚠️ ДУБЛЬ'} DRAIN #{i+1}: -{ev.get('volume')} л ({ev.get('time')}) x{len(d_cl[i])}")
            else:
                lines.append(f"❌ DRAIN #{i+1}: ПРОПУСК")
        return {
            "ok": not has_missing and not has_duplicates,
            "has_missing": has_missing,
            "has_duplicates": has_duplicates,
            "summary": "\n".join(lines) + f"\n\nℹ️ Звіт не перевірено: {report.get('reason')}",
        }

    rep_refills = report.get("refills", [])
    rep_drains = report.get("drains", [])
    report_driven = bool(cfg.get("report_driven_count", False))

    if report_driven:
        if len(rep_refills) < exp_r or len(rep_drains) < exp_d:
            has_missing = True
            lines.append(
                f"⚠️ Звіт №{report['report_id']}: очікувалось мінімум {exp_r} заправок і {exp_d} зливів, "
                f"отримано {len(rep_refills)} заправок і {len(rep_drains)} зливів"
            )
    elif len(rep_refills) != exp_r or len(rep_drains) != exp_d:
        has_missing = (len(rep_refills) < exp_r) or (len(rep_drains) < exp_d)
        lines.append(
            f"⚠️ Звіт №{report['report_id']}: очікувалось {exp_r} заправок і {exp_d} зливів, "
            f"отримано {len(rep_refills)} заправок і {len(rep_drains)} зливів"
        )

    def _sensor_tag(ev: dict | None, fallback: dict | None = None) -> str:
        s = (ev or {}).get("sensor") or (ev or {}).get("sensor_name") or (fallback or {}).get("sensor_name") or (fallback or {}).get("sensor")
        return f" [{s}]" if s else ""

    def match_group(label: str, rep_events: list[dict], notifs: list[dict], sign: str, exp_cnt: int) -> None:
        nonlocal has_missing, has_duplicates
        used_notif_ids: set = set()

        if not rep_events and notifs:
            clusters = _cluster_events(notifs)
            for idx, cl in enumerate(clusters, 1):
                n0 = cl[0]
                n0_vol = n0.get("volume")
                n0_vol_str = f"{sign}{n0_vol:.1f} л" if n0_vol is not None else "—"
                t_str = _fmt_ts_kyiv(n0.get("start_ts"))
                stag = _sensor_tag(n0)
                if len(cl) > 1:
                    has_duplicates = True
                    lines.append(
                        f"⚠️ {label} #{idx}{stag}: Є В ІСТОРІЇ (ДУБЛЬ x{len(cl)}), НЕМАЄ У ЗВІТІ №{report['report_id']} | алерт: {n0_vol_str} ({t_str})"
                    )
                else:
                    lines.append(
                        f"⚠️ {label} #{idx}{stag}: Є В ІСТОРІЇ, НЕМАЄ У ЗВІТІ №{report['report_id']} | алерт: {n0_vol_str} ({t_str})"
                    )
            if len(clusters) > exp_cnt and not report_driven:
                has_duplicates = True
            return

        # Двопрохідна прив'язка: спершу всі події звіту з відомим start_ts, потім fallback без start_ts по вільних кластерах
        matched_per_row: list[list[dict]] = [[] for _ in rep_events]

        for i, rep_ev in enumerate(rep_events):
            r_start = rep_ev.get("start_ts")
            if r_start is None:
                continue
            r_sens = str(rep_ev.get("sensor") or "").strip().lower()
            for n in notifs:
                n_start = n.get("start_ts")
                n_sens = str(n.get("sensor_name") or "").strip().lower()
                sens_ok = not r_sens or not n_sens or r_sens == n_sens
                if sens_ok and n_start is not None and abs(n_start - r_start) <= 90.0:
                    matched_per_row[i].append(n)
                    used_notif_ids.add(n["id"])

        for i, rep_ev in enumerate(rep_events):
            if rep_ev.get("start_ts") is not None:
                continue
            unused = [n for n in notifs if n["id"] not in used_notif_ids]
            clusters = _cluster_events(unused) if unused else []
            if clusters:
                r_sens = str(rep_ev.get("sensor") or "").strip().lower()
                r_vol = rep_ev.get("volume")
                cand_cls = [
                    cl for cl in clusters
                    if not r_sens or not cl[0].get("sensor_name") or cl[0].get("sensor_name", "").strip().lower() == r_sens
                ] or clusters
                if r_vol is not None:
                    cand_cls.sort(key=lambda cl: abs((cl[0].get("volume") or r_vol) - r_vol))
                chosen = cand_cls[0]
                matched_per_row[i] = chosen
                for n in chosen:
                    used_notif_ids.add(n["id"])

        for idx, (rep_ev, matched) in enumerate(zip(rep_events, matched_per_row), 1):
            r_start = rep_ev.get("start_ts")
            r_fin = rep_ev.get("finish_ts")
            r_vol = rep_ev.get("volume")
            rep_vol_str = f"{sign}{r_vol:.1f} л" if r_vol is not None else "—"
            rep_t_str = _fmt_ts_kyiv(r_start)
            stag = _sensor_tag(rep_ev, matched[0] if matched else None)

            if not matched:
                has_missing = True
                lines.append(f"❌ {label} #{idx}{stag}: ПРОПУСК | звіт: {rep_vol_str} ({rep_t_str}) | сповіщень: 0")
            elif len(matched) == 1:
                n = matched[0]
                n_vol = n.get("volume")
                n_vol_str = f"{sign}{n_vol:.1f} л" if n_vol is not None else "—"
                delay_str = _fmt_delay(r_fin or n.get("finish_ts"), n.get("created_ts"))
                lines.append(
                    f"✅ {label} #{idx}{stag}: OK | звіт: {rep_vol_str} ({rep_t_str}) | "
                    f"алерт: {n_vol_str} ({_fmt_ts_kyiv(n.get('start_ts'))}), затримка: {delay_str}"
                )
            else:
                has_duplicates = True
                n0 = matched[0]
                n0_vol = n0.get("volume")
                n0_vol_str = f"{sign}{n0_vol:.1f} л" if n0_vol is not None else "—"
                delay_str = _fmt_delay(r_fin or n0.get("finish_ts"), n0.get("created_ts"))
                lines.append(
                    f"⚠️ {label} #{idx}{stag}: ДУБЛЬ (x{len(matched)}) | звіт: {rep_vol_str} ({rep_t_str}) | "
                    f"алерт: {n0_vol_str} ({_fmt_ts_kyiv(n0.get('start_ts'))}), затримка: {delay_str}"
                )

        # Шукаємо неприв'язані сповіщення (ДУБЛЬ ЗІ ЗСУВОМ або розбіжність зі неповним звітом)
        unmatched = [n for n in notifs if n["id"] not in used_notif_ids]
        unmatched_clusters = _cluster_events(unmatched) if unmatched else []
        for n in unmatched:
            n_vol = n.get("volume")
            n_vol_str = f"{sign}{n_vol:.1f} л" if n_vol is not None else "—"
            stag = _sensor_tag(n)
            shifted_from = ""
            if n_vol is not None:
                n_sens = str(n.get("sensor_name") or "").strip().lower()
                for idx, rep_ev in enumerate(rep_events, 1):
                    r_sens = str(rep_ev.get("sensor") or "").strip().lower()
                    if r_sens and n_sens and r_sens != n_sens:
                        continue
                    rv = rep_ev.get("volume")
                    if rv is not None and abs(rv - n_vol) <= max(10.0, 0.15 * rv):
                        shifted_from = f" (схоже на {label} #{idx}, зсув початку!)"
                        break
            if shifted_from or len(rep_events) >= exp_cnt or len(_cluster_events(notifs)) > exp_cnt:
                has_duplicates = True
                lines.append(
                    f"⚠️ {label}{stag}: ДУБЛЬ ЗІ ЗСУВОМ{shifted_from} | алерт: {n_vol_str} (початок {_fmt_ts_kyiv(n.get('start_ts'))})"
                )
            else:
                lines.append(
                    f"⚠️ {label}{stag}: Є В ІСТОРІЇ, НЕ ЗБІГСЯ ЗІ ЗВІТОМ | алерт: {n_vol_str} (початок {_fmt_ts_kyiv(n.get('start_ts'))})"
                )
        if any(len(cl) > 1 for cl in unmatched_clusters):
            has_duplicates = True

    match_group("REFILL", rep_refills, refills_found, "+", exp_r)
    match_group("DRAIN", rep_drains, drains_found, "-", exp_d)

    count_ok = (len(rep_refills) >= exp_r and len(rep_drains) >= exp_d) if report_driven else (len(rep_refills) == exp_r and len(rep_drains) == exp_d)
    return {
        "ok": (not has_missing) and (not has_duplicates) and count_ok,
        "has_missing": has_missing,
        "has_duplicates": has_duplicates,
        "summary": "\n".join(lines),
    }


def wait_for_events(
    cfg: dict,
    start_ts: float,
    initial_event_ids: set,
    max_initial_id: int,
    wait_full_window: bool = False,
) -> tuple[list[dict], list[dict]]:
    log.info("Початкова пауза (%d с) перед опитуванням історії...", cfg["initial_wait_sec"])
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    start_poll = time.monotonic()
    exp_refills = cfg.get("expected_refills", 2)
    exp_drains = cfg.get("expected_drains", 1)

    while True:
        refills_found, drains_found = collect_history_events(cfg, start_ts, initial_event_ids, max_initial_id)
        r_cl = _cluster_events(refills_found)
        d_cl = _cluster_events(drains_found)

        if not wait_full_window and len(r_cl) >= exp_refills and len(d_cl) >= exp_drains:
            elapsed = int(time.monotonic() - start_poll)
            log.info(
                "Усі очікувані події знайдено в історії (через %d с): заправок %d(%d)/%d, зливів %d(%d)/%d",
                elapsed,
                len(r_cl),
                len(refills_found),
                exp_refills,
                len(d_cl),
                len(drains_found),
                exp_drains,
            )
            return refills_found, drains_found

        if time.monotonic() >= deadline:
            return refills_found, drains_found

        elapsed = int(time.monotonic() - start_poll)
        log.info(
            "Опитування історії (%d/%d с): заправок ун./всього = %d(%d)/%d, зливів = %d(%d)/%d...",
            elapsed,
            cfg["max_wait_sec"],
            len(r_cl),
            len(refills_found),
            exp_refills,
            len(d_cl),
            len(drains_found),
            exp_drains,
        )
        time.sleep(cfg["poll_interval_sec"])


def load_state(cfg: dict) -> dict:
    return json.loads(cfg["state_file"].read_text()) if cfg["state_file"].exists() else {"incident_open": False}


def save_state(cfg: dict, state: dict) -> None:
    cfg["state_file"].write_text(json.dumps(state))


def send_telegram(token: str, chat_id: str, text: str) -> None:
    if token and chat_id:
        try:
            GLOBAL_SESSION.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
        except Exception as exc:
            log.warning("Помилка надсилання в Telegram: %s", exc)


def _send_both_tg(cfg: dict, text: str) -> None:
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def run_canary(cfg: dict, send_fn: Callable[[dict], float]) -> int:
    """Повний життєвий цикл канарки: відправка точок -> очікування -> звірка зі Звітом v2 №1180 -> Telegram."""
    state = load_state(cfg)
    label = cfg["scenario_label"]
    log.info("Запуск %s для %s (IMEI: %s)...", label, cfg["device_name"], cfg["tracker_imei"])
    resolve_device_id(cfg)

    start_ts = time.time()
    initial_ids, max_initial_id = get_existing_event_ids(cfg)
    log.info(
        "Фіксація старту: %d наявних сповіщень в історії (max_id=%s, start_ts=%.0f)",
        len(initial_ids),
        max_initial_id,
        start_ts,
    )

    try:
        send_fn(cfg)
    except Exception as exc:
        log.error("Критична помилка в send_fn: %s", exc, exc_info=True)
        _send_both_tg(
            cfg,
            f"🔴 {label}: Помилка відправки точок по TCP!\nОб'єкт: {cfg['device_name']}\n\n{exc}",
        )
        return 1

    refills_found, drains_found = wait_for_events(
        cfg,
        start_ts,
        initial_ids,
        max_initial_id,
        wait_full_window=cfg.get("wait_full_window", False),
    )

    # У короткому сценарії даємо ще 10 с, у довгому — одразу звіряємо зі Звітом v2 №1180
    report = fetch_report_events(cfg, start_ts)
    audit = audit_notifications_vs_report(cfg, report, refills_found, drains_found)
    summary = audit["summary"]

    if audit["ok"]:
        log.info("Успіх без пропусків і дублів!\n%s", summary)
        if state.get("incident_open"):
            _send_both_tg(
                cfg,
                f"🟢 {label}: Сповіщення відновилися і збігаються зі звітом (без дублів)!\nОб'єкт: {cfg['device_name']}\n\n{summary}",
            )
        else:
            _send_both_tg(
                cfg,
                f"✅ {label}: Усе чисто (1-в-1 зі звітом №{cfg['report_id']}, без дублів)!\nОб'єкт: {cfg['device_name']}\n\n{summary}",
            )
        state["incident_open"] = False
        save_state(cfg, state)
        return 0

    if audit["has_duplicates"] and not audit["has_missing"]:
        log.warning("Усі події є, але виявлено ДУБЛІ!\n%s", summary)
        _send_both_tg(
            cfg,
            f"⚠️ {label}: Виявлено ДУБЛІ сповіщень!\nОб'єкт: {cfg['device_name']}\n\n{summary}",
        )
        state["incident_open"] = True
        save_state(cfg, state)
        return 1

    log.error("Виявлено пропуски/відхилення!\n%s", summary)
    _send_both_tg(
        cfg,
        f"🔴 {label}: Пропуски або збій сповіщень!\nОб'єкт: {cfg['device_name']}\n\n{summary}",
    )
    state["incident_open"] = True
    save_state(cfg, state)
    return 1
