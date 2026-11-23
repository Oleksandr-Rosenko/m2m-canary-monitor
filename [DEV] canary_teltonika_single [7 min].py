#!/usr/bin/env python3
"""
Канареечний скрипт: [DEV] Canary Monitor (Teltonika Single Refill & Drain) [7 min].
Цільовий об'єкт: Refill_Drain_UNIT (id=47613).
Тривалість заправки: 7 хв (420 с)
Тривалість зливу: 7 хв (420 с)
Об'єм: до 1000 л із сотими частинами.
Діагностика: перевірка звіту №1180 при збої сповіщень.
"""

import json
import logging
import os
import sys
import time
import socket
import struct
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_single_7m")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

CONFIG = {
    "device_id": 47613,
    "device_name": "Refill_Drain_UNIT",
    "report_id": 1180,
    "tracker_imei": "12345678901234567890",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка (Сценарій 1 - Single)",
    "template_drain": "Злив (Сценарій 1 - Single)",

    "geo_lat": 51.201641,
    "geo_lon": 23.892584,
    "altitude": 178,
    "bearing": 210,
    "sat": 12,

    "api_base_url": os.getenv("M2M_API_BASE_URL", "https://dev-stag.m2m.eu/api"),
    "api_email": os.getenv("M2M_EMAIL", ""),
    "api_password": os.getenv("M2M_PASSWORD", ""),
    "api_token": os.getenv("M2M_API_TOKEN", ""),
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 100},

    "initial_wait_sec": 60,
    "poll_interval_sec": 20,
    "max_wait_sec": 1200,
    "http_timeout_sec": 60,

    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_7m_dev_state.json")),
}

# --------------------------------------------------------------------------
# HTTP Session
# --------------------------------------------------------------------------

def get_resilient_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

GLOBAL_SESSION = get_resilient_session()


def crc16_arc(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0) -> None:
    host, port, imei = cfg["ingest_host"], cfg["ingest_port"], cfg["tracker_imei"]
    timestamp_ms = int(time.time() * 1000)

    avl_data = struct.pack(
        ">Q b i i h h B h",
        timestamp_ms, 0,
        int(lon * 10000000), int(lat * 10000000),
        cfg["altitude"], cfg["bearing"], cfg["sat"], speed
    )

    io_data = bytearray()
    io_data.append(0)  # Event IO ID
    io_data.append(3)  # Total IO elements: 3

    # 1-byte elements: Ignition (239) and Movement (240)
    io_data.append(2)
    io_data.extend(struct.pack(">B B", 239, 1 if speed == 0 else 1))
    io_data.extend(struct.pack(">B B", 240, 0 if speed == 0 else 1))

    # 2-byte elements: Fuel Level Sensor 1 (LLS 1, IO ID 270)
    io_data.append(1)
    io_data.extend(struct.pack(">B H", 270, int(round(level))))

    # 4-byte and 8-byte elements: 0
    io_data.append(0)
    io_data.append(0)

    record = avl_data + io_data
    payload = bytearray()
    payload.append(8)   # Codec ID: 0x08
    payload.append(1)   # Number of Data 1
    payload.extend(record)
    payload.append(1)   # Number of Data 2

    data_field = bytes(payload)
    data_length = len(data_field)
    crc = crc16_arc(data_field)
    packet = struct.pack(">I I", 0, data_length) + data_field + struct.pack(">I", crc)

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with socket.create_connection((host, port), timeout=15) as s:
                imei_bytes = imei.encode("ascii")
                imei_packet = struct.pack(">H", len(imei_bytes)) + imei_bytes
                s.sendall(imei_packet)
                resp = s.recv(1)
                if not resp or resp[0] != 1:
                    raise ConnectionError(f"IMEI handshake rejected: resp={resp}")

                s.sendall(packet)
                ack = s.recv(4)
                if len(ack) < 4:
                    raise ConnectionError("Не отримано коректний ACK від сервера")
                accepted = struct.unpack(">I", ack)[0]
                if accepted != 1:
                    log.warning("Сервер повернув ACK=%d (очікувався 1)", accepted)
                log.info("Точка успішно доставлена: рівень=%.2f л, lat=%.6f, lon=%.6f, spd=%d",
                         level, lat, lon, speed)
                return
        except Exception as e:
            if attempt == max_retries:
                log.error("TCP Error вичерпано всі спроби (%d): %s", max_retries, e)
                raise
            delay = 3 * attempt
            log.warning("Помилка підключення (спроба %d/%d). Повтор через %d с: %s", attempt, max_retries, delay, e)
            time.sleep(delay)


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)

    # Координати АЗС біля М-07 (Римачі / Ягодин, Волинська обл.):
    lat_gas, lon_gas = 51.201641, 23.892584
    # Координати другого пункту зупинки для зливу:
    lat_drain, lon_drain = 51.206059, 23.925201

    start_level = 110.35
    final_level = 760.75
    drain_level = 410.25

    refill_levels = [
        180.2,
        260.5,
        340.1,
        410.8,
        475.3,
        530.6,
        575.2,
        610.4,
        638.1,
        660.5,
        660.5,
        660.5,
        660.5,
        680.2,
        698.4,
        715.1,
        729.5,
        741.2,
        750.3,
        755.6,
        758.4,
        758.4,
        758.4,
        758.4,
        759.5,
        760.2,
        760.75,
        760.75
    ]

    drain_levels = [
        748.25,
        735.7,
        723.2,
        710.7,
        698.2,
        685.7,
        673.15,
        660.65,
        648.15,
        635.6,
        623.1,
        610.6,
        598.1,
        585.6,
        573.05,
        560.55,
        548.05,
        535.55,
        523.0,
        510.5,
        498.0,
        485.5,
        472.95,
        460.45,
        447.95,
        435.4,
        422.85,
        410.25
    ]

    log.info(">>> СТАРТ СИМУЛЯЦІЇ: Single [7 min] (Заправка 7 хв, Злив 7 хв)...")

    # -------------------------------------------------------------
    # 1. ПОЇЗДКА 1 (Під'їзд до АЗС по М-07): плавне гальмування
    # Тривалість: ~64 с, дистанція > 1.5 км
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 1: Під'їзд до АЗС по трасі М-07 (гальмування з 70 до 0 км/год)...")
    approach_points = [
        (51.197353, 23.857990, 70, start_level),
        (51.199303, 23.872787, 65, start_level),
        (51.200611, 23.882740, 45, start_level),
        (51.201313, 23.888157, 25, start_level),
    ]
    for lat, lon, spd, lvl in approach_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    # -------------------------------------------------------------
    # 2. СТОЯНКА 1 (АЗС, Реалістична заправка тривалістю 7 хв):
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Стоянка на АЗС під заправку 7 хв...")

    # 2.1. Передзаправна полиця: 60 с
    log.info(">>> ЕТАП 2.1: Передзаправна полиця на рівні %.2f л (60 с)...", start_level)
    for _ in range(4):
        _send_point(cfg, level=start_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.2. Налив пального: рівно 28 точок по 15 с = 420 с (7 хв)
    log.info(">>> ЕТАП 2.2: Налив пального рівно 7 хв (%.2f -> %.2f л)...", start_level, final_level)
    for lvl in refill_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.3. Фінальне плато стабілізації: 60 с
    log.info(">>> ЕТАП 2.3: Фінальне плато стабілізації на рівні %.2f л (60 с)...", final_level)
    for _ in range(4):
        _send_point(cfg, level=final_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 3. ПОЇЗДКА 2 (Виїзд з АЗС на трасу М-07, закриває стоянку заправки):
    # Тривалість: ~64 с, швидкість до 75 км/год
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 3: Виїзд з АЗС на трасу М-07 (закриває стоянку заправки в M2M)...")
    depart_points = [
        (51.201660, 23.892738, 15, final_level),
        (51.202324, 23.896040, 45, final_level),
        (51.203232, 23.902967, 65, final_level),
        (51.204708, 23.914114, 75, final_level),
    ]
    for lat, lon, spd, lvl in depart_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    # -------------------------------------------------------------
    # 4. СТОЯНКА 2 (Пункт відстою, Злив пального тривалістю 7 хв):
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 4: Стоянка під злив пального 7 хв (%.2f -> %.2f л)...", final_level, drain_level)

    # 4.1. Передзливна полиця: 45 с (3 точки по 15 с)
    for _ in range(3):
        _send_point(cfg, level=final_level, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.2. Плавний злив пального: рівно 28 точок по 15 с = 420 с (7 хв)
    log.info(">>> ЕТАП 4.1: Плавний злив пального рівно 7 хв (%.2f -> %.2f л)...", final_level, drain_level)
    for lvl in drain_levels:
        _send_point(cfg, level=lvl, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.3. Плато стабілізації після зливу: 60 с
    log.info(">>> ЕТАП 4.2: Плато стабілізації після зливу (%.2f л, 60 с)...", drain_level)
    for _ in range(4):
        _send_point(cfg, level=drain_level, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 5. ПОЇЗДКА 3 (Від'їзд зі стоянки зливу, закриває сесію зливу):
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 5: Від'їзд зі стоянки зливу (закриває сесію зливу в M2M)...")
    drain_depart_points = [
        (51.206028, 23.927141, 35, drain_level),
        (51.205265, 23.930920, 55, drain_level),
        (51.204517, 23.932686, 75, drain_level),
        (51.202877, 23.936026, 80, drain_level),
    ]
    for lat, lon, spd, lvl in drain_depart_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    log.info("Усі точки Single [7 min] у реальному часі відправлено успішно!")
    return start_time


# --------------------------------------------------------------------------
# API & Verification
# --------------------------------------------------------------------------

def refresh_api_token(cfg: dict) -> None:
    try:
        resp = GLOBAL_SESSION.post(
            f"{cfg['api_base_url']}{cfg['login_path']}",
            json={"email": cfg["api_email"], "password": cfg["api_password"]},
            timeout=cfg["http_timeout_sec"]
        )
        resp.raise_for_status()
        cfg["api_token"] = resp.json().get("token")
        log.info("Отримано новий токен M2M API")
    except Exception as e:
        log.warning("Не вдалося оновити токен API: %s", e)


def fetch_history(cfg: dict) -> dict:
    if not cfg["api_token"]:
        refresh_api_token(cfg)
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    try:
        resp = GLOBAL_SESSION.get(
            f"{cfg['api_base_url']}{cfg['history_path']}",
            headers=headers,
            params=cfg["history_query"],
            timeout=cfg["http_timeout_sec"]
        )
        if resp.status_code in (401, 403):
            refresh_api_token(cfg)
            headers = {"Authorization": f"Bearer {cfg['api_token']}"}
            resp = GLOBAL_SESSION.get(
                f"{cfg['api_base_url']}{cfg['history_path']}",
                headers=headers,
                params=cfg["history_query"],
                timeout=cfg["http_timeout_sec"]
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning("Помилка запиту історії сповіщень: %s", e)
        return {}


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
            for d in items:
                if d.get("name") == cfg["device_name"] or str(d.get("uniqueId")) == str(cfg["tracker_imei"]):
                    cfg["device_id"] = d.get("id")
                    log.info("Автоматично визначено device_id=%s для %s", cfg["device_id"], cfg["device_name"])
                    return
    except Exception as e:
        log.warning("Не вдалося автоматично визначити device_id: %s", e)


try:
    from zoneinfo import ZoneInfo
    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    from datetime import timezone, timedelta
    KYIV_TZ = timezone(timedelta(hours=2))


def parse_item_timestamp(item: dict) -> float | None:
    for field in ("createdAt", "eventTime", "serverTime", "time"):
        val = item.get(field)
        if isinstance(val, (int, float)):
            return float(val) / 1000.0 if val > 1e11 else float(val)
        if isinstance(val, str) and val.strip():
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
    msg = str(item.get("message") or "")
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?', msg)
    if m:
        try:
            day, month, year, hour, minute = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))
            sec = int(m.group(6)) if m.group(6) else 0
            dt = datetime(year, month, day, hour, minute, sec, tzinfo=KYIV_TZ)
            return dt.timestamp()
        except Exception:
            pass
    return None


def extract_event_time(item: dict) -> str:
    msg = str(item.get("message") or "")
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}:\d{2})', msg)
    if m:
        return m.group(4)
    ts = parse_item_timestamp(item)
    if ts:
        try:
            return datetime.fromtimestamp(ts, tz=KYIV_TZ).strftime("%H:%M")
        except Exception:
            pass
    return ""


def extract_event_duration(item: dict) -> str:
    msg = str(item.get("message") or "")
    m_dur = re.search(r'триваліст[^\:\d]*[:\s]+([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)', msg, re.I)
    if m_dur:
        raw_dur = m_dur.group(1)
        parts = [int(p) for p in raw_dur.split(":")]
        if len(parts) == 2:
            return f"{parts[0]} хв" if parts[1] == 0 else f"{parts[0]} хв {parts[1]} с"
        elif len(parts) == 3:
            h, mn, sc = parts
            res = []
            if h: res.append(f"{h} год")
            if mn: res.append(f"{mn} хв")
            if sc: res.append(f"{sc} с")
            return " ".join(res) if res else f"{mn} хв"
        return raw_dur

    meta = item.get("metadata", {})
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    if isinstance(meta, dict):
        dur = meta.get("duration") or meta.get("eventDuration") or meta.get("fuelDuration")
        if dur is not None:
            if isinstance(dur, (int, float)):
                sec = int(dur)
                mn, sc = divmod(sec, 60)
                hr, mn = divmod(mn, 60)
                res = []
                if hr: res.append(f"{hr} год")
                if mn: res.append(f"{mn} хв")
                if sc: res.append(f"{sc} с")
                return " ".join(res) if res else f"{mn} хв"
            elif isinstance(dur, str) and dur.strip():
                return dur.strip()

    return ""


def get_existing_event_ids(cfg: dict) -> tuple[set, int]:
    try:
        res = fetch_history(cfg)
        items = res.get("items") or res.get("data") or []
        ids = {item.get("id") for item in items if isinstance(item, dict) and item.get("id") is not None}
        max_id = max(ids) if ids else 0
        return ids, max_id
    except Exception:
        return set(), 0


def check_events_in_history(cfg: dict, start_ts: float, initial_event_ids: set, max_initial_id: int) -> tuple[bool, str, int, int]:
    res = fetch_history(cfg)
    items = res.get("items") or res.get("data") or []
    refills_found = []
    drains_found = []

    target_name = cfg.get("device_name", "")
    target_id = cfg.get("device_id")
    target_imei = str(cfg.get("tracker_imei", ""))
    target_refill = cfg["template_refill"].lower().rstrip(":")
    target_drain = cfg["template_drain"].lower().rstrip(":")

    for item in items:
        if not isinstance(item, dict):
            continue
        event_id = item.get("id")

        if event_id in initial_event_ids:
            continue
        if max_initial_id and isinstance(event_id, int) and event_id <= max_initial_id:
            continue

        ev_ts = parse_item_timestamp(item)
        if ev_ts is not None and ev_ts < (start_ts - 60):
            continue

        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        dev_id = item.get("deviceId")
        dev_name = str(item.get("deviceName") or "").strip()
        unique_id = str(item.get("uniqueId") or "").strip()

        raw_type = str(item.get("type", "")).upper()
        template_type = str(item.get("templateType", "")).upper()
        template_name = str(item.get("templateName", "")).lower()
        message = str(item.get("message", "")).lower()

        # Суворе відсікання подій з інших сценаріїв (Double або Prod):
        if (
            "double" in dev_name.lower()
            or dev_name.lower().endswith("_double")
            or "double" in template_name
            or "сценарій 2" in template_name
            or "refill_drain_unit_double" in message
            or "test_notification_o.rosenko" in dev_name.lower()
        ):
            continue

        matched = False
        if target_id and dev_id == target_id:
            matched = True
        elif target_imei and (unique_id == target_imei or target_imei in item_str):
            matched = True
        elif target_name and dev_name.lower() == target_name.lower():
            matched = True
        elif target_name and (
            f" {target_name.lower()} " in message
            or f" {target_name.lower()}." in message
            or f" {target_name.lower()}:" in message
            or f"'{target_name.lower()}'" in message
            or f'"{target_name.lower()}"' in message
        ):
            matched = True
        elif (
            (target_refill in template_name or target_drain in template_name)
            and "double" not in template_name
        ):
            matched = True

        if not matched:
            continue

        # Об'єм пального
        volume = None
        try:
            meta = item.get("metadata", {})
            if isinstance(meta, str):
                meta = json.loads(meta)
            if isinstance(meta, dict):
                volume = meta.get("fuelVolume")
                if volume is None:
                    finish = meta.get("fuelFinishValue")
                    start_val = meta.get("fuelStartValue")
                    if isinstance(finish, (int, float)) and isinstance(start_val, (int, float)):
                        volume = round(abs(finish - start_val), 2)
        except Exception:
            pass

        if volume is None:
            m = re.search(r"об[\'’`]?ємом\s+([\d\.]+)\s*л", str(item.get("message", "")) + " " + item_str, re.I)
            if m:
                try:
                    volume = float(m.group(1))
                except Exception:
                    pass

        ev_time = extract_event_time(item)
        ev_dur = extract_event_duration(item)

        if (
            raw_type in ("REFILL", "REFIL")
            or template_type in ("REFILL", "REFIL")
            or target_refill in template_name
            or target_refill in item_lower
            or "заправк" in template_name
            or "заправк" in message
        ):
            if not any(r["id"] == event_id for r in refills_found):
                refills_found.append({"id": event_id, "volume": volume, "time": ev_time, "duration": ev_dur})
        elif (
            raw_type in ("DRAIN",)
            or template_type in ("DRAIN",)
            or target_drain in template_name
            or target_drain in item_lower
            or "злив" in template_name
            or "злив" in message
        ):
            if not any(d["id"] == event_id for d in drains_found):
                drains_found.append({"id": event_id, "volume": volume, "time": ev_time, "duration": ev_dur})

    refills_found.sort(key=lambda x: x["id"])
    drains_found.sort(key=lambda x: x["id"])

    report_lines = []
    if len(refills_found) >= 1:
        t_parts = []
        if refills_found[0].get("time"):
            t_parts.append(f"час: {refills_found[0]['time']}")
        dur = refills_found[0].get("duration") or "7 хв"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = refills_found[0].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка")

    if len(drains_found) >= 1:
        t_parts = []
        if drains_found[0].get("time"):
            t_parts.append(f"час: {drains_found[0]['time']}")
        dur = drains_found[0].get("duration") or "7 хв"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = drains_found[0].get("volume")
        v_str = f": -{vol:.2f} л" if isinstance(vol, (int, float)) else (f": -{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Злив{v_str}{t_str}")
    else:
        report_lines.append("❌ Злив")

    all_ok = (len(refills_found) >= 1) and (len(drains_found) >= 1)
    return all_ok, "\n".join(report_lines), len(refills_found), len(drains_found)


def wait_for_events(cfg: dict, start_ts: float, initial_event_ids: set, max_initial_id: int) -> tuple[bool, str]:
    log.info("Початкова пауза (%d с) перед опитуванням історії...", cfg["initial_wait_sec"])
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    start_poll = time.monotonic()

    while True:
        ok, detail, n_ref, n_drain = check_events_in_history(cfg, start_ts, initial_event_ids, max_initial_id)
        if ok:
            elapsed = int(time.monotonic() - start_poll)
            log.info("Усі очікувані сповіщення успішно знайдено (через %d с від початку опитування)!\n%s", elapsed, detail)
            return True, detail
        if time.monotonic() >= deadline:
            return False, f"Таймаут очікування сповіщень ({cfg['max_wait_sec']} с)\n\n{detail}"

        elapsed = int(time.monotonic() - start_poll)
        log.info(
            "Опитування історії API (пройшло %d/%d с, знайдено заправок: %d/1, зливів: %d/1)...",
            elapsed, cfg["max_wait_sec"], n_ref, n_drain
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
                timeout=10
            )
        except Exception as e:
            log.warning("Помилка надсилання в Telegram: %s", e)


def check_events_in_report(cfg: dict, start_ts: float) -> dict:
    report_id = cfg.get("report_id")
    device_id = cfg.get("device_id")
    if not report_id or not device_id:
        return {"checked": False, "reason": "report_id або device_id не налаштовано"}

    if not cfg.get("api_token"):
        refresh_api_token(cfg)

    t_from = datetime.fromtimestamp(start_ts, tz=timezone.utc) - timedelta(minutes=15)
    t_to = datetime.now(timezone.utc) + timedelta(minutes=5)
    date_from = t_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to = t_to.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "elementId": device_id,
        "dateFrom": date_from,
        "dateTo": date_to
    }
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}

    endpoints = [
        f"{cfg['api_base_url']}/v2/report/template/{report_id}/execute",
        f"{cfg['api_base_url']}/report/template/{report_id}/execute"
    ]

    resp = None
    last_err = ""
    for url in endpoints:
        try:
            log.info("Запит звіту №%s (elementId=%s, from=%s, to=%s) через %s...",
                     report_id, device_id, date_from, date_to, url)
            r = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
            if r.status_code in (401, 403):
                refresh_api_token(cfg)
                headers = {"Authorization": f"Bearer {cfg['api_token']}"}
                r = GLOBAL_SESSION.get(url, headers=headers, params=params, timeout=cfg["http_timeout_sec"])
            if r.status_code == 200:
                resp = r
                break
            else:
                last_err = f"HTTP {r.status_code}: {r.text[:120]}"
        except Exception as e:
            last_err = str(e)

    if resp is None or resp.status_code != 200:
        log.warning("Не вдалося отримати звіт №%s: %s", report_id, last_err)
        return {"checked": False, "reason": f"Помилка API ({last_err})"}

    try:
        data = resp.json()
    except Exception as e:
        return {"checked": False, "reason": f"Некоректний JSON відповіді: {e}"}

    tables = data.get("tables") or []
    refill_count = 0
    drain_count = 0

    for tbl in tables:
        t_type = str(tbl.get("tableType") or "").upper()
        title = str(tbl.get("title") or "").lower()

        rows = tbl.get("ungroupedRows") or []
        if not rows and tbl.get("groupedRows"):
            for g in tbl.get("groupedRows"):
                nested = g.get("nestedRows")
                if isinstance(nested, list):
                    rows.extend(nested)
                elif isinstance(nested, dict):
                    rows.extend(nested.get("rows", []))
                else:
                    rows.append(g)

        cnt = len(rows)
        if cnt == 0 and tbl.get("total"):
            cnt = len(tbl.get("total"))

        if "REFILL" in t_type or "заправ" in title:
            refill_count += cnt
        elif "DRAIN" in t_type or "злив" in title:
            drain_count += cnt

    log.info("Результат перевірки звіту №%s: заправок=%d, зливів=%d", report_id, refill_count, drain_count)
    return {
        "checked": True,
        "report_id": report_id,
        "refill_count": refill_count,
        "drain_count": drain_count
    }


def notify_incident(cfg: dict, detail: str, start_ts: float = 0.0) -> None:
    diag_line = ""
    if start_ts > 0:
        rep = check_events_in_report(cfg, start_ts)
        if rep.get("checked"):
            r_cnt = rep.get("refill_count", 0)
            d_cnt = rep.get("drain_count", 0)
            rep_id = rep.get("report_id")
            if r_cnt > 0 or d_cnt > 0:
                parts = []
                if r_cnt > 0:
                    parts.append(f"Заправка: {r_cnt}")
                if d_cnt > 0:
                    parts.append(f"Злив: {d_cnt}")
                found_str = ", ".join(parts)
                diag_line = (
                    f"⚠️ Діагностика: Сповіщень нема в історії, але є в звіті №{rep_id} ({found_str})\n"
                    f"👉 Проблема в конвеєрі сповіщень (події зафіксовано в системі, але алерт не створено)!"
                )
            else:
                diag_line = (
                    f"⚠️ Діагностика: Події відсутні і в сповіщеннях, і в звіті №{rep_id}\n"
                    f"👉 Дані не порахувалися ядром (можлива проблема з розпізнаванням пального або треком)!"
                )
        elif rep.get("reason"):
            diag_line = f"ℹ️ Діагностика звіту №{cfg.get('report_id')}: {rep.get('reason')}"

    diag_section = f"\n\n{diag_line}" if diag_line else ""
    text = f"🔴 [DEV] [Teltonika Single] Не спрацювали сповіщення пального! [7 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}{diag_section}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict, detail: str) -> None:
    text = f"🟢 [DEV] [Teltonika Single] Сповіщення пального відновилися! [7 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [DEV] [Teltonika Single] Заправка та Злив працюють коректно! [7 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Single [7 min] для %s (IMEI: %s)...", cfg["device_name"], cfg["tracker_imei"])
    resolve_device_id(cfg)

    start_ts = time.time()
    initial_ids, max_initial_id = get_existing_event_ids(cfg)
    log.info("Фіксація старту: %d наявних сповіщень в історії (max_id=%s, start_ts=%.0f)",
             len(initial_ids), max_initial_id, start_ts)

    try:
        send_canary_events(cfg)
    except Exception as e:
        notify_incident(cfg, f"Помилка відправки точок по TCP: {e}")
        return 1

    ok, detail = wait_for_events(cfg, start_ts, initial_ids, max_initial_id)
    if ok:
        log.info("Успіх!\n%s", detail)
        if state.get("incident_open"):
            notify_recovery(cfg, detail)
        else:
            notify_success(cfg, detail)
        state["incident_open"] = False
    else:
        log.error("Невдача!\n%s", detail)
        if not state.get("incident_open"):
            notify_incident(cfg, detail, start_ts=start_ts)
        state["incident_open"] = True

    save_state(cfg, state)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
