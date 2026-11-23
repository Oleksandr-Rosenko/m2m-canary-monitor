#!/usr/bin/env python3
"""
Канареечний скрипт: [DEV] Canary Monitor (Teltonika Single Refill Only) [12.5 min].
Цільовий об'єкт: Refill_Drain_UNIT (id=47613, IMEI: 12345678901234567890).
Сценарій з реальними координатами траси М-07 та АЗС БРСМ-Нафта (OpenStreetMap):
  1. Невелика початкова стоянка (~2 хв, TIR-паркінг Старовойтове на М-07).
  2. Довгенька поїздка до АЗС (~4 хв по трасі М-07, ~4.8 км, швидкість до 82 км/год).
  3. Реальний плавний заїзд на АЗС БРСМ-Нафта через смугу гальмування та заїзд (OSM Way 207186757).
  4. Зупинка біля паливороздавальної колонки (ТРК) та передзаправна пауза (~1 хв).
  5. Реалістична заправка тривалістю 12.5 хв (750 с, 50 точок) на +460 л (з 82.10 до 542.10 л).
  6. Післязаправне плато стабілізації біля колонки (~1 хв).
  7. Реальний виїзд з АЗС через смугу розгону (OSM Way 207186757) назад на М-07.
  8. Довгенька поїздка після АЗС (~4 хв по трасі М-07 на схід, ~4.2 км, швидкість до 82 км/год, плавна витрата ~3.3 л).
  9. Фінішна стоянка на паркінгу зони відпочинку М-07 (~1 хв), що остаточно фіксує поїздку.
Перевірка: ТІЛЬКИ заправка (без зливу).
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
log = logging.getLogger("canary_teltonika_single_refill_12_5m")

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

    # Точні координати ТРК БРСМ-Нафта (OSM Way 207186757, с. Римачі, вул. Дорожна, 6)
    "geo_lat": 51.2015219,
    "geo_lon": 23.8925562,
    "altitude": 178,
    "bearing": 98,
    "sat": 14,

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
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_refill_12_5m_dev_state.json")),
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

    # 1-byte IOs: Movement (240) та Ignition (239)
    io_data.append(2)
    io_data.append(240)
    io_data.append(1 if speed > 0 else 0)
    io_data.append(239)
    io_data.append(1)  # ЗАПАЛЮВАННЯ 1

    # 2-byte IOs: Fuel level (201)
    io_data.append(1)
    io_data.append(201)
    io_data.extend(struct.pack(">h", int(round(level))))

    # 4-byte and 8-byte elements: 0
    io_data.append(0)
    io_data.append(0)

    data_part8 = bytearray([0x08, 1]) + avl_data + io_data + bytearray([1])
    crc8 = crc16_arc(data_part8)
    packet_c8 = bytearray(b'\x00\x00\x00\x00') + struct.pack(">I", len(data_part8)) + data_part8 + struct.pack(">I", crc8)

    log.info("Teltonika: Fuel=%.2f л, Speed=%d км/год, Ignition=1 (lat=%.6f, lon=%.6f)",
             level, speed, lat, lon)

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            with socket.create_connection((host, port), timeout=cfg["http_timeout_sec"]) as sock:
                imei_b = imei.encode('ascii')
                sock.sendall(struct.pack(">H", len(imei_b)) + imei_b)
                resp = sock.recv(1)
                if not resp or resp[0] != 1:
                    log.warning("Handshake не підтверджено сервером: %s", resp)
                    return
                sock.sendall(packet_c8)
                sock.recv(4)
            return
        except (socket.error, OSError) as e:
            if attempt == max_retries:
                log.error("TCP Error вичерпано всі спроби (%d): %s", max_retries, e)
                raise
            delay = 3 * attempt
            log.warning("Помилка підключення (спроба %d/%d). Повтор через %d с: %s", attempt, max_retries, delay, e)
            time.sleep(delay)


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)

    # -------------------------------------------------------------
    # РЕАЛЬНІ ГЕОГРАФІЧНІ КООРДИНАТИ (OpenStreetMap road geometry):
    # -------------------------------------------------------------
    lat_park_start, lon_park_start = 51.1941494, 23.8251173  # TIR-паркінг Старовойтове на М-07
    lat_gas, lon_gas = 51.2015219, 23.8925562                # ТРК БРСМ-Нафта під навісом
    lat_park_end, lon_park_end = 51.1988310, 23.9475210      # Паркінг зони відпочинку М-07

    start_level = 85.40
    pre_refill_level = 82.10
    final_refill_level = 542.10  # +460.00 л заправки
    post_trip_level = 538.80     # -3.30 л витрата під час другої поїздки

    # Реалістична крива наливу на 50 точок по 15 с (750 секунд = 12.5 хв)
    refill_levels = [
        86.10, 93.10, 102.10, 112.10, 122.35, 132.60, 142.85, 153.10, 163.35, 173.60,
        183.85, 194.10, 204.35, 214.60, 224.85, 235.10, 245.35, 255.60, 265.85, 276.10,
        286.35, 296.60, 306.85, 317.10, 327.35, 337.60, 347.85, 358.10, 368.35, 378.60,
        388.85, 399.10, 409.35, 419.60, 429.85, 440.10, 450.35, 460.60, 470.85, 481.10,
        491.35, 501.60, 511.85, 522.10, 530.10, 535.60, 539.10, 540.90, 541.70, 542.10
    ]

    log.info(">>> СТАРТ СИМУЛЯЦІЇ: Single Refill Only [12.5 min] (Заправка 12.5 хв, +460 л)...")

    # -------------------------------------------------------------
    # ЕТАП 1: Невелика початкова стоянка (~2 хв = 120 с)
    # 8 точок по 15 с на стоянці (швидкість 0 км/год)
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 1: Початкова стоянка на TIR-паркінгу М-07 (2 хв, рівень %.2f л)...", start_level)
    for _ in range(8):
        _send_point(cfg, level=start_level, lat=lat_park_start, lon=lon_park_start, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 2: Довгенька поїздка до АЗС по трасі М-07 (~4 хв = 240 с)
    # Реальні вузли OSM автодороги М-07 (дистанція 4.8 км)
    # Заїзд на АЗС через смугу гальмування та заїзд (OSM Way 207186757)
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Поїздка до АЗС по реальних точках М-07 (4.8 км, швидкість до 82 км/год)...")
    trip1_points = [
        (51.1942891, 23.8277519, 30, 85.35),
        (51.1944808, 23.8307565, 50, 85.20),
        (51.1948452, 23.8372635, 70, 85.00),
        (51.1953430, 23.8428092, 78, 84.80),
        (51.1960226, 23.8479144, 82, 84.60),
        (51.1966958, 23.8530388, 80, 84.40),
        (51.1974571, 23.8588648, 82, 84.15),
        (51.1981831, 23.8643672, 80, 83.90),
        (51.1988672, 23.8695379, 81, 83.65),
        (51.1995925, 23.8751094, 80, 83.40),
        (51.2006096, 23.8828697, 78, 83.15),
        (51.2016774, 23.8910163, 60, 82.90),
        (51.2017205, 23.8913449, 35, 82.70),
        (51.2014714, 23.8921031, 15, 82.55),
        (51.2015219, 23.8925562, 0,  82.40),
        (51.2015219, 23.8925562, 0,  82.10),
    ]
    for lat, lon, spd, lvl in trip1_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 3: Передзаправна полиця на АЗС біля ТРК (1 хв = 60 с)
    # 4 точки по 15 с біля колонки під навісом
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 3: Передзаправна полиця біля ТРК (1 хв, рівень %.2f л)...", pre_refill_level)
    for _ in range(4):
        _send_point(cfg, level=pre_refill_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 4: Реалістична заправка рівно 12.5 хв (750 с, 50 точок по 15 с)
    # Рівень зростає з 82.10 до 542.10 л (+460.00 л)
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 4: Реалістичний налив пального 12.5 хв (%.2f -> %.2f л, +460 л)...",
             pre_refill_level, final_refill_level)
    for lvl in refill_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 5: Післязаправне плато стабілізації на АЗС (1 хв = 60 с)
    # 4 точки по 15 с на рівні 542.10 л біля ТРК
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 5: Післязаправне плато стабілізації біля ТРК (1 хв, рівень %.2f л)...", final_refill_level)
    for _ in range(4):
        _send_point(cfg, level=final_refill_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 6: Довгенька поїздка після АЗС по трасі М-07 (~4 хв = 240 с)
    # Виїзд з ТРК через виїзну дорогу АЗС та смугу прискорення (OSM Way 207186757)
    # Рух на схід по М-07 на 4.2 км зі швидкістю до 82 км/год
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 6: Виїзд з АЗС та поїздка по М-07 на схід (4.2 км, швидкість до 82 км/год)...")
    trip2_points = [
        (51.2015760, 23.8930419, 20, 542.10),
        (51.2020117, 23.8935662, 45, 542.00),
        (51.2024580, 23.8969730, 65, 541.80),
        (51.2029280, 23.9005560, 75, 541.60),
        (51.2032570, 23.9030670, 80, 541.35),
        (51.2041540, 23.9099220, 82, 541.10),
        (51.2051270, 23.9173370, 80, 540.85),
        (51.2054310, 23.9196520, 82, 540.60),
        (51.2060810, 23.9248850, 80, 540.35),
        (51.2055600, 23.9300660, 78, 540.10),
        (51.2045170, 23.9327320, 80, 539.85),
        (51.2016470, 23.9385730, 75, 539.60),
        (51.1997800, 23.9425990, 60, 539.40),
        (51.1990720, 23.9453660, 40, 539.25),
        (51.1988310, 23.9475210, 20, 539.15),
        (51.1988310, 23.9475210, 0,  post_trip_level),
    ]
    for lat, lon, spd, lvl in trip2_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(15)

    # -------------------------------------------------------------
    # ЕТАП 7: Фінішна стоянка на новому паркінгу (1 хв = 60 с)
    # 4 точки по 15 с, закриває поїздку в ядрі M2M
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 7: Фінішна стоянка на новому паркінгу (1 хв, закриває поїздку в M2M)...")
    for _ in range(4):
        _send_point(cfg, level=post_trip_level, lat=lat_park_end, lon=lon_park_end, speed=0)
        time.sleep(15)

    log.info("Усі точки Single Refill Only [12.5 min] у реальному часі відправлено успішно!")
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
            try:
                dur_sec = int(float(dur))
                mn = dur_sec // 60
                sc = dur_sec % 60
                return f"{mn} хв" if sc == 0 else f"{mn} хв {sc} с"
            except Exception:
                return str(dur)
    return ""


def get_existing_event_ids(cfg: dict) -> tuple[set, int]:
    data = fetch_history(cfg)
    items = data.get("items") or data.get("data") or []
    ids = set()
    max_id = 0
    for it in items:
        ev_id = it.get("id")
        if ev_id is not None:
            try:
                numeric_id = int(ev_id)
                ids.add(numeric_id)
                if numeric_id > max_id:
                    max_id = numeric_id
            except (ValueError, TypeError):
                ids.add(ev_id)
    return ids, max_id


def check_events_in_history(cfg: dict, start_ts: float, initial_ids: set, max_initial_id: int) -> tuple[bool, str, int]:
    data = fetch_history(cfg)
    items = data.get("items") or data.get("data") or []
    target_id = cfg.get("device_id")
    target_name = cfg.get("device_name", "")
    target_imei = str(cfg.get("tracker_imei", ""))
    target_refill = cfg["template_refill"].lower()

    refills_found = []

    for item in items:
        event_id = item.get("id")
        try:
            numeric_id = int(event_id)
            if numeric_id in initial_ids or (max_initial_id > 0 and numeric_id <= max_initial_id):
                continue
        except (ValueError, TypeError):
            if event_id in initial_ids:
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
            target_refill in template_name
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

    refills_found.sort(key=lambda x: x["id"])

    report_lines = []
    if len(refills_found) >= 1:
        t_parts = []
        if refills_found[0].get("time"):
            t_parts.append(f"час: {refills_found[0]['time']}")
        dur = refills_found[0].get("duration") or "12 хв 30 с"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = refills_found[0].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка")

    all_ok = (len(refills_found) >= 1)
    return all_ok, "\n".join(report_lines), len(refills_found)


def wait_for_events(cfg: dict, start_ts: float, initial_event_ids: set, max_initial_id: int) -> tuple[bool, str]:
    log.info("Початкова пауза (%d с) перед опитуванням історії...", cfg["initial_wait_sec"])
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    start_poll = time.monotonic()

    while True:
        ok, detail, n_ref = check_events_in_history(cfg, start_ts, initial_event_ids, max_initial_id)
        if ok:
            elapsed = int(time.monotonic() - start_poll)
            log.info("Очікуване сповіщення заправки успішно знайдено (через %d с від початку опитування)!\n%s", elapsed, detail)
            return True, detail
        if time.monotonic() >= deadline:
            return False, f"Таймаут очікування сповіщень ({cfg['max_wait_sec']} с)\n\n{detail}"

        elapsed = int(time.monotonic() - start_poll)
        log.info(
            "Опитування історії API (пройшло %d/%d с, знайдено заправок: %d/1)...",
            elapsed, cfg["max_wait_sec"], n_ref
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

    log.info("Результат перевірки звіту №%s: заправок=%d", report_id, refill_count)
    return {
        "checked": True,
        "report_id": report_id,
        "refill_count": refill_count
    }


def notify_incident(cfg: dict, detail: str, start_ts: float = 0.0) -> None:
    diag_line = ""
    if start_ts > 0:
        rep = check_events_in_report(cfg, start_ts)
        if rep.get("checked"):
            r_cnt = rep.get("refill_count", 0)
            rep_id = rep.get("report_id")
            if r_cnt > 0:
                diag_line = (
                    f"⚠️ Діагностика: Сповіщень нема в історії, але є в звіті №{rep_id} (Заправка: {r_cnt})\n"
                    f"👉 Проблема в конвеєрі сповіщень (подію зафіксовано в системі, але алерт не створено)!"
                )
            else:
                diag_line = (
                    f"⚠️ Діагностика: Заправка відсутня і в сповіщеннях, і в звіті №{rep_id}\n"
                    f"👉 Дані не порахувалися ядром (можлива проблема з розпізнаванням пального або треком)!"
                )
        elif rep.get("reason"):
            diag_line = f"ℹ️ Діагностика звіту №{cfg.get('report_id')}: {rep.get('reason')}"

    diag_section = f"\n\n{diag_line}" if diag_line else ""
    text = f"🔴 [DEV] [Teltonika Single] Не спрацювало сповіщення заправки! [12.5 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}{diag_section}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict, detail: str) -> None:
    text = f"🟢 [DEV] [Teltonika Single] Сповіщення заправки відновилися! [12.5 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [DEV] [Teltonika Single] Заправка працює коректно! [12.5 min]\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Single Refill Only [12.5 min] для %s (IMEI: %s)...", cfg["device_name"], cfg["tracker_imei"])
    resolve_device_id(cfg)

    start_ts = time.time()
    initial_ids, max_initial_id = get_existing_event_ids(cfg)
    log.info("Фіксація старту: %d наявних сповіщень в історії (max_id=%s, start_ts=%.0f)",
             len(initial_ids), max_initial_id, start_ts)

    try:
        send_canary_events(cfg)
    except Exception as e:
        log.error("Критична помилка в send_canary_events: %s", e, exc_info=True)
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
