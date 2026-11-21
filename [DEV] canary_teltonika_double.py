#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень M2M.
СЦЕНАРІЙ 2: [DEV] ПОДВІЙНА ЗАПРАВКА ПІДРЯД ТА ЗЛИВ (Double Refill + Drain)
ПРОТОКОЛ: TELTONIKA (Codec 8)
Об'єкт: Refill_Drain_UNIT_double (IMEI: 68162416785325812345)
Сервер прийому: 95.217.119.27:5027
API: https://dev-stag.m2m.eu/api

ВІДПОВІДНІСТЬ ПРОДУ ТА ДЕТЕКТОРУ ПОЇЗДОК M2M:
- Формат пакета: 100% як на проді (запалювання ID 239 ЗАВЖДИ 1 для розрахунку датчика на льоту!)
- Різниця початків заправок: РІВНО 6 ХВИЛИН (360 секунд між стартом #1 та стартом #2)
- Визначення стоянки: по GPS швидкості (speed = 0, movement = 0 тривалістю > 300 с)
- Поїздки: > 60 с, дистанція > 500 м
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
log = logging.getLogger("canary_teltonika_double")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

CONFIG = {
    "device_id": 47614,
    "report_id": 1180,
    "device_name": "Refill_Drain_UNIT_double",
    "tracker_imei": "68162416785325812345",
    
    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,
    
    "template_refill": "Заправка (Сценарій 2 - Double)",
    "template_drain": "Злив (Сценарій 2 - Double)",
    
    "geo_lat": 49.438923,
    "geo_lon": 32.085565,
    "altitude": 10,
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
    "max_wait_sec": 2100,
    "http_timeout_sec": 60,  
    
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_double_dev_state.json")),
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
    io_data.append(1)  # ЗАПАЛЮВАННЯ ЗАВЖДИ 1 (як на проді, щоб датчик рахувався на льоту!)
    
    # 2-byte IOs: Fuel level (201)
    io_data.append(1)
    io_data.append(201)
    io_data.extend(struct.pack(">h", int(level)))
    
    # 4-byte and 8-byte IOs: 0
    io_data.append(0)
    io_data.append(0)
    
    data_part8 = bytearray([0x08, 1]) + avl_data + io_data + bytearray([1])
    crc8 = crc16_arc(data_part8)
    packet_c8 = bytearray(b'\x00\x00\x00\x00') + struct.pack(">I", len(data_part8)) + data_part8 + struct.pack(">I", crc8)

    log.info("Teltonika: Fuel=%d л, Speed=%d км/год, Ignition=1 (lat=%.5f, lon=%.5f)", 
             int(level), speed, lat, lon)

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
    lat_gas, lon_gas = cfg["geo_lat"], cfg["geo_lon"]
    lat_drain, lon_drain = lat_gas + 0.009, lon_gas + 0.009

    # -------------------------------------------------------------
    # 1. ПОЇЗДКА 1 (Під'їзд до АЗС): ~64 с, дистанція > 500 м
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 1: Поїздка до АЗС (> 60 с, > 100 м, швидкість 35 км/год)...")
    trip1_coords = [
        (lat_gas - 0.006, lon_gas - 0.006),
        (lat_gas - 0.004, lon_gas - 0.004),
        (lat_gas - 0.002, lon_gas - 0.002),
        (lat_gas - 0.001, lon_gas - 0.001),
    ]
    for lat, lon in trip1_coords:
        _send_point(cfg, level=128, lat=lat, lon=lon, speed=35)
        time.sleep(16)

    # -------------------------------------------------------------
    # 2. СТОЯНКА 1 (Подвійна заправка): тривалість 525 с (8.75 хв > 300 с!)
    # Швидкість 0, рух 0, запалювання 1, координати константні!
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Стоянка під подвійну заправку (525 с > 300 с для Детектора поїздок)...")
    # Базова полиця: 30 с (2 точки по 15 с)
    for _ in range(2):
        _send_point(cfg, level=128, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ЗАПРАВКА #1: Старт на t=30 с. Зростання 128 -> 402 л (+274 л) за 75 с (5 точок по 15 с)
    log.info(">>> ЕТАП 2.1: СТАРТ Заправки #1 (з 128 л до 402 л)...")
    stg1_steps = [182, 237, 292, 347, 402]
    for lvl in stg1_steps:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ПРОМІЖНА ПОЛИЦЯ / ПАУЗА: триває з t=105 с до t=390 с (285 с = 4.75 хвилини, 19 точок по 15 с)
    # Забезпечує РІВНО 6 ХВИЛИН (360 с) між стартом #1 (t=30 с) і стартом #2 (t=390 с)!
    log.info(">>> ЕТАП 2.2: Проміжна полиця на 402 л (пауза до настання 6-ї хвилини)...")
    for i in range(1, 20):
        _send_point(cfg, level=402, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ЗАПРАВКА #2: Старт на t=390 с (рівно 6 хвилин після старту #1!). Зростання 402 -> 630 л (+228 л) за 60 с (4 точки по 15 с)
    log.info(">>> ЕТАП 2.3: СТАРТ Заправки #2 (РІВНО ЧЕРЕЗ 6 ХВИЛИН після першої!)...")
    stg2_steps = [459, 516, 573, 630]
    for lvl in stg2_steps:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ФІНАЛЬНЕ ПЛАТО ПІСЛЯ ЗАПРАВОК: 75 с (5 точок по 15 с)
    log.info(">>> ЕТАП 2.4: Фінальне плато після заправок (630 л)...")
    for _ in range(5):
        _send_point(cfg, level=630, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 3. ПОЇЗДКА 2 (Закриття стоянки подвійної заправки): ~64 с, дистанція > 500 м
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 3: Виїзд з АЗС (закриває стоянку заправок в M2M)...")
    trip2_coords = [
        (lat_gas + 0.002, lon_gas + 0.002),
        (lat_gas + 0.004, lon_gas + 0.004),
        (lat_gas + 0.006, lon_gas + 0.006),
        (lat_gas + 0.008, lon_gas + 0.008),
    ]
    for lat, lon in trip2_coords:
        _send_point(cfg, level=630, lat=lat, lon=lon, speed=35)
        time.sleep(16)

    # -------------------------------------------------------------
    # 4. СТОЯНКА 2 (Злив): тривалість 315 с (5 хв 15 с > 300 с!)
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 4: Стоянка під злив (315 с > 300 с)...")
    for _ in range(2):
        _send_point(cfg, level=630, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # Злив пального (-300 л): з 630 до 330 л за 195 с (13 точок по 15 с)
    log.info(">>> ЕТАП 4.1: Плавний злив (-300 л)...")
    drain_step = 300.0 / 13
    for i in range(1, 14):
        lvl = 630 - drain_step * i
        _send_point(cfg, level=lvl, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # Плато після зливу: 90 с (6 точок по 15 с)
    log.info(">>> ЕТАП 4.2: Плато після зливу (330 л)...")
    for _ in range(6):
        _send_point(cfg, level=330, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 5. ПОЇЗДКА 3 (Закриття стоянки зливу): ~64 с, дистанція > 500 м
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 5: Від'їзд зі стоянки (закриває стоянку зливу в M2M)...")
    trip3_coords = [
        (lat_drain + 0.002, lon_drain + 0.002),
        (lat_drain + 0.004, lon_drain + 0.004),
        (lat_drain + 0.006, lon_drain + 0.006),
        (lat_drain + 0.008, lon_drain + 0.008),
    ]
    for lat, lon in trip3_coords:
        _send_point(cfg, level=330, lat=lat, lon=lon, speed=35)
        time.sleep(16)

    log.info("Усі точки подвійної заправки та зливу в реальному часі відправлено успішно!")
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
            day, month, year, hour, minute = map(int, m.groups()[:5])
            sec = int(m.group(6)) if m.group(6) else 0
            return datetime(year, month, day, hour, minute, sec, tzinfo=KYIV_TZ).timestamp()
        except Exception:
            pass
    return None


def extract_event_time(item: dict) -> str:
    msg = str(item.get("message") or "")
    m_time = re.search(r'початку\s+[^\:]*:\s*(?:\d{2}\.\d{2}\.\d{4}\s+)?(\d{2}:\d{2})', msg, re.I)
    if m_time:
        return m_time.group(1)
    m_general = re.search(r'\b(\d{2}:\d{2})\b', msg)
    if m_general:
        return m_general.group(1)
    created = item.get("createdAt")
    if created and isinstance(created, str):
        m_iso = re.search(r'T(\d{2}:\d{2})', created)
        if m_iso:
            return m_iso.group(1)
    return ""


def extract_event_duration(item: dict) -> str:
    msg = str(item.get("message") or "")
    
    # 1. Пошук тривалості у повідомленні ("тривалість: 00:04:15" або "тривалістю 4 хв")
    m_dur = re.search(r'триваліст[^\:\d]*[:\s]+([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)', msg, re.I)
    if m_dur:
        raw_dur = m_dur.group(1)
        parts = [int(p) for p in raw_dur.split(":")]
        if len(parts) == 3:
            h, m, s = parts
            res = []
            if h > 0: res.append(f"{h} год")
            if m > 0 or h > 0: res.append(f"{m} хв")
            res.append(f"{s} с")
            return " ".join(res)
        elif len(parts) == 2:
            m, s = parts
            return f"{m} хв {s} с"
        return raw_dur

    m_text = re.search(r'триваліст[^\:\d]*[:\s]+([\d\.]+\s*(?:хв|год|с|сек))', msg, re.I)
    if m_text:
        return m_text.group(1).strip()

    # 2. Пошук у metadata
    try:
        meta = item.get("metadata", {})
        if isinstance(meta, str):
            meta = json.loads(meta)
        if isinstance(meta, dict):
            dur = meta.get("duration") or meta.get("eventDuration") or meta.get("fuelDuration")
            if dur is not None:
                if isinstance(dur, (int, float)):
                    sec = int(dur)
                    if sec > 360000: sec //= 1000
                    h = sec // 3600
                    m = (sec % 3600) // 60
                    s = sec % 60
                    res = []
                    if h > 0: res.append(f"{h} год")
                    if m > 0 or h > 0: res.append(f"{m} хв")
                    res.append(f"{s} с")
                    return " ".join(res)
                elif isinstance(dur, str) and dur.strip():
                    return dur.strip()

            start_t = meta.get("fuelStartTime") or meta.get("startTime")
            finish_t = meta.get("fuelFinishTime") or meta.get("finishTime") or meta.get("endTime")
            if start_t and finish_t and isinstance(start_t, (int, float)) and isinstance(finish_t, (int, float)):
                diff_s = int(abs(finish_t - start_t))
                if diff_s > 360000: diff_s //= 1000
                h = diff_s // 3600
                m = (diff_s % 3600) // 60
                s = diff_s % 60
                res = []
                if h > 0: res.append(f"{h} год")
                if m > 0 or h > 0: res.append(f"{m} хв")
                res.append(f"{s} с")
                return " ".join(res)
    except Exception:
        pass

    # 3. Пошук у кореневих полях item
    dur = item.get("duration") or item.get("eventDuration")
    if dur is not None and isinstance(dur, (int, float)):
        sec = int(dur)
        if sec > 360000: sec //= 1000
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        res = []
        if h > 0: res.append(f"{h} год")
        if m > 0 or h > 0: res.append(f"{m} хв")
        res.append(f"{s} с")
        return " ".join(res)

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
        
        # 1. Відкидаємо сповіщення, що вже існували до старту екшенса
        if event_id in initial_event_ids: 
            continue
        if max_initial_id and isinstance(event_id, int) and event_id <= max_initial_id:
            continue

        # 2. Відкидаємо сповіщення з часом, старішим за старт поточного запуску
        ev_ts = parse_item_timestamp(item)
        if ev_ts is not None and ev_ts < (start_ts - 60):
            continue
            
        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        dev_id = item.get("deviceId")
        dev_name = str(item.get("deviceName") or "")
        unique_id = str(item.get("uniqueId") or "")
        
        raw_type = str(item.get("type", "")).upper()
        template_type = str(item.get("templateType", "")).upper()
        template_name = str(item.get("templateName", "")).lower()
        message = str(item.get("message", "")).lower()

        # СУВОРЕ ВІДСІКАННЯ ЧУЖИХ СЦЕНАРІЇВ:
        # Для Double обов'язково відкидаємо Single та Prod!
        if (
            (dev_name.strip().lower() == "refill_drain_unit")
            or "сценарій 1" in template_name
            or "single" in template_name
            or "test_notification_o.rosenko" in dev_name.lower()
        ):
            continue

        # 1. Прив'язка до об'єкта (по ID, IMEI, назві АБО по унікальному шаблону об'єкта!)
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
            or f"\"{target_name.lower()}\"" in message
        ):
            matched = True
        elif (
            (target_refill in template_name or target_drain in template_name)
            and "single" not in template_name
            and "сценарій 1" not in template_name
        ):
            matched = True
            
        if not matched:
            continue
        
        # 2. Об'єм пального
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
            m = re.search(r'об[\'’`]?ємом\s+([\d\.]+)\s*л', str(item.get("message", "")) + " " + item_str, re.I)
            if m:
                try:
                    volume = float(m.group(1))
                except Exception:
                    pass

        ev_time = extract_event_time(item)
        ev_dur = extract_event_duration(item)

        # 3. Ідентифікація типу події
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
    
    # Заправка 1
    if len(refills_found) >= 1:
        t_parts = []
        if refills_found[0].get("time"):
            t_parts.append(f"час: {refills_found[0]['time']}")
        dur = refills_found[0].get("duration") or "6 хв"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = refills_found[0].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка 1{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка 1")

    # Заправка 2
    if len(refills_found) >= 2:
        t_parts = []
        if refills_found[1].get("time"):
            t_parts.append(f"час: {refills_found[1]['time']}")
        dur = refills_found[1].get("duration") or "6 хв"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = refills_found[1].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка 2{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка 2")

    # Злив
    if len(drains_found) >= 1:
        t_parts = []
        if drains_found[0].get("time"):
            t_parts.append(f"час: {drains_found[0]['time']}")
        dur = drains_found[0].get("duration") or "4 хв"
        t_parts.append(f"тривалість: {dur}")
        t_str = f" ({', '.join(t_parts)})"
        vol = drains_found[0].get("volume")
        v_str = f": -{vol:.2f} л" if isinstance(vol, (int, float)) else (f": -{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Злив{v_str}{t_str}")
    else:
        report_lines.append("❌ Злив")

    all_ok = (len(refills_found) >= 2) and (len(drains_found) >= 1)
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
            "Опитування історії API (пройшло %d/%d с, знайдено заправок: %d/2, зливів: %d/1)...", 
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
                    f"⚠️ Діагностика: Сповіщень нема в історії, але є в звіті №{rep_id} ({found_str})\n👉 Проблема в конвеєрі сповіщень (події зафіксовано в системі, але алерт не створено)!"
                )
            else:
                diag_line = (
                    f"⚠️ Діагностика: Події відсутні і в сповіщеннях, і в звіті №{rep_id}\n👉 Дані не порахувалися ядром (можлива проблема з розпізнаванням пального або треком)!"
                )
        elif rep.get("reason"):
            diag_line = f"ℹ️ Діагностика звіту №{cfg.get('report_id')}: {rep.get('reason')}"

    diag_section = f"\n\n{diag_line}" if diag_line else ""
    text = f"🔴 [DEV] [Teltonika Double] Не спрацювали сповіщення пального!\nОб'єкт: {cfg['device_name']}\n\n{detail}{diag_section}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict, detail: str) -> None:
    text = f"🟢 [DEV] [Teltonika Double] Сповіщення пального відновилися!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [DEV] [Teltonika Double] Подвійна заправка та Злив працюють коректно!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Double для %s (IMEI: %s)...", cfg["device_name"], cfg["tracker_imei"])
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
            notify_incident(cfg, detail)
        state["incident_open"] = True
        
    save_state(cfg, state)
    return 0 if ok else 1


if __name__ == "__main__": 
    sys.exit(main())
