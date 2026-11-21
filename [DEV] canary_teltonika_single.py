#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень M2M.
СЦЕНАРІЙ 1: [DEV] ОДИНОЧНА ЗАПРАВКА ТА ЗЛИВ (Single Refill + Drain)
ПРОТОКОЛ: TELTONIKA (Codec 8)
Об'єкт: Refill_Drain_UNIT (IMEI: 12345678901234567890)
Сервер прийому: 95.217.119.27:5027
API: https://dev-stag.m2m.eu/api

ВІДПОВІДНІСТЬ ПРОДУ ТА ДЕТЕКТОРУ ПОЇЗДОК M2M:
- Формат пакета: 100% як на проді (запалювання ID 239 ЗАВЖДИ 1 для розрахунку датчика на льоту!)
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
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_single")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

CONFIG = {
    "device_id": None,
    "device_name": "Refill_Drain_UNIT",
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
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_dev_state.json")),
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
    
    # Координати АЗС біля М-07 (Римачі / Ягодин, Волинська обл. з реального треку):
    lat_gas, lon_gas = 51.201641, 23.892584
    # Координати другого пункту зупинки для зливу (далі по трасі М-07):
    lat_drain, lon_drain = 51.206059, 23.925201

    # -------------------------------------------------------------
    # 1. ПОЇЗДКА 1 (Під'їзд до АЗС по М-07): плавне гальмування
    # Тривалість: ~65 с, дистанція > 1.5 км
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 1: Під'їзд до АЗС по трасі М-07 (гальмування з 70 до 0 км/год)...")
    approach_points = [
        (51.197353, 23.857990, 70, 1025),
        (51.199303, 23.872787, 65, 1028),
        (51.200611, 23.882740, 45, 1032),
        (51.201313, 23.888157, 25, 1035),
    ]
    for lat, lon, spd, lvl in approach_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    # -------------------------------------------------------------
    # 2. СТОЯНКА 1 (АЗС, Реалістична східчаста заправка):
    # Загальна тривалість: ~420 с (7.0 хв > 300 с для Детектора стоянок M2M)
    # Швидкість 0, рух 0, запалювання 1
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Стоянка на АЗС під заправку (> 300 с)...")
    
    # 2.1. Передзаправна полиця (очікування на колонці) з природними коливаннями ДРП:
    # 6 точок по 15 с = 90 с
    log.info(">>> ЕТАП 2.1: Передзаправна полиця з природною флуктуацією ДРП (~1010-1038)...")
    pre_refill_noise = [1010, 1025, 1032, 1036, 1038, 1038]
    for lvl in pre_refill_noise:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.2. Налив фаза 1 (первинний напір пістолета, стрибок до ~1609):
    # 3 точки по 15 с = 45 с
    log.info(">>> ЕТАП 2.2: Налив фаза 1 (зростання 1038 -> 1609)...")
    stage1_levels = [1225, 1420, 1609]
    for lvl in stage1_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.3. Проміжна полиця / пауза 1 (відсічка пістолета або пауза наливу):
    # 2 точки по 15 с = 30 с
    log.info(">>> ЕТАП 2.3: Проміжна полиця на 1609...")
    for _ in range(2):
        _send_point(cfg, level=1609, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.4. Налив фаза 2 (основний об'єм до ~3005):
    # 4 точки по 15 с = 60 с
    log.info(">>> ЕТАП 2.4: Налив фаза 2 (зростання 1609 -> 3005)...")
    stage2_levels = [1955, 2355, 2765, 3005]
    for lvl in stage2_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.5. Проміжна полиця / пауза 2:
    # 2 точки по 15 с = 30 с
    log.info(">>> ЕТАП 2.5: Проміжна полиця на 3005...")
    for _ in range(2):
        _send_point(cfg, level=3005, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.6. Налив фаза 3 (долив під кришку до ~3685):
    # 3 точки по 15 с = 45 с
    log.info(">>> ЕТАП 2.6: Налив фаза 3 (долив до повного бака 3005 -> 3685)...")
    stage3_levels = [3260, 3510, 3685]
    for lvl in stage3_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.7. Фінальне плато стабілізації після заправки:
    # 8 точок по 15 с = 120 с
    log.info(">>> ЕТАП 2.7: Фінальне плато стабілізації повного бака (3685)...")
    for _ in range(8):
        _send_point(cfg, level=3685, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 3. ПОЇЗДКА 2 (Виїзд з АЗС на трасу М-07, закриває стоянку заправки):
    # Тривалість: ~65 с, дистанція > 1.5 км, швидкість до 75 км/год
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 3: Виїзд з АЗС на трасу М-07 (закриває стоянку заправки в M2M)...")
    depart_points = [
        (51.201660, 23.892738, 15, 3685),
        (51.202324, 23.896040, 45, 3689),
        (51.203232, 23.902967, 65, 3685),
        (51.204708, 23.914114, 75, 3680),
    ]
    for lat, lon, spd, lvl in depart_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    # -------------------------------------------------------------
    # 4. СТОЯНКА 2 (Пункт відстою, Злив пального):
    # Загальна тривалість: ~330 с (5.5 хв > 300 с)
    # Швидкість 0, рух 0, координати константні
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 4: Стоянка під злив пального (> 300 с)...")
    # 4.1. Передзливна полиця: 45 с (3 точки по 15 с)
    for _ in range(3):
        _send_point(cfg, level=3685, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.2. Плавний злив пального (з 3685 до 1850 raw units / ~180-200 л):
    # 10 точок по 15 с = 150 с
    log.info(">>> ЕТАП 4.1: Плавний злив пального (3685 -> 1850)...")
    drain_step = (3685 - 1850) / 10.0
    for i in range(1, 11):
        lvl = int(3685 - drain_step * i)
        _send_point(cfg, level=lvl, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.3. Плато стабілізації після зливу: 9 точок по 15 с = 135 с
    log.info(">>> ЕТАП 4.2: Плато стабілізації після зливу (1850)...")
    for _ in range(9):
        _send_point(cfg, level=1850, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 5. ПОЇЗДКА 3 (Від'їзд зі стоянки зливу, закриває сесію зливу):
    # Тривалість: ~65 с, дистанція > 1.5 км
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 5: Від'їзд зі стоянки зливу (закриває сесію зливу в M2M)...")
    drain_depart_points = [
        (51.206028, 23.927141, 35, 1850),
        (51.205265, 23.930920, 55, 1852),
        (51.204517, 23.932686, 75, 1848),
        (51.202877, 23.936026, 80, 1850),
    ]
    for lat, lon, spd, lvl in drain_depart_points:
        _send_point(cfg, level=lvl, lat=lat, lon=lon, speed=spd)
        time.sleep(16)

    log.info("Усі точки в реальному часі відправлено успішно!")
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
        dev_name = str(item.get("deviceName") or "").strip()
        unique_id = str(item.get("uniqueId") or "").strip()
        
        raw_type = str(item.get("type", "")).upper()
        template_type = str(item.get("templateType", "")).upper()
        template_name = str(item.get("templateName", "")).lower()
        message = str(item.get("message", "")).lower()

        # СУВОРЕ ВІДСІКАННЯ ЧУЖИХ СЦЕНАРІЇВ:
        # Для Single обов'язково відкидаємо події з Double або Prod!
        if (
            "double" in dev_name.lower()
            or dev_name.lower().endswith("_double")
            or "double" in template_name
            or "сценарій 2" in template_name
            or "refill_drain_unit_double" in message
            or "test_notification_o.rosenko" in dev_name.lower()
        ):
            continue

        # 1. Прив'язка до об'єкта (строгий exact match або унікальний шаблон Single):
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
            and "double" not in template_name
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
                refills_found.append({"id": event_id, "volume": volume, "time": ev_time})
        elif (
            raw_type in ("DRAIN",)
            or template_type in ("DRAIN",)
            or target_drain in template_name
            or target_drain in item_lower
            or "злив" in template_name
            or "злив" in message
        ):
            if not any(d["id"] == event_id for d in drains_found):
                drains_found.append({"id": event_id, "volume": volume, "time": ev_time})

    refills_found.sort(key=lambda x: x["id"])
    drains_found.sort(key=lambda x: x["id"])

    report_lines = []
    if len(refills_found) >= 1:
        t_str = f" (час: {refills_found[0]['time']})" if refills_found[0].get("time") else ""
        vol = refills_found[0].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка")

    if len(drains_found) >= 1:
        t_str = f" (час: {drains_found[0]['time']})" if drains_found[0].get("time") else ""
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


def notify_incident(cfg: dict, detail: str) -> None:
    text = f"🔴 [DEV] [Teltonika Single] Не спрацювали сповіщення пального!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict, detail: str) -> None:
    text = f"🟢 [DEV] [Teltonika Single] Сповіщення пального відновилися!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [DEV] [Teltonika Single] Заправка та Злив працюють коректно!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Single для %s (IMEI: %s)...", cfg["device_name"], cfg["tracker_imei"])
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
