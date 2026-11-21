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


def select_refill_variant(cfg: dict, state: dict) -> tuple[int, str]:
    """
    Визначає варіант тривалості заправки з циклічною ротацією:
    Варіант 0: до 5 хвилин (~4 хв, об'єм ~355 л)
    Варіант 1: ~7 хвилин (~7 хв, об'єм ~650 л)
    Варіант 2: 10+ хвилин (~10.5 хв, об'єм ~860 л)
    """
    gh_run = os.getenv("GITHUB_RUN_NUMBER")
    if gh_run and gh_run.isdigit():
        variant_idx = (int(gh_run) - 1) % 3
    else:
        variant_idx = state.get("refill_variant", 0) % 3
        state["refill_variant"] = (variant_idx + 1) % 3
        save_state(cfg, state)
    
    variant_names = [
        "Варіант 1 (до 5 хв, ~355 л)", 
        "Варіант 2 (~7 хв, ~650 л)", 
        "Варіант 3 (10+ хв, ~860 л)"
    ]
    return variant_idx, variant_names[variant_idx]


def send_canary_events(cfg: dict, variant_idx: int = 0) -> datetime:
    start_time = datetime.now(timezone.utc)
    
    # Координати АЗС біля М-07 (Римачі / Ягодин, Волинська обл. з реального треку):
    lat_gas, lon_gas = 51.201641, 23.892584
    # Координати другого пункту зупинки для зливу (далі по трасі М-07):
    lat_drain, lon_drain = 51.206059, 23.925201

    # Налаштування профілю заправки відповідно до обраного варіанту (об'єм строго до 1000 л)
    if variant_idx == 0:
        # Варіант 1: ДО 5 ХВИЛИН (налив ~4 хв, чистий об'єм +355 л)
        start_level = 95
        final_level = 450
        shelf_pre_count = 4    # 60 с
        phase1_levels = [165, 235, 305, 350]               # 4 точки = 60 с
        pause1_count = 2                                    # 30 с на 350
        phase2_levels = [375, 400, 420, 435, 445]          # 5 точок = 75 с
        pause2_count = 2                                    # 30 с на 445
        phase3_levels = [448, 449, 450]                    # 3 точки = 45 с
        shelf_top_count = 4    # 60 с
        # Час наливу: 60 + 30 + 75 + 30 + 45 = 240 с (4 хв 00 с < 5 хв)
        # Загальна стоянка: 60 + 240 + 60 = 360 с (6.0 хв > 300 с)
        log.info(">>> ОБРАНО ПРОФІЛЬ ЗАПРАВКИ: ВАРІАНТ 1 (до 5 хв, об'єм +355 л: 95 -> 450)...")
    elif variant_idx == 1:
        # Варіант 2: 7 ХВИЛИН (налив ~7 хв, чистий об'єм +650 л)
        start_level = 110
        final_level = 760
        shelf_pre_count = 4    # 60 с
        phase1_levels = [200, 290, 380, 460, 520, 560]     # 6 точок = 90 с
        pause1_count = 3                                    # 45 с на 560
        phase2_levels = [590, 620, 650, 680, 705, 725, 740, 750]  # 8 точок = 120 с
        pause2_count = 3                                    # 45 с на 750
        phase3_levels = [752, 754, 756, 758, 759, 760, 760, 760]  # 8 точок = 120 с
        shelf_top_count = 4    # 60 с
        # Час наливу: 90 + 45 + 120 + 45 + 120 = 420 с (7 хв 00 с)
        # Загальна стоянка: 60 + 420 + 60 = 540 с (9.0 хв > 300 с)
        log.info(">>> ОБРАНО ПРОФІЛЬ ЗАПРАВКИ: ВАРІАНТ 2 (~7 хв, об'єм +650 л: 110 -> 760)...")
    else:
        # Варіант 3: 10+ ХВИЛИН (налив ~10.5 хв, чистий об'єм +860 л)
        start_level = 85
        final_level = 945
        shelf_pre_count = 4    # 60 с
        phase1_levels = [170, 255, 340, 420, 500, 570, 630, 680, 720, 750]  # 10 точок = 150 с
        pause1_count = 4                                                     # 60 с на 750
        phase2_levels = [770, 790, 810, 830, 850, 868, 884, 898, 910, 920, 928, 935]  # 12 точок = 180 с
        pause2_count = 4                                                     # 60 с на 935
        phase3_levels = [937, 939, 941, 942, 943, 944, 944, 945, 945, 945, 945, 945]  # 12 точок = 180 с
        shelf_top_count = 4    # 60 с
        # Час наливу: 150 + 60 + 180 + 60 + 180 = 630 с (10 хв 30 с = 10+ хв)
        # Загальна стоянка: 60 + 630 + 60 = 750 с (12.5 хв > 300 с)
        log.info(">>> ОБРАНО ПРОФІЛЬ ЗАПРАВКИ: ВАРІАНТ 3 (10+ хв, об'єм +860 л: 85 -> 945)...")

    # -------------------------------------------------------------
    # 1. ПОЇЗДКА 1 (Під'їзд до АЗС по М-07): плавне гальмування
    # Тривалість: ~65 с, дистанція > 1.5 км
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
    # 2. СТОЯНКА 1 (АЗС, Реалістична східчаста заправка):
    # Тривалість > 300 с для Детектора стоянок M2M
    # Швидкість 0, рух 0, запалювання 1
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Стоянка на АЗС під заправку...")
    
    # 2.1. Передзаправна полиця:
    log.info(">>> ЕТАП 2.1: Передзаправна полиця на рівні %d л (%d с)...", start_level, shelf_pre_count * 15)
    for _ in range(shelf_pre_count):
        _send_point(cfg, level=start_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.2. Налив фаза 1:
    log.info(">>> ЕТАП 2.2: Налив фаза 1 (до %d л)...", phase1_levels[-1])
    for lvl in phase1_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.3. Пауза 1:
    log.info(">>> ЕТАП 2.3: Пауза 1 на рівні %d л (%d с)...", phase1_levels[-1], pause1_count * 15)
    for _ in range(pause1_count):
        _send_point(cfg, level=phase1_levels[-1], lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.4. Налив фаза 2:
    log.info(">>> ЕТАП 2.4: Налив фаза 2 (до %d л)...", phase2_levels[-1])
    for lvl in phase2_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.5. Пауза 2:
    log.info(">>> ЕТАП 2.5: Пауза 2 на рівні %d л (%d с)...", phase2_levels[-1], pause2_count * 15)
    for _ in range(pause2_count):
        _send_point(cfg, level=phase2_levels[-1], lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.6. Налив фаза 3 (долив):
    log.info(">>> ЕТАП 2.6: Налив фаза 3 (долив до %d л)...", final_level)
    for lvl in phase3_levels:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # 2.7. Фінальне плато стабілізації повного бака:
    log.info(">>> ЕТАП 2.7: Фінальне плато стабілізації на %d л (%d с)...", final_level, shelf_top_count * 15)
    for _ in range(shelf_top_count):
        _send_point(cfg, level=final_level, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 3. ПОЇЗДКА 2 (Виїзд з АЗС на трасу М-07, закриває стоянку заправки):
    # Тривалість: ~65 с, дистанція > 1.5 км, швидкість до 75 км/год
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
    # 4. СТОЯНКА 2 (Пункт відстою, Злив пального):
    # Загальна тривалість: ~315 с (> 300 с)
    # Злив пального на ~210 л (строго до 1000 л)
    # -------------------------------------------------------------
    drain_level = max(50, final_level - 210)
    log.info(">>> ЕТАП 4: Стоянка під злив пального (%d -> %d л, тривалість > 300 с)...", final_level, drain_level)
    
    # 4.1. Передзливна полиця: 45 с (3 точки по 15 с)
    for _ in range(3):
        _send_point(cfg, level=final_level, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.2. Плавний злив пального: 8 точок по 15 с = 120 с
    log.info(">>> ЕТАП 4.1: Плавний злив пального (%d -> %d л)...", final_level, drain_level)
    drain_step = (final_level - drain_level) / 8.0
    for i in range(1, 9):
        lvl = int(final_level - drain_step * i)
        _send_point(cfg, level=lvl, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # 4.3. Плато стабілізації після зливу: 10 точок по 15 с = 150 с
    log.info(">>> ЕТАП 4.2: Плато стабілізації після зливу (%d л)...", drain_level)
    for _ in range(10):
        _send_point(cfg, level=drain_level, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # -------------------------------------------------------------
    # 5. ПОЇЗДКА 3 (Від'їзд зі стоянки зливу, закриває сесію зливу):
    # Тривалість: ~65 с, дистанція > 1.5 км
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
    if len(refills_found) >= 1:
        t_parts = []
        if refills_found[0].get("time"):
            t_parts.append(f"час: {refills_found[0]['time']}")
        if refills_found[0].get("duration"):
            t_parts.append(f"тривалість: {refills_found[0]['duration']}")
        t_str = f" ({', '.join(t_parts)})" if t_parts else ""
        vol = refills_found[0].get("volume")
        v_str = f": +{vol:.2f} л" if isinstance(vol, (int, float)) else (f": +{vol} л" if vol is not None else "")
        report_lines.append(f"✅ Заправка{v_str}{t_str}")
    else:
        report_lines.append("❌ Заправка")

    if len(drains_found) >= 1:
        t_parts = []
        if drains_found[0].get("time"):
            t_parts.append(f"час: {drains_found[0]['time']}")
        if drains_found[0].get("duration"):
            t_parts.append(f"тривалість: {drains_found[0]['duration']}")
        t_str = f" ({', '.join(t_parts)})" if t_parts else ""
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
    
    variant_idx, variant_name = select_refill_variant(cfg, state)
    log.info("Параметри запуску: %s", variant_name)

    start_ts = time.time()
    initial_ids, max_initial_id = get_existing_event_ids(cfg)
    log.info("Фіксація старту: %d наявних сповіщень в історії (max_id=%s, start_ts=%.0f)", 
             len(initial_ids), max_initial_id, start_ts)
    
    try:
        send_canary_events(cfg, variant_idx)
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
