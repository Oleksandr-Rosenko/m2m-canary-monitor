#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень M2M.
СЦЕНАРІЙ 2: ПОДВІЙНА ЗАПРАВКА НА ОДНІЙ СТОЯНЦІ ТА ЗЛИВ (Double Refill + Drain)
ТЕСТ НА ПРОДАКШЕН-ОБ'ЄКТІ: Test_Notification_o.rosenko (IMEI: 123456789011111)
СЕРВЕР: 213.239.234.94:5027 (hw.m2m.eu)

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
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_double_prod")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

CONFIG = {
    "device_id": 9405,
    "device_name": "Test_Notification_o.rosenko",
    "tracker_imei": "123456789011111",
    
    "ingest_host": "213.239.234.94",
    "ingest_port": 5027,
    
    "template_refill": "TEST_Notification-Rosenko_Refill",
    "template_drain": "TEST_Notification-Rosenko_Drain",
    
    "geo_lat": 49.438923,
    "geo_lon": 32.085565,
    "altitude": 10,
    "bearing": 210, 
    "sat": 12,      

    "api_base_url": "https://my.m2m.eu/api",
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
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_double_prod_state.json")),
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
        _send_point(cfg, level=400, lat=lat, lon=lon, speed=35)
        time.sleep(16)

    # -------------------------------------------------------------
    # 2. СТОЯНКА 1 (Подвійна заправка): тривалість 525 с (8.75 хв > 300 с!)
    # Швидкість 0, рух 0, запалювання 1, координати константні!
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 2: Стоянка під подвійну заправку (525 с > 300 с для Детектора поїздок)...")
    # Базова полиця: 30 с (2 точки по 15 с)
    for _ in range(2):
        _send_point(cfg, level=400, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ЗАПРАВКА #1: Старт на t=30 с. Зростання 400 -> 550 л (+150 л) за 75 с (5 точок по 15 с)
    log.info(">>> ЕТАП 2.1: СТАРТ Заправки #1 (з 400 л до 550 л)...")
    stg1_steps = [430, 460, 490, 520, 550]
    for lvl in stg1_steps:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ПРОМІЖНА ПОЛИЦЯ / ПАУЗА: триває з t=105 с до t=390 с (285 с = 4.75 хвилини, 19 точок по 15 с)
    # Забезпечує РІВНО 6 ХВИЛИН (360 с) між стартом #1 (t=30 с) і стартом #2 (t=390 с)!
    log.info(">>> ЕТАП 2.2: Проміжна полиця на 550 л (пауза до настання 6-ї хвилини)...")
    for i in range(1, 20):
        _send_point(cfg, level=550, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ЗАПРАВКА #2: Старт на t=390 с (рівно 6 хвилин після старту #1!). Зростання 550 -> 700 л (+150 л) за 60 с (4 точки по 15 с)
    log.info(">>> ЕТАП 2.3: СТАРТ Заправки #2 (РІВНО ЧЕРЕЗ 6 ХВИЛИН після першої!)...")
    stg2_steps = [587, 625, 662, 700]
    for lvl in stg2_steps:
        _send_point(cfg, level=lvl, lat=lat_gas, lon=lon_gas, speed=0)
        time.sleep(15)

    # ФІНАЛЬНЕ ПЛАТО ПІСЛЯ ЗАПРАВОК: 75 с (5 точок по 15 с)
    log.info(">>> ЕТАП 2.4: Фінальне плато після заправок (700 л)...")
    for _ in range(5):
        _send_point(cfg, level=700, lat=lat_gas, lon=lon_gas, speed=0)
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
        _send_point(cfg, level=700, lat=lat, lon=lon, speed=35)
        time.sleep(16)

    # -------------------------------------------------------------
    # 4. СТОЯНКА 2 (Злив): тривалість 315 с (5 хв 15 с > 300 с!)
    # -------------------------------------------------------------
    log.info(">>> ЕТАП 4: Стоянка під злив (315 с > 300 с)...")
    for _ in range(2):
        _send_point(cfg, level=700, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # Злив пального (-300 л): з 700 до 400 л за 195 с (13 точок по 15 с)
    log.info(">>> ЕТАП 4.1: Плавний злив (-300 л)...")
    drain_step = 300.0 / 13
    for i in range(1, 14):
        lvl = 700 - drain_step * i
        _send_point(cfg, level=lvl, lat=lat_drain, lon=lon_drain, speed=0)
        time.sleep(15)

    # Плато після зливу: 90 с (6 точок по 15 с)
    log.info(">>> ЕТАП 4.2: Плато після зливу (400 л)...")
    for _ in range(6):
        _send_point(cfg, level=400, lat=lat_drain, lon=lon_drain, speed=0)
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
        _send_point(cfg, level=400, lat=lat, lon=lon, speed=35)
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
        if resp.status_code == 401:
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


def get_existing_event_ids(cfg: dict) -> set:
    try:
        data = fetch_history(cfg)
        items = data.get("items") or data.get("data") or []
        return {item["id"] for item in items if isinstance(item, dict) and "id" in item}
    except Exception:
        return set()


def check_events_in_history(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    data = fetch_history(cfg)
    items = data.get("items") or data.get("data") or []
    
    target_name = cfg.get("device_name", "")
    target_id = cfg.get("device_id")
    target_imei = str(cfg.get("tracker_imei", ""))
    target_refill = cfg["template_refill"].lower().rstrip(":")
    target_drain = cfg["template_drain"].lower().rstrip(":")

    refills_found = []
    drains_found = []

    for item in items:
        if not isinstance(item, dict): 
            continue
        event_id = item.get("id")
        if event_id in initial_event_ids: 
            continue

        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        dev_name = str(item.get("deviceName") or "")
        dev_id = item.get("deviceId")
        unique_id = str(item.get("uniqueId") or "")
        
        # 1. Прив'язка до тестового прод-об'єкта
        matched = False
        if target_id and dev_id == target_id:
            matched = True
        elif target_imei and (unique_id == target_imei or target_imei in item_str):
            matched = True
        elif target_name and (
            target_name.lower() in dev_name.lower()
            or target_name.lower() in str(item.get("message", "")).lower()
            or target_name.lower() in item_lower
        ):
            matched = True

        if not matched:
            continue

        raw_type = str(item.get("type", "")).upper()
        template_type = str(item.get("templateType", "")).upper()
        template_name = str(item.get("templateName", "")).lower()
        message = str(item.get("message", "")).lower()
        
        # 2. Витягуємо об'єм пального
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

        vol_str = f" (+{volume} л)" if volume is not None else ""
        vol_drain_str = f" (-{volume} л)" if volume is not None else ""

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
                refills_found.append({"id": event_id, "vol_str": vol_str})
        elif (
            raw_type in ("DRAIN",)
            or template_type in ("DRAIN",)
            or target_drain in template_name
            or target_drain in item_lower
            or "злив" in template_name
            or "злив" in message
        ):
            if not any(d["id"] == event_id for d in drains_found):
                drains_found.append({"id": event_id, "vol_str": vol_drain_str})

    ok_refill = len(refills_found) >= 1
    ok_drain = len(drains_found) >= 1

    report_lines = []
    if len(refills_found) >= 2:
        report_lines.append(f"✅ Заправка 1{refills_found[0]['vol_str']}")
        report_lines.append(f"✅ Заправка 2{refills_found[1]['vol_str']}")
    elif len(refills_found) == 1:
        report_lines.append(f"✅ Заправка (згрупована){refills_found[0]['vol_str']}")
    else:
        report_lines.append("❌ Подвійна заправка")

    if ok_drain:
        report_lines.append(f"✅ Злив{drains_found[0]['vol_str']}")
    else:
        report_lines.append("❌ Злив")

    all_ok = ok_refill and ok_drain
    return all_ok, "\n".join(report_lines)


def wait_for_events(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    log.info("Початкова пауза (%d с) перед опитуванням історії...", cfg["initial_wait_sec"])
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    start_poll = time.monotonic()
    
    while True:
        ok, detail = check_events_in_history(cfg, start_time, initial_event_ids)
        if ok: 
            elapsed = int(time.monotonic() - start_poll)
            log.info("Усі очікувані сповіщення успішно знайдено (через %d с від початку опитування)!\n%s", elapsed, detail)
            return True, detail
        if time.monotonic() >= deadline: 
            return False, f"Таймаут очікування сповіщень ({cfg['max_wait_sec']} с)\n\n{detail}"
            
        elapsed = int(time.monotonic() - start_poll)
        log.info("Опитування історії API (пройшло %d/%d с)...", elapsed, cfg["max_wait_sec"])
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
    text = f"🔴 [Teltonika Double Prod Test] Не спрацювали сповіщення пального!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict) -> None:
    text = f"🟢 [Teltonika Double Prod Test] Сповіщення пального відновилися!\nОб'єкт: {cfg['device_name']}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [Teltonika Double Prod Test] Подвійна заправка та Злив на проді працюють коректно!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Double Prod Test для %s (IMEI: %s, Server: %s:%d)...", 
             cfg["device_name"], cfg["tracker_imei"], cfg["ingest_host"], cfg["ingest_port"])
    resolve_device_id(cfg)
    initial_ids = get_existing_event_ids(cfg)
    
    try:
        start = send_canary_events(cfg)
    except Exception as e:
        notify_incident(cfg, f"Помилка відправки точок по TCP: {e}")
        return 1

    ok, detail = wait_for_events(cfg, start, initial_ids)
    if ok:
        log.info("Успіх!\n%s", detail)
        if state.get("incident_open"): 
            notify_recovery(cfg)
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
