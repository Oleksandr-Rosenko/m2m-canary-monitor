#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень M2M.
СЦЕНАРІЙ 1: ОДИНОЧНА ЗАПРАВКА ТА ЗЛИВ (Single Refill + Drain)
ПРОТОКОЛ: TELTONIKA (Codec 8)
Об'єкт: Refill_Drain_UNIT (IMEI: 12345678901234567890)
Сервер: 95.217.119.27:5027
"""

import json
import logging
import os
import sys
import time
import socket
import struct
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
    "device_id": None,  # Визначається автоматично або через назву об'єкта
    "device_name": "Refill_Drain_UNIT",
    "tracker_imei": "12345678901234567890",
    
    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,
    
    "template_refill": "Заправка (Сценарій 1 - Single)",
    "template_drain": "Злив (Сценарій 1 - Single)",
    
    "geo_lat": 49.438923,
    "geo_lon": 32.085565,
    "altitude": 120,
    "bearing": 180, 
    "sat": 14,      

    "api_base_url": "https://my.m2m.eu/api",
    "api_email": os.getenv("M2M_EMAIL", ""),        
    "api_password": os.getenv("M2M_PASSWORD", ""),  
    "api_token": os.getenv("M2M_API_TOKEN", ""),    
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 50},

    # Параметри реалістичного профілю пального (1:1 л по ID 201)
    "baseline_level_l": 150,     # Рівень перед заправкою
    "refill_target_l": 450,      # Рівень після заправки (+300 л)
    "drain_target_l": 200,       # Рівень після зливу (-250 л)
    
    # Таймінги та кроки точок
    "point_interval_sec": 6,     # Інтервал між точками
    "baseline_points": 3,        # Точок спокою перед подією
    "ramp_points": 6,            # Точок для плавного зростання/падіння
    "plateau_points": 3,         # Точок стабілізації після події
    "trip_points": 2,            # Точок поїздки для закриття стоянки
    "trip_speed": 28,            # Швидкість руху для закриття стоянки (км/год)

    "initial_wait_sec": 30,      # Пауза перед першим опитуванням API
    "poll_interval_sec": 15,     # Інтервал опитування API
    "max_wait_sec": 300,         # Максимальний таймаут очікування сповіщень
    "http_timeout_sec": 30,  
    
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_state.json")),
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


def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0, ignition: int = 0) -> None:
    host, port, imei = cfg["ingest_host"], cfg["ingest_port"], cfg["tracker_imei"]
    timestamp_ms = int(time.time() * 1000)
    
    # Codec 8 AVL Record
    # timestamp(8), priority(1), lon(4), lat(4), altitude(2), angle(2), sat(1), speed(2)
    avl_data = struct.pack(
        ">Q b i i h h B h", 
        timestamp_ms, 0, 
        int(lon * 10000000), int(lat * 10000000), 
        cfg["altitude"], cfg["bearing"], cfg["sat"], speed
    )
    
    # IO Data
    io_data = bytearray()
    io_data.append(0)  # Event IO ID
    io_data.append(3)  # Total IO elements: 3 (Movement, Ignition, Fuel)
    
    # 1-byte IOs: 2 elements
    io_data.append(2)  
    io_data.append(240)  # Movement (IO ID 240)
    io_data.append(1 if speed > 0 else 0)
    io_data.append(239)  # Ignition (IO ID 239)
    io_data.append(1 if ignition else 0)
    
    # 2-byte IOs: 1 element
    io_data.append(1)  
    io_data.append(201)  # Fuel level (IO ID 201, LLS Fuel 1)
    io_data.extend(struct.pack(">h", int(level)))
    
    # 4-byte and 8-byte IOs: 0 elements
    io_data.append(0)
    io_data.append(0)
    
    # Codec 8 Part
    data_part8 = bytearray([0x08, 1]) + avl_data + io_data + bytearray([1])
    crc8 = crc16_arc(data_part8)
    packet_c8 = bytearray(b'\x00\x00\x00\x00') + struct.pack(">I", len(data_part8)) + data_part8 + struct.pack(">I", crc8)

    log.info("Teltonika: Fuel=%d л, Speed=%d км/год, Ignition=%d, Lat=%.6f, Lon=%.6f", 
             int(level), speed, ignition, lat, lon)

    max_retries = 3
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
                log.error("TCP Error вичерпано %d спроб: %s", max_retries, e)
                raise
            delay = 2 * attempt
            log.warning("Помилка підключення до %s:%d (спроба %d/%d). Повтор через %d с: %s", 
                        host, port, attempt, max_retries, delay, e)
            time.sleep(delay)


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    interval = cfg["point_interval_sec"]
    cur_lat, cur_lon = cfg["geo_lat"], cfg["geo_lon"]
    base_l = cfg["baseline_level_l"]
    refill_l = cfg["refill_target_l"]
    drain_l = cfg["drain_target_l"]

    # --- ЕТАП 1: ПОЧАТКОВА ПОЇЗДКА (Прибуття на АЗС) ---
    log.info(">>> ЕТАП 1: Поїздка (під'їзд до АЗС)...")
    for _ in range(cfg["trip_points"]):
        cur_lat += 0.0001
        cur_lon += 0.0001
        _send_point(cfg, level=base_l, lat=cur_lat, lon=cur_lon, speed=cfg["trip_speed"], ignition=1)
        time.sleep(interval)

    # --- ЕТАП 2: ЗУПИНКА ТА ГЛУШІННЯ ДВИГУНА (Стоянка перед заправкою) ---
    log.info(">>> ЕТАП 2: Зупинка та глушіння двигуна (Базовий рівень: %d л)...", base_l)
    for _ in range(cfg["baseline_points"]):
        _send_point(cfg, level=base_l, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 3: ОДИНОЧНА ЗАПРАВКА (Плавне зростання рівня) ---
    log.info(">>> ЕТАП 3: Заправка (+%d л)...", refill_l - base_l)
    ramp_pts = cfg["ramp_points"]
    step = (refill_l - base_l) / ramp_pts
    for i in range(1, ramp_pts + 1):
        lvl = base_l + step * i
        _send_point(cfg, level=lvl, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 4: ФІКСАЦІЯ ПІСЛЯ ЗАПРАВКИ (Плато стабілізації) ---
    log.info(">>> ЕТАП 4: Плато стабілізації заправки (%d л)...", refill_l)
    for _ in range(cfg["plateau_points"]):
        _send_point(cfg, level=refill_l, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 5: ЗАПУСК ДВИГУНА ТА ВИЇЗД (Закриття стоянки заправки) ---
    log.info(">>> ЕТАП 5: Виїзд з АЗС (закриття стоянки заправки)...")
    for _ in range(cfg["trip_points"]):
        cur_lat += 0.0001
        cur_lon += 0.0001
        _send_point(cfg, level=refill_l, lat=cur_lat, lon=cur_lon, speed=cfg["trip_speed"], ignition=1)
        time.sleep(interval)

    # --- ЕТАП 6: ЗУПИНКА НА СТОЯНКУ (Підготовка до зливу) ---
    log.info(">>> ЕТАП 6: Зупинка на стоянку (вихідний рівень: %d л)...", refill_l)
    for _ in range(cfg["baseline_points"]):
        _send_point(cfg, level=refill_l, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 7: ЗЛИВ (Плавне ступінчасте падіння рівня) ---
    log.info(">>> ЕТАП 7: Злив пального (-%d л)...", refill_l - drain_l)
    drain_step = (refill_l - drain_l) / ramp_pts
    for i in range(1, ramp_pts + 1):
        lvl = refill_l - drain_step * i
        _send_point(cfg, level=lvl, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 8: ФІКСАЦІЯ ПІСЛЯ ЗЛИВУ (Плато на нижньому рівні) ---
    log.info(">>> ЕТАП 8: Плато після зливу (%d л)...", drain_l)
    for _ in range(cfg["plateau_points"]):
        _send_point(cfg, level=drain_l, lat=cur_lat, lon=cur_lon, speed=0, ignition=0)
        time.sleep(interval)

    # --- ЕТАП 9: ЗАПУСК ДВИГУНА ТА ВІД'ЇЗД (Закриття стоянки зливу) ---
    log.info(">>> ЕТАП 9: Від'їзд зі стоянки (закриття стоянки зливу)...")
    for _ in range(cfg["trip_points"]):
        cur_lat += 0.0001
        cur_lon += 0.0001
        _send_point(cfg, level=drain_l, lat=cur_lat, lon=cur_lon, speed=cfg["trip_speed"], ignition=1)
        time.sleep(interval)

    log.info("Усі точки відправлено успішно!")
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


def get_existing_event_ids(cfg: dict) -> set:
    try: 
        return {item.get("id") for item in fetch_history(cfg).get("items", []) if item.get("id") is not None}
    except Exception: 
        return set()


def check_events_in_history(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    items = fetch_history(cfg).get("items", [])
    refills_found = []
    drains_found = []

    for item in items:
        event_id = item.get("id")
        if event_id in initial_event_ids: 
            continue
            
        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        dev_id = item.get("deviceId")
        dev_name = item.get("deviceName")
        
        target_name = cfg.get("device_name", "")
        target_id = cfg.get("device_id")
        
        matched = False
        if target_id and dev_id == target_id:
            matched = True
        elif target_name and (dev_name == target_name or target_name.lower() in item_lower):
            matched = True
            
        if not matched:
            continue
            
        raw_type = item.get("type", "").upper()
        template_name = item.get("templateName") or ""
        
        # Об'єм
        volume = None
        try:
            meta = item.get("metadata", {})
            if isinstance(meta, str): 
                meta = json.loads(meta)
            volume = meta.get("fuelVolume")
            if volume is None:
                finish = meta.get("fuelFinishValue")
                start_val = meta.get("fuelStartValue")
                if isinstance(finish, (int, float)) and isinstance(start_val, (int, float)):
                    volume = round(abs(finish - start_val), 2)
        except Exception:
            pass

        vol_str = f" (+{volume} л)" if volume is not None else ""
        vol_drain_str = f" (-{volume} л)" if volume is not None else ""

        target_refill = cfg["template_refill"].lower()
        target_drain = cfg["template_drain"].lower()

        if (
            target_refill in template_name.lower() 
            or target_refill in item_lower 
            or (raw_type in ("REFILL", "REFIL") and "single" in (dev_name or "").lower())
        ):
            refills_found.append(f"Заправка{vol_str}")
        elif (
            target_drain in template_name.lower() 
            or target_drain in item_lower 
            or (raw_type == "DRAIN" and "single" in (dev_name or "").lower())
        ):
            drains_found.append(f"Злив{vol_drain_str}")

    ok_refill = len(refills_found) >= 1
    ok_drain = len(drains_found) >= 1

    report_lines = []
    if ok_refill:
        report_lines.append(f"✅ {refills_found[0]}")
    else:
        report_lines.append("❌ Заправка")

    if ok_drain:
        report_lines.append(f"✅ {drains_found[0]}")
    else:
        report_lines.append("❌ Злив")

    all_ok = ok_refill and ok_drain
    return all_ok, "\n".join(report_lines)


def wait_for_events(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    log.info("Очікування сповіщень (%d с)...", cfg["initial_wait_sec"])
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    while True:
        ok, detail = check_events_in_history(cfg, start_time, initial_event_ids)
        if ok: 
            return True, detail
        if time.monotonic() >= deadline: 
            return False, f"Таймаут очікування сповіщень\n\n{detail}"
        log.info("Опитування історії API...")
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
    text = f"🔴 [Teltonika Single] Не спрацювали сповіщення пального!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_recovery(cfg: dict) -> None:
    text = f"🟢 [Teltonika Single] Сповіщення пального відновилися!\nОб'єкт: {cfg['device_name']}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [Teltonika Single] Заправка та Злив працюють коректно!\nОб'єкт: {cfg['device_name']}\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)


def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    log.info("Запуск Canary Teltonika Single для %s (IMEI: %s)...", cfg["device_name"], cfg["tracker_imei"])
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
