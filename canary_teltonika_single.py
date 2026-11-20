#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень M2M.
СЦЕНАРІЙ 1: ОДИНОЧНА ЗАПРАВКА ТА ЗЛИВ (Single Refill + Drain)
ПРОТОКОЛ: TELTONIKA (Codec 8)
Об'єкт: Refill_Drain_UNIT (IMEI: 12345678901234567890)
Сервер: 95.217.119.27:5027

Вимоги детектора поїздок M2M:
- Мін. час стоянки: 300 с (стоянки зроблені по 12-15 хв!)
- Мін. час поїздки: 60 с (поїздки по 2 хв!)
- Мін. дистанція поїздки: 100 м (дистанція 600-800 м!)
- Мін. швидкість: 1 км/год (швидкість 30-36 км/год!)
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
    "device_id": None,
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

    "initial_wait_sec": 30,
    "poll_interval_sec": 15,
    "max_wait_sec": 300,
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


def build_codec8_packet(cfg: dict, pt: dict) -> bytearray:
    speed = int(pt["speed"])
    ignition = int(pt["ignition"])
    level = int(pt["level"])
    lat = float(pt["lat"])
    lon = float(pt["lon"])
    timestamp_ms = int(pt["timestamp_ms"])

    avl_data = struct.pack(
        ">Q b i i h h B h", 
        timestamp_ms, 0, 
        int(lon * 10000000), int(lat * 10000000), 
        cfg["altitude"], cfg["bearing"], cfg["sat"], speed
    )
    
    io_data = bytearray()
    io_data.append(0)  # Event IO ID
    io_data.append(3)  # Total IO elements: 3
    
    # 1-byte IOs: Movement (240) and Ignition (239)
    io_data.append(2)
    io_data.append(240)
    io_data.append(1 if speed > 0 else 0)
    io_data.append(239)
    io_data.append(1 if ignition else 0)
    
    # 2-byte IOs: Fuel level (201)
    io_data.append(1)
    io_data.append(201)
    io_data.extend(struct.pack(">h", level))
    
    # 4-byte and 8-byte IOs: 0
    io_data.append(0)
    io_data.append(0)
    
    data_part8 = bytearray([0x08, 1]) + avl_data + io_data + bytearray([1])
    crc8 = crc16_arc(data_part8)
    return bytearray(b'\x00\x00\x00\x00') + struct.pack(">I", len(data_part8)) + data_part8 + struct.pack(">I", crc8)


def generate_single_points(cfg: dict) -> list[dict]:
    """
    Формує реалістичну хронологію точок із повною відповідністю Детектору поїздок M2M:
    - Поїздки > 60 сек, відстань > 100 м, швидкість > 1 км/год
    - Стоянки > 300 сек (12-15 хвилин кожна)
    - Плавний налив (+300 л) та плавний злив (-250 л)
    Кінцева точка збігається з поточним часом (now).
    """
    timeline = []
    
    # Початкові координати першої стоянки (АЗС)
    lat_gas, lon_gas = cfg["geo_lat"], cfg["geo_lon"]
    # Координати другої стоянки (майданчик зливу)
    lat_drain, lon_drain = lat_gas + 0.009, lon_gas + 0.009  # ~1 км зміщення

    # 1. ПОЇЗДКА 1 (Під'їзд до АЗС): тривалість 120 с, дистанція ~600 м
    t = 0
    timeline.append({"t": t, "spd": 35, "ign": 1, "lat": lat_gas - 0.006, "lon": lon_gas - 0.006, "lvl": 150})
    t += 60
    timeline.append({"t": t, "spd": 30, "ign": 1, "lat": lat_gas - 0.003, "lon": lon_gas - 0.003, "lvl": 150})
    t += 60

    # 2. СТОЯНКА 1 (АЗС, Заправка): тривалість 900 с (15 ХВИЛИН!)
    # Зупинка та глушіння двигуна
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_gas, "lon": lon_gas, "lvl": 150})
    t += 120
    # Базова полиця
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_gas, "lon": lon_gas, "lvl": 150})
    
    # Налив пального (+300 л) протягом 600 с (10 хвилин, ~30 л/хв)
    for step in range(1, 6):
        t += 120
        lvl = 150 + int(60 * step)  # 210, 270, 330, 390, 450
        timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_gas, "lon": lon_gas, "lvl": lvl})
    
    # Плато стабілізації після заправки (180 с = 3 хвилини)
    t += 90
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_gas, "lon": lon_gas, "lvl": 450})
    t += 90
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_gas, "lon": lon_gas, "lvl": 450})

    # 3. ПОЇЗДКА 2 (Між АЗС та майданчиком): тривалість 120 с, дистанція ~1000 м
    t += 10
    timeline.append({"t": t, "spd": 0, "ign": 1, "lat": lat_gas, "lon": lon_gas, "lvl": 450})  # запуск мотора
    t += 50
    timeline.append({"t": t, "spd": 35, "ign": 1, "lat": lat_gas + 0.003, "lon": lon_gas + 0.003, "lvl": 450})
    t += 60
    timeline.append({"t": t, "spd": 30, "ign": 1, "lat": lat_gas + 0.006, "lon": lon_gas + 0.006, "lvl": 450})
    t += 60

    # 4. СТОЯНКА 2 (Майданчик, Злив): тривалість 720 с (12 ХВИЛИН!)
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_drain, "lon": lon_drain, "lvl": 450})
    t += 120
    # Базова полиця
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_drain, "lon": lon_drain, "lvl": 450})
    
    # Злив пального (-250 л) протягом 360 с (6 хвилин, ~40 л/хв)
    drops = [400, 350, 300, 250, 200]
    for lvl in drops:
        t += 72
        timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_drain, "lon": lon_drain, "lvl": lvl})

    # Плато стабілізації після зливу (240 с = 4 хвилини)
    t += 120
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_drain, "lon": lon_drain, "lvl": 200})
    t += 120
    timeline.append({"t": t, "spd": 0, "ign": 0, "lat": lat_drain, "lon": lon_drain, "lvl": 200})

    # 5. ПОЇЗДКА 3 (Виїзд на маршрут): тривалість 120 с, дистанція ~600 м
    t += 10
    timeline.append({"t": t, "spd": 0, "ign": 1, "lat": lat_drain, "lon": lon_drain, "lvl": 200})
    t += 50
    timeline.append({"t": t, "spd": 35, "ign": 1, "lat": lat_drain + 0.003, "lon": lon_drain + 0.003, "lvl": 200})
    t += 60
    timeline.append({"t": t, "spd": 40, "ign": 1, "lat": lat_drain + 0.006, "lon": lon_drain + 0.006, "lvl": 200})

    # Перетворюємо відносні секунди 't' на абсолютні UTC timestamp_ms так,
    # щоб остання точка припадала на поточний момент (now)!
    now_ms = int(time.time() * 1000)
    total_duration_ms = t * 1000
    start_ms = now_ms - total_duration_ms

    points = []
    for item in timeline:
        pt_ms = start_ms + (item["t"] * 1000)
        points.append({
            "timestamp_ms": pt_ms,
            "speed": item["spd"],
            "ignition": item["ign"],
            "lat": item["lat"],
            "lon": item["lon"],
            "level": item["lvl"],
        })

    return points


def send_points_stream(cfg: dict, points: list[dict]) -> None:
    host, port, imei = cfg["ingest_host"], cfg["ingest_port"], cfg["tracker_imei"]
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        try:
            log.info("Підключення до %s:%d (спроба %d/%d)...", host, port, attempt, max_retries)
            with socket.create_connection((host, port), timeout=cfg["http_timeout_sec"]) as sock:
                imei_b = imei.encode('ascii')
                sock.sendall(struct.pack(">H", len(imei_b)) + imei_b)
                resp = sock.recv(1)
                if not resp or resp[0] != 1:
                    raise OSError(f"Handshake не підтверджено сервером: {resp}")
                
                log.info("Handshake успішний. Відправка %d точок єдиним потоком...", len(points))
                for idx, pt in enumerate(points):
                    packet = build_codec8_packet(cfg, pt)
                    sock.sendall(packet)
                    ack = sock.recv(4)
                    time.sleep(0.05)  # 50 мс між пакетами
                log.info("Усі %d точок успішно передано сервером!", len(points))
                return
        except (socket.error, OSError) as e:
            if attempt == max_retries:
                log.error("TCP Error вичерпано всі спроби: %s", e)
                raise
            delay = 3 * attempt
            log.warning("Помилка сокету (%s). Повтор через %d с...", e, delay)
            time.sleep(delay)


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    points = generate_single_points(cfg)
    send_points_stream(cfg, points)
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
        report_lines.append(f"✅ Злив{drains_found[0]}")
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
