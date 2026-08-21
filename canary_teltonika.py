#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень.
ПРОТОКОЛ: TELTONIKA (Codec 8 + Codec 12)
"""

import ctypes
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

CONFIG = {
    "device_id": 9405,
    "device_name": "Test_Notification_o.rosenko",
    
    # Шаблони
    "template_refill": "TEST_Notification-Rosenko_Refill",
    "template_drain": "TEST_Notification-Rosenko_Drain",
    "template_geo_in": "TEST_Notification-Rosenko_GEOFENCE_IN",
    "template_geo_out": "TEST_Notification-Rosenko_GEOFENCE_OUT",
    "template_overspeed": "TEST_Notification-Rosenko_Overspeed",
    "template_sensor": "TEST_Notification-Rosenko_SensorValue",
    "template_idle": "TEST_Notification-Rosenko_Idle",
    "template_command": "Результат команди",  
    
    "tracker_imei": "123456789011111",
    
    # КООРДИНАТИ
    "geo_in_lat": 49.438923,
    "geo_in_lon": 32.085565,
    "geo_out_lat": 49.430000,
    "geo_out_lon": 32.085565,

    "altitude": 10,
    "bearing": 210, 
    "sat": 12,      

    # --- TELTONIKA ПАРАМЕТРИ ---
    "ingest_host": "213.239.234.94",
    "ingest_port": 5027,
    
    # --- ПАРАМЕТРИ ПЛАТФОРМНОГО API ---
    "api_base_url": "https://my.m2m.eu/api",
    "api_email": os.getenv("M2M_EMAIL", ""),        
    "api_password": os.getenv("M2M_PASSWORD", ""),  
    "api_token": os.getenv("M2M_API_TOKEN", ""),    
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 50},

    # --- ПАРАМЕТРИ ПАЛЬНОГО ---
    "start_level_l": 400,
    "end_level_l": 700,
    "num_points": 15,                 
    "point_interval_sec": 5,          
    "edge_repeats": 3,                
    "edge_repeat_interval_sec": 5,    

    # --- ПАРАМЕТРИ ГЕОЗОН ---
    "geo_cycles": 2,          
    "geo_points_count": 3,    
    "geo_wait_sec": 60,      

    # --- ПАРАМЕТРИ ШВИДКІСТЬ/СЕНСОР ---
    "overspeed_value": 15,         
    "sensor_trigger_value": 750,   
    
    # --- ПАРАМЕТРИ ПРОСТОЮ (IDLE) ---
    "idle_points_count": 12,       
    "idle_interval_sec": 30,

    # --- ПАРАМЕТРИ ЗЛИВУ (DRAIN) ---
    "drain_target_value": 350,       
    "drain_interval_sec": 15,      

    "initial_wait_sec": 120,   
    "poll_interval_sec": 30,   
    "max_wait_sec": 900,      
    "http_timeout_sec": 20,

    "desktop_popup": True,   
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),

    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_state.json")),
}


# --------------------------------------------------------------------------
# 1. Відправка тестових даних (TCP / TELTONIKA)
# --------------------------------------------------------------------------

def crc16_arc(data: bytes) -> int:
    """Вирахування Teltonika CRC-16 (ARC)."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0, result_param: str = None) -> None:
    host = cfg["ingest_host"]
    port = cfg["ingest_port"]
    imei = cfg["tracker_imei"]
    
    timestamp_ms = int(time.time() * 1000)
    
    # КОНСТРУКТОР CODEC 8 (Телеметрія)
    priority = 0
    lon_i = int(lon * 10000000)
    lat_i = int(lat * 10000000)
    alt = cfg["altitude"]
    angle = cfg["bearing"]
    sat = cfg["sat"]
    
    # Pack Basic AVL
    # Q=8 bytes, b=1 byte, i=4 bytes, h=2 bytes, B=1 byte
    avl_data = struct.pack(">Q b i i h h B h", timestamp_ms, priority, lon_i, lat_i, alt, angle, sat, speed)
    
    # IO Data
    io_data = bytearray()
    io_data.append(0)  # Event IO ID (0 = no event)
    io_data.append(2)  # Total IO elements
    
    # 1-byte elements (Motion ID 240)
    io_data.append(1)  # count
    io_data.append(240) # ID
    io_data.append(1 if speed > 0 else 0)
    
    # 2-byte elements (Fuel ID 201 - LLS 1)
    io_data.append(1)  # count
    io_data.append(201) # ID
    io_data.extend(struct.pack(">h", int(level))) # Signed 2-byte fuel
    
    # 4-byte and 8-byte
    io_data.append(0)
    io_data.append(0)
    
    record = avl_data + io_data
    
    data_part8 = bytearray()
    data_part8.append(0x08) # Codec 8
    data_part8.append(1)    # 1 Record
    data_part8.extend(record)
    data_part8.append(1)    # 1 Record
    
    crc8 = crc16_arc(data_part8)
    
    packet_c8 = bytearray()
    packet_c8.extend(b'\x00\x00\x00\x00') # Preamble
    packet_c8.extend(struct.pack(">I", len(data_part8))) # Length
    packet_c8.extend(data_part8)
    packet_c8.extend(struct.pack(">I", crc8)) # CRC
    
    # КОНСТРУКТОР CODEC 12 (Відповідь на команду)
    packet_c12 = None
    if result_param:
        res_bytes = result_param.encode('utf-8')
        data_part12 = bytearray()
        data_part12.append(0x0C) # Codec 12
        data_part12.append(1)    # Num commands
        data_part12.append(0x06) # Type: Response
        data_part12.extend(struct.pack(">I", len(res_bytes))) # String length
        data_part12.extend(res_bytes) # String
        data_part12.append(1)    # Num commands
        
        crc12 = crc16_arc(data_part12)
        
        packet_c12 = bytearray()
        packet_c12.extend(b'\x00\x00\x00\x00')
        packet_c12.extend(struct.pack(">I", len(data_part12)))
        packet_c12.extend(data_part12)
        packet_c12.extend(struct.pack(">I", crc12))

    log.info(f"Teltonika: Fuel (ID201)={int(level)} л, Speed (ID240)={speed} км/год. Sending Binary Codec 8...")

    try:
        with socket.create_connection((host, port), timeout=cfg["http_timeout_sec"]) as sock:
            # Handshake
            imei_b = imei.encode('ascii')
            handshake = struct.pack(">H", len(imei_b)) + imei_b
            sock.sendall(handshake)
            
            resp = sock.recv(1)
            if not resp or resp[0] != 1:
                log.warning(f"Teltonika Handshake failed! Server returned: {resp}")
                return
                
            # Send telemetry
            sock.sendall(packet_c8)
            sock.recv(4) # Ack 1 record
            
            # Send command response if needed
            if packet_c12:
                log.info(f"Teltonika: Sending Codec 12 (Command Response) -> '{result_param}'")
                sock.sendall(packet_c12)
                try:
                    # Чекаємо відповідь на Codec 12 всього 3 сек (щоб не вішати скрипт)
                    sock.settimeout(3.0) 
                    sock.recv(4)
                except (socket.timeout, TimeoutError):
                    log.warning("Teltonika: Сервер промовчав на Codec 12 (це нормально, йдемо далі)")
                finally:
                    sock.settimeout(cfg["http_timeout_sec"]) # Повертаємо стандартний таймаут
                
    except Exception as e:
        log.error(f"TCP Socket Error (Teltonika): {e}")
        raise


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    lo, hi = cfg["start_level_l"], cfg["end_level_l"]
    
    in_lat, in_lon = cfg["geo_in_lat"], cfg["geo_in_lon"]
    out_lat, out_lon = cfg["geo_out_lat"], cfg["geo_out_lon"]

    log.info("--- ФАЗА 1/6: ПАЛЬНЕ (ЗАПРАВКА) ---")
    log.info("Стабілізація старту (%s л)", lo)
    for i in range(cfg["edge_repeats"]):
        _send_point(cfg, lo, in_lat, in_lon)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("Швидка заправка %s -> %s л", lo, hi)
    step = (hi - lo) / (cfg["num_points"] - 1) if cfg["num_points"] > 1 else 0
    for i in range(cfg["num_points"]):
        level = lo + step * i
        _send_point(cfg, level, in_lat, in_lon)
        time.sleep(cfg["point_interval_sec"])

    log.info("Стабілізація фінішу (%s л)", hi)
    for i in range(cfg["edge_repeats"]):
        _send_point(cfg, hi, in_lat, in_lon)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("--- ФАЗА 2/6: ГЕОЗОНИ ---")
    for cycle in range(cfg["geo_cycles"]):
        log.info("Відправка ВИХОДУ (GEO_OUT)")
        for i in range(cfg["geo_points_count"]):
            _send_point(cfg, hi, out_lat, out_lon)
            time.sleep(5)
                
        time.sleep(cfg["geo_wait_sec"])

        log.info("Відправка ВХОДУ (GEO_IN)")
        for i in range(cfg["geo_points_count"]):
            _send_point(cfg, hi, in_lat, in_lon)
            time.sleep(5)
                
        if cycle < cfg["geo_cycles"] - 1:
            time.sleep(cfg["geo_wait_sec"])

    log.info("--- ФАЗА 3/6: ШВИДКІСТЬ (OVERSPEED) ---")
    current_lat, current_lon = in_lat, in_lon
    
    for cycle in range(2):
        log.info(f"Рух: швидкість {cfg['overspeed_value']} км/год")
        for i in range(3):
            current_lat += 0.0002 
            current_lon += 0.0002
            _send_point(cfg, hi, current_lat, current_lon, speed=cfg["overspeed_value"])
            time.sleep(5)
            
        log.info("Зупинка: швидкість 0 км/год")
        for i in range(3):
            _send_point(cfg, hi, current_lat, current_lon, speed=0)
            time.sleep(5)

    log.info("--- ФАЗА 4/6: ЗНАЧЕННЯ ДАТЧИКА ---")
    log.info(f"Відправка рівня пального {cfg['sensor_trigger_value']} л (Тригер 701-801)")
    for i in range(3):
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0)
        time.sleep(5)

    log.info("--- ФАЗА 5/6: ПРОСТІЙ ТА КОМАНДА ---")
    log.info("1. Рух для скидання таймера стоянки...")
    current_lat += 0.0002
    current_lon += 0.0002
    _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=cfg["overspeed_value"])
    time.sleep(10)

    log.info(f"2. Стоянка 5.5 хвилин для тригеру (>5 хв). Відправка {cfg['idle_points_count']} точок кожні {cfg['idle_interval_sec']} сек...")
    for i in range(cfg["idle_points_count"]):
        if i == 0:
            log.info("   -> Додаємо параметр result=CommandSentSuccess у першу точку простою (через Codec 12)")
            res_val = "CommandSentSuccess"
        else:
            res_val = None
            
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0, result_param=res_val)
        
        if i < cfg["idle_points_count"] - 1:
            time.sleep(cfg["idle_interval_sec"])

    log.info("--- ФАЗА 6/6: ЗЛИВ (DRAIN) ---")
    start_drain = cfg["sensor_trigger_value"]
    end_drain = cfg["drain_target_value"]
    
    drain_levels = [
        start_drain - 80,  
        start_drain - 160, 
        start_drain - 240, 
        start_drain - 300, 
        start_drain - 340, 
        start_drain - 370, 
        end_drain          
    ]
    
    log.info(f"Стоянка (швидкість 0) + Плавний злив {start_drain} -> {end_drain} л (точок: {len(drain_levels)}, інтервал: {cfg['drain_interval_sec']}с)...")
    
    for current_level in drain_levels:
        _send_point(cfg, current_level, current_lat, current_lon, speed=0)
        time.sleep(cfg["drain_interval_sec"])

    log.info(f"Стабілізація після зливу (плато на рівні {end_drain} л)...")
    for i in range(cfg["edge_repeats"]):
        _send_point(cfg, end_drain, current_lat, current_lon, speed=0)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("Весь профіль повністю відправлено!")
    return start_time


# --------------------------------------------------------------------------
# 2. Робота з API та перевірка історії
# --------------------------------------------------------------------------

def refresh_api_token(cfg: dict) -> None:
    if not cfg["api_email"] or not cfg["api_password"]:
        raise ValueError("Відсутні облікові дані (M2M_EMAIL, M2M_PASSWORD) для автооновлення токена.")

    url = f"{cfg['api_base_url']}{cfg['login_path']}"
    payload = {
        "email": cfg["api_email"],
        "password": cfg["api_password"]
    }
    
    log.info("Спроба отримання нового API-токена (авторизація)...")
    resp = requests.post(url, json=payload, timeout=cfg["http_timeout_sec"])
    
    if resp.status_code != 200:
        raise PermissionError(f"Помилка авторизації! Статус: {resp.status_code}. Перевірте логін та пароль.")
        
    data = resp.json()
    new_token = data.get("token")
    
    if not new_token:
        raise ValueError("Токен не знайдено у відповіді сервера.")

    cfg["api_token"] = new_token
    log.info("API-токен успішно отримано/оновлено!")


def fetch_history(cfg: dict) -> dict:
    if not cfg["api_token"]:
        refresh_api_token(cfg)

    url = f"{cfg['api_base_url']}{cfg['history_path']}"
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    
    resp = requests.get(url, headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
    
    if resp.status_code == 401:
        log.warning("API-токен прострочений (401). Виконую автооновлення...")
        refresh_api_token(cfg)
        headers = {"Authorization": f"Bearer {cfg['api_token']}"}
        resp = requests.get(url, headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
        
    resp.raise_for_status()
    return resp.json()


def get_existing_event_ids(cfg: dict) -> set:
    try:
        data = fetch_history(cfg)
        return {item.get("id") for item in data.get("items", []) if item.get("id") is not None}
    except PermissionError as e:
        raise e
    except Exception as e:
        log.warning(f"Не вдалося отримати початковий стан історії: {e}")
        return set()


def check_events_in_history(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    log.info("Перевіряю історію: %s%s", cfg["api_base_url"], cfg["history_path"])
    data = fetch_history(cfg)
    items = data.get("items", [])

    status = {
        "REFILL": False,
        "DRAIN": False,
        "GEO_IN": False,
        "GEO_OUT": False,
        "OVERSPEED": False,
        "SENSOR": False,
        "IDLE": False,
        "COMMAND": False
    }
    refill_volume_str = ""
    drain_volume_str = ""

    for item in items:
        event_id = item.get("id")
        if event_id in initial_event_ids:
            continue

        item_str = json.dumps(item, ensure_ascii=False)
        item_lower = item_str.lower()
        
        match_id = (item.get("deviceId") == cfg["device_id"])
        match_dev_name = (item.get("deviceName") == cfg["device_name"])
        
        if not (match_id or match_dev_name or cfg["device_name"].lower() in item_lower):
            continue
            
        raw_type = item.get("type", "").upper()
        template_name = item.get("templateName") or ""
        
        ev_type = None
        
        if raw_type in ("REFILL", "REFIL") or cfg["template_refill"] in template_name or cfg["template_refill"] in item_str:
            ev_type = "REFILL"
        elif raw_type == "DRAIN" or cfg["template_drain"] in template_name or cfg["template_drain"] in item_str:
            ev_type = "DRAIN"
        elif cfg["template_geo_in"] in template_name or cfg["template_geo_in"] in item_str or (raw_type == "GEOFENCE" and "вхід" in item_lower):
            ev_type = "GEO_IN"
        elif cfg["template_geo_out"] in template_name or cfg["template_geo_out"] in item_str or (raw_type == "GEOFENCE" and "вихід" in item_lower):
            ev_type = "GEO_OUT"
        elif raw_type == "OVERSPEED" or cfg["template_overspeed"] in template_name or cfg["template_overspeed"] in item_str:
            ev_type = "OVERSPEED"
        elif raw_type == "SENSOR_VALUE" or cfg["template_sensor"] in template_name or cfg["template_sensor"] in item_str:
            ev_type = "SENSOR_VALUE"
        elif raw_type == "IDLE" or cfg["template_idle"] in template_name or cfg["template_idle"] in item_str:
            ev_type = "IDLE"
        elif raw_type == "COMMAND_RESULT" or cfg["template_command"] in template_name or cfg["template_command"] in item_str or "commandsentsuccess" in item_lower:
            ev_type = "COMMAND"
            
        if not ev_type:
            continue

        if ev_type == "IDLE":
            status["IDLE"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ IDLE (id={event_id})")
        elif ev_type == "GEO_IN":
            status["GEO_IN"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ GEO_IN (id={event_id})")
        elif ev_type == "GEO_OUT":
            status["GEO_OUT"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ GEO_OUT (id={event_id})")
        elif ev_type == "OVERSPEED":
            status["OVERSPEED"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ OVERSPEED (id={event_id})")
        elif ev_type == "SENSOR_VALUE":
            status["SENSOR"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ SENSOR_VALUE (id={event_id})")
        elif ev_type == "COMMAND":
            status["COMMAND"] = True
            log.info(f"✨ ЗНАЙДЕНО ПОДІЮ COMMAND (id={event_id})")

        if ev_type in ("REFILL", "DRAIN"):
            status[ev_type] = True
            try:
                meta = item.get("metadata", {})
                if isinstance(meta, str):
                    meta = json.loads(meta)
                
                finish = meta.get("fuelFinishValue", "невідомо")
                start_val = meta.get("fuelStartValue", "невідомо")
                volume = meta.get("fuelVolume")
                
                if volume is None:
                    if isinstance(finish, (int, float)) and isinstance(start_val, (int, float)):
                        volume = round(abs(finish - start_val), 2)
                    else:
                        volume = "невідомо"
                        
                if ev_type == "REFILL":
                    refill_volume_str = f"({volume} л)"
                else:
                    drain_volume_str = f"({volume} л)"
                    
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id}): об'єм={volume} л.")
            except (TypeError, ValueError, json.JSONDecodeError):
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id}): об'єм прочитати не вдалося.")

    report_lines = [
        f"{'✅' if status['REFILL'] else '❌'} Заправка {refill_volume_str}".strip(),
        f"{'✅' if status['DRAIN'] else '❌'} Злив {drain_volume_str}".strip(),
        f"{'✅' if status['GEO_OUT'] else '❌'} Вихід з геозони",
        f"{'✅' if status['GEO_IN'] else '❌'} Вхід у геозону",
        f"{'✅' if status['OVERSPEED'] else '❌'} Швидкість",
        f"{'✅' if status['SENSOR'] else '❌'} Значення датчика",
        f"{'✅' if status['IDLE'] else '❌'} Простій",
        f"{'✅' if status['COMMAND'] else '❌'} Результат команди" 
    ]
    report_str = "\n".join(report_lines)

    if all(status.values()):
        return True, report_str
    
    return False, report_str


def wait_for_events(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    wait_mins = cfg["initial_wait_sec"] // 60
    log.info(f"⏳ Даємо бекенду {wait_mins} хвилин на обробку даних перед перевіркою історії...")
    time.sleep(cfg["initial_wait_sec"])
    
    deadline = time.monotonic() + cfg["max_wait_sec"]
    attempt = 0
    while True:
        attempt += 1
        elapsed = cfg["max_wait_sec"] - max(0, deadline - time.monotonic())
        log.info("Перевірка #%d (минуло ~%.0f сек з max %d сек)...", attempt, elapsed, cfg["max_wait_sec"])
        
        ok, detail = check_events_in_history(cfg, start_time, initial_event_ids)
        
        if ok:
            return True, detail
            
        if time.monotonic() >= deadline:
            return False, f"Таймаут ({cfg['max_wait_sec']} сек).\n\n{detail}"
            
        time.sleep(cfg["poll_interval_sec"])


# --------------------------------------------------------------------------
# 3. Алертинг з дедуплікацією
# --------------------------------------------------------------------------

def load_state(cfg: dict) -> dict:
    if cfg["state_file"].exists():
        return json.loads(cfg["state_file"].read_text())
    return {"incident_open": False}


def save_state(cfg: dict, state: dict) -> None:
    cfg["state_file"].write_text(json.dumps(state))


def send_telegram(cfg: dict, text: str) -> None:
    if not cfg["telegram_bot_token"] or not cfg["telegram_chat_id"]:
        return
    url = f"https://api.telegram.org/bot{cfg['telegram_bot_token']}/sendMessage"
    requests.post(url, json={"chat_id": cfg["telegram_chat_id"], "text": text}, timeout=10)


def send_slack(cfg: dict, text: str) -> None:
    if not cfg["slack_webhook_url"]:
        return
    requests.post(cfg["slack_webhook_url"], json={"text": text}, timeout=10)


def show_popup(cfg: dict, title: str, text: str, is_error: bool) -> None:
    if not cfg["desktop_popup"] or sys.platform != "win32":
        return
    icon = 0x10 if is_error else 0x40  
    try:
        ctypes.windll.user32.MessageBoxW(0, text, title, icon)
    except Exception:
        pass


def notify_incident(cfg: dict, detail: str) -> None:
    text = (
        "🔴 [Teltonika] Спрацювали не всі сповіщення !\n\n"
        f"{detail}\n\n"
        "Ймовірно завис worker/consumer на беку -- перевір логи."
    )
    log.error(text)
    show_popup(cfg, "Canary Teltonika: ПРОБЛЕМА", text, is_error=True)
    send_telegram(cfg, text)
    send_slack(cfg, text)


def notify_recovery(cfg: dict) -> None:
    text = "🟢 [Teltonika] Конвеєр сповіщень відновився, всі типи сповіщень працюють коректно!"
    log.info(text)
    show_popup(cfg, "Canary Teltonika: Відновлено", text, is_error=False)
    send_telegram(cfg, text)
    send_slack(cfg, text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [Teltonika] Всі типи сповіщень працюють коректно !\n\n{detail}"
    show_popup(cfg, "Canary Teltonika: УСПІХ", text, is_error=False)
    send_telegram(cfg, text)
    send_slack(cfg, text)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    cfg = CONFIG

    if not cfg["api_token"] and not (cfg["api_email"] and cfg["api_password"]):
        log.error("🚨 Не задано облікових даних! Вкажіть M2M_API_TOKEN або пару M2M_EMAIL та M2M_PASSWORD.")
        return 1

    state = load_state(cfg)

    log.info("Збираю поточний стан історії для фільтрації старих подій...")
    try:
        initial_event_ids = get_existing_event_ids(cfg)
        log.info("Знайдено старих подій в історії: %d", len(initial_event_ids))
    except PermissionError as e:
        log.error(f"🚨 ПОМИЛКА АВТОРИЗАЦІЇ: {e}")
        return 1

    try:
        start_time = send_canary_events(cfg)
    except Exception as e:
        log.exception("Не вдалось відправити canary-подію (Teltonika)")
        notify_incident(cfg, f"Помилка відправки тестових подій TCP: {e}")
        return 1

    log.info("Очікую появу подій (перевірятиму до %d хв)...", cfg["max_wait_sec"] // 60)

    try:
        ok, detail = wait_for_events(cfg, start_time, initial_event_ids)
    except Exception as e:
        log.exception("Не вдалось перевірити історію через API")
        notify_incident(cfg, f"Помилка запиту до API історії:\n{e}")
        return 1

    if ok:
        log.info("Все добре: %s", detail)
        if state.get("incident_open"):
            notify_recovery(cfg)
        else:
            notify_success(cfg, detail)
            
        state["incident_open"] = False
    else:
        log.warning("Проблема: %s", detail)
        if not state.get("incident_open"):
            notify_incident(cfg, detail)
        state["incident_open"] = True

    save_state(cfg, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
