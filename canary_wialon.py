#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень.
ПРОТОКОЛ: WIALON IPS (TCP Socket)
"""

import ctypes
import json
import logging
import os
import sys
import time
import socket
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_wialon")

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
    
    "geo_in_lat": 49.438923,
    "geo_in_lon": 32.085565,
    "geo_out_lat": 49.430000,
    "geo_out_lon": 32.085565,
    "altitude": 10,
    "bearing": 210, 
    "sat": 12,      
    "hdop": 1,

    "ingest_host": "213.239.234.94",
    "ingest_port": 5039,
    
    "api_base_url": "https://my.m2m.eu/api",
    "api_email": os.getenv("M2M_EMAIL", ""),        
    "api_password": os.getenv("M2M_PASSWORD", ""),  
    "api_token": os.getenv("M2M_API_TOKEN", ""),    
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 50},

    "fuel_param": "fuel",
    "start_level_l": 400,
    "end_level_l": 700,
    "num_points": 15,                 
    "point_interval_sec": 5,          
    "edge_repeats": 3,                
    "edge_repeat_interval_sec": 5,    
    "geo_cycles": 2,          
    "geo_points_count": 3,    
    "geo_wait_sec": 60,      
    "overspeed_value": 15,         
    "sensor_trigger_value": 750,   
    "idle_points_count": 12,       
    "idle_interval_sec": 30,
    "drain_target_value": 350,       
    "drain_interval_sec": 15,      

    "initial_wait_sec": 120,   
    "poll_interval_sec": 30,   
    "max_wait_sec": 900,      
    "http_timeout_sec": 60,  
    "desktop_popup": True,   
    
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_wialon_state.json")),
}

def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0, result_param: str = None) -> None:
    host, port, imei = cfg["ingest_host"], cfg["ingest_port"], cfg["tracker_imei"]
    lat_dir = 'N' if lat >= 0 else 'S'
    lon_dir = 'E' if lon >= 0 else 'W'
    lat_deg, lat_min = int(abs(lat)), (abs(lat) - int(abs(lat))) * 60
    lon_deg, lon_min = int(abs(lon)), (abs(lon) - int(abs(lon))) * 60
    
    dt = datetime.now(timezone.utc)
    date_str, time_str = dt.strftime("%d%m%y"), dt.strftime("%H%M%S")
    
    params = [
        f"{cfg['fuel_param']}:2:{level:.2f}", 
        f"motion:1:{1 if speed > 0 else 0}",
        "ignition:1:1"
    ]
    if result_param: params.append(f"result:3:{result_param}")
        
    login_pkt = f"#L#{imei};NA\r\n"
    data_pkt = f"#D#{date_str};{time_str};{lat_deg:02d}{lat_min:07.4f};{lat_dir};{lon_deg:03d}{lon_min:07.4f};{lon_dir};{speed};{cfg['bearing']};{cfg['altitude']};{cfg['sat']};{cfg['hdop']};NA;NA;NA;NA;{','.join(params)}\r\n"
    
    log.info(f"Відправка Wialon IPS: {data_pkt.strip()}")
    
    # СИСТЕМА ПОВТОРНИХ СПРОБ (3 СПРОБИ)
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with socket.create_connection((host, port), timeout=cfg["http_timeout_sec"]) as sock:
                sock.sendall(login_pkt.encode('ascii'))
                sock.recv(1024)
                sock.sendall(data_pkt.encode('ascii'))
                sock.recv(1024)
            break  
        except Exception as e:
            if attempt < max_retries:
                log.warning(f"Спроба {attempt}/{max_retries} не вдалася ({e}). Чекаю 5 сек перед наступною...")
                time.sleep(5)
            else:
                log.error(f"TCP Error (Wialon IPS) після {max_retries} спроб: {e}")
                raise

def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    lo, hi = cfg["start_level_l"], cfg["end_level_l"]
    in_lat, in_lon = cfg["geo_in_lat"], cfg["geo_in_lon"]
    out_lat, out_lon = cfg["geo_out_lat"], cfg["geo_out_lon"]

    log.info("--- ФАЗА 1/6: ПАЛЬНЕ (ЗАПРАВКА) ---")
    log.info(f"Стабілізація старту ({lo} л)")
    for i in range(cfg["edge_repeats"]):
        _send_point(cfg, lo, in_lat, in_lon)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info(f"Швидка заправка {lo} -> {hi} л")
    step = (hi - lo) / (cfg["num_points"] - 1) if cfg["num_points"] > 1 else 0
    for i in range(cfg["num_points"]):
        _send_point(cfg, lo + step * i, in_lat, in_lon)
        time.sleep(cfg["point_interval_sec"])

    log.info(f"Стабілізація фінішу ({hi} л)")
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
            current_lat += 0.0002; current_lon += 0.0002
            _send_point(cfg, hi, current_lat, current_lon, speed=cfg["overspeed_value"])
            time.sleep(5)
            
        log.info("Зупинка: швидкість 0 км/год")
        for i in range(3):
            _send_point(cfg, hi, current_lat, current_lon, speed=0)
            time.sleep(5)

    log.info("--- ФАЗА 4/6: ЗНАЧЕННЯ ДАТЧИКА ---")
    log.info(f"Відправка рівня пального {cfg['sensor_trigger_value']} л")
    for i in range(3):
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0)
        time.sleep(5)

    log.info("--- ФАЗА 5/6: ПРОСТІЙ ТА КОМАНДА ---")
    log.info("Рух для скидання таймера стоянки...")
    current_lat += 0.0002; current_lon += 0.0002
    _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=cfg["overspeed_value"])
    time.sleep(10)

    log.info(f"Стоянка для тригеру простою. Відправка точок...")
    for i in range(cfg["idle_points_count"]):
        res_val = "CommandSentSuccess" if i == 0 else None
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0, result_param=res_val)
        if i < cfg["idle_points_count"] - 1:
            time.sleep(cfg["idle_interval_sec"])

    log.info("--- ФАЗА 6/6: ЗЛИВ (DRAIN) ---")
    start_drain = cfg["sensor_trigger_value"]
    end_drain = cfg["drain_target_value"]
    log.info(f"Плавний злив {start_drain} -> {end_drain} л...")
    for current_level in [start_drain - 80, start_drain - 160, start_drain - 240, start_drain - 300, start_drain - 340, start_drain - 370, end_drain]:
        _send_point(cfg, current_level, current_lat, current_lon, speed=0)
        time.sleep(cfg["drain_interval_sec"])

    log.info(f"Стабілізація після зливу (плато {end_drain} л)...")
    for i in range(cfg["edge_repeats"]):
        _send_point(cfg, end_drain, current_lat, current_lon, speed=0)
        time.sleep(cfg["edge_repeat_interval_sec"])

    return start_time

def refresh_api_token(cfg: dict) -> None:
    try:
        resp = requests.post(f"{cfg['api_base_url']}{cfg['login_path']}", json={"email": cfg["api_email"], "password": cfg["api_password"]}, timeout=cfg["http_timeout_sec"])
        resp.raise_for_status()
        cfg["api_token"] = resp.json().get("token")
    except Exception as e:
        log.warning(f"Не вдалося оновити токен API: {e}")

def fetch_history(cfg: dict) -> dict:
    if not cfg["api_token"]: refresh_api_token(cfg)
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    try:
        resp = requests.get(f"{cfg['api_base_url']}{cfg['history_path']}", headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
        if resp.status_code == 401:
            refresh_api_token(cfg)
            headers = {"Authorization": f"Bearer {cfg['api_token']}"}
            resp = requests.get(f"{cfg['api_base_url']}{cfg['history_path']}", headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"Помилка запиту історії: {e}")
        return {}

def get_existing_event_ids(cfg: dict) -> set:
    try: return {item.get("id") for item in fetch_history(cfg).get("items", []) if item.get("id") is not None}
    except Exception: return set()

def check_events_in_history(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    items = fetch_history(cfg).get("items", [])
    status = {"REFILL": False, "DRAIN": False, "GEO_IN": False, "GEO_OUT": False, "OVERSPEED": False, "SENSOR": False, "IDLE": False, "COMMAND": False}

    for item in items:
        if item.get("id") in initial_event_ids: continue
        item_str, item_lower = json.dumps(item, ensure_ascii=False), json.dumps(item, ensure_ascii=False).lower()
        if not (item.get("deviceId") == cfg["device_id"] or item.get("deviceName") == cfg["device_name"]): continue
            
        raw_type, template_name = item.get("type", "").upper(), item.get("templateName") or ""
        ev_type = None
        
        if raw_type in ("REFILL", "REFIL") or cfg["template_refill"] in template_name or cfg["template_refill"] in item_str: ev_type = "REFILL"
        elif raw_type == "DRAIN" or cfg["template_drain"] in template_name or cfg["template_drain"] in item_str: ev_type = "DRAIN"
        elif cfg["template_geo_in"] in template_name or cfg["template_geo_in"] in item_str or (raw_type == "GEOFENCE" and "вхід" in item_lower): ev_type = "GEO_IN"
        elif cfg["template_geo_out"] in template_name or cfg["template_geo_out"] in item_str or (raw_type == "GEOFENCE" and "вихід" in item_lower): ev_type = "GEO_OUT"
        elif raw_type == "OVERSPEED" or cfg["template_overspeed"] in template_name or cfg["template_overspeed"] in item_str: ev_type = "OVERSPEED"
        elif raw_type == "SENSOR_VALUE" or cfg["template_sensor"] in template_name or cfg["template_sensor"] in item_str: ev_type = "SENSOR_VALUE"
        elif raw_type == "IDLE" or cfg["template_idle"] in template_name or cfg["template_idle"] in item_str: ev_type = "IDLE"
        elif raw_type == "COMMAND_RESULT" or cfg["template_command"] in template_name or "commandsentsuccess" in item_lower: ev_type = "COMMAND"
            
        if not ev_type: continue
        status[ev_type] = True

    report = "\n".join([f"{'✅' if status[k] else '❌'} {k}" for k in status.keys()])
    return all(status.values()), report

def wait_for_events(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    while True:
        ok, detail = check_events_in_history(cfg, start_time, initial_event_ids)
        if ok: return True, detail
        if time.monotonic() >= deadline: return False, f"Таймаут\n\n{detail}"
        time.sleep(cfg["poll_interval_sec"])

def load_state(cfg: dict) -> dict: return json.loads(cfg["state_file"].read_text()) if cfg["state_file"].exists() else {"incident_open": False}
def save_state(cfg: dict, state: dict) -> None: cfg["state_file"].write_text(json.dumps(state))

def send_telegram(token: str, chat_id: str, text: str) -> None:
    if token and chat_id:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=10)
        except Exception as e:
            log.warning(f"Не вдалося відправити Telegram: {e}")

def notify_incident(cfg: dict, detail: str) -> None:
    text = f"🔴 [Wialon IPS] Спрацювали не всі сповіщення !\n\n{detail}\n\nЙмовірно завис worker/consumer."
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)

def notify_recovery(cfg: dict) -> None:
    text = "🟢 [Wialon IPS] Конвеєр сповіщень відновився!"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)

def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [Wialon IPS] Всі типи сповіщень працюють !\n\n{detail}"
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)

def main() -> int:
    cfg, state = CONFIG, load_state(CONFIG)
    initial_ids = get_existing_event_ids(cfg)
    
    try:
        start = send_canary_events(cfg)
    except Exception as e:
        log.exception("Не вдалось відправити canary-подію (Wialon IPS)")
        notify_incident(cfg, f"Помилка відправки тестових подій TCP: {e}")
        return 1

    ok, detail = wait_for_events(cfg, start, initial_ids)
    if ok:
        if state.get("incident_open"): notify_recovery(cfg)
        else: notify_success(cfg, detail)
        state["incident_open"] = False
    else:
        if not state.get("incident_open"): notify_incident(cfg, detail)
        state["incident_open"] = True
    save_state(cfg, state)
    return 0

if __name__ == "__main__": sys.exit(main())
