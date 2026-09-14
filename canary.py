#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень.

Алгоритм:
- ФАЗА 1: Заправка (400 -> 700 л).
- ФАЗА 2: Геозони (Вихід -> Вхід).
- ФАЗА 3: Швидкість (Рух 15 км/год).
- ФАЗА 4: Датчик (Пальне 750 л для тригеру 701-801).
- ФАЗА 5: Простій (Стоянка 5.5 хв) + Результат команди.
- ФАЗА 6: Злив (Плавне падіння 750 -> 350 л) + СТАБІЛІЗАЦІЯ (Плато).
- Захист мережі: Retry + Reconnect на рівні HTTP Session та _send_point.
"""

import ctypes
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary")

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
    "hdop": 1,

    "ingest_base_url": "http://213.239.234.94:5055",
    
    # --- ПАРАМЕТРИ ПЛАТФОРМНОГО API ---
    "api_base_url": "https://my.m2m.eu/api",
    "api_email": os.getenv("M2M_EMAIL", ""),        
    "api_password": os.getenv("M2M_PASSWORD", ""),  
    "api_token": os.getenv("M2M_API_TOKEN", ""),    
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 50},

    # --- ПАРАМЕТРИ ПАЛЬНОГО (м'якший таймінг для уникання rate-limit) ---
    "fuel_param": "fuel",
    "start_level_l": 400,
    "end_level_l": 700,
    "num_points": 15,                 
    "point_interval_sec": 8,          # було 5
    "edge_repeats": 3,                
    "edge_repeat_interval_sec": 8,    # було 5

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
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),

    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_state.json")),
}


# --------------------------------------------------------------------------
# Пул сесій з надійними повторними спробами
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


# --------------------------------------------------------------------------
# 1. Відправка тестових даних з реконектами
# --------------------------------------------------------------------------

def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0, result_param: str = None) -> None:
    timestamp = int(time.time())
    fuel_int = int(level) 
    
    url = (
        f"{cfg['ingest_base_url']}?"
        f"id={cfg['tracker_imei']}&"
        f"location={lat:.6f}, {lon:.6f}&"
        f"motion={'true' if speed > 0 else 'false'}&"
        f"sat={cfg['sat']}&"
        f"hdop={cfg['hdop']}&"
        f"speed={speed}&"
        f"altitude={cfg['altitude']}&"
        f"course={cfg['bearing']}&"
        f"motionState={'true' if speed > 0 else 'false'}&"
        f"valid=true&"
        f"ignition=true&"
        f"{cfg['fuel_param']}={fuel_int}&"
        f"timestamp={timestamp}"
    )
    
    if result_param:
        url += f"&result={result_param}"
    
    max_connection_attempts = 4
    for attempt in range(1, max_connection_attempts + 1):
        try:
            resp = GLOBAL_SESSION.get(url, timeout=cfg["http_timeout_sec"])
            resp.raise_for_status()
            return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt == max_connection_attempts:
                log.error("Критичний збій мережі після %d спроб: %s", max_connection_attempts, exc)
                raise
            sleep_time = 5 * attempt
            log.warning("Тимчасова відмова з'єднання (спроба %d/%d). Пауза %d с: %s", 
                        attempt, max_connection_attempts, sleep_time, exc)
            time.sleep(sleep_time)


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    lo, hi = cfg["start_level_l"], cfg["end_level_l"]
    
    in_lat, in_lon = cfg["geo_in_lat"], cfg["geo_in_lon"]
    out_lat, out_lon = cfg["geo_out_lat"], cfg["geo_out_lon"]

    log.info("--- ФАЗА 1/6: ПАЛЬНЕ (ЗАПРАВКА) ---")
    log.info("Стабілізація старту (%s л)", lo)
    for _ in range(cfg["edge_repeats"]):
        _send_point(cfg, lo, in_lat, in_lon)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("Швидка заправка %s -> %s л", lo, hi)
    step = (hi - lo) / (cfg["num_points"] - 1) if cfg["num_points"] > 1 else 0
    for i in range(cfg["num_points"]):
        level = lo + step * i
        _send_point(cfg, level, in_lat, in_lon)
        time.sleep(cfg["point_interval_sec"])

    log.info("Стабілізація фінішу (%s л)", hi)
    for _ in range(cfg["edge_repeats"]):
        _send_point(cfg, hi, in_lat, in_lon)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("--- ФАЗА 2/6: ГЕОЗОНИ ---")
    for cycle in range(cfg["geo_cycles"]):
        log.info("Відправка ВИХОДУ (GEO_OUT)")
        for _ in range(cfg["geo_points_count"]):
            _send_point(cfg, hi, out_lat, out_lon)
            time.sleep(8)
                
        time.sleep(cfg["geo_wait_sec"])

        log.info("Відправка ВХОДУ (GEO_IN)")
        for _ in range(cfg["geo_points_count"]):
            _send_point(cfg, hi, in_lat, in_lon)
            time.sleep(8)
                
        if cycle < cfg["geo_cycles"] - 1:
            time.sleep(cfg["geo_wait_sec"])

    log.info("--- ФАЗА 3/6: ШВИДКІСТЬ (OVERSPEED) ---")
    current_lat, current_lon = in_lat, in_lon
    
    for _ in range(2):
        log.info("Рух: швидкість %s км/год", cfg['overspeed_value'])
        for _ in range(3):
            current_lat += 0.0002 
            current_lon += 0.0002
            _send_point(cfg, hi, current_lat, current_lon, speed=cfg["overspeed_value"])
            time.sleep(8)
            
        log.info("Зупинка: швидкість 0 км/год")
        for _ in range(3):
            _send_point(cfg, hi, current_lat, current_lon, speed=0)
            time.sleep(8)

    log.info("--- ФАЗА 4/6: ЗНАЧЕННЯ ДАТЧИКА ---")
    log.info("Відправка рівня пального %s л (Тригер 701-801)", cfg['sensor_trigger_value'])
    for _ in range(3):
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0)
        time.sleep(8)

    log.info("--- ФАЗА 5/6: ПРОСТІЙ ТА КОМАНДА ---")
    current_lat += 0.0002
    current_lon += 0.0002
    _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=cfg["overspeed_value"])
    time.sleep(10)

    log.info("Стоянка 5.5 хвилин для тригеру (>5 хв). Точок: %s...", cfg['idle_points_count'])
    for i in range(cfg["idle_points_count"]):
        res_val = "CommandSentSuccess" if i == 0 else None
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
    
    for current_level in drain_levels:
        _send_point(cfg, current_level, current_lat, current_lon, speed=0)
        time.sleep(cfg["drain_interval_sec"])

    for _ in range(cfg["edge_repeats"]):
        _send_point(cfg, end_drain, current_lat, current_lon, speed=0)
        time.sleep(cfg["edge_repeat_interval_sec"])

    log.info("Весь профіль повністю відправлено!")
    return start_time


# --------------------------------------------------------------------------
# 2. Робота з API та перевірка історії
# --------------------------------------------------------------------------

def refresh_api_token(cfg: dict) -> None:
    if not cfg["api_email"] or not cfg["api_password"]:
        raise ValueError("Відсутні облікові дані для автооновлення токена.")

    url = f"{cfg['api_base_url']}{cfg['login_path']}"
    payload = {"email": cfg["api_email"], "password": cfg["api_password"]}
    
    resp = GLOBAL_SESSION.post(url, json=payload, timeout=cfg["http_timeout_sec"])
    if resp.status_code != 200:
        raise PermissionError(f"Помилка авторизації: {resp.status_code}")
        
    data = resp.json()
    new_token = data.get("token")
    if not new_token:
        raise ValueError("Токен не знайдено у відповіді сервера.")

    cfg["api_token"] = new_token
    log.info("API-токен оновлено!")


def fetch_history(cfg: dict) -> dict:
    if not cfg["api_token"]:
        refresh_api_token(cfg)

    url = f"{cfg['api_base_url']}{cfg['history_path']}"
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    
    resp = GLOBAL_SESSION.get(url, headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
    if resp.status_code == 401:
        refresh_api_token(cfg)
        headers = {"Authorization": f"Bearer {cfg['api_token']}"}
        resp = GLOBAL_SESSION.get(url, headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
        
    resp.raise_for_status()
    return resp.json()


def get_existing_event_ids(cfg: dict) -> set:
    try:
        data = fetch_history(cfg)
        return {item.get("id") for item in data.get("items", []) if item.get("id") is not None}
    except Exception as e:
        log.warning("Не вдалося отримати початковий стан історії: %s", e)
        return set()


def check_events_in_history(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    data = fetch_history(cfg)
    items = data.get("items", [])

    status = {
        "REFILL": False, "DRAIN": False, "GEO_IN": False, "GEO_OUT": False,
        "OVERSPEED": False, "SENSOR": False, "IDLE": False, "COMMAND": False
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
            ev_type = "SENSOR"
        elif raw_type == "IDLE" or cfg["template_idle"] in template_name or cfg["template_idle"] in item_str:
            ev_type = "IDLE"
        elif raw_type == "COMMAND_RESULT" or cfg["template_command"] in template_name or "commandsentsuccess" in item_lower:
            ev_type = "COMMAND"
            
        if not ev_type:
            continue

        status[ev_type] = True

        if ev_type in ("REFILL", "DRAIN"):
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
                        
                if volume is not None:
                    if ev_type == "REFILL":
                        refill_volume_str = f"({volume} л)"
                    else:
                        drain_volume_str = f"({volume} л)"
            except Exception:
                pass

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
    return all(status.values()), "\n".join(report_lines)


def wait_for_events(cfg: dict, start_time: datetime, initial_event_ids: set) -> tuple[bool, str]:
    time.sleep(cfg["initial_wait_sec"])
    deadline = time.monotonic() + cfg["max_wait_sec"]
    while True:
        ok, detail = check_events_in_history(cfg, start_time, initial_event_ids)
        if ok:
            return True, detail
        if time.monotonic() >= deadline:
            return False, f"Таймаут ({cfg['max_wait_sec']} сек).\n\n{detail}"
        time.sleep(cfg["poll_interval_sec"])


# --------------------------------------------------------------------------
# 3. Алертинг
# --------------------------------------------------------------------------

def load_state(cfg: dict) -> dict:
    if cfg["state_file"].exists():
        return json.loads(cfg["state_file"].read_text())
    return {"incident_open": False}


def save_state(cfg: dict, state: dict) -> None:
    cfg["state_file"].write_text(json.dumps(state))


def send_telegram(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        GLOBAL_SESSION.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception as exc:
        log.warning("Не вдалося відправити повідомлення в Telegram: %s", exc)


def send_slack(cfg: dict, text: str) -> None:
    if not cfg["slack_webhook_url"]:
        return
    try:
        GLOBAL_SESSION.post(cfg["slack_webhook_url"], json={"text": text}, timeout=10)
    except Exception:
        pass


def show_popup(cfg: dict, title: str, text: str, is_error: bool) -> None:
    if not cfg["desktop_popup"] or sys.platform != "win32":
        return
    icon = 0x10 if is_error else 0x40  
    try:
        ctypes.windll.user32.MessageBoxW(0, text, title, icon)
    except Exception:
        pass


def notify_incident(cfg: dict, detail: str) -> None:
    text = f"🔴 [OsmAnd] Спрацювали не всі сповіщення !\n\n{detail}\n\nЙмовірно завис worker/consumer на беку -- перевір логи."
    log.error(text)
    show_popup(cfg, "Canary: ПРОБЛЕМА", text, is_error=True)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)
    send_slack(cfg, text)


def notify_recovery(cfg: dict) -> None:
    text = "🟢 [OsmAnd] Конвеєр сповіщень відновився, всі типи сповіщень працюють коректно!"
    log.info(text)
    show_popup(cfg, "Canary: Відновлено", text, is_error=False)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)
    send_slack(cfg, text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ [OsmAnd] Всі типи сповіщень працюють коректно !\n\n{detail}"
    show_popup(cfg, "Canary: УСПІХ", text, is_error=False)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_chat_id"], text)
    send_telegram(cfg["telegram_bot_token"], cfg["telegram_dm_chat_id"], text)
    send_slack(cfg, text)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    cfg = CONFIG

    if not cfg["api_token"] and not (cfg["api_email"] and cfg["api_password"]):
        log.error("🚨 Не задано облікових даних API.")
        return 1

    state = load_state(cfg)
    try:
        initial_event_ids = get_existing_event_ids(cfg)
    except Exception as e:
        log.error("Помилка конфігурації/авторизації: %s", e)
        return 1

    try:
        start_time = send_canary_events(cfg)
    except Exception as e:
        notify_incident(cfg, f"Помилка відправки тестових подій: {e}")
        return 1

    try:
        ok, detail = wait_for_events(cfg, start_time, initial_event_ids)
    except Exception as e:
        notify_incident(cfg, f"Помилка запиту до API історії:\n{e}")
        return 1

    if ok:
        if state.get("incident_open"):
            notify_recovery(cfg)
        else:
            notify_success(cfg, detail)
        state["incident_open"] = False
    else:
        if not state.get("incident_open"):
            notify_incident(cfg, detail)
        state["incident_open"] = True

    save_state(cfg, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
