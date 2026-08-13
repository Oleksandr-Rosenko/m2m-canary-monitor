#!/usr/bin/env python3
"""
Canary-моніторинг конвеєра сповіщень.

Алгоритм (ПОВНИЙ КЛОН + АВТОАВТОРИЗАЦІЯ):
- Автоматичне отримання та оновлення API-токена при 401 Unauthorized.
- ФАЗА 1 (ПАЛЬНЕ): Швидка заправка (пробиває фільтр згладжування).
- ФАЗА 2 (ГЕОЗОНИ): Вихід -> Вхід (по 3 точки кожні 5 сек).
- ФАЗА 3 (ШВИДКІСТЬ): Рух (швидкість 15) -> Зупинка -> Рух -> Зупинка.
- ФАЗА 4 (СЕНСОР): Статична відправка пального з рівнем 750 (тригер 701-801).
- ФАЗА 5 (ПРОСТІЙ): Рух для скидання таймера -> Стоянка 6.5 хвилин (speed=0).
- Очікування та перевірка історії на наявність усіх 6 подій.
"""

import ctypes
import json
import logging
import os
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path

import requests

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
    "api_email": os.getenv("M2M_EMAIL", ""),        # НОВЕ: Логін
    "api_password": os.getenv("M2M_PASSWORD", ""),  # НОВЕ: Пароль
    "api_token": os.getenv("M2M_API_TOKEN", ""),    # Залишається як опція, але не обов'язково
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 50},

    # --- ПАРАМЕТРИ ПАЛЬНОГО ---
    "fuel_param": "fuel",
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
    "idle_points_count": 13,       # 13 точок по 30 сек = 390 сек (6.5 хвилин)
    "idle_interval_sec": 30,

    "initial_wait_sec": 120,   
    "poll_interval_sec": 30,   
    "max_wait_sec": 900,      
    "http_timeout_sec": 20,

    "desktop_popup": True,   
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),

    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_state.json")),
}


# --------------------------------------------------------------------------
# 1. Відправка тестових даних
# --------------------------------------------------------------------------

def _send_point(cfg: dict, level: float, lat: float, lon: float, speed: int = 0) -> None:
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
        f"{cfg['fuel_param']}={fuel_int}&"
        f"timestamp={timestamp}"
    )
    
    resp = requests.get(url, timeout=cfg["http_timeout_sec"])
    resp.raise_for_status()


def send_canary_events(cfg: dict) -> datetime:
    start_time = datetime.now(timezone.utc)
    lo, hi = cfg["start_level_l"], cfg["end_level_l"]
    
    in_lat, in_lon = cfg["geo_in_lat"], cfg["geo_in_lon"]
    out_lat, out_lon = cfg["geo_out_lat"], cfg["geo_out_lon"]

    # ======================================================================
    # БЛОК 1: СТОЯНКА ТА ЗАПРАВКА
    # ======================================================================
    log.info("--- ФАЗА 1/5: ПАЛЬНЕ ---")
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

    # ======================================================================
    # БЛОК 2: ГЕОЗОНИ
    # ======================================================================
    log.info("--- ФАЗА 2/5: ГЕОЗОНИ ---")
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

    # ======================================================================
    # БЛОК 3: ПЕРЕВИЩЕННЯ ШВИДКОСТІ
    # ======================================================================
    log.info("--- ФАЗА 3/5: ШВИДКІСТЬ (OVERSPEED) ---")
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

    # ======================================================================
    # БЛОК 4: ЗНАЧЕННЯ ДАТЧИКА (SENSOR VALUE)
    # ======================================================================
    log.info("--- ФАЗА 4/5: ЗНАЧЕННЯ ДАТЧИКА ---")
    log.info(f"Відправка рівня пального {cfg['sensor_trigger_value']} л (Тригер 701-801)")
    for i in range(3):
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0)
        time.sleep(5)

    # ======================================================================
    # БЛОК 5: ПРОСТІЙ (IDLE / СТОЯНКА)
    # ======================================================================
    log.info("--- ФАЗА 5/5: ПРОСТІЙ (IDLE) ---")
    log.info("1. Рух для скидання таймера стоянки...")
    current_lat += 0.0002
    current_lon += 0.0002
    _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=cfg["overspeed_value"])
    time.sleep(10)

    log.info(f"2. Стоянка 6.5 хвилин для тригеру (>5 хв). Відправка {cfg['idle_points_count']} точок кожні {cfg['idle_interval_sec']} сек...")
    for i in range(cfg["idle_points_count"]):
        _send_point(cfg, cfg["sensor_trigger_value"], current_lat, current_lon, speed=0)
        if i < cfg["idle_points_count"] - 1:
            time.sleep(cfg["idle_interval_sec"])

    log.info("Весь профіль повністю відправлено!")
    return start_time


# --------------------------------------------------------------------------
# 2. Робота з API та перевірка історії
# --------------------------------------------------------------------------

def refresh_api_token(cfg: dict) -> None:
    """Отримує новий API-токен за допомогою логіна та пароля."""
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
    """Отримує історію з автооновленням токена при 401 помилці."""
    if not cfg["api_token"]:
        refresh_api_token(cfg)

    url = f"{cfg['api_base_url']}{cfg['history_path']}"
    headers = {"Authorization": f"Bearer {cfg['api_token']}"}
    
    resp = requests.get(url, headers=headers, params=cfg["history_query"], timeout=cfg["http_timeout_sec"])
    
    # Якщо токен протух - оновлюємо і повторюємо запит 1 раз
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

    found_refill = False
    found_geo_in = False
    found_geo_out = False
    found_overspeed = False
    found_sensor = False
    found_idle = False
    details_list = []

    for item in items:
        event_id = item.get("id")
        if event_id in initial_event_ids:
            continue

        item_str = json.dumps(item, ensure_ascii=False)
        
        match_id = (item.get("deviceId") == cfg["device_id"])
        match_dev_name = (item.get("deviceName") == cfg["device_name"])
        
        match_tpl_refill = (cfg["template_refill"] in item_str)
        match_tpl_drain = (cfg["template_drain"] in item_str)
        match_tpl_geo_in = (cfg["template_geo_in"] in item_str)
        match_tpl_geo_out = (cfg["template_geo_out"] in item_str)
        match_tpl_overspeed = (cfg["template_overspeed"] in item_str)
        match_tpl_sensor = (cfg["template_sensor"] in item_str)
        match_tpl_idle = (cfg["template_idle"] in item_str)
        
        if not (match_id or match_dev_name or match_tpl_refill or match_tpl_drain or match_tpl_geo_in or match_tpl_geo_out or match_tpl_overspeed or match_tpl_sensor or match_tpl_idle):
            continue
            
        ev_type = item.get("type")
        if match_tpl_refill: ev_type = "REFILL"
        elif match_tpl_drain: ev_type = "DRAIN"
        elif match_tpl_geo_in: ev_type = "GEO_IN"
        elif match_tpl_geo_out: ev_type = "GEO_OUT"
        elif match_tpl_overspeed: ev_type = "OVERSPEED"
        elif match_tpl_sensor: ev_type = "SENSOR_VALUE"
        elif match_tpl_idle: ev_type = "IDLE"
        elif ev_type == "geofenceEnter": ev_type = "GEO_IN"
        elif ev_type == "geofenceExit": ev_type = "GEO_OUT"
            
        if ev_type not in ("REFILL", "DRAIN", "GEO_IN", "GEO_OUT", "OVERSPEED", "SENSOR_VALUE", "IDLE"):
            continue

        created_at_raw = item.get("createdAt")
        try:
            created_at = datetime.fromisoformat(created_at_raw)
        except (TypeError, ValueError):
            continue
            
        if created_at < start_time:
            continue  

        try:
            meta = json.loads(item.get("metadata", "{}"))
            
            if ev_type in ("REFILL", "DRAIN"):
                finish = meta.get("fuelFinishValue", "невідомо")
                start_val = meta.get("fuelStartValue", "невідомо")
                volume = meta.get("fuelVolume")
                if volume is None:
                    if isinstance(finish, (int, float)) and isinstance(start_val, (int, float)):
                        volume = round(abs(finish - start_val), 2)
                    else:
                        volume = "невідомо"
                
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id}): об'єм={volume} л.")
                if ev_type == "REFILL":
                    found_refill = True
                    event_str = f"REFILL ({volume} л)"
                    if event_str not in details_list: details_list.append(event_str)
            
            elif ev_type in ("GEO_IN", "GEO_OUT"):
                zone_name = meta.get("geofenceName", "невідома зона")
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id}): зона='{zone_name}'")
                
                if ev_type == "GEO_IN":
                    found_geo_in = True
                    event_str = "ВХІД (GEO_IN)"
                    if event_str not in details_list: details_list.append(event_str)
                elif ev_type == "GEO_OUT":
                    found_geo_out = True
                    event_str = "ВИХІД (GEO_OUT)"
                    if event_str not in details_list: details_list.append(event_str)

            elif ev_type == "OVERSPEED":
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id})")
                found_overspeed = True
                event_str = "ПЕРЕВИЩЕННЯ ШВИДКОСТІ"
                if event_str not in details_list: details_list.append(event_str)

            elif ev_type == "SENSOR_VALUE":
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id})")
                found_sensor = True
                event_str = "ДАТЧИК (SENSOR_VALUE)"
                if event_str not in details_list: details_list.append(event_str)

            elif ev_type == "IDLE":
                log.info(f"✨ ЗНАЙДЕНО ПОДІЮ {ev_type} (id={event_id})")
                found_idle = True
                event_str = "ПРОСТІЙ (IDLE)"
                if event_str not in details_list: details_list.append(event_str)
                    
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    if found_refill and found_geo_in and found_geo_out and found_overspeed and found_sensor and found_idle:
        found_str = ", ".join(details_list)
        return True, f"Знайдено всі необхідні події:\n{found_str}"

    missing = []
    if not found_refill: missing.append("REFILL")
    if not found_geo_out: missing.append("ВИХІД (GEO_OUT)")
    if not found_geo_in: missing.append("ВХІД (GEO_IN)")
    if not found_overspeed: missing.append("ШВИДКІСТЬ")
    if not found_sensor: missing.append("ДАТЧИК")
    if not found_idle: missing.append("ПРОСТІЙ")
    
    found_str = ", ".join(details_list) if details_list else "нічого"
    return False, f"Бракує: {', '.join(missing)}. Знайдено поки що: {found_str}"


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
            return False, f"Таймаут ({cfg['max_wait_sec']} сек). {detail}"
            
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
        "🔴 Конвеєр сповіщень працює з перебоями!\n"
        f"Canary перевірка не дочекалася всіх подій.\n{detail}\n"
        "Ймовірно завис worker/consumer на беку -- перевір логи."
    )
    log.error(text)
    show_popup(cfg, "Canary: ПРОБЛЕМА", text, is_error=True)
    send_telegram(cfg, text)
    send_slack(cfg, text)


def notify_recovery(cfg: dict) -> None:
    text = "🟢 Конвеєр сповіщень відновився, canary-перевірка пройдена повністю."
    log.info(text)
    show_popup(cfg, "Canary: Відновлено", text, is_error=False)
    send_telegram(cfg, text)
    send_slack(cfg, text)


def notify_success(cfg: dict, detail: str) -> None:
    text = f"✅ Тестування завершено успішно!\n\n{detail}"
    show_popup(cfg, "Canary: УСПІХ", text, is_error=False)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def inspect_mode(cfg: dict) -> int:
    try:
        data = fetch_history(cfg)
        items = data.get("items", [])
        print(f"Всього записів на сторінці: {len(items)}")
        for item in items[:10]:
            print(f"  id={item.get('id')} type={item.get('type')} deviceId={item.get('deviceId')} "
                  f"deviceName={item.get('deviceName')!r} createdAt={item.get('createdAt')}")
        return 0
    except Exception as e:
        log.error(f"Помилка під час інспекції: {e}")
        return 1


def main() -> int:
    cfg = CONFIG

    if "--inspect" in sys.argv:
        return inspect_mode(cfg)

    # Перевірка наявності хоча б якогось способу авторизації
    if not cfg["api_token"] and not (cfg["api_email"] and cfg["api_password"]):
        log.error("🚨 Не задано облікових даних! Вкажіть M2M_API_TOKEN або пару M2M_EMAIL та M2M_PASSWORD.")
        return 1

    state = load_state(cfg)

    log.info("Збираю поточний стан історії для фільтрації старих подій...")
    try:
        initial_event_ids = get_existing_event_ids(cfg)
        log.info("Знайдено старих подій в історії: %d (вони будуть ігноруватись)", len(initial_event_ids))
    except PermissionError as e:
        log.error(f"🚨 ПОМИЛКА АВТОРИЗАЦІЇ: {e}")
        return 1
    except ValueError as e:
        log.error(f"🚨 ПОМИЛКА КОНФІГУРАЦІЇ: {e}")
        return 1

    try:
        start_time = send_canary_events(cfg)
    except Exception as e:
        log.exception("Не вдалось відправити canary-подію")
        notify_incident(cfg, f"Помилка відправки тестових подій: {e}")
        return 1

    log.info("Очікую появу подій (перевірятиму до %d хв)...", cfg["max_wait_sec"] // 60)

    try:
        ok, detail = wait_for_events(cfg, start_time, initial_event_ids)
    except PermissionError as e:
        log.error(f"🚨 ПОМИЛКА АВТОРИЗАЦІЇ ПІД ЧАС ПЕРЕВІРКИ: {e}")
        notify_incident(cfg, "Помилка авторизації під час очікування. Перевірте облікові дані.")
        return 1
    except Exception as e:
        log.exception("Не вдалось перевірити історію через API")
        notify_incident(cfg, f"Помилка запиту до API історії: {e}")
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
