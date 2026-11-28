#!/usr/bin/env python3
"""
Повний регресійний прогін PROD (~5 год): [PROD] Teltonika Multievent Regression [5h, 300 BB].
Об'єкт: Test_Notification_o.rosenko (device_id: 9405, IMEI: 123456789011111).
Сервер прийому: 213.239.234.94:5027 | API: https://my.m2m.eu/api.

Перевіряє всі типи сповіщень на PROD без прив'язки до звітів (по фактах та статистиці в історії):
  1. Пальне (REFILL x3 включно з Double 1316 + DRAIN x2), плюс 3-год сон CAN.
  2. Геозони (GEO_OUT x3 / GEO_IN x3 між 49.438923,32.085565 та 49.430000,32.085565).
  3. Перевищення швидкості (OVERSPEED: розгони 110..120 км/год).
  4. Значення датчика (SENSOR: подвійний вхід у тригер 701..801 л по io_201 -> 755 л і 760 л).
  5. Простій (IDLE: тривалі стоянки > 5 хв із ignition=1).
  6. Результат команди (COMMAND: Teltonika Codec 12 CommandSentSuccess x2).
  7. Офлайн blackbox на 300 точок (6 пакетів Codec 8 по 50 записів) із подіями всередині.
"""

import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import TrackPacer, run_multievent_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_multievent_prod")

CONFIG = {
    "device_id": 9405,
    "device_name": "Test_Notification_o.rosenko",
    "tracker_imei": "123456789011111",
    "scenario_mode": "multievent_prod",
    "scenario_label": "[PROD] Teltonika Multievent [5h, 300 BB]",

    "ingest_host": "213.239.234.94",
    "ingest_port": 5027,

    # Всі налаштовані на Test_Notification_o.rosenko шаблони сповіщень
    "template_refill": "TEST_Notification-Rosenko_Refill",
    "template_drain": "TEST_Notification-Rosenko_Drain",
    "template_geo_in": "TEST_Notification-Rosenko_GEOFENCE_IN",
    "template_geo_out": "TEST_Notification-Rosenko_GEOFENCE_OUT",
    "template_overspeed": "TEST_Notification-Rosenko_Overspeed",
    "template_sensor": "TEST_Notification-Rosenko_SensorValue",
    "template_idle": "TEST_Notification-Rosenko_Idle",
    "template_command": "Результат команди",

    # Очікувані пороги аудиту (пальне — точно; інші — мінімум подій без пропусків і дублів)
    "expected_refills": 3,
    "expected_drains": 2,
    "expected_min_counts": {
        "GEO_OUT": 2,
        "GEO_IN": 2,
        "OVERSPEED": 2,
        "SENSOR": 2,
        "IDLE": 2,
        "COMMAND": 2,
    },

    "geo_in_lat": 49.438923,
    "geo_in_lon": 32.085565,
    "geo_out_lat": 49.430000,
    "geo_out_lon": 32.085565,
    "altitude": 10,
    "bearing": 210,
    "sat": 12,

    "initial_wait_sec": 480,
    "poll_interval_sec": 30,
    "max_wait_sec": 600,
    "http_timeout_sec": 60,

    "api_base_url": os.getenv("M2M_API_BASE_URL", "https://my.m2m.eu/api"),
    "api_email": os.getenv("M2M_EMAIL", ""),
    "api_password": os.getenv("M2M_PASSWORD", ""),
    "api_token": os.getenv("M2M_API_TOKEN", ""),
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 100},

    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_double_prod_state.json")
    ),
}


def _linear_waypoints(
    lat0: float,
    lon0: float,
    lat1: float,
    lon1: float,
    count: int,
    speed: int,
) -> list[tuple[float, float, int]]:
    """Генерує лінійну траєкторію між двома координатами з однаковою швидкістю."""
    if count <= 1:
        return [(lat1, lon1, speed)]
    return [
        (
            lat0 + (lat1 - lat0) * (i / (count - 1)),
            lon0 + (lon1 - lon0) * (i / (count - 1)),
            speed,
        )
        for i in range(count)
    ]


def _ramp(start_lvl: float, end_lvl: float, count: int) -> list[float]:
    return [round(start_lvl + (end_lvl - start_lvl) * (i / count), 1) for i in range(1, count + 1)]


def send_canary_events(cfg: dict) -> dict:
    """Надсилає ~4г41хв регресійний трек із 8 типами подій та 300-точковим blackbox (batch=50)."""
    start_wall = time.time()
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=180)
    lat_in, lon_in = cfg["geo_in_lat"], cfg["geo_in_lon"]
    lat_out, lon_out = cfg["geo_out_lat"], cfg["geo_out_lon"]
    lat_far = lat_out - 0.012  # ~2.3 км на південь від геозони для реалістичних 115..120 км/год

    # 1. СТАРТ У ГЕОЗОНІ -> ВИЇЗД (GEO_OUT #1) -> РОЗГІН (OVERSPEED #1) -> В'ЇЗД (GEO_IN #1) (~7.5 хв)
    log.info(">>> ЕТАП 1: Геозона OUT #1 + Перевищення #1 (115 км/год) + Геозона IN #1...")
    pacer.send_plateau(360, lat_in, lon_in, count=4)
    pacer.send_trip(_linear_waypoints(lat_in, lon_in, lat_out, lon_out, 6, 40), 360, 360)
    pacer.send_plateau(360, lat_out, lon_out, count=6)
    pacer.send_trip(_linear_waypoints(lat_out, lon_out, lat_far, lon_out, 4, 115), 360, 360)
    pacer.send_trip(_linear_waypoints(lat_far, lon_out, lat_in, lon_in, 6, 115), 360, 360)
    pacer.send_plateau(360, lat_in, lon_in, count=4)

    # 2. СТОЯНКА 1 У ГЕОЗОНІ: Double 1316 (+170 л, +225 л -> 755 л) + SENSOR #1 + COMMAND #1 + IDLE #1 (~19 хв)
    log.info(">>> ЕТАП 2: Стоянка 1 (Double 1316: +170 л та +225 л -> 755 л [Sensor], Codec 12 #1, Idle #1)...")
    pacer.send_plateau(360, lat_in, lon_in, count=16)
    pacer.send_curve(_ramp(360, 530, 12), lat_in, lon_in)  # Заправка #1: +170 л (3 хв)
    pacer.send_plateau(530, lat_in, lon_in, count=11)
    pacer.emit(level=530, lat=lat_in, lon=lon_in, speed=0, command_result="CommandSentSuccess")
    pacer.send_curve(_ramp(530, 755, 12), lat_in, lon_in)  # Заправка #2: +225 л -> 755 л (тригер 701-801)
    pacer.send_plateau(755, lat_in, lon_in, count=24)  # 6 хв (> 5 хв для Idle #1 + утримання 755 л)

    # 3. ВИЇЗД У GEO_OUT #2 -> ЗЛИВ #1 (-360 л) + IDLE #2 -> РОЗГІН (OVERSPEED #2) -> GEO_IN #2 -> ЗАПРАВКА #3 (+365 л, SENSOR #2) (~24.5 хв)
    log.info(">>> ЕТАП 3: Виїзд (GEO_OUT #2) -> Злив #1 (-360 л) -> OVERSPEED #2 -> GEO_IN #2 -> Заправка #3 (+365 л)...")
    pacer.send_trip(_linear_waypoints(lat_in, lon_in, lat_out, lon_out, 6, 40), 755, 755)
    pacer.send_plateau(755, lat_out, lon_out, count=8)
    pacer.send_curve(_ramp(755, 395, 16), lat_out, lon_out)  # Злив #1: -360 л (нижче порогу 701)
    pacer.send_plateau(395, lat_out, lon_out, count=24)  # 6 хв стоянки (> 5 хв для Idle #2)
    pacer.send_trip(_linear_waypoints(lat_out, lon_out, lat_far, lon_out, 4, 110), 395, 395)
    pacer.send_trip(_linear_waypoints(lat_far, lon_out, lat_in, lon_in, 6, 110), 395, 395)
    pacer.send_plateau(395, lat_in, lon_in, count=4)
    pacer.send_curve(_ramp(395, 760, 12), lat_in, lon_in)  # Заправка #3: +365 л -> 760 л (повторний Sensor #2)
    pacer.send_plateau(760, lat_in, lon_in, count=20)

    # 4. ДОВГИЙ 3-ГОДИННИЙ ХВІСТ СНУ CAN (180 хв) -> перевірка стабільності 3-год вікна
    log.info(">>> ЕТАП 4: 3-годинний сон CAN (18 точок по 10 хв на 760 л)...")
    pacer.send_plateau(760, lat_in, lon_in, count=18, step_sec=600.0)

    # 5. ФІНАЛЬНИЙ BLACKBOX-БУФЕР НА 300 ТОЧОК (50 хв офлайн, крок 10 с, 6 пакетів Codec 8 по 50)
    log.info(">>> ЕТАП 5: Буферизація 300 точок blackbox (50 хв: GEO_OUT #3 + OVERSPEED #3 + DRAIN #2 -410 л + GEO_IN #3)...")
    pacer.begin_blackbox(start_delay_sec=10.0)
    pacer.step_sec = 10.0
    try:
        # 18 тч (3 хв): виїзд з geo_in + розгін 120 км/год -> geo_out
        pacer.send_trip(_linear_waypoints(lat_in, lon_in, lat_out, lon_out, 6, 45), 760, 760)
        pacer.send_trip(_linear_waypoints(lat_out, lon_out, lat_far, lon_out, 6, 120), 760, 760)
        pacer.send_trip(_linear_waypoints(lat_far, lon_out, lat_out, lon_out, 6, 80), 760, 760)
        # 36 тч (6 хв): стоянка у geo_out перед зливом
        pacer.send_plateau(760, lat_out, lon_out, count=36)
        # 30 тч (5 хв): Злив #2 (-410 л: 760 -> 350 л) всередині blackbox
        pacer.send_curve(_ramp(760, 350, 30), lat_out, lon_out)
        # 90 тч (15 хв): плато після зливу на 350 л (Idle #3 + закриття медіани)
        pacer.send_plateau(350, lat_out, lon_out, count=90)
        # 24 тч (4 хв): виїзд назад у geo_in з ділянкою 118 км/год (Overspeed #4 + GEO_IN #3)
        pacer.send_trip(_linear_waypoints(lat_out, lon_out, lat_far, lon_out, 8, 118), 350, 350)
        pacer.send_trip(_linear_waypoints(lat_far, lon_out, lat_in, lon_in, 16, 118), 350, 350)
        # 102 тч (17 хв): фінішна стоянка у geo_in (закриває поїздку й 15-хв таймаут після зливу)
        pacer.send_plateau(350, lat_in, lon_in, count=102)
    finally:
        pacer.step_sec = 15.0

    pacer.flush_blackbox(wait_until_end=True, batch_size=50)
    # Контрольна онлайн-точка + Codec 12 Command #2 після вивантаження 300-точкового батчу
    pacer.emit(level=350, lat=lat_in, lon=lon_in, speed=0, command_result="CommandSentSuccess")

    log.info(
        "Трек завершено: %d онлайн + %d blackbox (%d пакетів) за %.1f хв",
        pacer.online_points,
        pacer.blackbox_points,
        pacer.blackbox_batches,
        (time.time() - start_wall) / 60.0,
    )
    return {
        "online_points": pacer.online_points,
        "blackbox_points": pacer.blackbox_points,
        "blackbox_batches": pacer.blackbox_batches,
        "duration_sec": time.time() - start_wall,
    }


if __name__ == "__main__":
    sys.exit(run_multievent_canary(CONFIG, send_canary_events))
