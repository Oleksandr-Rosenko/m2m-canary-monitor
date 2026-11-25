#!/usr/bin/env python3
"""
Канареечний скрипт (~47 хв): [DEV] Canary Monitor (Teltonika Double: Back-to-Back Refills + Drain).
Цільовий об'єкт: Refill_Drain_UNIT_double (id=47614, IMEI: 68162416785325812345, фільтр N=35).

Відтворює кейс DAF CA 1316 (дві реальні заправки впритул + рідкі точки на першій) у межах ~47 хв
з достатнім буфером точок (20+ точок = 5+ хв до і після) під медіанний фільтр N=35:
  1. Початкова стоянка на Митниці у Черкасах (1 хв) + Поїздка 1 по вул. Володимира Великого (2.75 хв, 1.54 км).
  2. Стоянка 1 на АЗС Укрнафта (23 хв):
     - щільна полиця 7 хв (28 точок >> N/2=17), щоб старт не виїхав за межі STOP;
     - Заправка A у стилі 1316 (6.5 хв = 390 с > 300 с, рідкі точки через 75/150/165 с, +112 л: 126 -> 238 л);
     - Заправка B одразу через 2 с після A (4.5 хв, щільні 15 с точки, +290 л: 238 -> 528 л);
     - плато стабілізації 5 хв (20 точок > 17).
  3. Коротка поїздка 2 по Черкасах (2.25 хв, 1.22 км -> закриває стоянку 1).
  4. Стоянка 2 (13.75 хв): полиця 5 хв (20 точок) + Злив 3.75 хв (-265 л: 526 -> 261 л) + плато 5 хв (20 точок).
  5. Поїздка 3 (2.75 хв, 1.39 км) + фінішна стоянка (2 хв).

Перевірка: звірка 1-в-1 зі Звітом v2 №1180 по metadata.fuelStartTime.
"""

import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import TrackPacer, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_double")

CONFIG = {
    "device_id": 47614,
    "device_name": "Refill_Drain_UNIT_double",
    "report_id": 1180,
    "tracker_imei": "68162416785325812345",
    "scenario_mode": "double",
    "scenario_label": "[DEV] [Teltonika Double 1316+Drain]",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка (Сценарій 2 - Double)",
    "template_drain": "Злив (Сценарій 2 - Double)",
    "expected_refills": 2,
    "expected_drains": 1,

    "altitude": 102,
    "bearing": 285,
    "sat": 13,

    "api_base_url": os.getenv("M2M_API_BASE_URL", "https://dev-stag.m2m.eu/api"),
    "api_email": os.getenv("M2M_EMAIL", ""),
    "api_password": os.getenv("M2M_PASSWORD", ""),
    "api_token": os.getenv("M2M_API_TOKEN", ""),
    "login_path": "/login",
    "history_path": "/notification/history",
    "history_query": {"page": 1, "per_page": 100},

    "initial_wait_sec": 60,
    "poll_interval_sec": 20,
    "max_wait_sec": 1800,
    "http_timeout_sec": 60,

    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_double_dev_state.json")),
}


def send_canary_events(cfg: dict) -> float:
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=285)

    lat_park_start, lon_park_start = 49.4389230, 32.0855650   # Митниця
    lat_gas, lon_gas = 49.4445840, 32.0722610                 # АЗС Укрнафта (вул. Володимира Великого)
    lat_drain, lon_drain = 49.4482960, 32.0597260             # Стоянка для зливу (б-р Шевченка)
    lat_park_end, lon_park_end = 49.4554100, 32.0465530       # Фінішна стоянка

    # 1. Початкова стоянка (1 хв) + Поїздка 1 по Черкасах (2.75 хв, 1.54 км)
    log.info(">>> ЕТАП 1: Початкова стоянка на Митниці (1 хв) та поїздка до АЗС Укрнафта (128 -> 126 л)...")
    pacer.send_plateau(level=128, lat=lat_park_start, lon=lon_park_start, count=4)
    trip1_points = [
        (49.4389887, 32.0838235, 22),
        (49.4397184, 32.0823141, 38),
        (49.4410230, 32.0804261, 48),
        (49.4397887, 32.0782886, 52),
        (49.4384375, 32.0761466, 52),
        (49.4396114, 32.0740755, 52),
        (49.4412672, 32.0728553, 50),
        (49.4428933, 32.0725124, 44),
        (49.4440410, 32.0720592, 32),
        (lat_gas, lon_gas, 16),
        (lat_gas, lon_gas, 0),
    ]
    pacer.send_trip(trip1_points, start_level=128, end_level=126)

    # 2. Стоянка 1 на АЗС (23 хв) — ДВІ ЗАПРАВКИ ВПРИТУЛ (кейс 1316 під N=35)
    log.info(">>> ЕТАП 2.1: Передзаправна полиця на АЗС (7 хв = 28 точок під фільтр N=35, рівень 126 л)...")
    pacer.send_plateau(level=126, lat=lat_gas, lon=lon_gas, count=28)

    log.info(">>> ЕТАП 2.2: Заправка A у стилі 1316 (6.5 хв = 390 с > 300 с, рідкі точки, +112 л: 126 -> 238 л)...")
    pacer.send_sparse_steps([(75.0, 136), (150.0, 188), (165.0, 238)], lat=lat_gas, lon=lon_gas)

    log.info(">>> ЕТАП 2.3: Заправка B одразу через 2 с після A (4.5 хв, +290 л: 238 -> 528 л) + стабілізація 5 хв...")
    pacer.advance(2.0)
    refill_b_curve = [
        245, 259, 276, 296, 318, 341, 365, 389, 413,
        437, 459, 479, 497, 511, 521, 525, 527, 528,
    ]
    pacer.send_curve(refill_b_curve, lat=lat_gas, lon=lon_gas)
    pacer.send_plateau(level=528, lat=lat_gas, lon=lon_gas, count=20)

    # 3. Коротка поїздка 2 по Черкасах (2.25 хв, 1.22 км)
    log.info(">>> ЕТАП 3: Коротка поїздка 2 від АЗС до стоянки (1.22 км, витрата 528 -> 526 л)...")
    trip2_points = [
        (49.4452427, 32.0715053, 24),
        (49.4467946, 32.0708498, 43),
        (49.4486270, 32.0698932, 54),
        (49.4494556, 32.0669864, 56),
        (49.4499813, 32.0638784, 56),
        (49.4494822, 32.0616972, 49),
        (49.4486276, 32.0605749, 34),
        (lat_drain, lon_drain, 17),
        (lat_drain, lon_drain, 0),
    ]
    pacer.send_trip(trip2_points, start_level=528, end_level=526)

    # 4. Стоянка 2 + Злив (5 хв полиця + 3.75 хв злив -265 л + 5 хв плато під N=35)
    log.info(">>> ЕТАП 4: Стоянка під злив — полиця (5 хв) + Злив (3.75 хв, 526 -> 261 л, -265 л) + плато (5 хв)...")
    pacer.send_plateau(level=526, lat=lat_drain, lon=lon_drain, count=20)
    drain_curve = [519, 505, 487, 466, 443, 419, 395, 371, 348, 326, 306, 289, 276, 267, 261]
    pacer.send_curve(drain_curve, lat=lat_drain, lon=lon_drain)
    pacer.send_plateau(level=261, lat=lat_drain, lon=lon_drain, count=20)

    # 5. Поїздка 3 + фінішна стоянка
    log.info(">>> ЕТАП 5: Поїздка 3 (1.39 км) та фінішна стоянка (закриває злив у ядрі M2M)...")
    trip3_points = [
        (49.4488996, 32.0609452, 24),
        (49.4484998, 32.0586090, 42),
        (49.4490667, 32.0557706, 52),
        (49.4501500, 32.0537800, 56),
        (49.4513200, 32.0516400, 56),
        (49.4524488, 32.0495676, 56),
        (49.4534500, 32.0477100, 52),
        (49.4541500, 32.0469200, 44),
        (49.4548758, 32.0469184, 30),
        (lat_park_end, lon_park_end, 15),
        (lat_park_end, lon_park_end, 0),
    ]
    pacer.send_trip(trip3_points, start_level=261, end_level=259)
    pacer.send_plateau(level=259, lat=lat_park_end, lon=lon_park_end, count=8)

    log.info("Усі 167 точок [Short Double 1316: 2 Back-to-Back Refills + Drain] відправлено успішно!")
    return time.time()


if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
