#!/usr/bin/env python3
"""
Канареечний скрипт (~90 хв): [DEV] 5_canary_triple_dual_sensor [90m].
Цільовий об'єкт: Refill_Drain_UNIT_triple (IMEI: 87811433972935812345, два датчики: fuel=io_201, fuel_Adblue=io_203).

Сценарій із 6 подіями по двох датчиках рівня пального (~90 хв):
  1. Початкова стоянка (3 хв) + Поїздка 1 по М-07 (6.5 хв, 6.78 км).
  2. Стоянка 1 (22 хв): пауза 6 хв + дві послідовні заправки впритул на io_201 (стиль Double 1316: +112 л + +245 л) + плато 5 хв.
  3. Поїздка 2 по М-07 (5.5 хв, 5.96 км).
  4. Стоянка 2 (21.25 хв): пауза 5 хв + Заправка #3 на io_201 (4.5 хв, +150 л) + пауза 3 хв + Заправка #4 на io_203 (3.75 хв, +65 л) + плато 5 хв.
  5. Поїздка 3 по М-07 (5.5 хв, 5.97 км).
  6. Стоянка 3 (20 хв): пауза 5 хв + Злив #1 на io_201 (3.75 хв, -215 л) + пауза 3 хв + Злив #2 на io_203 (3.25 хв, -50 л) + плато 5 хв.
  7. Фінальна поїздка 4 по М-07 (3 хв, 2.95 км) + фінішна стоянка (3 хв).

Перевірка: пагіноване збирання всіх сповіщень з історії та звірка 1-в-1 зі Звітом v2 №1180 по датчиках.
"""

import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import TrackPacer, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_triple")

CONFIG = {
    "device_id": None,
    "device_name": "Refill_Drain_UNIT_triple",
    "report_id": 1180,
    "tracker_imei": "87811433972935812345",
    "scenario_mode": "triple",
    "scenario_label": "[DEV] 5_canary_triple_dual_sensor [90m]",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка",
    "template_drain": "Злив",
    "expected_refills": 4,
    "expected_drains": 2,

    "altitude": 178,
    "bearing": 82,
    "sat": 14,

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
    "state_file": Path(os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_triple_dev_state.json")),
}


def send_canary_events(cfg: dict) -> float:
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=82)

    lat_s0, lon_s0 = 51.1941494, 23.8251173
    lat_s1, lon_s1 = 51.2055949, 23.9209032
    lat_s2, lon_s2 = 51.2021945, 24.0031504
    lat_s3, lon_s3 = 51.2042110, 24.0889386
    lat_end, lon_end = 51.2065306, 24.1318402

    # 1. Початкова стоянка (3 хв) + Поїздка 1 по М-07 (6.5 хв, 6.78 км)
    log.info(">>> ЕТАП 1: Початкова стоянка (3 хв) та Поїздка 1 по М-07 (io_201: 122->118 л, io_203: 26->25 л)...")
    pacer.send_plateau(level=122, lat=lat_s0, lon=lon_s0, count=12, extra_ios={203: 26})
    trip1_points = [
        (51.1941575, 23.8254725, 12), (51.1942100, 23.8265310, 18), (51.1943224, 23.8282747, 29),
        (51.1944761, 23.8306804, 40), (51.1946374, 23.8337173, 51), (51.1948493, 23.8373353, 61),
        (51.1951793, 23.8414669, 70), (51.1957689, 23.8460085, 78), (51.1964252, 23.8509409, 84),
        (51.1971075, 23.8562026, 85), (51.1978324, 23.8617079, 85), (51.1985797, 23.8673797, 85),
        (51.1993343, 23.8731369, 85), (51.2000887, 23.8788943, 85), (51.2008324, 23.8845677, 85),
        (51.2015537, 23.8900746, 85), (51.2022435, 23.8953344, 85), (51.2028905, 23.9002707, 84),
        (51.2034853, 23.9048115, 78), (51.2040190, 23.9088906, 70), (51.2044867, 23.9124473, 61),
        (51.2048775, 23.9154316, 51), (51.2051877, 23.9177991, 40), (51.2054130, 23.9195151, 29),
        (51.2055492, 23.9205549, 18), (lat_s1, lon_s1, 0),
    ]
    pacer.send_trip(trip1_points, start_level=122, end_level=118, extra_levels={203: (26, 25)})

    # 2. Стоянка 1: 6 хв пауза + Заправка #1 і #2 впритул по io_201 (як Double 1316)
    log.info(">>> ЕТАП 2: Стоянка 1 (6 хв) + Заправка #1 [io_201, +112 л, 1316] -> 2 с -> Заправка #2 [io_201, +245 л]...")
    pacer.send_plateau(level=118, lat=lat_s1, lon=lon_s1, count=24, extra_ios={203: 25})
    pacer.send_sparse_steps([(75.0, 129), (150.0, 178), (165.0, 230)], lat=lat_s1, lon=lon_s1, extra_ios={203: 25})
    pacer.advance(2.0)
    refill_2_curve = [237, 250, 266, 284, 304, 325, 347, 369, 391, 412, 431, 447, 459, 467, 471, 473, 474, 475]
    pacer.send_curve(refill_2_curve, lat=lat_s1, lon=lon_s1, extra_ios={203: 25})
    pacer.send_plateau(level=475, lat=lat_s1, lon=lon_s1, count=20, extra_ios={203: 25})

    # 3. Поїздка 2 по М-07 (5.5 хв, 5.96 км)
    log.info(">>> ЕТАП 3: Поїздка 2 по М-07 (io_201: 475->472 л, io_203: 25 л)...")
    trip2_points = [
        (51.2056510, 23.9213323, 14), (51.2058185, 23.9226107, 22), (51.2060672, 23.9247196, 36),
        (51.2060093, 23.9276562, 49), (51.2052317, 23.9310986, 61), (51.2036130, 23.9345870, 73),
        (51.2017074, 23.9384521, 82), (51.1997117, 23.9427964, 85), (51.1988226, 23.9483125, 85),
        (51.1993009, 23.9542621, 85), (51.1999176, 23.9603101, 85), (51.2005146, 23.9663631, 85),
        (51.2010965, 23.9722938, 85), (51.2015613, 23.9780006, 85), (51.2017411, 23.9833842, 85),
        (51.2018520, 23.9882991, 82), (51.2019506, 23.9926404, 73), (51.2020347, 23.9963197, 62),
        (51.2021065, 23.9992617, 49), (51.2021561, 24.0014073, 36), (51.2021851, 24.0027123, 22),
        (lat_s2, lon_s2, 0),
    ]
    pacer.send_trip(trip2_points, start_level=475, end_level=472, extra_levels={203: (25, 25)})

    # 4. Стоянка 2: Заправка #3 [io_201] -> через 3 хв Заправка #4 [io_203]
    log.info(">>> ЕТАП 4: Стоянка 2 (5 хв) + Заправка #3 [io_201, +150 л] -> пауза 3 хв -> Заправка #4 [io_203, +65 л]...")
    pacer.send_plateau(level=472, lat=lat_s2, lon=lon_s2, count=20, extra_ios={203: 25})
    refill_3_curve = [477, 487, 499, 513, 528, 543, 558, 573, 587, 599, 608, 614, 618, 620, 621, 621.5, 622, 622]
    pacer.send_curve(refill_3_curve, lat=lat_s2, lon=lon_s2, extra_ios={203: 25})
    pacer.send_plateau(level=622, lat=lat_s2, lon=lon_s2, count=12, extra_ios={203: 25})

    refill_4_adblue = [28, 33, 39, 46, 53, 60, 67, 74, 80, 84, 87, 88.5, 89.5, 90, 90]
    for ad_lvl in refill_4_adblue:
        pacer.emit(level=622, lat=lat_s2, lon=lon_s2, speed=0, extra_ios={203: ad_lvl})
    pacer.send_plateau(level=622, lat=lat_s2, lon=lon_s2, count=20, extra_ios={203: 90})

    # 5. Поїздка 3 по М-07 (5.5 хв, 5.97 км)
    log.info(">>> ЕТАП 5: Поїздка 3 по М-07 (io_201: 622->619 л, io_203: 90->89 л)...")
    trip3_points = [
        (51.2022033, 24.0035884, 14), (51.2022319, 24.0048935, 22), (51.2022822, 24.0070390, 36),
        (51.2023474, 24.0099815, 49), (51.2024078, 24.0136617, 62), (51.2021937, 24.0179878, 73),
        (51.2017623, 24.0228575, 82), (51.2013033, 24.0281993, 85), (51.2012834, 24.0339466, 85),
        (51.2016034, 24.0399279, 85), (51.2019328, 24.0460331, 85), (51.2022483, 24.0521400, 85),
        (51.2025395, 24.0581252, 85), (51.2028512, 24.0638600, 85), (51.2031431, 24.0692320, 85),
        (51.2034083, 24.0741321, 82), (51.2036425, 24.0784602, 73), (51.2038410, 24.0821284, 62),
        (51.2039998, 24.0850619, 49), (51.2041169, 24.0872008, 36), (51.2041876, 24.0885019, 22),
        (lat_s3, lon_s3, 0),
    ]
    pacer.send_trip(trip3_points, start_level=622, end_level=619, extra_levels={203: (90, 89)})

    # 6. Стоянка 3: Злив #1 [io_201] -> 3 хв пауза -> Злив #2 [io_203] -> Поїздка 4 + Фініш
    log.info(">>> ЕТАП 6: Стоянка 3 (5 хв) + Злив #1 [io_201, -215 л] -> пауза 3 хв -> Злив #2 [io_203, -50 л]...")
    pacer.send_plateau(level=619, lat=lat_s3, lon=lon_s3, count=20, extra_ios={203: 89})
    drain_1_curve = [612, 600, 584, 566, 546, 525, 504, 483, 463, 445, 430, 418, 410, 406, 404]
    pacer.send_curve(drain_1_curve, lat=lat_s3, lon=lon_s3, extra_ios={203: 89})
    pacer.send_plateau(level=404, lat=lat_s3, lon=lon_s3, count=12, extra_ios={203: 89})

    drain_2_adblue = [86, 82, 77, 71, 65, 59, 53, 48, 44, 41.5, 40, 39.5, 39]
    for ad_lvl in drain_2_adblue:
        pacer.emit(level=404, lat=lat_s3, lon=lon_s3, speed=0, extra_ios={203: ad_lvl})
    pacer.send_plateau(level=404, lat=lat_s3, lon=lon_s3, count=20, extra_ios={203: 39})

    log.info(">>> ЕТАП 7: Поїздка 4 (3 хв) та фінішна стоянка (3 хв)...")
    trip4_points = [
        (51.2042503, 24.0896695, 23), (51.2043658, 24.0918125, 36), (51.2045613, 24.0952198, 57),
        (51.2048106, 24.0996611, 75), (51.2050826, 24.1048358, 85), (51.2053775, 24.1103885, 85),
        (51.2056595, 24.1159426, 85), (51.2059357, 24.1211167, 85), (51.2061912, 24.1255572, 75),
        (51.2063670, 24.1289674, 57), (51.2064855, 24.1311100, 36), (lat_end, lon_end, 0),
    ]
    pacer.send_trip(trip4_points, start_level=404, end_level=402, extra_levels={203: (39, 39)})
    pacer.send_plateau(level=402, lat=lat_end, lon=lon_end, count=12, extra_ios={203: 39})

    log.info("Усі 336 точок [Triple Dual-Sensor: 4 Refills + 2 Drains] відправлено успішно!")
    return time.time()


if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
