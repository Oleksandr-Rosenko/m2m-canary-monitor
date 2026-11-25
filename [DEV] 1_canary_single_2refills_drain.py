#!/usr/bin/env python3
"""
Канареечний скрипт (~42 хв): [DEV] Canary Monitor (Teltonika Single: 2 Refills + Trip + Drain).
Цільовий об'єкт: Refill_Drain_UNIT (id=47613, IMEI: 12345678901234567890, фільтр N=15).

Швидкий регулярний прогін (5-10 хв до та після подій, достатньо для стабілізації медіани N=15):
  1. Початкова стоянка на TIR-паркінгу Старовойтове (4 хв, 16 точок по 15 с, 86 л).
  2. Поїздка 1 по М-07 до АЗС БРСМ-Нафта Римачі (4 хв, 4.57 км, до 82 км/год, 86 -> 82 л).
  3. Стоянка 1 на БРСМ (8 хв > 300 с) + Заправка #1 (5.5 хв, +340 л: 82 -> 422 л).
  4. Поїздка 2 по М-07 до АЗС Укрнафта Римачі (2.5 хв, 1.77 км, 422 -> 420 л).
  5. Стоянка 2 на Укрнафті (8.5 хв > 300 с) + Заправка #2 з розривом зв'язку 2.25 хв посередині (як DAF 4130, +110 л: 420 -> 530 л).
  6. Поїздка 3 по М-07 до паркінгу Вишнів (2.75 хв, 2.20 км, 530 -> 528 л).
  7. Стоянка 3 у Вишневі (8 хв > 300 с) + Злив (3.5 хв, -96 л: 528 -> 432 л) + плато стабілізації 3 хв.
  8. Поїздка 4 по М-07 (2.25 хв, 1.62 км) + фінішна стоянка (3 хв) — сумарно 29 точок після зливу (>> N/2 = 7).

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
log = logging.getLogger("canary_teltonika_single_2refill_drain")

CONFIG = {
    "device_id": 47613,
    "device_name": "Refill_Drain_UNIT",
    "report_id": 1180,
    "tracker_imei": "12345678901234567890",
    "scenario_mode": "single",
    "scenario_label": "[DEV] 1_canary_single_2refills_drain [43m]",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка (Сценарій 1 - Single)",
    "template_drain": "Злив (Сценарій 1 - Single)",
    "expected_refills": 2,
    "expected_drains": 1,

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
    "max_wait_sec": 1500,
    "http_timeout_sec": 60,

    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_2refill_drain_dev_state.json")
    ),
}


def send_canary_events(cfg: dict) -> float:
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=82)

    lat_park_start, lon_park_start = 51.1941494, 23.8251173   # TIR-паркінг Старовойтове
    lat_gas1, lon_gas1 = 51.2015219, 23.8925562               # АЗС #1 БРСМ-Нафта Римачі
    lat_gas2, lon_gas2 = 51.2047870, 23.9182590               # АЗС #2 Укрнафта Римачі
    lat_drain, lon_drain = 51.1988310, 23.9475210             # Стоянка #3 Вишнів
    lat_park_end, lon_park_end = 51.2010670, 23.9719840       # Фінішна стоянка на М-07

    # 1. Початкова стоянка (4 хв = 16 точок)
    log.info(">>> ЕТАП 1: Початкова стоянка на Старовойтове (4 хв, рівень 86 л)...")
    pacer.send_plateau(level=86, lat=lat_park_start, lon=lon_park_start, count=16)

    # 2. Поїздка 1 по М-07 до БРСМ (4 хв, 4.57 км)
    log.info(">>> ЕТАП 2: Поїздка 1 по М-07 до АЗС БРСМ (86 -> 82 л)...")
    trip1_points = [
        (51.1942891, 23.8277519, 30),
        (51.1944808, 23.8307565, 50),
        (51.1948452, 23.8372635, 70),
        (51.1953430, 23.8428092, 78),
        (51.1960226, 23.8479144, 82),
        (51.1966958, 23.8530388, 80),
        (51.1974571, 23.8588648, 82),
        (51.1981831, 23.8643672, 80),
        (51.1988672, 23.8695379, 81),
        (51.1995925, 23.8751094, 80),
        (51.2006096, 23.8828697, 78),
        (51.2016774, 23.8910163, 60),
        (51.2017205, 23.8913449, 35),
        (51.2014714, 23.8921031, 15),
        (lat_gas1, lon_gas1, 0),
        (lat_gas1, lon_gas1, 0),
    ]
    pacer.send_trip(trip1_points, start_level=86, end_level=82)

    # 3. Заправка #1 на БРСМ (5.5 хв, +340 л)
    log.info(">>> ЕТАП 3: АЗС БРСМ — Заправка #1 (5.5 хв, 82 -> 422 л, +340 л)...")
    pacer.send_plateau(level=82, lat=lat_gas1, lon=lon_gas1, count=4)
    pacer.send_curve(
        [92, 108, 126, 145, 165, 185, 206, 227, 248, 269, 290, 311, 331, 351, 370, 388, 403, 413, 417, 420, 421, 422],
        lat=lat_gas1,
        lon=lon_gas1,
    )
    pacer.send_plateau(level=422, lat=lat_gas1, lon=lon_gas1, count=6)

    # 4. Поїздка 2 по М-07 до Укрнафти (2.5 хв, 1.77 км)
    log.info(">>> ЕТАП 4: Поїздка 2 по М-07 до АЗС Укрнафта (422 -> 420 л)...")
    trip2_points = [
        (51.2019658, 23.8935104, 21),
        (51.2023262, 23.8959663, 43),
        (51.2027966, 23.8995528, 61),
        (51.2033317, 23.9036377, 70),
        (51.2038662, 23.9077228, 70),
        (51.2043898, 23.9117073, 68),
        (51.2048334, 23.9150949, 58),
        (51.2051270, 23.9173367, 38),
        (lat_gas2, lon_gas2, 19),
        (lat_gas2, lon_gas2, 0),
    ]
    pacer.send_trip(trip2_points, start_level=422, end_level=420)

    # 5. Заправка #2 на Укрнафті (як DAF 4130: з розривом зв'язку 2.25 хв посередині наливу, +110 л)
    log.info(">>> ЕТАП 5: АЗС Укрнафта — Заправка #2 з розривом 2.25 хв (420 -> 530 л, +110 л)...")
    pacer.send_plateau(level=420, lat=lat_gas2, lon=lon_gas2, count=6)
    pacer.send_curve([429, 442, 456, 469, 480], lat=lat_gas2, lon=lon_gas2)
    log.info("...пауза/розрив зв'язку під час наливу (135 с)...")
    pacer.advance(135.0)
    pacer.send_curve([493, 505, 515, 523, 528, 530], lat=lat_gas2, lon=lon_gas2)
    pacer.send_plateau(level=530, lat=lat_gas2, lon=lon_gas2, count=8)

    # 6. Поїздка 3 до Вишнева (2.75 хв, 2.20 км)
    log.info(">>> ЕТАП 6: Поїздка 3 по М-07 до паркінгу Вишнів (530 -> 528 л)...")
    trip3_points = [
        (51.2052762, 23.9193942, 25),
        (51.2057610, 23.9221716, 49),
        (51.2060809, 23.9263429, 71),
        (51.2053299, 23.9308295, 78),
        (51.2035548, 23.9347063, 80),
        (51.2016919, 23.9384832, 80),
        (51.2000108, 23.9420373, 74),
        (51.1991489, 23.9449359, 54),
        (51.1989071, 23.9466504, 29),
        (lat_drain, lon_drain, 15),
        (lat_drain, lon_drain, 0),
    ]
    pacer.send_trip(trip3_points, start_level=530, end_level=528)

    # 7. Злив (3.5 хв, -96 л: 528 -> 432 л) + 3 хв плато
    log.info(">>> ЕТАП 7: Паркінг Вишнів — Злив (3.5 хв, 528 -> 432 л, -96 л)...")
    pacer.send_plateau(level=528, lat=lat_drain, lon=lon_drain, count=6)
    pacer.send_curve(
        [524, 518, 510, 501, 491, 481, 471, 462, 454, 447, 441, 437, 434, 432],
        lat=lat_drain,
        lon=lon_drain,
    )
    pacer.send_plateau(level=432, lat=lat_drain, lon=lon_drain, count=12)

    # 8. Поїздка 4 + фінішна стоянка (3 хв)
    log.info(">>> ЕТАП 8: Поїздка 4 на схід по М-07 та фінішна стоянка (закриває стоянку зливу)...")
    trip4_points = [
        (51.1988408, 23.9490177, 25),
        (51.1990850, 23.9519864, 50),
        (51.1995078, 23.9562458, 72),
        (51.1999706, 23.9608593, 78),
        (51.2004226, 23.9654755, 78),
        (51.2007807, 23.9690239, 60),
        (51.2009813, 23.9710959, 35),
        (lat_park_end, lon_park_end, 15),
        (lat_park_end, lon_park_end, 0),
    ]
    pacer.send_trip(trip4_points, start_level=432, end_level=430)
    pacer.send_plateau(level=430, lat=lat_park_end, lon=lon_park_end, count=12)

    log.info("Усі 163 точки [Short Single: 2 Refills + Trip + Drain] відправлено успішно!")
    return time.time()


if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
