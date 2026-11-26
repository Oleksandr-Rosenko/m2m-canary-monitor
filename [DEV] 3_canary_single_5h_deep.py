#!/usr/bin/env python3
"""
Глибока перевірка фіксу WEB-2433 (~5 год): [DEV] Canary Monitor (Teltonika 5h Deep Check: MAN + 4130 + Drain).
Цільовий об'єкт: Refill_Drain_UNIT (id=47613, IMEI: 12345678901234567890, фільтр N=15).
Запускається окремо від регулярних коротких канарків (з блокуванням перетину в workflow).

Відтворює в одному довгому прогоні ВСІ проблемні кейси WEB-2433 із проходу 3-год вікна:
  1. Поїздка 1 по М-07 до БРСМ (4 хв, 86 -> 82 л).
  2. Сон CAN-шини на стоянці перед заправкою: 60 хв (7 точок раз на 10 хв) -> кейс MAN CA 4639!
  3. Заправка #1 стрибком (+190 л: 82 -> 272 л за 10 с) + 4 хв щільних точок.
  4. Поїздка 2 по М-07 до Укрнафти (2.5 хв, 272 -> 270 л).
  5. Заправка #2 як DAF CA 4130: +108 л (270 -> 378 л) з розривом зв'язку 2.25 хв посередині наливу.
  6. Поїздка 3 по М-07 до Вишнева (2.75 хв, 378 -> 376 л).
  7. Злив на стоянці (4.75 хв, -288 л: 376 -> 88 л) + 3 хв плато.
  8. Поїздка 4 по М-07 (2.25 хв, 88 -> 86 л).
  9. 3.5-годинний хвіст стоянки (20 точок раз на 10 хв = 200 хв після 5 хв поїздки = 205 хв після зливу, рівень 86 л = старту для 0 л стику),
     щоб кожна з 3 подій гарантовано пройшла через лівий край 3-годинного вікна!

Фінальна перевірка після хвоста: очікування останнього 10-хв флаша і повна звірка 1-в-1 зі Звітом v2 №1180.
"""

import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import TrackPacer, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_long_5h")

CONFIG = {
    "device_id": 47613,
    "device_name": "Refill_Drain_UNIT",
    "report_id": 1180,
    "tracker_imei": "12345678901234567890",
    "scenario_mode": "single",
    "scenario_label": "[DEV] 3_canary_single_5h_deep [5h]",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка (Сценарій 1 - Single)",
    "template_drain": "Злив (Сценарій 1 - Single)",
    "expected_refills": 2,
    "expected_drains": 1,

    # Наприкінці 3.5-год хвоста чекаємо повний останній флеш (~11 хв), після чого аудитуємо весь 5-год інтервал
    "wait_full_window": True,
    "initial_wait_sec": 660,
    "poll_interval_sec": 30,
    "max_wait_sec": 120,
    "http_timeout_sec": 60,

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

    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    "telegram_dm_chat_id": os.getenv("TELEGRAM_DM_CHAT_ID", ""),
    "state_file": Path(
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_long_5h_dev_state.json")
    ),
}


def send_canary_events(cfg: dict) -> float:
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=82)

    lat_gas1, lon_gas1 = 51.2015219, 23.8925562               # АЗС #1 БРСМ-Нафта Римачі
    lat_gas2, lon_gas2 = 51.2047870, 23.9182590               # АЗС #2 Укрнафта Римачі
    lat_drain, lon_drain = 51.1988310, 23.9475210             # Паркінг Вишнів
    lat_park_end, lon_park_end = 51.2010670, 23.9719840       # Фінішна стоянка на М-07

    # 1. Поїздка 1 по М-07 до БРСМ (4 хв, 4.57 км)
    log.info(">>> ЕТАП 1: Поїздка 1 по М-07 до АЗС БРСМ (86 -> 82 л)...")
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

    # 2. Сон CAN на стоянці перед Заправкою #1: 60 хв (7 точок * 10 хв = 60 хв)
    log.info(">>> ЕТАП 2: Сон CAN на стоянці БРСМ (60 хв, 1 точка раз на 10 хв, 82 л — кейс MAN)...")
    pacer.send_sparse_steps([(600.0, 82)] * 7, lat=lat_gas1, lon=lon_gas1)

    # 3. Заправка #1 стрибком (як MAN) + щільні точки 4 хв
    log.info(">>> ЕТАП 3: Пробудження CAN та Заправка #1 стрибком (+190 л: 82 -> 272 л) + 4 хв плато...")
    pacer.send_sparse_steps([(180.0, 177), (10.0, 272)], lat=lat_gas1, lon=lon_gas1)
    pacer.send_plateau(level=272, lat=lat_gas1, lon=lon_gas1, count=16)

    # 4. Поїздка 2 по М-07 до Укрнафти (2.5 хв, 1.77 км)
    log.info(">>> ЕТАП 4: Поїздка 2 по М-07 до АЗС Укрнафта (272 -> 270 л)...")
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
    pacer.send_trip(trip2_points, start_level=272, end_level=270)

    # 5. Заправка #2 як DAF 4130: +108 л з розривом зв'язку 2.25 хв
    log.info(">>> ЕТАП 5: Заправка #2 з розривом зв'язку 2.25 хв (+108 л: 270 -> 378 л — кейс 4130)...")
    pacer.send_plateau(level=270, lat=lat_gas2, lon=lon_gas2, count=8)
    pacer.send_curve([279, 291, 304, 316, 326], lat=lat_gas2, lon=lon_gas2)
    pacer.advance(135.0)
    pacer.send_curve([339, 351, 362, 370, 375, 378], lat=lat_gas2, lon=lon_gas2)
    pacer.send_plateau(level=378, lat=lat_gas2, lon=lon_gas2, count=12)

    # 6. Поїздка 3 по М-07 до Вишнева (2.75 хв, 2.20 км)
    log.info(">>> ЕТАП 6: Поїздка 3 по М-07 до Вишнева (378 -> 376 л)...")
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
    pacer.send_trip(trip3_points, start_level=378, end_level=376)

    # 7. Злив (4.75 хв, -288 л: 376 -> 88 л) + 3 хв плато
    log.info(">>> ЕТАП 7: Стоянка Вишнів — Злив (4.75 хв, 376 -> 88 л, -288 л)...")
    pacer.send_plateau(level=376, lat=lat_drain, lon=lon_drain, count=8)
    pacer.send_curve([370, 360, 347, 331, 313, 293, 272, 250, 228, 206, 185, 165, 146, 130, 116, 104, 96, 90, 88], lat=lat_drain, lon=lon_drain)
    pacer.send_plateau(level=88, lat=lat_drain, lon=lon_drain, count=12)

    # 8. Поїздка 4 по М-07 + >3-год хвіст стоянки (19 точок * 10 хв = 190 хв, рівень 86 л = старту)
    log.info(">>> ЕТАП 8: Поїздка 4 (2.25 хв, 88 -> 86 л) + хвіст стоянки ~3.2 год (1 точка раз на 10 хв, рівень 86 л)...")
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
    pacer.send_trip(trip4_points, start_level=88, end_level=86)
    pacer.send_sparse_steps([(600.0, 86)] * 19, lat=lat_park_end, lon=lon_park_end)

    log.info("Усі точки 5-годинного сценарію [MAN + 4130 + Drain + Tail] відправлено успішно!")
    return time.time()


if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
