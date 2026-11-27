#!/usr/bin/env python3
"""
Повний регресійний прогін (~5 год 28 хв): [DEV] 7_canary_full_regression [5h30].
Цільовий об'єкт: Refill_Drain_UNIT_full (IMEI: 86986299164344112345).
Датчики: fuel (io_201, фільтр N=35) + fuel_Adblue (io_203, фільтр N=15).

Перевіряє в одному прогоні ВСІ фікси WEB-2433 та крайні випадки:
  1. Поїздка 1 по М-07 (io_201: 130 -> 126 л, io_203: 28 -> 27 л).
  2. Сон CAN 60 хв (1 точка / 10 хв) -> стрибок MAN БЕЗ проміжної полиці одразу на N=15 (io_203, +64 л) та N=35 (io_201, +180 л).
  3. Поїздка 2 + подвійна заправка впритул 1316 (+114 л рідкі точки) -> пауза 2 с -> 4130 (+240 л з розривом зв'язку 2.25 хв).
  4. Поїздка 3 + розділені зливи на io_201 (-154 л -> пауза 6.5 хв всередині з DRAIN io_203 -61 л -> -220 л)
     + коротке плато 2.5 хв (< ceil(35/2) = 18 точок) і миттєвий перехід у сон CAN.
  5. Хвіст сну CAN 180 хв (18 точок * 10 хв): перевіряє швидкість закриття правої межі при сні CAN
     і протягує всі 7 попередніх подій через лівий край 3-год вікна.
  6. Реальний offline blackbox (~12 хв без зв'язку: Поїздка 4 + Злив #4 io_201 -146 л + 5 хв плато 130/28 л = старту),
     який вивантажується багатозаписними пакетами Teltonika Codec 8 із ретро-таймстемпами.
"""

import logging
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import TrackPacer, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_full_5h30")

CONFIG = {
    "device_id": None,
    "device_name": "Refill_Drain_UNIT_full",
    "report_id": 1180,
    "tracker_imei": "86986299164344112345",
    "scenario_mode": "full",
    "scenario_label": "[DEV] 7_canary_full_regression [5h30]",

    "ingest_host": "95.217.119.27",
    "ingest_port": 5027,

    "template_refill": "Заправка Refill_Drain_UNIT_full",
    "template_drain": "Злив Refill_Drain_UNIT_full",
    "expected_refills": 4,
    "expected_drains": 4,

    # Чекаємо завершення останньої сесії після вивантаження blackbox + 1 хв фінального онлайну (~16 хв)
    "wait_full_window": True,
    "initial_wait_sec": 660,
    "poll_interval_sec": 30,
    "max_wait_sec": 300,
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
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_full_5h30_dev_state.json")
    ),
}


def send_canary_events(cfg: dict) -> float:
    pacer = TrackPacer(cfg, step_sec=15.0, initial_bearing=82)

    lat_gas1, lon_gas1 = 51.2015219, 23.8925562
    lat_gas2, lon_gas2 = 51.2047870, 23.9182590
    lat_drain, lon_drain = 51.1988310, 23.9475210
    lat_end, lon_end = 51.2010670, 23.9719840

    # 1. Поїздка 1 по М-07 до АЗС #1 (~4 хв)
    log.info(">>> ЕТАП 1: Поїздка 1 по М-07 (io_201: 130->126 л, io_203: 28->27 л)...")
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
    pacer.send_trip(trip1_points, start_level=130, end_level=126, extra_levels={203: (28, 27)})

    # 2. Сон CAN 60 хв (6 * 10 хв -> разом 7 точок 126/27 перед стрибком)
    log.info(">>> ЕТАП 2: Сон CAN на АЗС #1 (60 хв, 1 точка раз на 10 хв, io_201=126, io_203=27)...")
    pacer.send_sparse_steps([(600.0, 126)] * 6, lat=lat_gas1, lon=lon_gas1, extra_ios={203: 27})

    # 3. MAN-стрибки БЕЗ проміжної полиці: io_203 (N=15, +64 л), через 30 с io_201 (N=35, +180 л)
    log.info(">>> ЕТАП 3: MAN-пробудження без полиці — REFILL #1 [io_203 N=15, +64 л], через 30 с REFILL #2 [io_201 N=35, +180 л]...")
    pacer.send_sparse_steps([(180.0, 126)], lat=lat_gas1, lon=lon_gas1, extra_ios={203: 59})
    pacer.send_sparse_steps([(10.0, 126)], lat=lat_gas1, lon=lon_gas1, extra_ios={203: 91})
    pacer.send_plateau(level=126, lat=lat_gas1, lon=lon_gas1, count=1, extra_ios={203: 91})
    pacer.emit(level=216, lat=lat_gas1, lon=lon_gas1, speed=0, extra_ios={203: 91})
    pacer.send_plateau(level=306, lat=lat_gas1, lon=lon_gas1, count=20, extra_ios={203: 91})

    # 4. Поїздка 2 до АЗС #2 (~2.5 хв)
    log.info(">>> ЕТАП 4: Поїздка 2 по М-07 до АЗС #2 (io_201: 306->304, io_203: 91->90)...")
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
    pacer.send_trip(trip2_points, start_level=306, end_level=304, extra_levels={203: (91, 90)})

    # 5. Подвійна заправка впритул: REFILL #3 (1316) -> 2 с -> REFILL #4 (4130 з розривом 2.25 хв)
    log.info(">>> ЕТАП 5: REFILL #3 [io_201, 1316 +114 л] -> 2 с -> REFILL #4 [io_201, 4130 +240 л з розривом 2.25 хв]...")
    pacer.send_plateau(level=304, lat=lat_gas2, lon=lon_gas2, count=20, extra_ios={203: 90})
    pacer.send_sparse_steps([(75.0, 316), (150.0, 368), (165.0, 418)], lat=lat_gas2, lon=lon_gas2, extra_ios={203: 90})
    pacer.advance(2.0)
    pacer.send_curve([425, 438, 455, 475, 496, 518], lat=lat_gas2, lon=lon_gas2, extra_ios={203: 90})
    pacer.send_sparse_steps([(135.0, 562)], lat=lat_gas2, lon=lon_gas2, extra_ios={203: 90})
    pacer.send_curve([582, 602, 620, 635, 646, 653, 656, 658], lat=lat_gas2, lon=lon_gas2, extra_ios={203: 90})
    pacer.send_plateau(level=658, lat=lat_gas2, lon=lon_gas2, count=20, extra_ios={203: 90})

    # 6. Поїздка 3 до паркінгу зливів (~2.75 хв)
    log.info(">>> ЕТАП 6: Поїздка 3 по М-07 (io_201: 658->654, io_203: 90->89)...")
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
    pacer.send_trip(trip3_points, start_level=658, end_level=654, extra_levels={203: (90, 89)})

    # 7. Розділені зливи io_201 + злив io_203 + коротке плато 2.5 хв перед миттєвим сном CAN
    log.info(">>> ЕТАП 7: DRAIN #1 [io_201 -154 л] -> пауза 6.5 хв (всередині DRAIN #2 [io_203 -61 л]) -> DRAIN #3 [io_201 -220 л] + коротке плато 2.5 хв...")
    pacer.send_plateau(level=654, lat=lat_drain, lon=lon_drain, count=20, extra_ios={203: 89})
    drain1 = [round(654 - 154 * 0.5 * (1 - math.cos(math.pi * k / 13))) for k in range(1, 14)]
    pacer.send_curve(drain1, lat=lat_drain, lon=lon_drain, extra_ios={203: 89})

    # Пауза 6.5 хв між зливами io_201: 1.5 хв плато -> DRAIN #2 io_203 (3.5 хв) -> 1.5 хв плато
    pacer.send_plateau(level=500, lat=lat_drain, lon=lon_drain, count=6, extra_ios={203: 89})
    drain2_adblue = [round(89 - 61 * 0.5 * (1 - math.cos(math.pi * k / 14))) for k in range(1, 15)]
    for ad_lvl in drain2_adblue:
        pacer.emit(level=500, lat=lat_drain, lon=lon_drain, speed=0, extra_ios={203: ad_lvl})
    pacer.send_plateau(level=500, lat=lat_drain, lon=lon_drain, count=6, extra_ios={203: 28})

    drain3 = [round(500 - 220 * 0.5 * (1 - math.cos(math.pi * k / 18))) for k in range(1, 19)]
    pacer.send_curve(drain3, lat=lat_drain, lon=lon_drain, extra_ios={203: 28})
    # Спеціально коротке плато (10 точок = 2.5 хв < ceil(35/2) = 18) перед переходом у 10-хв сон CAN
    pacer.send_plateau(level=280, lat=lat_drain, lon=lon_drain, count=10, extra_ios={203: 28})

    # 8. 3-годинний хвіст сну CAN (18 точок * 10 хв = 180 хв)
    log.info(">>> ЕТАП 8: Хвіст сну CAN 180 хв (18 точок раз на 10 хв, io_201=280, io_203=28) — прохід крізь лівий край 3-год вікна...")
    pacer.send_sparse_steps([(600.0, 280)] * 18, lat=lat_drain, lon=lon_drain, extra_ios={203: 28})

    # 9. Реальний offline blackbox (~12 хв без TCP) -> вивантаження multi-record Codec 8 -> фінал 130/28 = старту
    log.info(">>> ЕТАП 9: Офлайн blackbox (~12 хв: Поїздка 4 + DRAIN #4 [io_201 -146 л] + плато 5 хв) + вивантаження Codec 8 batch...")
    pacer.begin_blackbox(start_delay_sec=15.0)
    trip4_points = [
        (51.1988408, 23.9490177, 25),
        (51.1990850, 23.9519864, 50),
        (51.1995078, 23.9562458, 72),
        (51.1999706, 23.9608593, 78),
        (51.2004226, 23.9654755, 78),
        (51.2007807, 23.9690239, 60),
        (51.2009813, 23.9710959, 35),
        (lat_end, lon_end, 15),
        (lat_end, lon_end, 0),
    ]
    pacer.send_trip(trip4_points, start_level=280, end_level=276, extra_levels={203: (28, 28)})
    pacer.send_plateau(level=276, lat=lat_end, lon=lon_end, count=4, extra_ios={203: 28})
    drain4 = [round(276 - 146 * 0.5 * (1 - math.cos(math.pi * k / 16))) for k in range(1, 17)]
    pacer.send_curve(drain4, lat=lat_end, lon=lon_end, extra_ios={203: 28})
    pacer.send_plateau(level=130, lat=lat_end, lon=lon_end, count=20, extra_ios={203: 28})
    pacer.flush_blackbox(wait_until_end=True, batch_size=20)

    # Фінальний 1-хв онлайн-хвіст (рівні 130/28 = старту для 0 л стику)
    log.info(">>> ФІНАЛ: 1 хв онлайн-стоянки після вивантаження blackbox (130/28 л = старту)...")
    pacer.send_plateau(level=130, lat=lat_end, lon=lon_end, count=4, extra_ios={203: 28})

    log.info("Усі точки повного регресійного сценарію [5h30] відправлено успішно!")
    return time.time()


if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
