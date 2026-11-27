#!/usr/bin/env python3
"""
Повний регресійний прогін PROD (~5 год 34 хв): [PROD] 7_canary_full_regression [5h30].
Цільовий об'єкт: Refill_Drain_UNIT_FULL (IMEI: 51779199942448912345, N=35 fuel + N=15 fuel_Adblue).
Сервер прийому: 213.239.234.94:5027 | API: https://my.m2m.eu/api | Звіт №34.

Сценарій повністю успадкований із `[DEV] 7_canary_full_regression_5h30.py`:
  сон CAN -> MAN без полиці (N=15 та N=35) -> 1316 + 4130 -> розділені зливи + коротке плато перед сном CAN
  -> 3-год хвіст сну CAN -> реальний offline blackbox Codec 8 batch -> звірка 1-в-1 зі Звітом №34.
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import load_scenario_sender, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_full_5h30_prod")

CONFIG = {
    "device_id": None,
    "device_name": "Refill_Drain_UNIT_FULL",
    "report_id": 34,
    "tracker_imei": "51779199942448912345",
    "scenario_mode": "full",
    "scenario_label": "[PROD] 7_canary_full_regression [5h30]",

    "ingest_host": "213.239.234.94",
    "ingest_port": 5027,

    "template_refill": "Заправка Refill_Drain_UNIT_FULL",
    "template_drain": "Злив Refill_Drain_UNIT_FULL",
    "expected_refills": 4,
    "expected_drains": 4,

    "wait_full_window": True,
    "initial_wait_sec": 660,
    "poll_interval_sec": 30,
    "max_wait_sec": 480,
    "http_timeout_sec": 60,

    "altitude": 178,
    "bearing": 82,
    "sat": 14,

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
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_full_5h30_prod_state.json")
    ),
}

send_canary_events = load_scenario_sender("[DEV] 7_canary_full_regression_5h30.py")

if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
