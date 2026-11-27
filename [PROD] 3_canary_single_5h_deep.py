#!/usr/bin/env python3
"""
Глибока перевірка PROD (~5 год): [PROD] 3_canary_single_5h_deep [5h].
Цільовий об'єкт: Refill_Drain_UNIT (IMEI: 12345678901234567890, фільтр N=15).
Сервер прийому: 213.239.234.94:5027 | API: https://my.m2m.eu/api | Звіт №34.

Сценарій повністю успадкований із `[DEV] 3_canary_single_5h_deep.py`:
  MAN сон CAN 60 хв -> стрибок +190 л -> 4130 із розривом 2.25 хв (+108 л) -> злив -288 л -> 3.2-год хвіст сну CAN.
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from teltonika_canary_common import load_scenario_sender, run_canary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("canary_teltonika_single_5h_prod")

CONFIG = {
    "device_id": None,
    "device_name": "Refill_Drain_UNIT",
    "report_id": 34,
    "tracker_imei": "12345678901234567890",
    "scenario_mode": "single",
    "scenario_label": "[PROD] 3_canary_single_5h_deep [5h]",

    "ingest_host": "213.239.234.94",
    "ingest_port": 5027,

    "template_refill": "Заправка Refill_Drain_UNIT",
    "template_drain": "Злив Refill_Drain_UNIT",
    "expected_refills": 2,
    "expected_drains": 1,

    "wait_full_window": True,
    "initial_wait_sec": 660,
    "poll_interval_sec": 30,
    "max_wait_sec": 120,
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
        os.getenv("CANARY_STATE_FILE", "notification_canary_teltonika_single_5h_prod_state.json")
    ),
}

send_canary_events = load_scenario_sender("[DEV] 3_canary_single_5h_deep.py")

if __name__ == "__main__":
    sys.exit(run_canary(CONFIG, send_canary_events))
