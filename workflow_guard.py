#!/usr/bin/env python3
"""
Спільний guard для GitHub Actions: запобігає накладанню коротких і 5-годинних канарків на одному девайсі.
- mode='skip': миттєво пропускає короткий запуск, якщо 5h workflow активний або щойно завершився (< cooldown_min).
- mode='wait': перед стартом 5h workflow чекає завершення активного короткого запуску на тому ж девайсі.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def fetch_workflow_runs(repo: str, workflow_file: str, token: str, per_page: int = 5) -> list[dict]:
    wf = urllib.parse.quote(workflow_file)
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/runs?per_page={per_page}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "canary-guard/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8")).get("workflow_runs", [])


def should_skip_short_run(runs: list[dict], cooldown_min: float, now: datetime | None = None) -> tuple[bool, str]:
    now_dt = now or datetime.now(timezone.utc)
    for run in runs:
        st = run.get("status")
        rid = run.get("id")
        if st in ("in_progress", "queued", "waiting"):
            return True, f"5h run #{rid} is active ({st}) -> skipping."
        upd = run.get("updated_at")
        if upd and cooldown_min > 0:
            dt = datetime.fromisoformat(upd.replace("Z", "+00:00"))
            age_min = (now_dt - dt).total_seconds() / 60.0
            if age_min < cooldown_min:
                return True, f"5h run #{rid} finished {age_min:.1f}m ago (<{cooldown_min:.0f}m) -> skipping."
    return False, "5h workflow is idle -> proceeding."


def run_skip_mode(repo: str, target_wf: str, token: str, cooldown_min: float) -> None:
    skip = False
    try:
        runs = fetch_workflow_runs(repo, target_wf, token, per_page=5)
        skip, msg = should_skip_short_run(runs, cooldown_min)
        print(msg)
    except Exception as exc:
        print(f"Guard check skipped due to API error (proceeding): {exc}")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"skip={'true' if skip else 'false'}\n")


def run_wait_mode(repo: str, target_wf: str, token: str, timeout_sec: int, poll_sec: int) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            runs = fetch_workflow_runs(repo, target_wf, token, per_page=3)
            active = [x for x in runs if x.get("status") in ("in_progress", "queued", "waiting")]
            if not active:
                print(f"Target {target_wf} is idle -> starting 5h deep check.")
                return
            print(f"Waiting for {target_wf} run #{active[0].get('id')} ({active[0].get('status')})...")
        except Exception as exc:
            print(f"Guard check error (proceeding): {exc}")
            return
        time.sleep(poll_sec)
    print(f"Wait timeout ({timeout_sec}s) reached -> proceeding.")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="Target workflow filename, e.g. '[DEV] 3_canary_single_5h_deep.yml'")
    parser.add_argument("--mode", choices=["skip", "wait"], required=True)
    parser.add_argument("--cooldown-min", type=float, default=15.0)
    parser.add_argument("--timeout-sec", type=int, default=3000)
    parser.add_argument("--poll-sec", type=int, default=30)
    args = parser.parse_args()

    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN", "")
    if not repo or not token:
        print("GITHUB_REPOSITORY or GH_TOKEN not set -> skipping guard check.")
        return 0

    if args.mode == "skip":
        run_skip_mode(repo, args.target, token, args.cooldown_min)
    else:
        run_wait_mode(repo, args.target, token, args.timeout_sec, args.poll_sec)
    return 0


if __name__ == "__main__":
    sys.exit(main())
