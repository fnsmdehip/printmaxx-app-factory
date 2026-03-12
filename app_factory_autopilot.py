#!/usr/bin/env python3
"""Run the full app-factory alpha refresh chain with partial-failure tolerance."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parent.parent
AUTOMATIONS = PROJECT / "AUTOMATIONS"
AUTONOMY = AUTOMATIONS / "agent" / "autonomy"
STATUS_PATH = AUTONOMY / "app_factory_autopilot_status.json"
QUEUE_PATH = AUTONOMY / "app_factory_priority_queue.json"
LOG_DIR = AUTOMATIONS / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_step(name: str, cmd: list[str], timeout: int, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return {
            "name": name,
            "command": cmd,
            "status": "DRY_RUN",
            "returncode": 0,
            "output_tail": "dry run",
        }

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT,
        )
        output = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part).strip()
        return {
            "name": name,
            "command": cmd,
            "status": "OK" if result.returncode == 0 else "FAILED",
            "returncode": result.returncode,
            "output_tail": output[-2000:],
        }
    except subprocess.TimeoutExpired as exc:
        partial = ""
        if exc.stdout:
            partial += str(exc.stdout)
        if exc.stderr:
            partial += "\n" + str(exc.stderr)
        return {
            "name": name,
            "command": cmd,
            "status": "TIMEOUT",
            "returncode": -1,
            "output_tail": partial[-2000:] or f"timed out after {timeout}s",
        }
    except Exception as exc:
        return {
            "name": name,
            "command": cmd,
            "status": "ERROR",
            "returncode": -1,
            "output_tail": str(exc),
        }


def summarize_queue() -> list[dict[str, Any]]:
    if not QUEUE_PATH.exists():
        return []
    try:
        payload = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for row in payload.get("queue", [])[:8]:
        out.append(
            {
                "rank": row.get("rank"),
                "score": row.get("score"),
                "decision": row.get("build_decision"),
                "target": row.get("existing_app_target"),
                "title": row.get("title"),
            }
        )
    return out


def write_status(payload: dict[str, Any]) -> None:
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def print_summary(payload: dict[str, Any]) -> None:
    print(f"App Factory Autopilot @ {payload['ran_at']}")
    for step in payload.get("steps", []):
        print(f"- {step['name']}: {step['status']}")
    if payload.get("top_queue"):
        print("Top queue:")
        for row in payload["top_queue"]:
            print(
                f"  {row['rank']}. {row['decision']} | {row['target']} | {row['title']}"
            )


def build_commands(args: argparse.Namespace) -> list[tuple[str, list[str], int]]:
    py = sys.executable
    steps: list[tuple[str, list[str], int]] = []

    if not args.skip_bookmarks:
        steps.append(
            (
                "twitter_bookmarks_scraper",
                [py, str(AUTOMATIONS / "twitter_bookmarks_scraper.py"), "--scrape", "--limit", str(args.bookmarks_limit)],
                420,
            )
        )

    if not args.skip_accounts and args.accounts_limit > 0:
        steps.append(
            (
                "twitter_alpha_scraper_accounts",
                [
                    py,
                    str(AUTOMATIONS / "twitter_alpha_scraper.py"),
                    "--accounts",
                    "--limit",
                    str(args.accounts_limit),
                    "--days",
                    str(args.days),
                ],
                900,
            )
        )

    steps.extend(
        [
            (
                "alpha_auto_approver",
                [py, str(AUTOMATIONS / "alpha_auto_approver.py"), "--tick", "--max", str(args.approval_max)],
                180,
            ),
            (
                "alpha_auto_processor",
                [py, str(AUTOMATIONS / "alpha_auto_processor.py"), "--process-new", "--batch-size", str(args.processor_batch)],
                300,
            ),
            (
                "alpha_to_ops",
                [py, str(AUTOMATIONS / "alpha_to_ops.py"), "--process"],
                420,
            ),
            (
                "app_factory_command_center",
                [py, str(AUTOMATIONS / "app_factory_command_center.py"), "--refresh", "--top", "8", "--limit", str(args.queue_limit)],
                180,
            ),
        ]
    )
    return steps


def run_autopilot(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ran_at": now_iso(),
        "mode": "DRY_RUN" if args.dry_run else "LIVE",
        "config": {
            "bookmarks_limit": args.bookmarks_limit,
            "accounts_limit": args.accounts_limit,
            "approval_max": args.approval_max,
            "processor_batch": args.processor_batch,
            "queue_limit": args.queue_limit,
            "skip_bookmarks": args.skip_bookmarks,
            "skip_accounts": args.skip_accounts,
        },
        "steps": [],
        "top_queue": [],
    }

    for name, cmd, timeout in build_commands(args):
        step = run_step(name, cmd, timeout=timeout, dry_run=args.dry_run)
        payload["steps"].append(step)

    payload["top_queue"] = summarize_queue()
    write_status(payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the app-factory autopilot pipeline.")
    parser.add_argument("--run", action="store_true", help="execute the autopilot chain")
    parser.add_argument("--status", action="store_true", help="show last autopilot status")
    parser.add_argument("--dry-run", action="store_true", help="show steps without executing them")
    parser.add_argument("--skip-bookmarks", action="store_true", help="skip curated X bookmark scrape")
    parser.add_argument("--skip-accounts", action="store_true", help="skip high-signal account scrape")
    parser.add_argument("--bookmarks-limit", type=int, default=60, help="max bookmarks to pull per run")
    parser.add_argument("--accounts-limit", type=int, default=12, help="max high-signal accounts to scrape")
    parser.add_argument("--approval-max", type=int, default=80, help="max alpha entries to auto-approve")
    parser.add_argument("--processor-batch", type=int, default=120, help="max pending alpha rows to process")
    parser.add_argument("--queue-limit", type=int, default=40, help="max ranked queue size to write")
    parser.add_argument("--days", type=int, default=30, help="days window for targeted account scrape")
    args = parser.parse_args()

    if args.status and STATUS_PATH.exists() and not args.run:
        payload = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        print_summary(payload)
        return

    if not args.run and not args.dry_run:
        parser.print_help()
        return

    payload = run_autopilot(args)
    print_summary(payload)


if __name__ == "__main__":
    main()
