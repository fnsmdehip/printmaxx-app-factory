#!/usr/bin/env python3
"""Rank live app alpha into a build queue the APP factory can actually use."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parent.parent
AUTOMATIONS = PROJECT / "AUTOMATIONS"
OPS = PROJECT / "OPS"
LEDGER = PROJECT / "LEDGER"
RESULTS = AUTOMATIONS / "agent" / "autonomy" / "results"
APP_SPECS_DIR = AUTOMATIONS / "auto_ops" / "app_specs"
QUEUE_PATH = AUTOMATIONS / "agent" / "autonomy" / "app_factory_priority_queue.json"
COMMAND_CENTER_PATH = OPS / "APP_FACTORY_ALPHA_COMMAND_CENTER.md"

ALPHA_STAGING_PATH = LEDGER / "ALPHA_STAGING.csv"
APP_METHODS_PATH = LEDGER / "APP_FACTORY_METHODS.csv"
REDDIT_FINDINGS_PATH = RESULTS / "reddit_alpha_cycle_mar8_1552.json"

ROI_SCORES = {
    "HIGHEST": 30,
    "HIGH": 22,
    "MEDIUM": 12,
    "LOW": 3,
}

STATUS_SCORES = {
    "APPROVED": 18,
    "AUTO_APPROVED": 20,
    "ROUTED_TO_VENTURE": 14,
    "PENDING_REVIEW": 8,
    "QUEUED": 6,
    "HIGH": 6,
    "THIS_WEEK": 8,
    "IMMEDIATE": 10,
    "ACTIVE": 10,
    "ARCHIVED": -18,
}

PRIORITY_SCORES = {
    "IMMEDIATE": 10,
    "THIS_WEEK": 8,
    "HIGH": 6,
    "MEDIUM": 3,
    "LOW": 0,
    "HIGHEST": 8,
}

SOURCE_SCORES = {
    "alpha_staging": 28,
    "app_factory_methods": 32,
    "reddit_findings": 30,
}

APP_SIGNAL_KEYWORDS = [
    "ios",
    "android",
    "subscription",
    "paywall",
    "trial",
    "revenuecat",
    "habit",
    "streak",
    "fitness",
    "sleep",
    "focus",
    "productivity",
    "meal",
    "nutrition",
    "faith",
    "bible",
    "prayer",
    "ramadan",
    "quran",
    "wrapper",
    "gpt",
    "pdf",
    "resume",
    "review prompt",
    "rating",
]

BONUS_KEYWORDS = {
    "7-day trial": 7,
    "trial": 4,
    "swipe": 8,
    "local": 5,
    "privacy": 7,
    "offline": 5,
    "wrapper": 8,
    "gpt": 7,
    "pdf": 6,
    "resume": 4,
    "habit": 5,
    "streak": 5,
    "review prompt": 6,
    "rating": 4,
    "quiz": 5,
    "gamification": 6,
    "pet": 7,
    "minimum viable day": 8,
    "step": 4,
    "walk": 4,
    "sleep": 4,
    "fitness": 5,
    "faith": 5,
    "bible": 6,
    "prayer": 5,
}

DIRECT_ACTION_KEYWORDS = [
    "review prompt",
    "swipe",
    "minimum viable day",
    "privacy-first",
    "local-first",
    "soft paywall",
    "tier pricing",
    "lifetime pricing",
    "7-day trial",
    "free trial",
    "value moment",
    "share card",
    "referral",
    "chat layer",
    "localization",
    "first win",
]

MARKET_INTEL_PENALTY_KEYWORDS = [
    "market",
    "cagr",
    "downloads",
    "top apps",
    "north america",
    "tam",
    "millions",
    "billions",
    "starts feb",
    "dominant",
]

TOOLING_PENALTY_KEYWORDS = [
    "replit",
    "pwabuilder",
    "bubblewrap",
    "store listing",
    "no xcode",
    "builder",
    "package pwa",
]

CLUSTERS = {
    "faith": {
        "keywords": ["faith", "bible", "prayer", "ramadan", "quran", "church", "scripture"],
        "market_room": "MEDIUM_HIGH",
        "aesthetic": "midnight blue, warm gold, quiet motion, modern-reverent typography",
        "onboarding": "belief or practice selector -> daily cadence setup -> first devotional win -> reminder -> paywall",
        "paywall": "after first devotional value moment, annual-first anchor",
        "primary_monetization": "subscription with 7-day trial",
        "secondary_monetization": "books, study tools, or affiliate offers only after retention exists",
    },
    "habits": {
        "keywords": ["habit", "streak", "minimum viable day", "tracker", "discipline", "routine"],
        "market_room": "HIGH",
        "aesthetic": "warm ivory, emerald progress rings, crisp cards, satisfying celebrations",
        "onboarding": "goal picker -> starter pack -> first completion -> reminder permission -> paywall",
        "paywall": "after first completion, show streak preview and annual plan",
        "primary_monetization": "subscription with 7-day trial and annual anchor",
        "secondary_monetization": "light affiliate add-ons only if they reinforce the streak outcome",
    },
    "fitness": {
        "keywords": ["fitness", "workout", "walk", "step", "health", "gym", "cardio"],
        "market_room": "HIGH",
        "aesthetic": "sunrise gradients, bold counters, motion-heavy metrics, tactile progress states",
        "onboarding": "goal setup -> baseline selection -> one immediate action -> reminder permission -> paywall",
        "paywall": "after plan preview or first logged action",
        "primary_monetization": "subscription with 7-day trial and annual-first pricing",
        "secondary_monetization": "supplement, gear, or wellness affiliate offers after conversion tests",
    },
    "sleep": {
        "keywords": ["sleep", "circadian", "melatonin", "bedtime", "wind down"],
        "market_room": "HIGH",
        "aesthetic": "navy, moonlight teal, blur layers, slow transitions, low-light contrast",
        "onboarding": "sleep issue quiz -> bedtime target -> tonight preview -> reminder permission -> paywall",
        "paywall": "after personalized sleep plan preview",
        "primary_monetization": "subscription with annual anchor",
        "secondary_monetization": "audio packs or wellness affiliate offers only after activation",
    },
    "focus": {
        "keywords": ["focus", "productivity", "deep work", "pomodoro", "lock"],
        "market_room": "MEDIUM_HIGH",
        "aesthetic": "graphite base, one high-voltage accent, dense information, tactile controls",
        "onboarding": "attention problem selector -> session goal -> first focus win -> reminder -> paywall",
        "paywall": "after first completed work session",
        "primary_monetization": "subscription with annual anchor",
        "secondary_monetization": "paid upfront test if the app is privacy-first and local-only",
    },
    "meal": {
        "keywords": ["meal", "nutrition", "recipe", "shopping list", "food waste"],
        "market_room": "MEDIUM",
        "aesthetic": "paper white, herb green, strong food imagery, calendar-first layout",
        "onboarding": "dietary quiz -> household size -> weekly preview -> recipe preview -> paywall",
        "paywall": "after weekly meal plan preview",
        "primary_monetization": "subscription with annual anchor",
        "secondary_monetization": "grocery or kitchen affiliate offers after retention exists",
    },
    "ai_utility": {
        "keywords": ["ai", "gpt", "wrapper", "pdf", "resume", "chat", "detector", "local", "privacy"],
        "market_room": "HIGH",
        "aesthetic": "clean white or ink black, one sharp accent, no fake futuristic chrome",
        "onboarding": "drop input -> show first useful result fast -> personalize -> paywall",
        "paywall": "after the first clear output, never before time-to-value",
        "primary_monetization": "subscription with fast time-to-value and annual anchor",
        "secondary_monetization": "paid upfront for privacy-first local tools; avoid ads unless usage is massive",
    },
}

EXISTING_APP_TARGETS = {
    "faith": "Hilal / PrayerLock / Scripture Streak",
    "habits": "Streakr",
    "fitness": "Steplock",
    "sleep": "Dusk",
    "focus": "Vault",
    "meal": "Mise",
}

HARD_GATES = [
    "No fake paywalls. Real RevenueCat or real billing path before shipping.",
    "No single-file HTML monolith as the final App Store build target.",
    "Native-feeling interactions required: haptics, offline states, privacy URL, and post-value review prompt timing.",
    "If an alpha item clearly upgrades an existing app, iterate that app before starting a greenfield clone.",
]


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:80] or "untitled"


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    return text


def read_lenient_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            headers = [clean_text(cell) for cell in next(reader)]
        except StopIteration:
            return []

        rows: list[dict[str, str]] = []
        width = len(headers)
        for raw in reader:
            if not raw or not any(cell.strip() for cell in raw):
                continue
            if len(raw) < width:
                raw = raw + [""] * (width - len(raw))
            elif len(raw) > width:
                raw = raw[: width - 1] + [" | ".join(raw[width - 1 :])]
            row = {headers[idx]: clean_text(raw[idx]) for idx in range(width)}
            rows.append(row)
        return rows


def row_blob(row: dict[str, str]) -> str:
    keys = [
        "title",
        "summary",
        "source",
        "source_url",
        "tactic",
        "extracted_method",
        "reviewer_notes",
        "compliance_notes",
        "applicable_methods",
        "applicable_niches",
        "existing_assets_applicable",
    ]
    return " ".join(clean_text(row.get(key, "")) for key in keys).lower()


def has_app_signal(row: dict[str, str]) -> bool:
    blob = row_blob(row)
    category = clean_text(row.get("category", "")).upper()
    methods = row.get("applicable_methods", "").upper()

    if category == "APP_FACTORY":
        return True
    if "APP_FACTORY" in methods:
        return True
    return any(keyword in blob for keyword in APP_SIGNAL_KEYWORDS)


def best_evidence(row: dict[str, str]) -> str:
    for key in ("extracted_method", "summary", "tactic", "reviewer_notes"):
        value = clean_text(row.get(key, ""))
        if len(value) >= 20:
            return value
    return clean_text(row.get("title", "") or row.get("source", "unknown alpha"))


def detect_cluster(blob: str) -> str:
    best_cluster = "ai_utility"
    best_score = 0
    for cluster, config in CLUSTERS.items():
        score = sum(1 for keyword in config["keywords"] if keyword in blob)
        if score > best_score:
            best_cluster = cluster
            best_score = score
    return best_cluster


def detect_target(cluster: str, blob: str) -> tuple[str, str]:
    if cluster == "faith" and any(token in blob for token in ("ai", "gpt", "wrapper", "chat")):
        return "PrayerLock / Scripture Streak AI layer", "ITERATE_EXISTING"
    if any(token in blob for token in ("review prompt", "share card", "referral", "soft paywall", "lifetime pricing", "localization")) and not any(
        token in blob for token in ("dental", "vet", "medical", "pdf", "resume", "bible", "prayer", "habit", "streak", "fitness", "sleep", "meal")
    ):
        return "PORTFOLIO_WIDE_APP_UPGRADE", "ITERATE_EXISTING"
    if cluster in EXISTING_APP_TARGETS:
        return EXISTING_APP_TARGETS[cluster], "ITERATE_EXISTING"
    return "NEW_BUILD", "NEW_BUILD"


def detect_existing_spec(alpha_id: str) -> str | None:
    if not alpha_id:
        return None
    path = APP_SPECS_DIR / f"APP_SPEC_{alpha_id}.md"
    if path.exists():
        return str(path.relative_to(PROJECT))
    return None


def monetization_plan(cluster: str, blob: str) -> tuple[str, str, str]:
    config = CLUSTERS[cluster]
    primary = config["primary_monetization"]
    secondary = config["secondary_monetization"]
    price_test = "$24.99 to $39.99/yr first" if cluster != "ai_utility" else "$6.99 to $9.99/mo and $39.99 to $59.99/yr"

    if cluster == "ai_utility" and any(token in blob for token in ("privacy", "local", "offline")):
        secondary = "paid upfront or one-time pro unlock is valid here because privacy is the wedge"
        price_test = "$9.99 to $29.99 upfront, plus optional pro unlock"
    if any(token in blob for token in ("ads", "ad-supported", "remove ads")):
        secondary = "test free with ads + remove-ads unlock only if usage frequency is very high"
    return primary, secondary, price_test


def experiment_stack(cluster: str, blob: str) -> list[str]:
    experiments = [
        "paywall timing: after first value moment vs after onboarding preview",
        "pricing: annual-first anchor vs cheaper monthly plan",
        "review prompt: milestone trigger only, never session-one prompt",
    ]
    if cluster in {"faith", "fitness", "meal"}:
        experiments.append("affiliate placement: post-conversion home tab vs no affiliate module")
    elif cluster == "ai_utility" and any(token in blob for token in ("privacy", "local", "offline")):
        experiments.append("monetization: paid upfront vs free trial subscription")
    else:
        experiments.append("onboarding length: 3-screen fast path vs 5-screen personalized path")
    return experiments


def risk_score(blob: str, row: dict[str, str]) -> int:
    risk = 0
    if any(token in blob for token in ("adult", "nsfw", "findom", "onlyfans")):
        risk += 35
    if any(token in clean_text(row.get("compliance_notes", "")).lower() for token in ("ftc", "adult", "privacy", "tracking")):
        risk += 8
    if clean_text(row.get("engagement_authenticity", "")).upper() == "SUSPICIOUS":
        risk += 8
    if clean_text(row.get("status", "")).upper() == "ARCHIVED":
        risk += 10
    return risk


def score_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    blob = row_blob(candidate)
    cluster = detect_cluster(blob)
    target, execution_lane = detect_target(cluster, blob)
    primary, secondary, price_test = monetization_plan(cluster, blob)
    risk = risk_score(blob, candidate)

    score = SOURCE_SCORES.get(candidate["source_type"], 20)
    score += ROI_SCORES.get(candidate.get("roi_potential", "").upper(), 6)
    score += STATUS_SCORES.get(candidate.get("status", "").upper(), 0)
    score += PRIORITY_SCORES.get(candidate.get("priority", "").upper(), 0)

    authenticity = clean_text(candidate.get("engagement_authenticity", "")).upper()
    if authenticity == "AUTHENTIC":
        score += 5
    elif authenticity == "SUSPICIOUS":
        score -= 4

    verified = clean_text(candidate.get("earnings_verified", "")).upper()
    if verified in {"TRUE", "SCREENSHOT", "VERIFIED"}:
        score += 6

    for keyword, weight in BONUS_KEYWORDS.items():
        if keyword in blob:
            score += weight

    action_hits = sum(1 for keyword in DIRECT_ACTION_KEYWORDS if keyword in blob)
    if action_hits:
        score += min(30, action_hits * 10)
    else:
        if any(keyword in blob for keyword in MARKET_INTEL_PENALTY_KEYWORDS):
            score -= 35
        if any(keyword in blob for keyword in TOOLING_PENALTY_KEYWORDS):
            score -= 30

    if execution_lane == "ITERATE_EXISTING":
        score += 8
    if candidate.get("existing_spec"):
        score += 4

    score = max(0, score - (risk // 2))

    if risk >= 25:
        build_decision = "VALIDATE_FIRST"
    elif execution_lane == "ITERATE_EXISTING" and score >= 74:
        build_decision = "ITERATE_EXISTING_NOW"
    elif execution_lane == "NEW_BUILD" and score >= 76:
        build_decision = "BUILD_NEW_NOW"
    elif score >= 60:
        build_decision = "SPEC_AND_TEST"
    else:
        build_decision = "PARK"

    candidate.update(
        {
            "score": score,
            "cluster": cluster,
            "market_room": CLUSTERS[cluster]["market_room"],
            "existing_app_target": target,
            "execution_lane": execution_lane,
            "build_decision": build_decision,
            "onboarding_model": CLUSTERS[cluster]["onboarding"],
            "paywall_variant": CLUSTERS[cluster]["paywall"],
            "aesthetic_direction": CLUSTERS[cluster]["aesthetic"],
            "primary_monetization": primary,
            "secondary_monetization": secondary,
            "price_test": price_test,
            "experiments": experiment_stack(cluster, blob),
            "risk_score": risk,
        }
    )
    return candidate


def normalize_alpha_row(row: dict[str, str]) -> dict[str, Any] | None:
    if not has_app_signal(row):
        return None
    aid = clean_text(row.get("alpha_id", ""))
    title = clean_text(row.get("title", "") or row.get("tactic", "") or "")
    if (
        not title
        or title.upper() in {"APP_FACTORY", "PRODUCT_LAUNCH", "COMPETITOR_SIGNAL", "APP_LAUNCH"}
        or title == title.upper()
    ):
        title = best_evidence(row)[:120]
    if not title:
        title = aid or "app alpha"
    return {
        "candidate_id": aid or f"alpha_{slugify(title)}",
        "title": title,
        "source": clean_text(row.get("source", "alpha_staging")),
        "source_url": clean_text(row.get("source_url", "")),
        "source_type": "alpha_staging",
        "roi_potential": clean_text(row.get("roi_potential", "MEDIUM")) or "MEDIUM",
        "priority": clean_text(row.get("implementation_priority", "") or row.get("priority", "")),
        "status": clean_text(row.get("status", "PENDING_REVIEW")),
        "engagement_authenticity": clean_text(row.get("engagement_authenticity", "")),
        "earnings_verified": clean_text(row.get("earnings_verified", "")),
        "applicable_niches": clean_text(row.get("applicable_niches", "")),
        "applicable_methods": clean_text(row.get("applicable_methods", "")),
        "evidence": best_evidence(row),
        "compliance_notes": clean_text(row.get("compliance_notes", "")),
        "reviewer_notes": clean_text(row.get("reviewer_notes", "")),
        "existing_spec": detect_existing_spec(aid),
        "raw": row,
    }


def load_alpha_staging_candidates() -> list[dict[str, Any]]:
    rows = read_lenient_csv(ALPHA_STAGING_PATH)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        candidate = normalize_alpha_row(row)
        if candidate:
            candidates.append(candidate)
    return candidates


def load_method_candidates() -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    if not APP_METHODS_PATH.exists():
        return candidates

    with APP_METHODS_PATH.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for raw in reader:
            if not raw or not any(cell.strip() for cell in raw):
                continue

            aid = clean_text(raw[0]) if len(raw) >= 1 else ""
            title = ""
            category = ""
            summary = ""
            roi = "HIGH"
            routed_at = ""
            status = "APPROVED"

            if len(raw) == 4:
                category = clean_text(raw[1])
                summary = clean_text(raw[2])
                routed_at = clean_text(raw[3])
            elif len(raw) >= 6 and clean_text(raw[2]).upper() == "APP_FACTORY":
                title = clean_text(raw[1])
                category = clean_text(raw[2])
                summary = clean_text(raw[3])
                roi = clean_text(raw[4]) or "HIGH"
                routed_at = clean_text(raw[5])
                if len(raw) >= 7:
                    status = clean_text(raw[6]) or "APPROVED"
            elif len(raw) >= 7 and clean_text(raw[2]).lower() == "app_factory":
                title = clean_text(raw[0])
                category = clean_text(raw[2])
                roi = clean_text(raw[3]) or "HIGH"
                summary = clean_text(raw[4])
                routed_at = clean_text(raw[5])
                status = clean_text(raw[6]) or "APPROVED"
            else:
                continue

            if category.upper() != "APP_FACTORY" or not summary:
                continue

            aid = aid or f"method_{slugify(summary[:40])}"
            candidate = {
                "candidate_id": aid,
                "title": title or summary[:90],
                "source": "APP_FACTORY_METHODS.csv",
                "source_url": "",
                "source_type": "app_factory_methods",
                "roi_potential": roi,
                "priority": "HIGH" if roi in {"HIGHEST", "HIGH"} else "MEDIUM",
                "status": status,
                "engagement_authenticity": "AUTHENTIC",
                "earnings_verified": "FALSE",
                "applicable_niches": "",
                "applicable_methods": "APP_FACTORY",
                "evidence": summary,
                "compliance_notes": "",
                "reviewer_notes": routed_at,
                "existing_spec": detect_existing_spec(aid),
                "raw": {"alpha_id": aid, "title": title, "summary": summary},
            }
            candidates.append(candidate)
    return candidates


def load_reddit_candidates() -> list[dict[str, Any]]:
    if not REDDIT_FINDINGS_PATH.exists():
        return []
    payload = json.loads(REDDIT_FINDINGS_PATH.read_text(encoding="utf-8"))
    findings = payload.get("top_findings", [])
    candidates: list[dict[str, Any]] = []
    for finding in findings:
        blob = " ".join(
            [
                clean_text(str(finding.get("title", ""))),
                clean_text(str(finding.get("summary", ""))),
                " ".join(clean_text(step) for step in finding.get("action_plan", [])),
                " ".join(clean_text(asset) for asset in finding.get("existing_assets_applicable", [])),
            ]
        ).lower()
        if not has_app_signal({"category": str(finding.get("category", "")), "summary": blob, "title": str(finding.get("title", ""))}):
            continue
        fid = clean_text(str(finding.get("finding_id", ""))) or f"reddit_{slugify(str(finding.get('title', 'app finding')))}"
        candidate = {
            "candidate_id": fid,
            "title": clean_text(str(finding.get("title", ""))) or fid,
            "source": clean_text(str(finding.get("subreddit", "reddit"))),
            "source_url": clean_text(str(finding.get("source_url", ""))),
            "source_type": "reddit_findings",
            "roi_potential": clean_text(str(finding.get("roi_potential", "HIGH"))) or "HIGH",
            "priority": "HIGH" if int(finding.get("actionability", 0) or 0) >= 8 else "MEDIUM",
            "status": "APPROVED",
            "engagement_authenticity": clean_text(str(finding.get("engagement_authenticity", "AUTHENTIC"))) or "AUTHENTIC",
            "earnings_verified": clean_text(str(finding.get("earnings_verified", "FALSE"))) or "FALSE",
            "applicable_niches": clean_text(str(finding.get("category", ""))),
            "applicable_methods": "APP_FACTORY",
            "evidence": clean_text(str(finding.get("summary", ""))),
            "compliance_notes": clean_text(str(finding.get("earnings_skepticism_note", ""))),
            "reviewer_notes": " | ".join(clean_text(step) for step in finding.get("action_plan", [])),
            "existing_spec": detect_existing_spec(fid),
            "raw": finding,
        }
        candidates.append(candidate)
    return candidates


def dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        key = clean_text(candidate.get("candidate_id", "")) or slugify(candidate.get("title", "candidate"))
        current = deduped.get(key)
        if not current:
            deduped[key] = candidate
            continue

        current_score = SOURCE_SCORES.get(current["source_type"], 0)
        next_score = SOURCE_SCORES.get(candidate["source_type"], 0)
        if next_score > current_score:
            merged = candidate
            merged["reviewer_notes"] = " | ".join(filter(None, [current.get("reviewer_notes", ""), candidate.get("reviewer_notes", "")]))
            deduped[key] = merged
        else:
            current["reviewer_notes"] = " | ".join(filter(None, [current.get("reviewer_notes", ""), candidate.get("reviewer_notes", "")]))
    return list(deduped.values())


def build_queue(limit: int) -> dict[str, Any]:
    alpha_candidates = load_alpha_staging_candidates()
    method_candidates = load_method_candidates()
    reddit_candidates = load_reddit_candidates()

    combined = dedupe_candidates(alpha_candidates + method_candidates + reddit_candidates)
    scored = [score_candidate(candidate) for candidate in combined]
    scored.sort(key=lambda item: (item["score"], item["build_decision"] != "PARK"), reverse=True)

    queue = scored[:limit]
    for idx, candidate in enumerate(queue, start=1):
        candidate["rank"] = idx

    decision_counts = Counter(candidate["build_decision"] for candidate in scored)
    cluster_counts = Counter(candidate["cluster"] for candidate in scored)

    return {
        "generated_at": now_iso(),
        "sources": {
            "alpha_staging_candidates": len(alpha_candidates),
            "app_factory_methods_candidates": len(method_candidates),
            "reddit_findings_candidates": len(reddit_candidates),
            "existing_specs_detected": len([item for item in scored if item.get("existing_spec")]),
        },
        "hard_gates": HARD_GATES,
        "summary": {
            "total_candidates": len(scored),
            "decision_counts": dict(decision_counts),
            "cluster_counts": dict(cluster_counts),
        },
        "queue": queue,
    }


def write_outputs(payload: dict[str, Any]) -> None:
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COMMAND_CENTER_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUEUE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    COMMAND_CENTER_PATH.write_text(render_markdown(payload), encoding="utf-8")


def md_escape(text: str) -> str:
    return clean_text(text).replace("|", "\\|")


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No items in this section.\n"

    lines = [
        "| Rank | Opportunity | Target | Score | Market | Monetization | Core Test |",
        "|------|-------------|--------|-------|--------|--------------|-----------|",
    ]
    for row in rows:
        lines.append(
            "| {rank} | {title} | {target} | {score} | {market_room} | {monetization} | {experiment} |".format(
                rank=row.get("rank", "-"),
                title=md_escape(row.get("title", ""))[:80],
                target=md_escape(row.get("existing_app_target", "NEW_BUILD"))[:55],
                score=row.get("score", 0),
                market_room=row.get("market_room", ""),
                monetization=md_escape(row.get("primary_monetization", ""))[:55],
                experiment=md_escape((row.get("experiments") or [""])[0])[:65],
            )
        )
    return "\n".join(lines) + "\n"


def render_markdown(payload: dict[str, Any]) -> str:
    queue = payload.get("queue", [])
    iterate_now = [row for row in queue if row["build_decision"] == "ITERATE_EXISTING_NOW"]
    build_now = [row for row in queue if row["build_decision"] == "BUILD_NEW_NOW"]
    spec_and_test = [row for row in queue if row["build_decision"] == "SPEC_AND_TEST"]
    validate_first = [row for row in queue if row["build_decision"] == "VALIDATE_FIRST"]

    top_rows = iterate_now[:5] + build_now[:5]
    top_rows.sort(key=lambda item: item["score"], reverse=True)
    top_rows = top_rows[:5]

    lines = [
        "# App Factory Alpha Command Center",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "## Inputs",
        f"- ALPHA_STAGING app candidates: {payload['sources']['alpha_staging_candidates']}",
        f"- APP_FACTORY_METHODS candidates: {payload['sources']['app_factory_methods_candidates']}",
        f"- Reddit findings consumed: {payload['sources']['reddit_findings_candidates']}",
        f"- Existing app specs detected: {payload['sources']['existing_specs_detected']}",
        "",
        "## Hard Gates",
    ]
    lines.extend([f"- {gate}" for gate in payload["hard_gates"]])
    lines.extend(
        [
            "",
            "## Top Queue",
            render_table(top_rows),
            "## Iterate Existing Now",
            render_table(iterate_now[:8]),
            "## Build New Now",
            render_table(build_now[:8]),
            "## Spec And Test",
            render_table(spec_and_test[:8]),
            "## Validate First",
            render_table(validate_first[:8]),
            "## Operating Rules",
            "- Run `python3 AUTOMATIONS/app_factory_command_center.py --refresh` before any app-spec or build cycle.",
            "- If the queue maps an alpha item to an existing app, ship that upgrade before greenfield work.",
            "- Default pricing is lower-cost annual-first testing, not vanity $99 plans with no evidence.",
            "- Affiliate links are secondary. Core retention and billing come first.",
            "- Review prompts only fire after a real win, never in the first dead minutes of onboarding.",
            "",
            "## Current Human Blockers",
            "- Store account and payment rails still gate real app revenue. Queue quality is no longer the blocker.",
            "- If App Store, Stripe, and platform auth are still missing, the system should keep ranking/specifying and avoid pretending the launch bottleneck is solved.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def print_summary(payload: dict[str, Any], top: int) -> None:
    print(f"App factory queue refreshed at {payload['generated_at']}")
    print(f"Candidates: {payload['summary']['total_candidates']}")
    print(f"Decisions: {payload['summary']['decision_counts']}")
    print("Top priorities:")
    for row in payload.get("queue", [])[:top]:
        print(
            f"  {row['rank']:>2}. {row['title']} | {row['build_decision']} | "
            f"score={row['score']} | target={row['existing_app_target']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the ranked app-factory alpha queue.")
    parser.add_argument("--refresh", action="store_true", help="recompute queue and rewrite outputs")
    parser.add_argument("--status", action="store_true", help="print current queue summary")
    parser.add_argument("--top", type=int, default=8, help="number of queue items to print")
    parser.add_argument("--limit", type=int, default=40, help="max queue size to write")
    args = parser.parse_args()

    if args.status and not args.refresh and QUEUE_PATH.exists():
        payload = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
        print_summary(payload, args.top)
        return

    payload = build_queue(limit=args.limit)
    write_outputs(payload)
    print_summary(payload, args.top)


if __name__ == "__main__":
    main()
