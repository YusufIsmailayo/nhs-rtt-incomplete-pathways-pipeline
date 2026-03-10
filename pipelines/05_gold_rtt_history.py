"""
Gold Layer — RTT History (Multi-Snapshot)
==========================================
Rule: answer one question per script.

Question: How are waiting times trending across months?

Reads all Gold waits snapshots and combines them into
a single history table for trend analysis.

Usage:
    python pipelines/05_gold_rtt_history.py
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR     = PROJECT_ROOT / "data" / "processed" / "gold" / "rtt_incomplete_pathways"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def main():
    print("\n── Gold: RTT History ────────────────────────────────────────")

    # Find all monthly snapshot files
    snapshot_files = sorted([
        f for f in GOLD_DIR.glob("gold_rtt_waits_*.parquet")
        if "history" not in f.name
    ])

    if not snapshot_files:
        raise FileNotFoundError(
            f"No snapshot files found in {GOLD_DIR}\n"
            "Run 04_gold_rtt_waits.py --month YYYY-MM for each month first."
        )

    print(f"  Found {len(snapshot_files)} snapshot(s):")
    for f in snapshot_files:
        print(f"    - {f.name}")

    # Combine all snapshots
    frames = [pd.read_parquet(f) for f in snapshot_files]
    history = pd.concat(frames, ignore_index=True)

    # Ensure snapshot_month is sorted correctly
    history = history.sort_values(
        ["snapshot_month", "provider_code", "treatment_function_code"]
    ).reset_index(drop=True)

    history["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Quality check
    months = sorted(history["snapshot_month"].unique())
    print(f"\n  Snapshot months: {months}")
    print(f"  Total rows: {len(history):,}")
    print(f"  Total pathways (latest month):")
    latest = history[history["snapshot_month"] == months[-1]]
    print(f"    {latest['total_incomplete_pathways'].sum():,.0f}")

    # Save
    out = GOLD_DIR / "gold_rtt_history.parquet"
    history.to_parquet(out, index=False)
    print(f"\n  Saved → {out.relative_to(PROJECT_ROOT)}")
    print("\n── Gold history complete ────────────────────────────────────\n")


if __name__ == "__main__":
    main()
