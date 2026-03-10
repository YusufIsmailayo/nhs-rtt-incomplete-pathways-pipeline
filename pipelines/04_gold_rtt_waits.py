"""
Gold Layer — RTT Waiting Times Analysis
========================================
Loops through all Silver provider Parquet files.
Builds Gold waits summary per month:
  - total pathways, 18w compliance, 52w+/65w+/78w+ long waits
  - median and P92 estimated wait

Usage:
    python pipelines/04_gold_rtt_waits.py
"""

from pathlib import Path
from datetime import datetime, timezone
import re
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR   = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_DIR     = PROJECT_ROOT / "data" / "processed" / "gold" / "rtt_incomplete_pathways"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

NON_MEASURE = {
    "region_code", "provider_code", "provider_name",
    "treatment_function_code", "treatment_function",
    "source_file", "load_timestamp", "snapshot_month"
}


def parse_week_lower(col: str):
    m = re.match(r"^(\d+)_(\d+|plus)$", col)
    return float(m.group(1)) if m else None


def get_week_columns(df):
    cols = [
        c for c in df.columns
        if c not in NON_MEASURE
        and pd.api.types.is_numeric_dtype(df[c])
        and parse_week_lower(c) is not None
    ]
    return sorted(cols, key=parse_week_lower)


def weighted_percentile(row, week_cols, pct):
    weeks, counts = [], []
    for col in week_cols:
        w = parse_week_lower(col)
        val = row[col]
        if w is not None and not pd.isna(val) and val > 0:
            weeks.append(w)
            counts.append(val)
    if not counts or sum(counts) == 0:
        return np.nan
    total = sum(counts)
    target = pct * total
    cumulative = 0.0
    for w, c in zip(weeks, counts):
        cumulative += c
        if cumulative >= target:
            return w
    return weeks[-1] if weeks else np.nan


def build_gold(df, week_cols, snapshot_month):
    dims = ["region_code", "provider_code", "provider_name",
            "treatment_function_code", "treatment_function"]
    result = df[dims].copy()

    result["total_incomplete_pathways"] = df[week_cols].sum(axis=1)
    within_18 = [c for c in week_cols if parse_week_lower(c) < 18]
    result["total_within_18w"] = df[within_18].sum(axis=1)
    result["pct_within_18w"] = (
        result["total_within_18w"] / result["total_incomplete_pathways"]
    ).round(4)

    for t in [52, 65, 78]:
        cols = [c for c in week_cols if parse_week_lower(c) >= t]
        result[f"total_{t}w_plus"] = df[cols].sum(axis=1)

    result["pct_52w_plus"] = (
        result["total_52w_plus"] / result["total_incomplete_pathways"]
    ).round(4)

    result["median_wait_weeks"] = df.apply(
        lambda r: weighted_percentile(r, week_cols, 0.50), axis=1)
    result["p92_wait_weeks"] = df.apply(
        lambda r: weighted_percentile(r, week_cols, 0.92), axis=1)

    result["snapshot_month"] = snapshot_month
    result["load_timestamp"]  = datetime.now(timezone.utc).isoformat()
    return result


def process_month(silver_path: Path):
    month = silver_path.stem.replace("silver_rtt_provider_", "")
    print(f"\n  [{month}]")

    df = pd.read_parquet(silver_path)
    df = df[df["provider_code"].notna()]
    df = df[~df["provider_code"].astype(str).str.lower().isin(["nan", "none", ""])]

    week_cols = get_week_columns(df)
    gold = build_gold(df, week_cols, month)

    total    = gold["total_incomplete_pathways"].sum()
    avg_comp = gold["pct_within_18w"].mean()
    lw52     = gold["total_52w_plus"].sum()

    print(f"    Total pathways   : {total:,.0f}")
    print(f"    18w compliance   : {avg_comp:.1%}")
    print(f"    52w+ long waits  : {lw52:,.0f}")
    print(f"    Rows             : {len(gold):,}")

    out = GOLD_DIR / f"gold_rtt_waits_{month}.parquet"
    gold.to_parquet(out, index=False)
    print(f"    Saved → {out.relative_to(PROJECT_ROOT)}")


def main():
    print("\n── Gold: RTT Waits ──────────────────────────────────────────")

    silver_files = sorted(SILVER_DIR.glob("silver_rtt_provider_*.parquet"))
    if not silver_files:
        raise FileNotFoundError("No Silver provider files found. Run 02_silver_rtt_provider.py first.")

    print(f"  Found {len(silver_files)} month(s)")
    for f in silver_files:
        process_month(f)

    print("\n── Gold waits complete ──────────────────────────────────────\n")


if __name__ == "__main__":
    main()
