"""
Silver Layer — RTT Provider (Incomplete Pathways)
==================================================
Loops through all Bronze provider Parquet files.
Cleans column names, enforces types, drops artefacts.
Writes one Silver Parquet per month.

Usage:
    python pipelines/02_silver_rtt_provider.py
"""

from pathlib import Path
from datetime import datetime, timezone
import re
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR   = PROJECT_ROOT / "data" / "processed" / "bronze"
SILVER_DIR   = PROJECT_ROOT / "data" / "processed" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

DIMENSION_COLS = [
    "region_code", "provider_code", "provider_name",
    "treatment_function_code", "treatment_function",
]

RENAME_MAP = {
    "Region Code":             "region_code",
    "Provider Code":           "provider_code",
    "Provider Name":           "provider_name",
    "Treatment Function Code": "treatment_function_code",
    "Treatment Function":      "treatment_function",
}


def drop_artefacts(df):
    df = df.loc[:, ~df.columns.str.match(r"^col_\d+$")]
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    return df


def clean_column_names(df):
    new_cols = {}
    for col in df.columns:
        if col in RENAME_MAP.values():
            new_cols[col] = col
        else:
            clean = (
                col.strip().lower()
                .replace("\u2013", "_").replace("\u2014", "_").replace("\u2212", "_")
                .replace(">", "").replace("<", "").replace("-", "_").replace(" ", "_")
            )
            clean = re.sub(r"[^a-z0-9_]", "_", clean)
            clean = re.sub(r"_+", "_", clean).strip("_")
            new_cols[col] = clean
    return df.rename(columns=new_cols)


def coerce_types(df):
    stay_string = set(DIMENSION_COLS) | {"source_file", "load_timestamp", "snapshot_month"}
    for col in df.columns:
        if col in stay_string:
            df[col] = df[col].astype(str).str.strip()
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def process_month(bronze_path: Path):
    month = bronze_path.stem.replace("bronze_rtt_provider_", "")
    print(f"\n  [{month}]")

    df = pd.read_parquet(bronze_path)
    df = df.rename(columns=RENAME_MAP)
    df = drop_artefacts(df)
    df = clean_column_names(df)
    df = coerce_types(df)
    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    missing = [c for c in DIMENSION_COLS if c not in df.columns]
    if missing:
        print(f"    WARNING: Missing dimensions: {missing}")
    else:
        print(f"    Dimensions: OK | Shape: {df.shape} | Nulls in dims: {df[DIMENSION_COLS].isna().sum().sum()}")

    out = SILVER_DIR / f"silver_rtt_provider_{month}.parquet"
    df.to_parquet(out, index=False)
    print(f"    Saved → {out.relative_to(PROJECT_ROOT)}")


def main():
    print("\n── Silver: RTT Provider ─────────────────────────────────────")

    bronze_files = sorted(BRONZE_DIR.glob("bronze_rtt_provider_*.parquet"))
    if not bronze_files:
        raise FileNotFoundError("No Bronze provider files found. Run 01_bronze_rtt_loader.py first.")

    print(f"  Found {len(bronze_files)} month(s)")
    for f in bronze_files:
        process_month(f)

    print("\n── Silver provider complete ─────────────────────────────────\n")


if __name__ == "__main__":
    main()
