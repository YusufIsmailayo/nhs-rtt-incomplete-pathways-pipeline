"""
Bronze Layer — RTT Incomplete Pathways
=======================================
Rule: preserve, never modify.

Loops through all monthly RTT Excel files in data/raw/waiting_times/
and loads both provider and commissioner sheets into Bronze as Parquet.
No cleaning, no type coercion. Adds source_file and load_timestamp only.

Usage:
    python pipelines/01_bronze_rtt_loader.py
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import re

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR      = PROJECT_ROOT / "data" / "raw" / "waiting_times"
BRONZE_DIR   = PROJECT_ROOT / "data" / "processed" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

HEADER_ROWS = [12, 13]


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns from two-row Excel headers."""
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for a, b in df.columns:
            a_str = str(a).strip()
            b_str = str(b).strip()
            if b_str and "Unnamed" not in b_str and b_str != "nan":
                new_cols.append(b_str)
            elif a_str and "Unnamed" not in a_str and a_str != "nan":
                new_cols.append(a_str)
            else:
                new_cols.append(f"col_{len(new_cols)}")
        df.columns = new_cols
    return df


def extract_month_label(filename: str) -> str:
    """
    Extract snapshot month from filename.
    'rtt_incomplete_provider_mar25.xlsx' → '2025-03'
    """
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    m = re.search(r"([a-z]{3})(\d{2})\.xlsx$", filename.lower())
    if m:
        mon, yr = m.group(1), m.group(2)
        return f"20{yr}-{month_map.get(mon, '00')}"
    return "unknown"


def load_bronze(path: Path, sheet_name: str, snapshot_month: str) -> pd.DataFrame:
    """Load one raw RTT Excel sheet with zero transformation."""
    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=HEADER_ROWS,
        dtype=str
    )
    df = flatten_columns(df)
    df["source_file"]     = path.name
    df["snapshot_month"]  = snapshot_month
    df["load_timestamp"]  = datetime.now(timezone.utc).isoformat()
    print(f"    Loaded sheet={sheet_name!r} → {df.shape} | First 4 cols: {list(df.columns[:4])}")
    return df


def process_month(provider_path: Path, commissioner_path: Path, month_label: str):
    print(f"\n  [{month_label}]")

    # Provider
    provider = load_bronze(provider_path, "Provider", month_label)
    out_p = BRONZE_DIR / f"bronze_rtt_provider_{month_label}.parquet"
    provider.to_parquet(out_p, index=False)
    print(f"    Saved → {out_p.relative_to(PROJECT_ROOT)}  ({len(provider):,} rows)")

    # Commissioner
    commissioner = load_bronze(commissioner_path, "National", month_label)
    out_c = BRONZE_DIR / f"bronze_rtt_commissioner_{month_label}.parquet"
    commissioner.to_parquet(out_c, index=False)
    print(f"    Saved → {out_c.relative_to(PROJECT_ROOT)}  ({len(commissioner):,} rows)")


def main():
    print("\n── Bronze: RTT Incomplete Pathways ──────────────────────────")

    # Find all provider files and pair with commissioner
    provider_files = sorted(RAW_DIR.glob("rtt_incomplete_provider_*.xlsx"))

    if not provider_files:
        raise FileNotFoundError(f"No provider files found in {RAW_DIR}")

    print(f"  Found {len(provider_files)} month(s) to process")

    for provider_path in provider_files:
        month_label = extract_month_label(provider_path.name)
        commissioner_name = provider_path.name.replace("provider", "commissioner")
        commissioner_path = RAW_DIR / commissioner_name

        if not commissioner_path.exists():
            print(f"  WARNING: No commissioner file for {month_label}, skipping")
            continue

        process_month(provider_path, commissioner_path, month_label)

    print("\n── Bronze complete ──────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
