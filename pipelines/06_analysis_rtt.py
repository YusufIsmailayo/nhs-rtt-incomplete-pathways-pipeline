"""
Analysis — RTT Incomplete Pathways
====================================
Queries Gold history to surface findings for the Medium article.

Outputs 6 analysis tables as CSV to data/processed/gold/rtt_incomplete_pathways/analysis/

  1. national_trend.csv          — monthly national totals
  2. worst_trusts_compliance.csv — bottom 20 trusts by avg 18w compliance
  3. worst_trusts_longwaits.csv  — top 20 trusts by total 52w+ waits (latest month)
  4. worst_specialties.csv       — specialties with most 52w+ waits nationally
  5. trust_movers.csv            — trusts with biggest change in 52w+ waits Mar→May
  6. compliance_gap.csv          — best vs worst trust per specialty (latest month)

Usage:
    python pipelines/06_analysis_rtt.py
"""

from pathlib import Path
import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR     = PROJECT_ROOT / "data" / "processed" / "gold" / "rtt_incomplete_pathways"
OUT_DIR      = GOLD_DIR / "analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LATEST_MONTH = "2025-05"
FIRST_MONTH  = "2025-03"

# ── Treatment function codes/names that are summary rows, not real specialties
EXCLUDE_TF = {
    "total", "all", "grand total", "not known", "other",
    "total (all)", "total all"
}


def is_summary_row(tf_code: str, tf_name: str) -> bool:
    """Return True if this row is a national/total summary, not a real specialty."""
    code = str(tf_code).strip().lower()
    name = str(tf_name).strip().lower()
    # Numeric codes are real specialties (e.g. 100, 101, 110...)
    # Non-numeric or known summary labels are excluded
    if any(excl in name for excl in EXCLUDE_TF):
        return True
    if any(excl in code for excl in EXCLUDE_TF):
        return True
    return False


def load_history() -> pd.DataFrame:
    path = GOLD_DIR / "gold_rtt_history.parquet"
    if not path.exists():
        raise FileNotFoundError(f"History not found: {path}\nRun 05_gold_rtt_history.py first.")
    df = pd.read_parquet(path)
    # Exclude national total rows (no real provider code)
    df = df[df["provider_code"].notna()]
    df = df[~df["provider_code"].astype(str).str.lower().isin(["nan", "none", ""])]
    return df


def filter_real_specialties(df: pd.DataFrame) -> pd.DataFrame:
    """Remove summary/total rows from specialty dimension."""
    mask = df.apply(
        lambda r: not is_summary_row(r["treatment_function_code"], r["treatment_function"]),
        axis=1
    )
    return df[mask]


def save(df: pd.DataFrame, name: str):
    out = OUT_DIR / name
    df.to_csv(out, index=False)
    print(f"  Saved → {out.relative_to(PROJECT_ROOT)}  ({len(df):,} rows)")


# ── 1. National trend ──────────────────────────────────────────────────────
def national_trend(df: pd.DataFrame) -> pd.DataFrame:
    trend = (
        df.groupby("snapshot_month")
        .agg(
            total_incomplete_pathways=("total_incomplete_pathways", "sum"),
            total_within_18w=("total_within_18w", "sum"),
            total_52w_plus=("total_52w_plus", "sum"),
            total_65w_plus=("total_65w_plus", "sum"),
            total_78w_plus=("total_78w_plus", "sum"),
        )
        .reset_index()
        .sort_values("snapshot_month")
    )
    trend["pct_within_18w"] = (
        trend["total_within_18w"] / trend["total_incomplete_pathways"]
    ).round(4)
    trend["pct_52w_plus"] = (
        trend["total_52w_plus"] / trend["total_incomplete_pathways"]
    ).round(4)
    trend["mom_52w_change"] = trend["total_52w_plus"].diff().fillna(0).astype(int)
    return trend


# ── 2. Worst trusts by 18w compliance ─────────────────────────────────────
def worst_trusts_compliance(df: pd.DataFrame) -> pd.DataFrame:
    latest = df[df["snapshot_month"] == LATEST_MONTH]
    latest = filter_real_specialties(latest)
    trust = (
        latest.groupby(["provider_code", "provider_name", "region_code"])
        .agg(
            total_incomplete_pathways=("total_incomplete_pathways", "sum"),
            total_within_18w=("total_within_18w", "sum"),
            total_52w_plus=("total_52w_plus", "sum"),
        )
        .reset_index()
    )
    trust = trust[trust["total_incomplete_pathways"] >= 100]
    trust["pct_within_18w"] = (
        trust["total_within_18w"] / trust["total_incomplete_pathways"]
    ).round(4)
    trust["pct_52w_plus"] = (
        trust["total_52w_plus"] / trust["total_incomplete_pathways"]
    ).round(4)
    return (
        trust.sort_values("pct_within_18w")
        .head(20)
        .reset_index(drop=True)
    )


# ── 3. Worst trusts by 52w+ volume ────────────────────────────────────────
def worst_trusts_longwaits(df: pd.DataFrame) -> pd.DataFrame:
    latest = df[df["snapshot_month"] == LATEST_MONTH]
    latest = filter_real_specialties(latest)
    trust = (
        latest.groupby(["provider_code", "provider_name", "region_code"])
        .agg(
            total_incomplete_pathways=("total_incomplete_pathways", "sum"),
            total_52w_plus=("total_52w_plus", "sum"),
            total_65w_plus=("total_65w_plus", "sum"),
            total_78w_plus=("total_78w_plus", "sum"),
        )
        .reset_index()
    )
    trust["pct_52w_plus"] = (
        trust["total_52w_plus"] / trust["total_incomplete_pathways"]
    ).round(4)
    return (
        trust.sort_values("total_52w_plus", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )


# ── 4. Worst specialties nationally ───────────────────────────────────────
def worst_specialties(df: pd.DataFrame) -> pd.DataFrame:
    latest = df[df["snapshot_month"] == LATEST_MONTH]
    latest = filter_real_specialties(latest)
    spec = (
        latest.groupby(["treatment_function_code", "treatment_function"])
        .agg(
            total_incomplete_pathways=("total_incomplete_pathways", "sum"),
            total_within_18w=("total_within_18w", "sum"),
            total_52w_plus=("total_52w_plus", "sum"),
            total_65w_plus=("total_65w_plus", "sum"),
        )
        .reset_index()
    )
    spec = spec[spec["total_incomplete_pathways"] >= 1000]
    spec["pct_within_18w"] = (
        spec["total_within_18w"] / spec["total_incomplete_pathways"]
    ).round(4)
    spec["pct_52w_plus"] = (
        spec["total_52w_plus"] / spec["total_incomplete_pathways"]
    ).round(4)
    return (
        spec.sort_values("total_52w_plus", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )


# ── 5. Trust movers — biggest change in 52w+ Mar→May ─────────────────────
def trust_movers(df: pd.DataFrame) -> pd.DataFrame:
    df = filter_real_specialties(df)
    first = df[df["snapshot_month"] == FIRST_MONTH]
    last  = df[df["snapshot_month"] == LATEST_MONTH]

    agg_cols = {
        "total_incomplete_pathways": "sum",
        "total_52w_plus": "sum",
    }
    first_agg = first.groupby("provider_code").agg(agg_cols).reset_index()
    last_agg  = last.groupby(["provider_code", "provider_name"]).agg(agg_cols).reset_index()

    merged = last_agg.merge(
        first_agg, on="provider_code", suffixes=("_may", "_mar")
    )
    merged["change_52w_plus"] = (
        merged["total_52w_plus_may"] - merged["total_52w_plus_mar"]
    ).astype(int)
    merged["pct_change_52w"] = (
        merged["change_52w_plus"] / merged["total_52w_plus_mar"].replace(0, np.nan)
    ).round(4)

    worst = merged.nlargest(10, "change_52w_plus")
    best  = merged.nsmallest(10, "change_52w_plus")
    result = pd.concat([worst, best]).drop_duplicates("provider_code")
    result["direction"] = result["change_52w_plus"].apply(
        lambda x: "deteriorating" if x > 0 else "improving"
    )
    return result.sort_values("change_52w_plus", ascending=False).reset_index(drop=True)


# ── 6. Compliance gap — best vs worst trust per specialty ─────────────────
def compliance_gap(df: pd.DataFrame) -> pd.DataFrame:
    latest = df[df["snapshot_month"] == LATEST_MONTH]
    latest = filter_real_specialties(latest)
    trust_spec = (
        latest.groupby(["treatment_function", "provider_code", "provider_name"])
        .agg(
            total=("total_incomplete_pathways", "sum"),
            within_18w=("total_within_18w", "sum"),
        )
        .reset_index()
    )
    trust_spec = trust_spec[trust_spec["total"] >= 200]
    trust_spec["pct_within_18w"] = (
        trust_spec["within_18w"] / trust_spec["total"]
    ).round(4)

    best  = trust_spec.loc[trust_spec.groupby("treatment_function")["pct_within_18w"].idxmax()]
    worst = trust_spec.loc[trust_spec.groupby("treatment_function")["pct_within_18w"].idxmin()]

    best  = best.rename(columns={"provider_name": "best_trust",  "pct_within_18w": "best_compliance"})
    worst = worst.rename(columns={"provider_name": "worst_trust", "pct_within_18w": "worst_compliance"})

    gap = best[["treatment_function", "best_trust", "best_compliance"]].merge(
        worst[["treatment_function", "worst_trust", "worst_compliance"]],
        on="treatment_function"
    )
    gap["compliance_gap"] = (gap["best_compliance"] - gap["worst_compliance"]).round(4)
    return (
        gap.sort_values("compliance_gap", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print("\n── Analysis: RTT Incomplete Pathways ────────────────────────")

    df = load_history()
    print(f"  History loaded: {df.shape} | Months: {sorted(df['snapshot_month'].unique())}")

    print("\n[1/6] National trend")
    t = national_trend(df)
    save(t, "national_trend.csv")
    for _, row in t.iterrows():
        print(f"  {row['snapshot_month']} | pathways={row['total_incomplete_pathways']:,.0f} | "
              f"18w={row['pct_within_18w']:.1%} | 52w+={row['total_52w_plus']:,.0f} "
              f"(MoM: {row['mom_52w_change']:+,.0f})")

    print("\n[2/6] Worst trusts by 18w compliance")
    wc = worst_trusts_compliance(df)
    save(wc, "worst_trusts_compliance.csv")
    print(wc[["provider_name", "pct_within_18w", "total_52w_plus"]].head(10).to_string(index=False))

    print("\n[3/6] Worst trusts by 52w+ volume")
    wl = worst_trusts_longwaits(df)
    save(wl, "worst_trusts_longwaits.csv")
    print(wl[["provider_name", "total_52w_plus", "pct_52w_plus"]].head(10).to_string(index=False))

    print("\n[4/6] Worst specialties nationally")
    ws = worst_specialties(df)
    save(ws, "worst_specialties.csv")
    print(ws[["treatment_function", "total_52w_plus", "pct_within_18w"]].head(10).to_string(index=False))

    print("\n[5/6] Trust movers Mar→May")
    tm = trust_movers(df)
    save(tm, "trust_movers.csv")
    print(tm[["provider_name", "change_52w_plus", "direction"]].head(10).to_string(index=False))

    print("\n[6/6] Compliance gap by specialty")
    cg = compliance_gap(df)
    save(cg, "compliance_gap.csv")
    print(cg[["treatment_function", "best_trust", "best_compliance",
               "worst_trust", "worst_compliance", "compliance_gap"]].head(10).to_string(index=False))

    print("\n── Analysis complete ────────────────────────────────────────")
    print(f"  All outputs in: data/processed/gold/rtt_incomplete_pathways/analysis/\n")


if __name__ == "__main__":
    main()
