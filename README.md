# NHS RTT Incomplete Pathways — Data Engineering Pipeline

A production-style medallion architecture pipeline processing NHS Referral to Treatment (RTT) incomplete pathways data across England. Covers **~14 million patient pathways** across **140+ NHS trusts** and **18 treatment specialties**, with 3 months of trend data (March–May 2025).

---

## What This Project Does

The NHS has a statutory target: 92% of patients should begin treatment within 18 weeks of referral. As of May 2025, the national compliance rate is **59.9%** — and the number of patients waiting over 52 weeks is **rising every month**.

This pipeline ingests the monthly RTT Excel releases published by NHS England, transforms them through a Bronze → Silver → Gold medallion architecture, and produces analytics-ready outputs that surface where the wait crisis is worst — by trust, by specialty, and over time.

---

## Key Findings (May 2025)

| Metric | Value |
|--------|-------|
| Total incomplete pathways | 13,925,262 |
| 18-week compliance (national) | 59.9% |
| NHS 18-week target | 92.0% |
| Patients waiting 52+ weeks | 387,162 |
| Change in 52w+ waits (Mar→May) | +33,686 |

**Worst specialty:** Ear Nose and Throat — 28,377 patients waiting over a year (50.7% compliance)

**Worst trust by compliance:** The Robert Jones and Agnes Hunt Orthopaedic Hospital NHS Foundation Trust — 43.9%

**Widest compliance gap:** Dermatology — 81.5 percentage points between best trust (Blackpool, 97.3%) and worst (Salisbury, 15.8%)

---

## Architecture

```
Raw Excel (NHS England)
        │
        ▼
┌───────────────┐
│    BRONZE     │  Parquet — raw data preserved exactly as received
│  (ingestion)  │  + source_file, snapshot_month, load_timestamp
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    SILVER     │  Parquet — standardised column names, enforced types
│  (cleaning)   │  snake_case, Unicode cleaned, summary rows dropped
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     GOLD      │  Parquet — analytics-ready business metrics
│  (analytics)  │  18w compliance, 52w+/65w+/78w+ long waits,
│               │  median & P92 estimated wait, monthly history
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   ANALYSIS    │  CSV — 6 findings tables for reporting
└───────────────┘
```

---

## Project Structure

```
nhs-rtt-incomplete-pathways-pipeline/
├── data/
│   ├── raw/
│   │   └── waiting_times/          # Raw NHS England Excel files
│   └── processed/
│       ├── bronze/                 # Raw Parquet (one per month per level)
│       ├── silver/                 # Cleaned Parquet
│       └── gold/
│           └── rtt_incomplete_pathways/
│               ├── gold_rtt_waits_YYYY-MM.parquet   # Monthly snapshots
│               ├── gold_rtt_history.parquet          # Combined trend table
│               └── analysis/                         # 6 analysis CSVs
├── pipelines/
│   ├── 01_bronze_rtt_loader.py
│   ├── 02_silver_rtt_provider.py
│   ├── 03_silver_rtt_commissioner.py
│   ├── 04_gold_rtt_waits.py
│   ├── 05_gold_rtt_history.py
│   └── 06_analysis_rtt.py
├── docs/
│   └── data_contracts/
└── README.md
```

---

## Pipeline Scripts

| Script | Layer | Description |
|--------|-------|-------------|
| `01_bronze_rtt_loader.py` | Bronze | Ingests all monthly Excel files, flattens two-row headers, preserves raw data as Parquet |
| `02_silver_rtt_provider.py` | Silver | Cleans provider data: snake_case columns, type coercion, lineage |
| `03_silver_rtt_commissioner.py` | Silver | Same for commissioner/ICB level data |
| `04_gold_rtt_waits.py` | Gold | Builds waiting time metrics: compliance, long waits, median/P92 per trust per specialty |
| `05_gold_rtt_history.py` | Gold | Combines monthly snapshots into a single trend table |
| `06_analysis_rtt.py` | Analysis | 6 findings tables: national trend, worst trusts, worst specialties, movers, compliance gaps |

---

## Running the Pipeline

```bash
# 1. Place raw Excel files in data/raw/waiting_times/
#    Naming convention: rtt_incomplete_provider_mar25.xlsx
#                       rtt_incomplete_commissioner_mar25.xlsx

# 2. Run pipeline in order
python pipelines/01_bronze_rtt_loader.py
python pipelines/02_silver_rtt_provider.py
python pipelines/03_silver_rtt_commissioner.py
python pipelines/04_gold_rtt_waits.py
python pipelines/05_gold_rtt_history.py
python pipelines/06_analysis_rtt.py
```

Adding a new month is as simple as dropping the new Excel files into `data/raw/waiting_times/` and re-running the pipeline. The history table builds automatically.

---

## Data Source

**NHS England — Referral to Treatment (RTT) Waiting Times Statistics**
Published monthly at: https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/

Data covers provider-level and commissioner-level incomplete pathways by treatment function and weekly wait band (0–1 weeks through 104+ weeks).

---

## Tech Stack

- **Python** — pandas, numpy, pathlib
- **Storage** — Parquet (Bronze/Silver/Gold), CSV (analysis outputs)
- **Architecture** — Medallion (Bronze → Silver → Gold)
- **Environment** — Anaconda, JupyterLab

---

## Related Work

- **Project 1:** [NHS Outpatient Attendance Pipeline — 226 million records](https://medium.com/@yusufismail_91982/i-processed-226-million-nhs-patient-records-heres-what-i-found-c35455d3c5f1)

---

*Built by [Yusuf Ismail](https://github.com/YusufIsmailayo) — Data Engineer focused on NHS pipelines and public sector analytics.*
