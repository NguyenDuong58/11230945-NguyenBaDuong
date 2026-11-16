# A Big-Data Pipeline for Bank Failure Early Warning

**BFT65B — Big Data Analytics in Finance | Parts 1–4: Acquisition, Storage, Data Quality, Scalable Processing and Feature Engineering**  
**Repository:** https://github.com/NguyenDuong58/11230945-NguyenBaDuong  
**Runtime:** Google Colab (Python); persistent artifacts: Google Drive; source and tests: GitHub.

## Research objective and grading requirements

Part 1 collects the specified FDIC datasets and establishes a verified storage design. The study covers all available Financials observations for the 170 calendar quarters from **1984Q1 through 2026Q2**, the complete FDIC Failures table, and the complete FDIC Institutions table. Downloaded record counts are **observations to be measured**, not predetermined acceptance constants.

The storage study compares losslessly equivalent uncompressed CSV and Snappy Parquet, with repeatable timing and memory measurements. A separate experiment measures physical file discovery, filtered scanning and selected Parquet fragments in the 170-quarter layout. Data acquisition, storage analysis and test/model development are separate stages: no future failure event or current institution status is introduced as a financial predictor.

**Completion criteria:** 170 verified quarters; complete reference datasets; zero invalid source artifacts; checksum-verified manifests and source provenance; CSV/Parquet equivalence; a controlled benchmark with at least three measured repetitions; physical pruning evidence; and an independently verified handoff for Part 2.

## Repository contents

```text
.
├── .github/workflows/tests.yml
├── .gitignore
├── README.md
├── requirements.txt
├── requirements_part3.txt
├── requirements_part4.txt
├── notebooks/
│   ├── 01_acquisition_colab.ipynb
│   ├── 02_storage_colab.ipynb
│   ├── 03_data_quality_panel_colab.ipynb
│   ├── 04_scalable_processing_colab.ipynb
│   └── 05_feature_pipeline_colab.ipynb
├── run_acquisition.py
├── run_storage_benchmark.py
├── run_storage_analysis.py
├── run_layout_benchmark.py
├── run_data_quality.py
├── run_scalable_processing.py
├── run_feature_pipeline.py
├── src/fdic_pipeline/
│   ├── __init__.py
│   ├── acquisition.py
│   ├── project_flow.py
│   ├── workflow.py
│   ├── storage_benchmark.py
│   ├── storage_analysis.py
│   ├── layout_benchmark.py
│   ├── part1_handoff.py
│   ├── data_quality.py
│   ├── panel.py
│   ├── processing_engines.py
│   ├── scalable_processing.py
│   ├── feature_policy.py
│   └── feature_pipeline.py
└── tests/
    ├── conftest.py
    ├── test_acquisition.py
    ├── test_parquet_integration.py
    ├── test_storage_benchmark.py
    ├── test_storage_analysis.py
    ├── test_layout_benchmark.py
    ├── test_layout_integration.py
    ├── test_part1_handoff.py
    ├── test_project_flow.py
    ├── test_workflow.py
    ├── test_data_quality.py
    ├── test_panel_integration.py
    ├── test_processing_engines.py
    ├── test_processing_evidence.py
    ├── test_processing_integration.py
    ├── test_feature_policy.py
    └── test_feature_pipeline_integration.py
```

Only source code, notebooks, tests and documentation are committed. **Do not commit** FDIC datasets, Parquet/CSV files, run manifests, experiment results, model files, logs or access tokens. Notebook outputs should be saved back to GitHub **after a successful run**; they should contain compact validation summaries, tables and charts rather than raw data or local authentication information.

## Data dictionary and source scope

### Financials: 33 selected fields

`CERT, REPDTE, STNAME, ASSET, DEP, DEPDOM, EQ, BRO, CHBAL, SC, LNLSGR, LNLSNET, LNRE, LNCI, LNCON, LNATRES, NCLNLS, P3ASSET, P9ASSET, NAASSET, NTLNLS, ORE, NETINC, INTINC, EINTEXP, NONII, NONIX, ELNATR, ROA, ROE, NIMY, RBC1AAJ, RBCRWAJ`.

- **Keys and context:** `CERT`, `REPDTE`, `STNAME`.
- **Assets, capital, deposits and liquidity:** `ASSET`, `DEP`, `DEPDOM`, `EQ`, `BRO`, `CHBAL`, `SC`.
- **Credit exposure and quality:** loan balances, noncurrent/past-due assets and loss provisions.
- **Earnings and regulatory ratios:** income, expenses, ROA/ROE, net interest margin and capital ratios.

These fields support bank-level history, financial quality diagnostics, efficient analytical transformations and candidate risk features; field inclusion does not assert that every field was reported during every quarter. Source nulls remain null, and numeric values are not winsorised or imputed during acquisition.

### Failures: complete endpoint

`CERT, NAME, CITYST, FAILDATE, FAILYR, COST, QBFASSET, QBFDEP, RESTYPE`.

Historical failures may lack CERT. They are retained with null certificate values, counted in the acquisition audit and addressed by the terminal-event linkage rules in Part 2. `COST` and dated event information are preserved as retrospective reference data, not financial predictors.

### Institutions: complete endpoint

`CERT, NAME, STNAME, BKCLASS, ACTIVE, ESTYMD, ENDEFYMD, CHANGEC1, INSDATE`.

`ACTIVE`, end dates and change fields are retained for retrospective exit and linkage auditing, not incorporated as if they were available at earlier reporting dates.

**Parquet schema:** `CERT` int64, `REPDTE` date32, numeric financial and cost variables float64, other selected fields UTF-8 string. This fixed schema is validated after write. Financials use `financials/year=YYYY/quarter=Q/part-00000.parquet`; references are stored as single Snappy Parquet files. The Financials key is `(CERT, REPDTE)`; Institutions use CERT; Failures retain source rows without assuming a unique event key.

FDIC API: https://api.fdic.gov/banks/docs/  
Financial definitions: https://api.fdic.gov/banks/docs/risview_properties.yaml  
Failures definitions: https://api.fdic.gov/banks/docs/failure_properties.yaml  
Institutions definitions: https://api.fdic.gov/banks/docs/institution_properties.yaml

## Execute on Google Colab

The notebooks are executed **in numeric order**. The first notebook is the source-of-truth for the dataset and experiment identity. The second uses the exact source stage registered by the first. No run ID is copied into a notebook.

1. Upload this repository's **extracted files** to GitHub, preserving the paths shown above. Do not upload this ZIP itself as a project file; GitHub does not unpack it automatically.
2. Open [Notebook 01 on Colab](https://colab.research.google.com/github/NguyenDuong58/11230945-NguyenBaDuong/blob/main/notebooks/01_acquisition_colab.ipynb).
3. Select **Runtime → Change runtime type → Python 3**, then **Runtime → Run all**. Authorise Google Drive access. The Drive workspace is `/content/drive/MyDrive/BFT65B_11230945/`.
4. Notebook 01 clones the repository, installs pinned dependencies, executes tests with zero skipped cases, selects or creates the active experiment, acquires FDIC source tables and verifies every quarter and reference table. Re-run Notebook 01 after a disconnect to resume incomplete work; do not delete its Drive directory.
5. Confirm that `Acquisition stage: PASSED` appears and that the final output has exactly 170 verified quarters, no invalid artifacts, and two verified reference tables. **Do not compare record counts to measurements from another retrieval date as strict invariants.**
6. Open [Notebook 02 on Colab](https://colab.research.google.com/github/NguyenDuong58/11230945-NguyenBaDuong/blob/main/notebooks/02_storage_colab.ipynb) and select **Runtime → Run all**. It restores the same pinned Git revision, validates the source handoff, stages files on local Colab disk, creates identical CSV/Parquet input, runs the two benchmark families, independently audits results and publishes the storage stage.
7. Confirm that `Part 1: PASSED`, `Pruning verified: True` and `Handoff state: PASSED` appear. Inspect all tables and figures for the actual experiment. **Do not edit recorded timing measurements or compare them as if they were recorded on the same hardware as other benchmark runs.**
8. Save each executed Colab notebook with outputs to the appropriate GitHub notebook path (GitHub Colab integration or export the `.ipynb` and replace the file). Do not upload output Parquet, CSV, test XML or personal Drive artifacts.

**Source-revision pinning:** `experiment_config.lock.json` stores the full Git commit used when the experiment was created. Re-opened notebooks check out that exact commit even if additional notebook-output commits appear on `main`. To intentionally run a different code revision, set `CREATE_NEW_EXPERIMENT = True` in Notebook 01, which creates a separate, independent experiment and updates the explicitly selected active pointer. Existing results remain unmodified.

**Fresh reproduction:** Using a separate Google account/Drive with no existing `BFT65B_11230945/` directory, open both notebooks in order. No manual creation of previous experiment directories or edits of run IDs are required. The evaluator will need a Google account with enough Drive storage, an available Colab runtime and access to the public FDIC API; the repository contains no source data.

## Drive artifact structure and contracts

```text
BFT65B_11230945/
├── control/active_experiment.json
└── experiments/<experiment_id>/
    ├── experiment_config.lock.json
    ├── experiment_manifest.json
    ├── acquisition/
    │   ├── financials/year=YYYY/quarter=Q/part-00000.parquet
    │   ├── failures.parquet
    │   ├── institutions.parquet
    │   ├── manifest.json
    │   ├── verification_report.json
    │   ├── acquisition_gate.json
    │   └── logs/
    └── storage/
        ├── benchmark/<run_id>/
        ├── analysis/<run_id>/
        ├── layout/<run_id>/
        └── part1_validation.json
```

`experiment_manifest.json` records the immutable SHA-256 of each published stage's validation artifact and the checksum of the upstream stage on which it depends. Files are referenced by the explicitly selected experiment, **not directory timestamp order**. A changed validation file or mismatched upstream reference causes the consumer to stop. Stage content also undergoes its own source/partition checksum checks during ingestion and preparation. Parts 2–5 use the same experiment contract, with separate stage names for quality, processing, features and modelling.

The source stage is `acquisition/`. Part 2 can later resolve this stage for the financial and reference tables. The `storage/` stage contains measurements and evidence only; no edited financial records enter it.

## Experimental protocol and interpretation

### Format benchmark

Inputs: one uncompressed flat CSV and one Snappy flat Parquet, written from the same Arrow batches of the verified 170-quarter dataset. The program asserts complete row, schema, value and null equality before measurements.

- **Full scan:** all 33 selected fields.
- **Projection:** six financial columns, with no date filter.
- **Filtered read:** same six columns for records with REPDTE on/after 2020-01-01.

Each format/workload uses **one warmup and five measured runs** with seeded randomized order and a fresh worker process per run. Wall-clock elapsed time covers read/materialisation, not Python process startup; peak RSS is recorded for each worker and is not assumed to be smaller for Parquet.

### Physical-layout benchmark

Inputs: flat Snappy Parquet and the 170 quarter-partitioned Snappy Parquet source, both staged to `/content` and read with PyArrow Dataset using the same six-column projection.

- **Full projection:** all reporting quarters.
- **One quarter:** 2010Q4 (31 December 2010).
- **One year:** 2010Q1–2010Q4.

The flat scan uses REPDTE predicates; the partitioned scan uses equivalent `year`/`quarter` directory predicates. Both materialize matching source records. In addition to time and peak RSS, the method records dataset-discovery time and actual selected fragment counts (`get_fragments(filter=...)`). The expected partitioned counts are **170, 1 and 4 fragments**, respectively; the flat layout has one fragment. This is a direct pruning check, not a claim about row-group-level byte reads. Partitioned scans may be slower for some workloads because they must open more files.

Each layout/workload uses **one warmup and five measured runs**. The two experiments are distinct: do not merge the CSV/flat timing protocol with the Parquet physical-layout protocol into a single engine ranking without specifying their differing scan paths.

All measured quantities, checksums and graphics remain on Google Drive. The README provides the protocol, not a pre-recorded performance conclusion.

## Verification and troubleshooting

```bash
# Run from the repository root in a Python environment with pinned dependencies
python -m pip install -r requirements.txt
python -m pytest -q -rs tests

# Acquisition can be restarted with its exact output directory and scope
python run_acquisition.py --out /path/to/experiment/acquisition

# Verify the acquisition without API requests
python run_acquisition.py --out /path/to/experiment/acquisition --verify-only

# Prepare identical benchmark inputs on a local disk
python run_storage_benchmark.py prepare \
  --source-dir /path/to/experiment/acquisition --work-dir /local/scratch/part1

# Run controlled CSV and flat Parquet measurements
python run_storage_benchmark.py benchmark \
  --work-dir /local/scratch/part1 --results-dir /path/to/experiment/storage/benchmark/run_example

# Audit those measured results
python run_storage_analysis.py \
  --benchmark-dir /path/to/experiment/storage/benchmark/run_example \
  --output-dir /path/to/experiment/storage/analysis/run_example

# Run and independently verify the partition-pruning experiment
python run_layout_benchmark.py run \
  --source-dir /path/to/experiment/acquisition \
  --work-dir /local/scratch/part1 \
  --results-dir /path/to/experiment/storage/layout/run_example
python run_layout_benchmark.py verify \
  --source-dir /path/to/experiment/acquisition \
  --work-dir /local/scratch/part1 \
  --results-dir /path/to/experiment/storage/layout/run_example
```

Commands above illustrate the individual CLI interfaces; the Colab notebooks supply actual experiment-specific paths and complete the registry publication automatically. Use real source directories and distinct run IDs in shell commands. Avoid touching a published run.

**Typical issues:** `FDIC API 429/5xx` triggers bounded exponential retry; an interrupted download is resumed with per-file hash verification. A storage benchmark must have sufficient free `/content` space, because it stages quarter partitions and creates separate CSV/Parquet files. If a run's source digest changed, create a separate experiment rather than bypassing the validation gate. If the connection fails before an experiment has been published, preserve the acquisition directory and rerun Notebook 01.

## Part 2 — Data Quality and Bank-Quarter Panel (20 points)

### Research objectives and source contract

Part 2 constructs a typed quarterly panel from the explicitly selected and verified Part 1 experiment. The input dataset is unchanged: all Financials reporting quarters 1984Q1–2026Q2, and the complete Failures and Institutions reference tables. This stage never makes an FDIC API request and never selects input by the latest timestamp or a hardcoded prior run ID.

The `quality_panel` stage is already defined in `src/fdic_pipeline/project_flow.py`. Its registry parent is `acquisition`, while `storage` is an additional mandatory evidence dependency verified by SHA-256. Stage files and every production Parquet input are checked against Part 1 manifests. Part 2 records its own source Git commit in `part2_manifest.json`: the base experiment continues to identify the code used for the previously completed acquisition. The two commit identities are explicit, not silently conflated.

### Notebook 03 — Colab execution

1. Upload the complete extracted Part 2 delivery to the same public GitHub repository. Preserve paths; **replace** the root `README.md` with the expanded version in the delivery, and **add** `run_data_quality.py`, `src/fdic_pipeline/data_quality.py`, `src/fdic_pipeline/panel.py`, `tests/test_data_quality.py`, `tests/test_panel_integration.py`, and `notebooks/03_data_quality_panel_colab.ipynb`. Leave all Part 1 files unchanged.
2. Check that [Notebook 01](notebooks/01_acquisition_colab.ipynb) and [Notebook 02](notebooks/02_storage_colab.ipynb) have already completed in the active Drive experiment. Both `acquisition` and `storage` must show `PASSED`.
3. Open [Notebook 03 in Google Colab](https://colab.research.google.com/github/NguyenDuong58/11230945-NguyenBaDuong/blob/main/notebooks/03_data_quality_panel_colab.ipynb). Choose **Runtime → Change runtime type → Python 3**, with CPU. Run all cells **in order**. Mount the same Google Drive account used for Notebooks 01 and 02.
4. The environment cell clones the current repository revision, or checks out a previously recorded Part 2 revision when the stage already exists. It installs the same pinned `requirements.txt` used in Part 1; no additional package file is required.
5. The test cell executes the entire repository test suite with **zero failures, errors and skipped tests**. On a standard Colab environment the pinned PyArrow package is installed. Synthetic integration tests must pass before any real FDIC processing begins.
6. The source-contract cell independently verifies the Part 1 source and storage gates. The fixed availability-lag assumption is **90 calendar days**, explicitly marked as an assumption rather than an observed release date.
7. The build cell scans all Financials partitions chronologically, creates the typed panel, writes quality and event audit files and figures, recomputes validation fingerprints, and publishes `quality_panel`. An interrupted **unpublished** output directory is discarded and rebuilt; a published stage is never overwritten or silently re-evaluated.
8. Inspect the source-row reconciliation, missingness tables, rule frequencies, terminal class distribution, actual dated event counts, and availability scenarios. The read-only audit then verifies the full source and output chain. Confirm **`Part 2: PASSED`** and **`Handoff state: PASSED`**.
9. Save the executed Notebook 03 **with compact results, figures and test output** in `notebooks/03_data_quality_panel_colab.ipynb` on GitHub. Keep all raw, panel and audit data on Google Drive; do not commit `.parquet`, `.csv`, experiment manifests, personal Drive identifiers, or run logs.

**Completion criteria:** 170 quarter-partitioned output files; `raw Financials = clean panel + quarantined` without silent deletion; source-wide, yearly and quarterly quality reporting; identified reporting gaps and anomalies; event linkage retaining failed banks without CERT; mutually exclusive terminal classification including uncertainty; an explicit, unverified publication-lag policy; no retrospective event fields in panel columns; SHA-256-verified handoff from both Part 1 stages; and reproducible notebook outputs.

### Data-quality decisions and temporal validity

| Evidence | Decision | Justification |
|---|---|---|
| Duplicate or null `(CERT, REPDTE)` | Reject invalid source | A bank-quarter must have one unique identity; Part 1 already enforces source keys |
| Source nulls in other financial fields | Preserve as null, count by field and year | Historical reporting practices and field availability change over time |
| Missing, nonfinite, or nonpositive `ASSET` | Quarantine the bank-quarter, retain audit record | A positive core balance-sheet exposure is required for the clean analytical panel |
| Financial ratios or balance-sheet inconsistencies | Preserve raw values and append screening flags | Extremes may reflect genuine bank distress rather than data corruption |
| Missing quarterly reports | Record actual gaps; do not interpolate or forward-fill | Filling future or absent reports would create unobserved information |
| Missing CERT or unparseable FAILDATE in Failures | Retain source rows and mark linkage uncertainty | Absence of an identifier or event date is not evidence of non-failure |
| Dated institution end without linked failure | Documented other exit, not automatically merger | An end date does not identify the precise exit mechanism |
| No dated exit or final-period observation | Unresolved discontinuation or right-censoring | Stopping reporting alone does not prove failure |
| Report publication date unavailable | `ASSUMED_AVAILABLE_FROM = REPDTE + 90 days` | Only a transparent approximation; historical publication timing remains unverified |

Screening flags use only the current Financials report and never look ahead. The predictor panel includes source Financials fields and prior-report history but excludes `FAILDATE`, `COST`, `ACTIVE`, `CHANGEC1`, `ENDEFYMD`, `TERMINAL_CLASS`, target labels and retrospective outcomes. The period-by-period failure counts in the audit are derived from actual dated failure events and **must not be interpreted as four-quarter positive-label counts**. Their reporting windows are illustrative; final model development and test periods are specified in the feature and validation stages.

Point-in-time correctness has a strict limitation: the selected FDIC source tables do not contain verified historical release dates for every financial observation. The 90-day lag is an **assumed information-availability scenario**, not a finding about actual public availability. Automated sensitivity counts are provided for 30-, 90- and 180-day assumptions. A subsequent label policy must preserve this distinction and must not claim fully verified historical as-of data.

### Part 2 output and verification contract

```text
BFT65B_11230945/
└── experiments/<experiment_id>/
    ├── acquisition/                        # Published Part 1 source; read only
    ├── storage/                            # Published Part 1 evidence; read only
    ├── quality_panel/
    │   ├── panel/year=YYYY/quarter=Q/part-00000.parquet
    │   ├── audits/
    │   │   ├── field_missingness.csv
    │   │   ├── field_year_missingness.csv
    │   │   ├── quarterly_coverage.csv
    │   │   ├── quality_rules.csv
    │   │   ├── quarantined_rows.csv
    │   │   ├── reporting_gaps.csv
    │   │   ├── anomaly_examples.csv
    │   │   ├── failure_linkage.csv
    │   │   ├── terminal_exits.csv
    │   │   ├── failure_counts_by_year.csv
    │   │   ├── failure_counts_by_period.csv
    │   │   ├── availability_scenarios.csv
    │   │   └── quality_findings.md
    │   ├── figures/
    │   ├── part2_manifest.json
    │   └── part2_validation.json
    └── run_logs/part2_quality_panel.log
```

Panel schema: 33 original Financials fields, `MISSING_FINANCIAL_COUNT`, `PRIOR_REPORT_COUNT`, `REPORT_GAP_QUARTERS`, `ASSUMED_AVAILABLE_FROM`, `DQ_FLAG_COUNT`, and 10 non-mutating current-quarter quality flags. `PRIOR_REPORT_COUNT` and gap computations use the source history, including reports whose asset value later causes quarantine. The exact source Financials values are not imputed, corrected or overwritten.

Production CLI, executed by Notebook 03 with exact experiment-specific values:

```bash
python run_data_quality.py build --drive-root /content/drive/MyDrive/BFT65B_11230945 \
  --code-commit <40_character_git_sha> \
  --availability-lag-days 90 --replace-incomplete
python run_data_quality.py verify --drive-root /content/drive/MyDrive/BFT65B_11230945
```

The validator checks source Parquet files and audit fingerprints, typed panel file schemas, all 170 quarter partitions, reconciliation, terminal-class coverage, reference-row counts and the recorded Part 1 stage identities. The experiment registry refuses to overwrite a published `quality_panel` validation file. The exact per-stage code commit is written to the Part 2 manifest and can be checked out later.

**Part 3 handoff:** `resolve_stage(DRIVE_ROOT, "quality_panel")` identifies the verified Parquet root at `quality_panel/panel/`. Part 3 must verify `part2_validation.json` and `part2_manifest.json` before any benchmark. Future event and institution-status audits remain outside the predictor dataset.

## Part 3 — Scalable Processing (25 points)

### Research objective and experimental scope

The study compares efficient native Polars LazyFrame expressions with an eager pandas baseline and equivalent PySpark SQL transformations on the **same verified Part 2 panel**. The benchmark assesses numerical equivalence, output integrity, runtime, process-tree memory consumption, the behaviour of the optimized plans and input-scale sensitivity. Results pertain to a single Google Colab VM, not a multi-node cluster.

The Part 3 data source is obtained with `resolve_stage(DRIVE_ROOT, "quality_panel")` and must satisfy the acquisition and storage source contracts. The processing stage has `quality_panel` as its registry parent and additionally records the storage validation SHA-256. Its independent code Git commit is recorded in `part3_manifest.json`; no Part 1/2 commit or data file is overwritten.

### Dependencies

`requirements_part3.txt` includes the pinned `requirements.txt` from Parts 1–2 and installs these exact additional packages:

- `pandas==2.2.3` — eager baseline;
- `polars==1.35.2` — lazy, optimized scan and transformations;
- `pyspark==4.0.1` — equivalent transformations with `SparkSession` using `local[2]`;
- `psutil==7.0.0` — sampled process-tree RSS (Python and Spark JVM).

PySpark requires **Java 17 or later**. Python packages satisfying the pinned versions are not forcibly reinstalled. The notebook verifies Java, installed package versions, and every repository unit/integration test with no failures, errors or skips. A fresh Colab CPU runtime is preferred to avoid memory contention and stale Spark sessions.

### Input corpus and three transformation workloads

The complete Part 2 panel contains 48 typed fields and 170 quarter-partitioned Parquet files. All files are copied to `/content` with source and destination SHA-256 verification before measurement. The most recent 85-quarter subset is created by hard-linking the corresponding local files where possible. There is no synthetic replication, imputation, or use of future event data. The exact row counts for each scale are recorded from the selected Part 2 manifest.

| Workload | Input columns and transformation | Output key / correctness considerations |
|---|---|---|
| `quarter_aggregation` | Filter REPDTE >= 2000-03-31; aggregate bank-quarter count, assets, deposits, mean ROA, and extreme ROE flags | REPDTE; null ROA ignored in mean; numeric sums over null deposits treated as zero |
| `adjacent_asset_growth` | Sort by CERT and REPDTE; lag assets within CERT; calculate asset growth only when reports are exactly one quarter apart; aggregate pairs and mean growth | REPDTE; a missing intermediate report is **not** one-quarter growth |
| `state_year_risk` | Group by calendar year and STNAME; sum assets/loans and diagnostic flags, mean ROA, count reports with missing financial measurements | YEAR, STNAME; null/blank state becomes `MISSING`; the source flags are non-mutating current-report diagnostics |

Each workload uses the same selected source fields across pandas, Polars and Spark. Polars starts from `scan_parquet(...)`, and optimized plans are saved. Spark uses DataFrame SQL transformations and window functions rather than Python UDFs, with a two-thread local master and eight shuffle partitions. Dates, counts, nulls and every floating aggregate are compared across engines (`rel_tol=1e-10`, `abs_tol=1e-5`). Entire grouped output tables are saved for every benchmark trial so the audit can recompute all comparisons independently.

The scales are **recent_85** (the latest 85 available reporting quarters) and **full_170** (all 170 quarters). They contain different historical bank populations and must not be interpreted as a controlled doubling of identical observations. The empirical size ratio and measured runtime ratios are reported together.

### Measurement protocol

Each of three workloads × three engines × two scales receives **one warmup and five measured repetitions**. This yields **18 warmup trials, 90 measured trials and 72 full-output numerical comparisons** (each of six rounds × three workloads × two scales compares pandas–Polars and pandas–Spark). A fixed seed `11230945` randomizes the ordering of all 18 tasks inside each round. Each task runs in a fresh Python worker; Spark initialization occurs before the timer, while the Parquet scan, transformation and materialization occur within it.

A 50 ms sampler estimates peak total RSS for the Python worker and its recursive children, including Spark's JVM. This is a sampled process-tree measure, not a precise maximum JVM heap measurement, and is not directly comparable with Part 1's single-process `ru_maxrss` metrics. Operating-system cache can persist between subprocesses even though engine objects are not reused.

The program saves six Polars/Spark optimized-plan text files (three each), a plan-inspection table, grouped reference outputs, raw trial measurements, full trial result tables, independent summary checks and three performance charts. It does not infer pushdown or shuffle benefits solely from wall-clock results: the plans must be inspected before discussing their physical execution.

### Running Notebook 04 in Google Colab

1. Upload all files supplied for Part 3 into the existing GitHub repository, preserving their relative paths and **replacing** the root `README.md`. All earlier `.ipynb`, Python modules and source data remain unchanged. Save the changes in one commit.
2. Open [Notebook 04](https://colab.research.google.com/github/NguyenDuong58/11230945-NguyenBaDuong/blob/main/notebooks/04_scalable_processing_colab.ipynb) with a fresh Python 3 CPU runtime, then execute the cells in order. The code mounts Drive at `/content/drive` and chooses the **explicit** active experiment; do not manually select a run directory.
3. Section 1 checks out the correct source commit and installs `requirements_part3.txt`. Section 2 asserts Java >=17, checks package versions and runs the **full test suite**. Stop if any test is skipped or fails.
4. Section 3 verifies the published `acquisition`, `storage` and `quality_panel` stages and SHA-256 checks the 170 input files. Section 4 prints the fixed workload definitions and trial counts.
5. Section 5 copies the verified source to local disk and measures all three engines in isolated worker processes. The benchmark is invalid unless all 72 cross-engine comparisons pass and all 90 measured trials are recorded. Source SHA checks and per-engine plans are saved in the processing stage.
6. Sections 6–8 display medians, speedups, scaling results, execution-plan indicators and charts. The independent validator in Section 9 rechecks hashes, medians, full numerical output comparisons and source lineage.
7. Confirm **`Part 3: PASSED`** and **`Handoff state: PASSED`** in the notebook's last two sections. Save the executed notebook with compact outputs on GitHub, but not raw FDIC Parquet, timing CSVs, saved plans, logs or JSON manifests.

**Expected final gate:** 170 verified source quarterly files; three equivalent workloads × three engines × two scales; 90 measured trials; 18 warmups; 72 full numerical comparisons; six execution plans; intact input and output SHA-256; a `PASSED` processing-stage validation and verified `quality_panel` parent lineage. Exact performance results are measured on Colab and must not be inferred from other experiments.

**Colab disk requirement:** Section 5 checks for at least 3 GiB free local `/content` space. All computation is performed locally; only verified artifacts are persisted on Drive. If a run stops before `processing` is published, rerun Notebook 04 to rebuild that unpublished stage. After publication, the notebook performs verification without replacing evidence.

### Processing-stage output and downstream contract

```text
BFT65B_11230945/
└── experiments/<experiment_id>/
    ├── acquisition/                    # verified Part 1 source; read-only
    ├── storage/                        # verified Part 1 benchmark; read-only
    ├── quality_panel/panel/            # verified Part 2 inputs; read-only
    ├── processing/
    │   ├── input_snapshot.json
    │   ├── environment.json
    │   ├── trials.csv
    │   ├── trial_results.jsonl
    │   ├── equivalence.csv
    │   ├── performance_summary.csv
    │   ├── scaling_summary.csv
    │   ├── plan_inspection.csv
    │   ├── plans/                      # 6 complete Polars and Spark plans
    │   ├── results/                    # 6 grouped pandas reference tables
    │   ├── figures/                    # runtime, scaling and process RSS
    │   ├── plan_discussion.md
    │   ├── part3_manifest.json
    │   └── part3_validation.json
    └── run_logs/part3_scalable_processing.log
```

Part 4 obtains the bank-quarter dataset through `resolve_stage(root, "quality_panel")`, not from the aggregated Part 3 benchmark results. The Part 3 stage is available separately for performance evidence, and its quality-panel parent SHA-256 must match the selected experiment. The source data and retrospective event audits remain separate from modelling features.

**Operational interpretation.** A multi-node Spark cluster is not justified merely because Spark is part of the course. Before proposing a cluster, inspect actual query plans, memory pressure and runtimes on realistic workloads. Distribution becomes relevant when the working set cannot be processed reliably within a single host's memory/disk limits, when multi-source joins/shuffles overwhelm the local executor, or when operational throughput and latency requirements are repeatedly missed. The current `local[2]` results cannot establish any empirical cluster speedup. The conclusions should also consider coordination, I/O and operational cost.

### Command-line verification

```bash
python -m pip install -r requirements_part3.txt
python -m pytest -q -rs tests

# The 40-character Part 3 source commit is the one returned by git rev-parse HEAD.
python run_scalable_processing.py build \
  --drive-root /content/drive/MyDrive/BFT65B_11230945 \
  --code-commit <40-character-git-commit> \
  --scratch /content/bft65b_part3_scratch \
  --repetitions 5 --warmups 1 --replace-incomplete

# Independent source and evidence verification without new measurements.
python run_scalable_processing.py verify \
  --drive-root /content/drive/MyDrive/BFT65B_11230945
```

## Academic sources and disclosure

1. FDIC, *BankFind Suite API Documentation*, https://api.fdic.gov/banks/docs/ (accessed 16 November 2026).
2. FDIC, *Financial, Failure, and Institution API field definitions*, YAML dictionary links above (accessed 16 November 2026).
3. Apache Arrow, *PyArrow Dataset and Parquet documentation*, https://arrow.apache.org/docs/python/dataset.html .
4. Apache Software Foundation, *PySpark 4.0.1 Installation and SQL Reference*, https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html .
5. Polars, *LazyFrame and Query Optimization documentation*, https://docs.pola.rs/api/python/stable/reference/lazyframe/ .
6. Google, *Colaboratory FAQ*, https://research.google.com/colaboratory/faq.html .
7. BFT65B, *Final Individual Report — A Big-Data Pipeline for Bank Failure Early Warning*, examination handout.
8. OpenAI ChatGPT (GPT-6), code drafting and review support, 16 November 2026. The analysis must be verified by executable tests and independently measured FDIC results; generated suggestions are not treated as evidence.

No downloaded bank records, experimental measurements, model parameters or future-outcome labels are supplied in the GitHub source repository. Part 2 audit outcomes are stored separately from prediction features. Part 3 benchmark results and execution-plan evidence are stored on Google Drive and referred to by SHA-256; only notebook summaries are committed. This research report must cite the applicable source APIs, engine documentation and AI assistance disclosed here.


## Part 4 — Feature Pipeline (15 points)

### Source installation and safe recovery

Part 4 consists of `notebooks/05_feature_pipeline_colab.ipynb`, `run_feature_pipeline.py`, `requirements_part4.txt`, `src/fdic_pipeline/feature_policy.py`, `src/fdic_pipeline/feature_pipeline.py`, and two Part 4 tests (`test_feature_policy.py`, `test_feature_pipeline_integration.py`). Replace these files at their existing repository paths as one commit. Preserve all upstream notebooks, their source modules, the experiment registry, and the `acquisition`, `storage`, `quality_panel`, and `processing` stages.

The feature-stage output path is `experiments/<experiment_id>/features/` on Google Drive. An absent or **unpublished** stage can be built or rebuilt through Notebook 05; the CLI explicitly replaces only unpublished `features/` output when requested. Do not manually modify `experiment_manifest.json` to clear a published stage. Once `features` has status `PASSED`, the published feature artifacts and their commit remain immutable. A genuinely new research run requires a separately identified experiment, not deletion of a verified stage.

Notebook 05 Section 2 prints and saves the complete pytest output. No failed, errored, or skipped test is acceptable before processing source data. The integration suite verifies individual derived feature/label values against the physical Parquet columns, excluding Hive partition keys inferred from directory names.


### Purpose and source contract

The feature stage transforms the 48-field, 170-quarter bank-quarter panel into an auditable table of 21 current-/past-report financial features, an explicitly time-bounded four-quarter outcome, and physically separated development and holdout samples. It consumes the published `quality_panel` stage, verifies all input partition SHA-256 fingerprints, requires the published `processing` stage, and records that stage's verification digest as an additional dependency. No aggregate benchmark outputs are used as predictors.

The experiment's `features` stage is registered with `quality_panel` as its direct parent. Its manifest records the exact Part 4 Git commit, the policy lock, the Part 2 and Part 3 validation hashes, the failure-source and terminal-audit hashes, the output partition hashes, the fitted preprocessing artifact checksum, and the sample counts. Published outputs are never overwritten; interrupted unpublished outputs can be rebuilt with `--replace-incomplete`.

### Event definition and observation policy

The FDIC Failures/Assistance source contains more than one transaction type. The target event is a linked, dated FDIC record with **`RESTYPE = FAILURE`**, not every row of the reference table. `ASSISTANCE` transactions are not labelled as failures and are conservatively treated as competing interventions when they occur in the prediction horizon. Unknown resolution types, unparseable dates, multiple conflicting failures, and inconsistent event timing are censored. The retrospective terminal reference is audited independently; an institution's cessation of reporting is not a failure.

The fixed study target is **`Y_FAIL_4Q = 1{FAILURE event in (REPDTE, REPDTE + 4 calendar quarters]}`**, conditional on the financial report's assumed availability before the event. The horizon remains anchored at `REPDTE`, not at the assumed publication date. The availability date is `REPDTE + 90 calendar days`, an explicitly **unverified historical disclosure assumption**. A failure/competing event occurring on or before that date makes the report ineligible. An observed non-failure exit or assistance within the horizon is censored rather than labelled healthy; a bank with insufficient forward follow-up is also censored. Reports after 2025Q2 do not have complete four-quarter outcomes as of 2026Q2.

The main chronological periods are fixed before model development:

| Role | Report-date interval | Use |
|---|---|---|
| Development | 1984Q1–2006Q4 | Fit preprocessing and select models using chronological development folds |
| Purge | 2007Q1–2007Q4 | Prevent overlap of four-quarter development horizons with test reporting dates |
| Test holdout | 2008Q1–2014Q4 | Reserved for one post-lock model evaluation in Part 5 |
| Later reports through 2025Q2 | 2015Q1–2025Q2 | Historical feature coverage only, not model selection or holdout evaluation |
| Label-immature reports | 2025Q3–2026Q2 | Features retained; four-quarter label unavailable |

The date-based FDIC source audit from Notebook 03 found **507 `FAILURE`** and **13 `ASSISTANCE`** rows dated 2008–2014. Part 4 independently recalculates the distinct qualifying failure CERTs linked to the Financials panel and requires at least 50; **these event counts must never be interchanged with positive bank-quarter labels**. Each source experiment generates its own measured sample counts; no row or label total is preset to a historical run's result.

### Features, information set, and missing-data processing

The 21 features are defined and ordered in `src/fdic_pipeline/feature_policy.py` and exported in `audits/feature_dictionary.csv`. Groups cover size, accounting and regulatory capital, funding, liquidity, credit exposure, earnings, quarter-on-quarter and year-on-year growth, reporting history, and data-quality diagnostics. Amount-based features use USD-thousands inputs; derived ratios and source profitability/capital ratios are interpreted as percentages. Growth calculations require exact calendar lags of one quarter or four quarters; reports separated by missing intermediate quarters are **never** treated as one-quarter growth.

No `CERT`, `REPDTE`, `FAILDATE`, `RESTYPE`, `COST`, `ACTIVE`, `ENDEFYMD`, retrospective terminal class, or outcome label appears in `FEATURE_NAMES`. Source missingness is preserved; denominators must be positive, and neither imputation nor winsorization is applied when constructing the feature table.

The model-data files retain `CERT`, `REPDTE` and `ASSUMED_AVAILABLE_FROM` as non-predictive metadata for independent bank-level warning-lead-time evaluation. **Only `FEATURE_NAMES` are predictors.** The withheld test metadata must not be inspected for development decisions.

The scikit-learn preprocessing template is an explicit `Pipeline(SimpleImputer(strategy='median', add_indicator=True, keep_empty_features=True), RobustScaler(quantile_range=(25,75)))`. A fitted artifact is created **only from eligible development feature columns**; the holdout file is not opened when fitting it. Part 5 must clone the **unfitted** pipeline and refit the imputer and scaler separately within each expanding chronological training fold. The saved full-development fit is not used in fold validation.

Availability-lag sensitivities of 30, 90 and 180 days are calculated using **development reports only** and reported separately from the fixed 90-day primary policy. They do not validate actual historical publication timestamps.

### Notebooks, dependencies, and tests

- `05_feature_pipeline_colab.ipynb` is the only Part 4 notebook. Its 11 code sections resolve the experiment, install dependencies, execute all tests, verify upstream data, lock the feature/label policy, review FDIC event counts, build data, inspect development-only diagnostics, display figures, independently verify every output row, and establish the Part 5 handoff.
- `requirements_part4.txt` includes the pinned processing dependencies from Part 3 and pins NumPy, scikit-learn and joblib. Python packages already satisfying pinned versions are not forcibly reinstalled.
- The GitHub Actions workflow sets up Java 17 and Python 3.11, installs `requirements_part4.txt`, and runs all repository unit and integration tests. All failures, errors, and skipped tests must be zero before constructing full FDIC features.
- Synthetic tests check calendar-boundary labels, exact-lag features, assistance exclusion, missing data, source/outcome separation, source/manifest integrity, Parquet split equivalence, and development-only preprocessing. Synthetic test results do not constitute a full-data FDIC execution.

**Notebook:** https://colab.research.google.com/github/NguyenDuong58/11230945-NguyenBaDuong/blob/main/notebooks/05_feature_pipeline_colab.ipynb

### Reproduction sequence in Google Colab

1. Upload the Part 4 source files to their existing repository paths, replacing `README.md` and `.github/workflows/tests.yml` and keeping all previous notebook and source files unchanged. Commit the files to `main` before opening the notebook. Do not upload source or derived FDIC data.
2. Run Notebook 05 on a Colab CPU runtime. Section 1 mounts `/content/drive`, reads `control/active_experiment.json`, clones the repository, restores the recorded Part 4 commit if a feature stage is published, and installs `requirements_part4.txt`.
3. Section 2 runs the complete repository test suite. Do not proceed when failures, errors or skips are nonzero. Section 3 checks SHA-256 and lineage of acquisition, storage, quality-panel and processing stages.
4. Sections 4–5 lock the feature and event policy and print the observed `RESTYPE` distribution. The exact number of distinct dated failures within the test calendar is verified independently of positive labels.
5. Section 6 streams all 170 quarters into the complete `feature_table/` and creates separate `model_data/development.parquet` and `model_data/test_holdout.parquet`. The full-development preprocessing artifact is fitted using the development file alone.
6. Sections 7–9 display sample accounting, label/censoring reasons, development missingness, availability sensitivity and charts. Section 10 performs full independent read-only source-to-feature-and-label verification. Section 11 confirms `Part 4: PASSED` and `Handoff state: PASSED` and prints the paths needed by Part 5.
7. Save the executed `.ipynb` file, including compact tables and figures, back to GitHub. Do not commit the Parquet feature table, model samples, fitted preprocessor, audit CSVs, experiment manifests or logs.

### Persisted feature-stage layout

```text
BFT65B_11230945/
└── experiments/<experiment_id>/
    ├── acquisition/                         # published source; read-only
    ├── storage/                             # published storage evidence; read-only
    ├── quality_panel/                       # published 48-field input panel; read-only
    ├── processing/                          # published performance evidence; read-only
    ├── features/
    │   ├── feature_policy.lock.json
    │   ├── feature_table/year=YYYY/quarter=Q/part-00000.parquet
    │   ├── model_data/
    │   │   ├── development.parquet
    │   │   ├── test_holdout.parquet
    │   │   └── development_preprocessor.joblib
    │   ├── audits/                          # feature, event, split, and fit evidence
    │   ├── figures/                         # development diagnostics and event history
    │   ├── part4_manifest.json
    │   └── part4_validation.json
    └── run_logs/part4_feature_pipeline.log
```

**Part 5 input contract.** Development model selection must load the separate `model_data/development.parquet` and use the ordered `FEATURE_NAMES` from `feature_policy.py`. The `test_holdout.parquet` is reserved for evaluation **after** the model and cost-sensitive threshold have been locked. The `features` stage's direct parent is the `quality_panel` validation SHA-256; the processing evidence SHA-256 is checked as a second immutable dependency.

**Reproducibility CLI:**

```bash
python -m pip install -r requirements_part4.txt
python -m pytest -q -rs tests
python run_feature_pipeline.py build \
  --drive-root /content/drive/MyDrive/BFT65B_11230945 \
  --code-commit <40-character-git-commit> --replace-incomplete
python run_feature_pipeline.py verify \
  --drive-root /content/drive/MyDrive/BFT65B_11230945
```

**Sources:** FDIC, *BankFind Suite API Documentation* (https://api.fdic.gov/banks/docs/); FDIC, *Failures and Assistance—Transaction Types and Definitions* (https://banks.data.fdic.gov/bankfind-suite/failures/help); scikit-learn, *Pipeline*, *SimpleImputer* and *RobustScaler* documentation (https://scikit-learn.org/stable/); course examination handout; OpenAI ChatGPT (GPT-6) used for code drafting/review support. Claims about pipeline outcomes require executed tests and experiment-specific saved evidence.
