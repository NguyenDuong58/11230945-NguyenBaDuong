# A Big-Data Pipeline for Bank Failure Early Warning

**BFT65B — Big Data Analytics in Finance | Part 1: Acquisition & Storage**  
**Repository:** https://github.com/NguyenDuong58/11230945-NguyenBaDuong  
**Runtime:** Google Colab (Python); persistent artifacts: Google Drive; source and tests: GitHub.

## Research objective and grading requirements

Part 1 collects the specified FDIC datasets and establishes a verified storage design. The study covers all available Financials observations for the 170 calendar quarters from **1984Q1 through 2026Q2**, the complete FDIC Failures table, and the complete FDIC Institutions table. Downloaded record counts are **observations to be measured**, not predetermined acceptance constants.

The storage study compares losslessly equivalent uncompressed CSV and Snappy Parquet, with repeatable timing and memory measurements. A separate experiment measures physical file discovery, filtered scanning and selected Parquet fragments in the 170-quarter layout. Data acquisition, storage analysis and test/model development are separate stages: no future failure event or current institution status is introduced as a financial predictor.

**Completion criteria:** 170 verified quarters; complete reference datasets; zero invalid source artifacts; signed manifests and source provenance; CSV/Parquet equivalence; a controlled benchmark with at least three measured repetitions; physical pruning evidence; and an independently verified handoff for Part 2.

## Repository contents

```text
.
├── .github/workflows/tests.yml
├── .gitignore
├── README.md
├── requirements.txt
├── notebooks/
│   ├── 01_acquisition_colab.ipynb
│   └── 02_storage_colab.ipynb
├── run_acquisition.py
├── run_storage_benchmark.py
├── run_storage_analysis.py
├── run_layout_benchmark.py
├── src/fdic_pipeline/
│   ├── __init__.py
│   ├── acquisition.py
│   ├── project_flow.py
│   ├── workflow.py
│   ├── storage_benchmark.py
│   ├── storage_analysis.py
│   ├── layout_benchmark.py
│   └── part1_handoff.py
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
    └── test_workflow.py
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

## Academic sources and disclosure

1. FDIC, *BankFind Suite API Documentation*, https://api.fdic.gov/banks/docs/ (accessed 16 November 2026).
2. FDIC, *Financial, Failure, and Institution API field definitions*, YAML dictionary links above (accessed 16 November 2026).
3. Apache Arrow, *PyArrow Dataset and Parquet documentation*, https://arrow.apache.org/docs/python/dataset.html .
4. Google, *Colaboratory FAQ*, https://research.google.com/colaboratory/faq.html .
5. BFT65B, *Final Individual Report — A Big-Data Pipeline for Bank Failure Early Warning*, examination handout.
6. OpenAI ChatGPT (GPT-6), code drafting and review support, 16 November 2026. The analysis must be verified by executable tests and independently measured FDIC results; generated suggestions are not treated as evidence.

No downloaded bank records, experimental measurements, model parameters or future-outcome labels are supplied in the GitHub source repository.
