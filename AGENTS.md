# Repository Guidelines

## Project Structure & Module Organization

- `code/` contains Databricks notebooks exported as `.py` scripts. They are organized by lakehouse layers and should be run in order:
  1. `code/00_Setup/` – creates the Unity Catalog schema and landing volumes.
  2. `code/01_Bronze_Layer/` and `code/01_Framework_Ingestion/`, `code/02_Operational_Ingestion/` – ingest raw framework and operational data.
  3. `code/02_Silver_Layer/` and `code/03_Medallion_Transformation/` – clean/validate and transform to silver/gold.
  4. `code/03_Gold_Layer/` – builds aggregations for reporting.
  5. `code/05_Machine_Learning/` – rule engine and ML risk prediction.
- `data/` holds input CSVs plus mock data generators (`data/generators/`) and example outputs (`data/mock/`).
- `code/04_Consumption/Dashboard/` contains the Databricks SQL dashboard JSON.

## Build, Test, and Development Commands

- No local build step. Development happens in Databricks Repos.
- Typical run sequence (Databricks notebook UI or jobs):
  - `code/00_Setup/setup_grc_lakehouse.py`
  - `code/01_Bronze_Layer/load_bronze_tables.py`
  - `code/02_Silver_Layer/transform_silver_tables.py`
  - `code/03_Gold_Layer/create_gold_tables.py`
  - `code/05_Machine_Learning/compliance_rules.py`
  - `code/05_Machine_Learning/risk_prediction.py`

## Coding Style & Naming Conventions

- Python, PySpark, Delta Lake. Use 4-space indentation and follow PEP 8 where practical.
- Prefer `snake_case` for variables/functions and clear, domain-based names (e.g., `controls_df`, `evidence_gap_analysis_df`).
- Keep notebooks idempotent: rerunning should not break state; use `mode("overwrite")` only where intended.
- Don’t add new dependencies unless Databricks runtimes already provide them.

## Testing Guidelines

- No dedicated unit test suite. Validate changes by running affected notebooks on mock data and checking:
  - expected table creation in `01_bronze/`, `02_silver/`, `03_gold/`
  - row counts and schema sanity
  - dashboard queries still resolve.

## Commit & Pull Request Guidelines

- Git history is minimal; use descriptive, imperative commits (e.g., “Add evidence gap rule”).
- PRs should include: summary of changes, notebooks affected, run results (table names/row counts), and screenshots for dashboard changes when applicable.

## Security & Configuration Tips

- Never hardcode workspace URLs, tokens, or secrets. Use Databricks secrets or job parameters.
- Keep sample data in `data/` non-sensitive and synthetic.

