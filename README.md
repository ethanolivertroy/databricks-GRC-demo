# GRC Compliance Lakehouse

Databricks-based compliance management system. Tracks NIST 800-53 and SOC2 controls, evidence collection, and audit readiness across enterprise systems.

![Dashboard](dash.png)

## What This Does

- Ingests NIST 800-53 Rev 5 controls, SOC2 Trust Service Criteria, plus simplified ISO 27001 and PCI‑DSS v4 catalogs for demo use
- Maps NIST controls across frameworks (sample crosswalks to SOC2, ISO, and PCI‑DSS)
- Tracks control assessments and evidence per system
- Runs compliance rules to auto-flag issues
- ML model predicts which controls are likely to fail
- Dashboard shows compliance posture, gaps, and risk

## Architecture

```
grc_compliance_dev (Unity Catalog)
|-- 00_landing   # Raw files in Volumes
|-- 01_bronze    # Raw tables (controls, systems, assessments, evidence)
|-- 02_silver    # Cleaned + validated (compliance scores, overdue flags)
|-- 03_gold      # Aggregations + ML outputs (scorecards, alerts, predictions)
```

## Medallion ETL Flow (Bronze → Silver → Gold)

The demo follows Databricks’ “medallion” pattern: each layer makes the data more usable and trustworthy.

- **Bronze = raw ingestion**. Data is loaded exactly as received from Volumes into Delta tables. Think “source of record.”
- **Silver = cleaned + validated**. IDs are standardized, types are fixed, and quality checks are applied so downstream logic is stable.
- **Gold = business outputs**. Aggregations, scorecards, rule‑based alerts, and ML predictions that power the dashboard.

```mermaid
flowchart LR
  subgraph Landing["00_landing (Volumes)"]
    L1[NIST / SOC2 / ISO / PCI framework files]
    L2[Operational files<br/>systems, assessments, evidence]
  end

  subgraph Bronze["01_bronze (Raw Delta tables)"]
    B1[nist_800_53_controls]
    B2[soc2_trust_criteria]
    B3[iso_27001_controls]
    B4[pci_dss_v4_controls]
    B5[control_mapping<br/>(NIST→SOC2/ISO/PCI)]
    B6[systems_inventory]
    B7[control_assessments]
    B8[evidence_records]
  end

  subgraph Silver["02_silver (Cleaned + validated)"]
    S1[nist_controls]
    S2[soc2_criteria]
    S3[iso_controls]
    S4[pci_controls]
    S5[systems]
    S6[assessments]
    S7[evidence]
  end

  subgraph Gold["03_gold (Business + ML outputs)"]
    G1[control_compliance_summary]
    G2[system_compliance_scorecard]
    G3[cross_framework_mapping]
    G3b[framework_control_compliance]
    G3c[framework_compliance_summary]
    G4[evidence_gap_analysis]
    G5[audit_readiness_metrics]
    G6[compliance_alerts]
    G7[control_risk_predictions]
  end

  L1 --> B1
  L1 --> B2
  L1 --> B3
  L1 --> B4
  L1 --> B5
  L2 --> B6
  L2 --> B7
  L2 --> B8

  B1 & B2 & B3 & B4 & B5 & B6 & B7 & B8 -->|`transform_silver_tables.py`| S1 & S2 & S3 & S4 & S5 & S6 & S7
  S1 & S2 & S3 & S4 & S5 & S6 & S7 -->|`create_gold_tables.py`| G1 & G2 & G3 & G3b & G3c & G4 & G5
  G4 & G2 -->|`compliance_rules.py`| G6
  G4 & S6 & S5 -->|`risk_prediction.py`| G7
```

### Bronze Layer
![Bronze Layer](bronze.png)

### Silver Layer
![Silver Layer](silver.png)

### Gold Layer
![Gold Layer](gold.png)

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog
- Compute cluster or SQL Warehouse. For `risk_prediction.py`, use a Databricks ML runtime (or preinstall `mlflow`, `scikit-learn`, `shap`).

### Configuration
Default catalog/schema names and volume paths live in `code/utils/config.py`. Update that file if you want to run the demo under a different catalog or landing location.

### Deploy

1. Clone repo to Databricks Workspace (Repos > Add Repo)

2. Run setup notebook:
   ```
   code/00_Setup/setup_grc_lakehouse.py
   ```

3. Upload data files to Volumes (REST API or CLI):
   ```bash
   # Framework data
   curl -X PUT "https://<workspace>/api/2.0/fs/files/Volumes/grc_compliance_dev/00_landing/frameworks/nist_800_53_rev5_controls.csv" \
     -H "Authorization: Bearer <token>" \
     --data-binary @data/frameworks/nist_800_53_rev5_controls.csv
   ```

4. Run notebooks in order:
   ```
   code/01_Bronze_Layer/load_bronze_tables.py
   code/02_Silver_Layer/transform_silver_tables.py
   code/03_Gold_Layer/create_gold_tables.py
   code/05_Machine_Learning/compliance_rules.py
   code/05_Machine_Learning/risk_prediction.py
   ```

4a. Quick validation (optional):
   - Bronze tables exist: `SHOW TABLES IN grc_compliance_dev.01_bronze`
   - Silver tables exist: `SHOW TABLES IN grc_compliance_dev.02_silver`
   - Gold tables exist: `SHOW TABLES IN grc_compliance_dev.03_gold`
   - `03_gold.audit_readiness_metrics` has a non‑null `audit_readiness_score`.

5. Import dashboard:
   - SQL > Dashboards > Import
   - Upload `code/04_Consumption/Dashboard/GRC_Compliance_Trust_Center.lvdash.json`

## Data Model

### Bronze Tables
| Table | Records | Description |
|-------|---------|-------------|
| nist_800_53_controls | 188 | NIST control catalog |
| soc2_trust_criteria | 64 | SOC2 TSC definitions |
| iso_27001_controls | 36 | ISO 27001 control catalog (simplified) |
| pci_dss_v4_controls | 12 | PCI‑DSS v4 requirement catalog (simplified) |
| control_mapping | sample | NIST crosswalk to SOC2 / ISO / PCI |
| systems_inventory | 50 | Systems under assessment |
| control_assessments | ~1500 | Assessment records |
| evidence_records | 200 | Evidence uploads |

### Gold Tables
| Table | Purpose |
|-------|---------|
| control_compliance_summary | Compliance % by control family |
| system_compliance_scorecard | Per-system posture |
| cross_framework_mapping | NIST mapped to SOC2 / ISO / PCI with compliance |
| framework_control_compliance | Derived compliance per SOC2/ISO/PCI control |
| framework_compliance_summary | High-level compliance by framework |
| evidence_gap_analysis | Missing/expiring evidence |
| audit_readiness_metrics | Executive readiness score |
| compliance_alerts | Rule engine output |
| control_risk_predictions | ML risk scores |

## Rule Engine

Auto-flags compliance issues:
- Expired evidence
- Overdue remediations
- Stale assessments (>9 months)
- Critical system coverage gaps
- Evidence gaps

Output: `03_gold.compliance_alerts`

## ML Model

Random Forest classifier predicts control failure risk.

Features:
- Historical compliance score
- Days to remediation
- Evidence count
- Gap severity
- System criticality
- Control family

Output: `03_gold.control_risk_predictions` with risk_score (0-1) and risk_level (High/Medium/Low)

## Dashboard

4 pages:
1. Executive Trust Center - Overall compliance posture
2. System Compliance Details - Per-system scorecard
3. Cross-Framework Mapping - NIST to SOC2 coverage
4. Evidence Tracking - Gaps and collection status

## Project Structure

```
databricks-GRC-demo/
|-- code/
|   |-- 00_Setup/
|   |-- 01_Bronze_Layer/
|   |-- 02_Silver_Layer/
|   |-- 03_Gold_Layer/
|   |-- 04_Consumption/Dashboard/
|   |-- 05_Machine_Learning/
|-- data/
|   |-- frameworks/    # NIST, SOC2, mapping CSVs
|   |-- generators/    # Mock data scripts
|   |-- mock/          # Generated test data
|-- README.md
```

## Tech Stack

- Databricks Unity Catalog
- Delta Lake
- PySpark
- MLflow
- Databricks SQL Dashboards

## Authoritative Sources

Framework inputs live in `data/frameworks/`. NIST is pulled from public‑domain CSRC controls; SOC2, ISO 27001, and PCI‑DSS catalogs are simplified **synthetic demo extracts** due to licensing. Gold rollups for SOC2/ISO/PCI are therefore derived from NIST assessments via the sample mappings and should be treated as illustrative, not audit‑ready. See `data/frameworks/SOURCES.md` for official sources and how to replace files with licensed versions.

## License

MIT
