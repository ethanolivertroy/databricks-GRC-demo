# Architecture

## System Overview

```
+------------------+     +-------------------+     +------------------+
|   Data Sources   |     |    Databricks     |     |   Consumption    |
+------------------+     +-------------------+     +------------------+
|                  |     |                   |     |                  |
| - NIST 800-53    |     |  Unity Catalog    |     | - SQL Dashboard  |
| - SOC2 TSC       |---->|  (grc_compliance) |---->| - ML Predictions |
| - ISO 27001      |     |                   |     | - Compliance     |
| - Systems CMDB   |     |  +-------------+  |     |   Alerts         |
| - Assessments    |     |  | 00_landing  |  |     |                  |
| - Evidence       |     |  +------+------+  |     +------------------+
|                  |     |         |         |
+------------------+     |         v         |
                         |  +-------------+  |
                         |  | 01_bronze   |  |
                         |  +------+------+  |
                         |         |         |
                         |         v         |
                         |  +-------------+  |
                         |  | 02_silver   |  |
                         |  +------+------+  |
                         |         |         |
                         |         v         |
                         |  +-------------+  |
                         |  | 03_gold     |  |
                         |  +-------------+  |
                         |         |         |
                         |         v         |
                         |  +-------------+  |
                         |  | ML Models   |  |
                         |  +-------------+  |
                         +-------------------+
```

## Medallion Architecture

### Landing (00_landing)
Raw files stored in Unity Catalog Volumes before ingestion.

```
/Volumes/grc_compliance_dev/00_landing/
├── frameworks/
│   ├── nist_800_53_rev5_controls.csv
│   ├── soc2_tsc_2017.csv
│   └── nist_to_soc2_mapping.csv
├── systems/
│   └── systems_inventory.csv
├── assessments/
│   └── control_assessments.csv
└── evidence/
    └── evidence_records.json
```

### Bronze Layer (01_bronze)
Raw data loaded as-is with ingestion metadata.

| Table | Source | Records |
|-------|--------|---------|
| nist_800_53_controls | NIST CSRC | 188 |
| soc2_trust_criteria | AICPA | 64 |
| control_mapping | Internal | 100 |
| systems_inventory | CMDB | 50 |
| control_assessments | Audit | ~1500 |
| evidence_records | GRC Tool | 200 |

Added columns: `_ingestion_timestamp`, `_source`

### Silver Layer (02_silver)
Cleaned, validated, and enriched data.

| Table | Transformations |
|-------|-----------------|
| nist_controls | Extracted control_family_code, standardized IDs |
| soc2_criteria | Parsed categories, added TSC mapping |
| systems | Added days_since_assessment, validated classifications |
| assessments | Added compliance_score, is_overdue, days_to_remediation |
| evidence | Added is_expired, days_until_expiry |

Data quality rules applied:
- Valid control IDs (regex validation)
- Valid status enums
- Date range validation
- Referential integrity checks

### Gold Layer (03_gold)
Business-ready aggregations and metrics.

| Table | Purpose |
|-------|---------|
| control_compliance_summary | Compliance % by control family |
| system_compliance_scorecard | Per-system posture metrics |
| cross_framework_mapping | NIST-SOC2 with compliance status |
| evidence_gap_analysis | Missing/expiring evidence |
| audit_readiness_metrics | Executive readiness score |
| compliance_alerts | Rule engine output |
| control_risk_predictions | ML risk scores |

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  CSV/JSON Files ──> Volumes ──> Bronze Tables                   │
│                                                                  │
│  - REST API upload (curl)                                        │
│  - Databricks CLI                                                │
│  - Manual upload via UI                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────────┐
│                      TRANSFORMATION                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Bronze ──> Silver (cleaning, validation, enrichment)           │
│  Silver ──> Gold (aggregation, metrics, scores)                 │
│                                                                  │
│  - PySpark transformations                                       │
│  - Data quality checks                                           │
│  - Schema enforcement                                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────────┐
│                    MACHINE LEARNING                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Compliance Rule Engine                                       │
│     - Expired evidence detection                                 │
│     - Overdue remediation flagging                               │
│     - Coverage gap analysis                                      │
│     Output: compliance_alerts                                    │
│                                                                  │
│  2. Risk Prediction Model                                        │
│     - Random Forest classifier                                   │
│     - Features: compliance_score, evidence_count, etc.           │
│     - Registered to Unity Catalog via MLflow                     │
│     Output: control_risk_predictions                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────────┐
│                      CONSUMPTION                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Dashboard (4 pages):                                            │
│  - Executive Trust Center                                        │
│  - System Compliance Details                                     │
│  - Cross-Framework Mapping                                       │
│  - Evidence Tracking                                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Databricks |
| Catalog | Unity Catalog |
| Storage | Delta Lake |
| Processing | PySpark |
| ML | MLflow, scikit-learn |
| Dashboard | Databricks SQL Dashboards |
| Version Control | Git/GitHub |

## Security & Governance

- Unity Catalog provides data governance
- Table-level access control
- Column-level lineage tracking
- Audit logging for data access
- ML model registry with versioning
