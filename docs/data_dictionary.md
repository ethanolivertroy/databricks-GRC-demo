# Data Dictionary

## Bronze Layer Tables

### nist_800_53_controls
NIST 800-53 Rev 5 security and privacy controls.

| Column | Type | Description |
|--------|------|-------------|
| control_id | STRING | Unique control identifier (e.g., AC-1, SC-7) |
| control_name | STRING | Control title |
| control_family | STRING | Control family name (e.g., Access Control) |
| control_description | STRING | Full control description |
| control_enhancements | STRING | Enhancement details if applicable |
| priority | STRING | Implementation priority (P1, P2, P3) |
| baseline_impact | STRING | Low, Moderate, High baseline |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |
| _source | STRING | Data source identifier |

### soc2_trust_criteria
SOC2 Trust Service Criteria (2017).

| Column | Type | Description |
|--------|------|-------------|
| tsc_id | STRING | Trust Service Criteria ID (e.g., CC1.1) |
| tsc_category | STRING | Category (CC, A, PI, C, P) |
| tsc_name | STRING | Criteria name |
| tsc_description | STRING | Full criteria description |
| points_of_focus | STRING | Implementation guidance |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |
| _source | STRING | Data source identifier |

### control_mapping
Cross-framework control mapping (NIST to SOC2).

| Column | Type | Description |
|--------|------|-------------|
| mapping_id | STRING | Unique mapping identifier |
| nist_control_id | STRING | NIST 800-53 control ID |
| soc2_tsc_id | STRING | SOC2 TSC ID |
| mapping_strength | STRING | Strong, Moderate, Weak |
| mapping_notes | STRING | Mapping justification |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |

### systems_inventory
Systems under compliance assessment.

| Column | Type | Description |
|--------|------|-------------|
| system_id | STRING | Unique system identifier |
| system_name | STRING | System display name |
| system_owner | STRING | Primary owner email |
| business_unit | STRING | Owning business unit |
| data_classification | STRING | Public, Internal, Confidential, Restricted |
| criticality | STRING | Critical, High, Medium, Low |
| certification_scope | STRING | ISO27001, SOC2, Both, None |
| last_assessment_date | DATE | Most recent assessment date |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |

### control_assessments
Control assessment results per system.

| Column | Type | Description |
|--------|------|-------------|
| assessment_id | STRING | Unique assessment identifier |
| control_id | STRING | Control being assessed |
| system_id | STRING | System being assessed |
| assessment_date | DATE | Date of assessment |
| implementation_status | STRING | Implemented, Partially Implemented, Not Implemented, Not Applicable |
| effectiveness_rating | STRING | Effective, Partially Effective, Not Effective |
| gap_description | STRING | Description of gaps found |
| remediation_due_date | DATE | Target remediation date |
| remediation_owner | STRING | Owner responsible for remediation |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |

### evidence_records
Evidence artifacts supporting control implementation.

| Column | Type | Description |
|--------|------|-------------|
| evidence_id | STRING | Unique evidence identifier |
| control_id | STRING | Control this evidence supports |
| system_id | STRING | System this evidence applies to |
| evidence_type | STRING | Screenshot, Configuration, Policy, Log, Report |
| evidence_path | STRING | Storage path or URL |
| uploaded_by | STRING | Uploader email |
| upload_timestamp | TIMESTAMP | When evidence was uploaded |
| assessment_period_start | DATE | Evidence validity start |
| assessment_period_end | DATE | Evidence validity end |
| status | STRING | Pending Review, Accepted, Rejected, Expired |
| _ingestion_timestamp | TIMESTAMP | When record was loaded |

---

## Silver Layer Tables

### nist_controls
Enriched NIST controls.

| Column | Type | Description |
|--------|------|-------------|
| (all Bronze columns) | | |
| control_family_code | STRING | Two-letter family code (AC, SC, etc.) |
| is_enhancement | BOOLEAN | True if this is a control enhancement |
| base_control_id | STRING | Parent control ID for enhancements |

### soc2_criteria
Enriched SOC2 criteria.

| Column | Type | Description |
|--------|------|-------------|
| (all Bronze columns) | | |
| category_name | STRING | Full category name |
| criteria_number | INT | Numeric portion of TSC ID |

### systems
Enriched systems data.

| Column | Type | Description |
|--------|------|-------------|
| (all Bronze columns) | | |
| days_since_assessment | INT | Days since last_assessment_date |
| assessment_status | STRING | Current, Stale, Overdue |
| in_soc2_scope | BOOLEAN | True if SOC2 in certification_scope |
| in_iso_scope | BOOLEAN | True if ISO27001 in certification_scope |

### assessments
Enriched assessment data.

| Column | Type | Description |
|--------|------|-------------|
| (all Bronze columns) | | |
| control_family_code | STRING | Control family from control_id |
| compliance_score | DOUBLE | 0-100 score based on status |
| is_overdue | BOOLEAN | True if remediation_due_date < today |
| days_to_remediation | INT | Days until/since remediation due |

### evidence
Enriched evidence data.

| Column | Type | Description |
|--------|------|-------------|
| (all Bronze columns) | | |
| is_expired | BOOLEAN | True if assessment_period_end < today |
| days_until_expiry | INT | Days until evidence expires |
| validity_days | INT | Total validity period length |

---

## Gold Layer Tables

### control_compliance_summary
Compliance metrics by control family.

| Column | Type | Description |
|--------|------|-------------|
| control_family_code | STRING | Two-letter family code |
| total_assessments | BIGINT | Total assessments in family |
| implemented_count | BIGINT | Fully implemented count |
| partial_count | BIGINT | Partially implemented count |
| not_implemented_count | BIGINT | Not implemented count |
| na_count | BIGINT | Not applicable count |
| compliance_percentage | DOUBLE | % implemented (includes partial at 50%) |

### system_compliance_scorecard
Per-system compliance posture.

| Column | Type | Description |
|--------|------|-------------|
| system_id | STRING | System identifier |
| system_name | STRING | System display name |
| business_unit | STRING | Owning business unit |
| criticality | STRING | System criticality |
| total_controls_assessed | BIGINT | Controls assessed for this system |
| implemented_count | BIGINT | Fully implemented |
| compliance_percentage | DOUBLE | Overall compliance % |
| compliance_status | STRING | Compliant, Partially Compliant, Non-Compliant |
| overdue_remediation_count | BIGINT | Count of overdue items |
| controls_with_evidence | BIGINT | Controls with valid evidence |
| days_since_assessment | INT | Days since last assessment |
| in_soc2_scope | BOOLEAN | SOC2 scope flag |
| in_iso_scope | BOOLEAN | ISO scope flag |

### cross_framework_mapping
NIST to SOC2 mapping with compliance status.

| Column | Type | Description |
|--------|------|-------------|
| nist_control_id | STRING | NIST control ID |
| nist_family_code | STRING | NIST family code |
| soc2_tsc_id | STRING | Mapped SOC2 TSC ID |
| mapping_strength | STRING | Mapping strength |
| assessment_count | BIGINT | Assessments for this control |
| avg_compliance_pct | DOUBLE | Average compliance % |

### evidence_gap_analysis
Controls with evidence gaps.

| Column | Type | Description |
|--------|------|-------------|
| control_id | STRING | Control identifier |
| system_id | STRING | System identifier |
| evidence_count | BIGINT | Total evidence records |
| accepted_evidence | BIGINT | Accepted evidence count |
| expired_evidence | BIGINT | Expired evidence count |
| pending_evidence | BIGINT | Pending review count |
| gap_severity | STRING | Critical, High, Medium, Low |
| days_since_last_evidence | INT | Days since last evidence upload |

### audit_readiness_metrics
Executive-level audit readiness.

| Column | Type | Description |
|--------|------|-------------|
| metric_date | DATE | Snapshot date |
| overall_compliance_pct | DOUBLE | Organization-wide compliance % |
| evidence_coverage_pct | DOUBLE | % controls with valid evidence |
| overdue_remediation_count | BIGINT | Total overdue items |
| critical_findings | BIGINT | High severity gaps |
| audit_readiness_score | DOUBLE | Composite readiness score (0-100) |

### compliance_alerts
Rule engine generated alerts.

| Column | Type | Description |
|--------|------|-------------|
| entity_type | STRING | Type of entity (evidence, assessment, system, etc.) |
| entity_id | STRING | ID of the flagged entity |
| control_id | STRING | Related control (if applicable) |
| system_id | STRING | Related system (if applicable) |
| rule_name | STRING | Rule that generated this alert |
| rule_result | STRING | PASS, WARN, FAIL, CRITICAL |
| severity | STRING | Alert severity |
| alert_date | DATE | When alert was generated |

### control_risk_predictions
ML model risk predictions.

| Column | Type | Description |
|--------|------|-------------|
| assessment_id | STRING | Assessment being scored |
| control_id | STRING | Control identifier |
| system_id | STRING | System identifier |
| control_family_code | STRING | Control family |
| risk_score | DOUBLE | Predicted risk probability (0-1) |
| predicted_at_risk | DOUBLE | Binary prediction (0 or 1) |
| risk_level | STRING | High, Medium, Low |
| prediction_date | DATE | When prediction was made |

---

## Compliance Rules

| Rule Name | Description | Severity |
|-----------|-------------|----------|
| expired_evidence | Evidence has expired or expires within 30 days | HIGH |
| overdue_remediation | Remediation is overdue or due within 14 days | HIGH |
| stale_assessment | System assessment is older than 9 months | MEDIUM |
| critical_system_coverage | Critical/High systems must meet compliance thresholds | HIGH |
| evidence_gap | Control has critical or high severity evidence gap | HIGH |

---

## Enum Values

### implementation_status
- Implemented
- Partially Implemented
- Not Implemented
- Not Applicable

### effectiveness_rating
- Effective
- Partially Effective
- Not Effective

### data_classification
- Public
- Internal
- Confidential
- Restricted

### criticality
- Critical
- High
- Medium
- Low

### evidence_status
- Pending Review
- Accepted
- Rejected
- Expired

### compliance_status
- Compliant (>= 90%)
- Partially Compliant (>= 70%)
- Non-Compliant (< 70%)

### risk_level
- High (risk_score >= 0.7)
- Medium (risk_score >= 0.4)
- Low (risk_score < 0.4)
