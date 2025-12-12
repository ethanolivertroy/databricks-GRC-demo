# Configuration for GRC Compliance Lakehouse

# Catalog and Schema Configuration
CATALOG = "grc_compliance_dev"
LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# Volume Paths
VOLUMES_BASE = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}"
FRAMEWORKS_PATH = f"{VOLUMES_BASE}/frameworks"
SYSTEMS_PATH = f"{VOLUMES_BASE}/systems"
ASSESSMENTS_PATH = f"{VOLUMES_BASE}/assessments"
EVIDENCE_PATH = f"{VOLUMES_BASE}/evidence"

# Table Names - Bronze
BRONZE_TABLES = {
    "nist_controls": f"{CATALOG}.{BRONZE_SCHEMA}.nist_800_53_controls",
    "soc2_criteria": f"{CATALOG}.{BRONZE_SCHEMA}.soc2_trust_criteria",
    "control_mapping": f"{CATALOG}.{BRONZE_SCHEMA}.control_mapping",
    "systems": f"{CATALOG}.{BRONZE_SCHEMA}.systems_inventory",
    "assessments": f"{CATALOG}.{BRONZE_SCHEMA}.control_assessments",
    "evidence": f"{CATALOG}.{BRONZE_SCHEMA}.evidence_records",
}

# Table Names - Silver
SILVER_TABLES = {
    "nist_controls": f"{CATALOG}.{SILVER_SCHEMA}.nist_controls",
    "soc2_criteria": f"{CATALOG}.{SILVER_SCHEMA}.soc2_criteria",
    "systems": f"{CATALOG}.{SILVER_SCHEMA}.systems",
    "assessments": f"{CATALOG}.{SILVER_SCHEMA}.assessments",
    "evidence": f"{CATALOG}.{SILVER_SCHEMA}.evidence",
}

# Table Names - Gold
GOLD_TABLES = {
    "compliance_summary": f"{CATALOG}.{GOLD_SCHEMA}.control_compliance_summary",
    "system_scorecard": f"{CATALOG}.{GOLD_SCHEMA}.system_compliance_scorecard",
    "framework_mapping": f"{CATALOG}.{GOLD_SCHEMA}.cross_framework_mapping",
    "evidence_gaps": f"{CATALOG}.{GOLD_SCHEMA}.evidence_gap_analysis",
    "audit_readiness": f"{CATALOG}.{GOLD_SCHEMA}.audit_readiness_metrics",
    "compliance_alerts": f"{CATALOG}.{GOLD_SCHEMA}.compliance_alerts",
    "risk_predictions": f"{CATALOG}.{GOLD_SCHEMA}.control_risk_predictions",
}

# Compliance Score Thresholds
COMPLIANCE_THRESHOLDS = {
    "compliant": 90,
    "partially_compliant": 70,
}

# Risk Score Thresholds
RISK_THRESHOLDS = {
    "high": 0.7,
    "medium": 0.4,
}

# Assessment Freshness (days)
ASSESSMENT_FRESHNESS = {
    "stale_warning": 270,  # 9 months
    "stale_critical": 365,  # 1 year
}

# Evidence Expiry Warning (days)
EVIDENCE_EXPIRY_WARNING = 30

# Remediation Warning (days)
REMEDIATION_WARNING = 14

# Implementation Status Scores
STATUS_SCORES = {
    "Implemented": 100,
    "Partially Implemented": 50,
    "Not Implemented": 0,
    "Not Applicable": None,
}

# Valid Enum Values
VALID_IMPLEMENTATION_STATUS = [
    "Implemented",
    "Partially Implemented",
    "Not Implemented",
    "Not Applicable",
]

VALID_CRITICALITY = ["Critical", "High", "Medium", "Low"]

VALID_DATA_CLASSIFICATION = ["Public", "Internal", "Confidential", "Restricted"]

VALID_EVIDENCE_STATUS = ["Pending Review", "Accepted", "Rejected", "Expired"]

# ML Model Configuration
ML_CONFIG = {
    "model_name": f"{CATALOG}.{GOLD_SCHEMA}.control_risk_model",
    "num_trees": 50,
    "max_depth": 5,
    "test_size": 0.2,
    "random_seed": 42,
}

# Control Family Codes
CONTROL_FAMILIES = {
    "AC": "Access Control",
    "AT": "Awareness and Training",
    "AU": "Audit and Accountability",
    "CA": "Assessment, Authorization, and Monitoring",
    "CM": "Configuration Management",
    "CP": "Contingency Planning",
    "IA": "Identification and Authentication",
    "IR": "Incident Response",
    "MA": "Maintenance",
    "MP": "Media Protection",
    "PE": "Physical and Environmental Protection",
    "PL": "Planning",
    "PM": "Program Management",
    "PS": "Personnel Security",
    "PT": "PII Processing and Transparency",
    "RA": "Risk Assessment",
    "SA": "System and Services Acquisition",
    "SC": "System and Communications Protection",
    "SI": "System and Information Integrity",
    "SR": "Supply Chain Risk Management",
}

# SOC2 Trust Service Categories
SOC2_CATEGORIES = {
    "CC": "Common Criteria",
    "A": "Availability",
    "PI": "Processing Integrity",
    "C": "Confidentiality",
    "P": "Privacy",
}
