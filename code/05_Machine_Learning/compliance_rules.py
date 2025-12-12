# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Compliance Rule Engine
# MAGIC
# MAGIC Automated compliance checks that flag issues before auditors find them.

# COMMAND ----------

from pyspark.sql.functions import expr, col, current_date, lit, when

import sys

try:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    repo_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
    code_path = f"{repo_root}/code"
    if code_path not in sys.path:
        sys.path.insert(0, code_path)
except Exception:
    pass

from utils.bootstrap import ensure_code_on_path

ensure_code_on_path(dbutils=dbutils)
from utils.config import (
    CATALOG,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    EVIDENCE_EXPIRY_WARNING,
    REMEDIATION_WARNING,
    ASSESSMENT_FRESHNESS,
    COMPLIANCE_THRESHOLDS,
)

SILVER = SILVER_SCHEMA
GOLD = GOLD_SCHEMA

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Compliance Rules

# COMMAND ----------

# Create rules table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER}.compliance_rules (
  rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
  rule_name STRING,
  rule_description STRING,
  check_code STRING,
  severity STRING,
  is_active BOOLEAN
)
""")

# COMMAND ----------

def insert_rule(name, description, code, severity, is_active=True):
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SILVER}.compliance_rules
    (rule_name, rule_description, check_code, severity, is_active)
    VALUES ('{name}', '{description}', '{code}', '{severity}', {is_active})
    """)

# Clear existing rules and reload
spark.sql(f"TRUNCATE TABLE {CATALOG}.{SILVER}.compliance_rules")

# COMMAND ----------

# Rule 1: Expired Evidence
expired_evidence_check = """
CASE
  WHEN is_expired = true THEN 'FAIL'
  WHEN days_until_expiry < {expiry_warn} THEN 'WARN'
  ELSE 'PASS'
END
""".format(expiry_warn=EVIDENCE_EXPIRY_WARNING)
insert_rule(
    'expired_evidence',
    'Evidence has expired or expires within 30 days',
    expired_evidence_check,
    'HIGH'
)

# COMMAND ----------

# Rule 2: Overdue Remediation
overdue_remediation_check = """
CASE
  WHEN is_overdue = true AND days_to_remediation < -30 THEN 'CRITICAL'
  WHEN is_overdue = true THEN 'FAIL'
  WHEN days_to_remediation < {rem_warn} THEN 'WARN'
  ELSE 'PASS'
END
""".format(rem_warn=REMEDIATION_WARNING)
insert_rule(
    'overdue_remediation',
    'Remediation is overdue or due within 14 days',
    overdue_remediation_check,
    'HIGH'
)

# COMMAND ----------

# Rule 3: Assessment Freshness
stale_assessment_check = """
CASE
  WHEN days_since_assessment > {stale_critical} THEN 'FAIL'
  WHEN days_since_assessment > {stale_warning} THEN 'WARN'
  ELSE 'PASS'
END
""".format(
    stale_warning=ASSESSMENT_FRESHNESS["stale_warning"],
    stale_critical=ASSESSMENT_FRESHNESS["stale_critical"],
)
insert_rule(
    'stale_assessment',
    'System assessment is older than 9 months',
    stale_assessment_check,
    'MEDIUM'
)

# COMMAND ----------

# Rule 4: Critical System Coverage
critical_system_check = """
CASE
  WHEN criticality = 'Critical' AND compliance_percentage < {crit_fail} THEN 'FAIL'
  WHEN criticality = 'Critical' AND compliance_percentage < {crit_warn} THEN 'WARN'
  WHEN criticality = 'High' AND compliance_percentage < {high_warn} THEN 'WARN'
  ELSE 'PASS'
END
""".format(
    crit_fail=COMPLIANCE_THRESHOLDS["compliant"],
    crit_warn=95,
    high_warn=80,
)
insert_rule(
    'critical_system_coverage',
    'Critical/High systems must meet compliance thresholds',
    critical_system_check,
    'HIGH'
)

# COMMAND ----------

# Rule 5: Evidence Gap
evidence_gap_check = """
CASE
  WHEN gap_severity = 'Critical' THEN 'FAIL'
  WHEN gap_severity = 'High' THEN 'WARN'
  ELSE 'PASS'
END
"""
insert_rule(
    'evidence_gap',
    'Control has critical or high severity evidence gap',
    evidence_gap_check,
    'HIGH'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Rules to Generate Alerts

# COMMAND ----------

# Load rules
rules = spark.sql(f"""
SELECT * FROM {CATALOG}.{SILVER}.compliance_rules
WHERE is_active = true
ORDER BY rule_id
""").collect()

print(f"Loaded {len(rules)} active rules")
for r in rules:
    print(f"  - {r.rule_name}: {r.severity}")

# COMMAND ----------

# Apply rules to evidence
df_evidence = spark.table(f"{CATALOG}.{SILVER}.evidence")

evidence_rule = [r for r in rules if r.rule_name == 'expired_evidence'][0]
df_evidence_alerts = (
    df_evidence
    .withColumn("rule_name", lit(evidence_rule.rule_name))
    .withColumn("rule_result", expr(evidence_rule.check_code))
    .withColumn("severity", lit(evidence_rule.severity))
    .filter(col("rule_result") != "PASS")
    .select(
        lit("evidence").alias("entity_type"),
        col("evidence_id").alias("entity_id"),
        col("control_id"),
        col("system_id"),
        col("rule_name"),
        col("rule_result"),
        col("severity"),
        current_date().alias("alert_date")
    )
)

# COMMAND ----------

# Apply rules to assessments
df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")

remediation_rule = [r for r in rules if r.rule_name == 'overdue_remediation'][0]
df_assessment_alerts = (
    df_assessments
    .withColumn("rule_name", lit(remediation_rule.rule_name))
    .withColumn("rule_result", expr(remediation_rule.check_code))
    .withColumn("severity", lit(remediation_rule.severity))
    .filter(col("rule_result") != "PASS")
    .select(
        lit("assessment").alias("entity_type"),
        col("assessment_id").alias("entity_id"),
        col("control_id"),
        col("system_id"),
        col("rule_name"),
        col("rule_result"),
        col("severity"),
        current_date().alias("alert_date")
    )
)

# COMMAND ----------

# Apply rules to systems
df_systems = spark.table(f"{CATALOG}.{GOLD}.system_compliance_scorecard")

system_rule = [r for r in rules if r.rule_name == 'critical_system_coverage'][0]
stale_rule = [r for r in rules if r.rule_name == 'stale_assessment'][0]

df_system_alerts = (
    df_systems
    .withColumn("crit_rule_name", lit(system_rule.rule_name))
    .withColumn("crit_rule_result", expr(system_rule.check_code))
    .withColumn("stale_rule_name", lit(stale_rule.rule_name))
    .withColumn("stale_rule_result", expr(stale_rule.check_code))
)

# Critical system alerts
df_crit_alerts = (
    df_system_alerts
    .filter(col("crit_rule_result") != "PASS")
    .select(
        lit("system").alias("entity_type"),
        col("system_id").alias("entity_id"),
        lit(None).cast("string").alias("control_id"),
        col("system_id"),
        col("crit_rule_name").alias("rule_name"),
        col("crit_rule_result").alias("rule_result"),
        lit(system_rule.severity).alias("severity"),
        current_date().alias("alert_date")
    )
)

# Stale assessment alerts
df_stale_alerts = (
    df_system_alerts
    .filter(col("stale_rule_result") != "PASS")
    .select(
        lit("system").alias("entity_type"),
        col("system_id").alias("entity_id"),
        lit(None).cast("string").alias("control_id"),
        col("system_id"),
        col("stale_rule_name").alias("rule_name"),
        col("stale_rule_result").alias("rule_result"),
        lit(stale_rule.severity).alias("severity"),
        current_date().alias("alert_date")
    )
)

# COMMAND ----------

# Apply rules to evidence gaps
df_gaps = spark.table(f"{CATALOG}.{GOLD}.evidence_gap_analysis")

gap_rule = [r for r in rules if r.rule_name == 'evidence_gap'][0]
df_gap_alerts = (
    df_gaps
    .withColumn("rule_name", lit(gap_rule.rule_name))
    .withColumn("rule_result", expr(gap_rule.check_code))
    .withColumn("severity", lit(gap_rule.severity))
    .filter(col("rule_result") != "PASS")
    .select(
        lit("evidence_gap").alias("entity_type"),
        col("control_id").alias("entity_id"),
        col("control_id"),
        col("system_id"),
        col("rule_name"),
        col("rule_result"),
        col("severity"),
        current_date().alias("alert_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Alerts

# COMMAND ----------

# Union all alerts
df_all_alerts = (
    df_evidence_alerts
    .union(df_assessment_alerts)
    .union(df_crit_alerts)
    .union(df_stale_alerts)
    .union(df_gap_alerts)
)

# Write to gold table
df_all_alerts.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.compliance_alerts")

alert_count = df_all_alerts.count()
print(f"Generated {alert_count} compliance alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   rule_name,
# MAGIC   rule_result,
# MAGIC   severity,
# MAGIC   COUNT(*) as alert_count
# MAGIC FROM grc_compliance_dev.03_gold.compliance_alerts
# MAGIC GROUP BY rule_name, rule_result, severity
# MAGIC ORDER BY
# MAGIC   CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
# MAGIC   alert_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Engine Complete
# MAGIC
# MAGIC Alerts written to `03_gold.compliance_alerts`
# MAGIC
# MAGIC Next: Run `risk_prediction.py` for ML-based risk scoring
