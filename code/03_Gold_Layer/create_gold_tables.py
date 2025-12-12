# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Lakehouse - Gold Layer
# MAGIC
# MAGIC Create business-ready Gold tables for compliance dashboards:
# MAGIC - Control compliance summary
# MAGIC - System compliance scorecard
# MAGIC - Cross-framework mapping
# MAGIC - Evidence gap analysis
# MAGIC - Audit readiness metrics

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, max, min, when, countDistinct,
    round as spark_round, current_date, lit, coalesce
)

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
from utils.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA, BRONZE_SCHEMA, COMPLIANCE_THRESHOLDS
from utils.config import FRAMEWORK_IDS

SILVER = SILVER_SCHEMA
GOLD = GOLD_SCHEMA
BRONZE = BRONZE_SCHEMA

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Control Compliance Summary
# MAGIC Compliance posture aggregated by control family

# COMMAND ----------

df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")

df_compliance_summary = (
    df_assessments
    .groupBy("control_family_code")
    .agg(
        count("*").alias("total_assessments"),
        sum(when(col("implementation_status") == "Implemented", 1).otherwise(0)).alias("implemented_count"),
        sum(when(col("implementation_status") == "Partially Implemented", 1).otherwise(0)).alias("partial_count"),
        sum(when(col("implementation_status") == "Not Implemented", 1).otherwise(0)).alias("not_implemented_count"),
        sum(when(col("implementation_status") == "Not Applicable", 1).otherwise(0)).alias("not_applicable_count"),
        sum(when(col("is_overdue") == True, 1).otherwise(0)).alias("overdue_count"),
        avg("compliance_score").alias("avg_compliance_score")
    )
    .withColumn("assessable_controls", col("total_assessments") - col("not_applicable_count"))
    .withColumn("compliance_percentage",
        spark_round(
            (col("implemented_count") + col("partial_count") * 0.5) / col("assessable_controls") * 100, 1
        ))
    .withColumn("snapshot_date", current_date())
)

df_compliance_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.control_compliance_summary")
print(f"Created gold.control_compliance_summary")
display(df_compliance_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. System Compliance Scorecard
# MAGIC Per-system compliance status

# COMMAND ----------

df_systems = spark.table(f"{CATALOG}.{SILVER}.systems")
df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")
df_evidence = spark.table(f"{CATALOG}.{SILVER}.evidence")

# Aggregate assessments by system
system_assessments = (
    df_assessments
    .groupBy("system_id")
    .agg(
        count("*").alias("total_controls_assessed"),
        sum(when(col("implementation_status") == "Implemented", 1).otherwise(0)).alias("implemented_count"),
        sum(when(col("implementation_status") == "Partially Implemented", 1).otherwise(0)).alias("partial_count"),
        sum(when(col("implementation_status") == "Not Implemented", 1).otherwise(0)).alias("not_implemented_count"),
        sum(when(col("is_overdue") == True, 1).otherwise(0)).alias("overdue_remediation_count"),
        avg("compliance_score").alias("avg_compliance_score"),
        max("assessment_date").alias("last_assessment_date")
    )
)

# Aggregate evidence by system
system_evidence = (
    df_evidence
    .filter(col("status") == "Accepted")
    .filter(col("is_current") == True)
    .groupBy("system_id")
    .agg(
        countDistinct("control_id").alias("controls_with_evidence"),
        count("*").alias("total_evidence_count")
    )
)

# Join everything
df_system_scorecard = (
    df_systems
    .join(system_assessments, "system_id", "left")
    .join(system_evidence, "system_id", "left")
    .select(
        col("system_id"),
        col("system_name"),
        col("business_unit"),
        col("criticality"),
        col("data_classification"),
        col("certification_scope"),
        col("in_soc2_scope"),
        col("in_iso_scope"),
        coalesce(col("total_controls_assessed"), lit(0)).alias("total_controls_assessed"),
        coalesce(col("implemented_count"), lit(0)).alias("implemented_count"),
        coalesce(col("partial_count"), lit(0)).alias("partial_count"),
        coalesce(col("not_implemented_count"), lit(0)).alias("not_implemented_count"),
        coalesce(col("overdue_remediation_count"), lit(0)).alias("overdue_remediation_count"),
        spark_round(coalesce(col("avg_compliance_score"), lit(0)), 1).alias("compliance_percentage"),
        coalesce(col("controls_with_evidence"), lit(0)).alias("controls_with_evidence"),
        coalesce(col("total_evidence_count"), lit(0)).alias("total_evidence_count"),
        col("days_since_assessment"),
        col("assessment_overdue")
    )
    .withColumn(
        "compliance_status",
        when(
            col("compliance_percentage") >= COMPLIANCE_THRESHOLDS["compliant"],
            "Compliant",
        )
        .when(
            col("compliance_percentage")
            >= COMPLIANCE_THRESHOLDS["partially_compliant"],
            "Partially Compliant",
        )
        .otherwise("Non-Compliant"),
    )
    .withColumn("snapshot_date", current_date())
)

df_system_scorecard.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.system_compliance_scorecard")
print(f"Created gold.system_compliance_scorecard")
display(df_system_scorecard.orderBy("compliance_percentage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cross-Framework Control Mapping
# MAGIC NIST 800-53 mapped to SOC2, ISO 27001, and PCI-DSS

# COMMAND ----------

df_nist = spark.table(f"{CATALOG}.{SILVER}.nist_controls")
df_soc2 = spark.table(f"{CATALOG}.{SILVER}.soc2_criteria")
df_iso = spark.table(f"{CATALOG}.{SILVER}.iso_controls")
df_pci = spark.table(f"{CATALOG}.{SILVER}.pci_controls")
df_mapping = spark.table(f"{CATALOG}.{BRONZE}.control_mapping")
df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")

# Get average compliance per control
control_compliance = (
    df_assessments
    .groupBy("control_id")
    .agg(
        avg("compliance_score").alias("avg_compliance_pct"),
        countDistinct("system_id").alias("systems_assessed"),
        count("*").alias("total_assessments")
    )
)

df_mapping_nist = df_mapping.filter(col("source_framework") == FRAMEWORK_IDS["nist"])

df_mapping_soc2 = df_mapping_nist.filter(col("target_framework") == FRAMEWORK_IDS["soc2"])
df_mapping_iso = df_mapping_nist.filter(col("target_framework") == FRAMEWORK_IDS["iso"])
df_mapping_pci = df_mapping_nist.filter(col("target_framework") == FRAMEWORK_IDS["pci"])

def build_cross_view(target_df, mapping_df, target_id_col, target_name_col, target_domain_col, target_framework):
    return (
        df_nist.alias("n")
        .join(mapping_df.alias("m"), col("n.control_id") == col("m.source_control_id"), "left")
        .join(target_df.alias("t"), col("m.target_control_id") == col(f"t.{target_id_col}"), "left")
        .join(control_compliance.alias("c"), col("n.control_id") == col("c.control_id"), "left")
        .select(
            col("n.control_id").alias("nist_control_id"),
            col("n.control_name").alias("nist_control_name"),
            col("n.control_family").alias("nist_family"),
            col("n.control_family_code").alias("nist_family_code"),
            col("n.baseline_low"),
            col("n.baseline_moderate"),
            col("n.baseline_high"),
            lit(target_framework).alias("target_framework"),
            col("m.target_control_id").alias("target_control_id"),
            col(f"t.{target_name_col}").alias("target_control_name"),
            col(f"t.{target_domain_col}").alias("target_domain"),
            col("m.mapping_type"),
            col("m.mapping_notes"),
            coalesce(col("c.systems_assessed"), lit(0)).alias("systems_in_scope"),
            spark_round(coalesce(col("c.avg_compliance_pct"), lit(0)), 1).alias("avg_compliance_pct"),
            coalesce(col("c.total_assessments"), lit(0)).alias("total_assessments"),
        )
        .withColumn(
            "mapping_coverage",
            when(col("target_control_id").isNotNull(), "Mapped").otherwise("Gap - No Mapping"),
        )
        .withColumn("snapshot_date", current_date())
    )

df_cross_soc2 = build_cross_view(
    target_df=df_soc2,
    mapping_df=df_mapping_soc2,
    target_id_col="tsc_id",
    target_name_col="tsc_name",
    target_domain_col="tsc_category",
    target_framework=FRAMEWORK_IDS["soc2"],
)

df_cross_iso = build_cross_view(
    target_df=df_iso,
    mapping_df=df_mapping_iso,
    target_id_col="control_id",
    target_name_col="control_name",
    target_domain_col="control_domain",
    target_framework=FRAMEWORK_IDS["iso"],
)

df_cross_pci = build_cross_view(
    target_df=df_pci,
    mapping_df=df_mapping_pci,
    target_id_col="control_id",
    target_name_col="control_name",
    target_domain_col="control_domain",
    target_framework=FRAMEWORK_IDS["pci"],
)

df_cross_framework = (
    df_cross_soc2.unionByName(df_cross_iso).unionByName(df_cross_pci)
)

df_cross_framework.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.cross_framework_mapping")
print(f"Created gold.cross_framework_mapping")
display(df_cross_framework.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3a. Framework Control Compliance (Derived)
# MAGIC Roll NIST assessment results up to mapped controls in other frameworks.

# COMMAND ----------

df_framework_control_compliance = (
    df_cross_framework
    .filter(col("target_control_id").isNotNull())
    .groupBy(
        "target_framework",
        "target_control_id",
        "target_control_name",
        "target_domain",
    )
    .agg(
        avg("avg_compliance_pct").alias("avg_compliance_pct"),
        countDistinct("nist_control_id").alias("mapped_nist_controls"),
        max("systems_in_scope").alias("systems_in_scope"),
        sum("total_assessments").alias("total_assessments"),
    )
    .withColumn("snapshot_date", current_date())
)

df_framework_control_compliance.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD}.framework_control_compliance"
)
print("Created gold.framework_control_compliance")

df_framework_compliance_summary = (
    df_framework_control_compliance
    .groupBy("target_framework")
    .agg(
        count("*").alias("controls_mapped"),
        spark_round(avg("avg_compliance_pct"), 1).alias("avg_compliance_pct"),
        sum("mapped_nist_controls").alias("total_mapped_nist_controls"),
    )
    .withColumn("snapshot_date", current_date())
)

df_framework_compliance_summary.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD}.framework_compliance_summary"
)
print("Created gold.framework_compliance_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evidence Gap Analysis
# MAGIC Controls with missing or expiring evidence

# COMMAND ----------

df_evidence = spark.table(f"{CATALOG}.{SILVER}.evidence")

# Get evidence status per control/system
evidence_summary = (
    df_evidence
    .groupBy("control_id", "system_id")
    .agg(
        count("*").alias("total_evidence_count"),
        sum(when((col("status") == "Accepted") & (col("is_current") == True), 1).otherwise(0)).alias("current_evidence_count"),
        max(when(col("is_current") == True, col("days_until_expiry"))).alias("days_until_expiry"),
        max("upload_timestamp").alias("last_evidence_date")
    )
    .withColumn("has_current_evidence", col("current_evidence_count") > 0)
    .withColumn("gap_severity",
        when(col("current_evidence_count") == 0, "Critical")
        .when(col("days_until_expiry") < 30, "High")
        .when(col("days_until_expiry") < 90, "Medium")
        .otherwise("Low"))
    .withColumn("evidence_status",
        when(col("has_current_evidence") == False, "Missing")
        .when(col("days_until_expiry") < 30, "Expiring Soon")
        .otherwise("Current"))
    .withColumn("snapshot_date", current_date())
)

evidence_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.evidence_gap_analysis")
print(f"Created gold.evidence_gap_analysis")
display(evidence_summary.groupBy("gap_severity").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit Readiness Metrics
# MAGIC Executive summary for audit preparation

# COMMAND ----------

df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")
df_evidence = spark.table(f"{CATALOG}.{SILVER}.evidence")
df_systems = spark.table(f"{CATALOG}.{SILVER}.systems")

# Overall assessment metrics
assessment_metrics = df_assessments.agg(
    count("*").alias("total_assessments"),
    countDistinct("control_id").alias("unique_controls_assessed"),
    countDistinct("system_id").alias("systems_assessed"),
    avg("compliance_score").alias("overall_compliance_pct"),
    sum(when(col("implementation_status") == "Implemented", 1).otherwise(0)).alias("implemented_count"),
    sum(when(col("implementation_status") == "Partially Implemented", 1).otherwise(0)).alias("partial_count"),
    sum(when(col("implementation_status") == "Not Implemented", 1).otherwise(0)).alias("gap_count"),
    sum(when(col("is_overdue") == True, 1).otherwise(0)).alias("overdue_remediation_count")
)

# Evidence metrics
evidence_metrics = df_evidence.agg(
    count("*").alias("total_evidence_items"),
    sum(when(col("status") == "Accepted", 1).otherwise(0)).alias("accepted_evidence"),
    sum(when(col("status") == "Pending Review", 1).otherwise(0)).alias("pending_review"),
    sum(when(col("is_expired") == True, 1).otherwise(0)).alias("expired_evidence"),
    countDistinct(when(col("is_current") == True, col("control_id"))).alias("controls_with_current_evidence")
)

# Systems metrics
systems_metrics = (
    df_systems
    .filter(col("in_soc2_scope") | col("in_iso_scope"))
    .agg(
        count("*").alias("systems_in_scope"),
        sum(when(col("assessment_overdue") == True, 1).otherwise(0)).alias("systems_assessment_overdue")
    )
)

# Combine all metrics
df_readiness = (
    assessment_metrics.crossJoin(evidence_metrics).crossJoin(systems_metrics)
    .withColumn("overall_compliance_pct", spark_round(col("overall_compliance_pct"), 1))
    .withColumn("evidence_coverage_pct",
        spark_round(col("controls_with_current_evidence") / col("unique_controls_assessed") * 100, 1))
    .withColumn("audit_readiness_score",
        spark_round(
            (col("overall_compliance_pct") * 0.5) +
            (col("evidence_coverage_pct") * 0.3) +
            ((1 - col("overdue_remediation_count") / col("total_assessments")) * 100 * 0.2),
            1
        ))
    .withColumn("readiness_status",
        when(col("audit_readiness_score") >= 85, "Ready")
        .when(col("audit_readiness_score") >= 70, "On Track")
        .when(col("audit_readiness_score") >= 50, "At Risk")
        .otherwise("Critical"))
    .withColumn("snapshot_date", current_date())
)

df_readiness.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.audit_readiness_metrics")
print(f"Created gold.audit_readiness_metrics")
display(df_readiness)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN grc_compliance_dev.03_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   overall_compliance_pct as `Overall Compliance %`,
# MAGIC   audit_readiness_score as `Audit Readiness Score`,
# MAGIC   readiness_status as `Status`,
# MAGIC   systems_in_scope as `Systems in Scope`,
# MAGIC   unique_controls_assessed as `Controls Assessed`,
# MAGIC   overdue_remediation_count as `Overdue Items`,
# MAGIC   evidence_coverage_pct as `Evidence Coverage %`
# MAGIC FROM grc_compliance_dev.03_gold.audit_readiness_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Complete!
# MAGIC
# MAGIC Tables created for dashboards:
# MAGIC - `control_compliance_summary` - Compliance by control family
# MAGIC - `system_compliance_scorecard` - Per-system posture
# MAGIC - `cross_framework_mapping` - NIST to SOC2 mapping
# MAGIC - `evidence_gap_analysis` - Evidence coverage gaps
# MAGIC - `audit_readiness_metrics` - Executive readiness score
