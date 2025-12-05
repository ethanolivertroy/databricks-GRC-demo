# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Silver to Gold Transformation
# MAGIC
# MAGIC This notebook creates business-ready Gold tables for compliance dashboards:
# MAGIC - Control compliance summary by framework/family
# MAGIC - System compliance scorecard
# MAGIC - Cross-framework control mapping with compliance status
# MAGIC - Evidence gap analysis
# MAGIC - Audit readiness metrics

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, max, min, when, countDistinct,
    round as spark_round, current_date, lit, coalesce
)

catalog = "grc_compliance_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"
bronze_schema = "01_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control Compliance Summary
# MAGIC Overall compliance posture by control family

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{gold_schema}.control_compliance_summary",
    comment="Compliance posture aggregated by control family",
    table_properties={
        "quality": "gold"
    }
)
def control_compliance_summary():
    assessments = dlt.read(f"{catalog}.{silver_schema}.assessments")

    return (
        assessments
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
        .withColumn("assessable_controls",
            col("total_assessments") - col("not_applicable_count"))
        .withColumn("compliance_percentage",
            spark_round(
                (col("implemented_count") + col("partial_count") * 0.5) /
                col("assessable_controls") * 100, 1
            ))
        .withColumn("snapshot_date", current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Compliance Scorecard
# MAGIC Per-system compliance status for all systems in scope

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{gold_schema}.system_compliance_scorecard",
    comment="Per-system compliance posture and metrics",
    table_properties={
        "quality": "gold"
    }
)
def system_compliance_scorecard():
    systems = dlt.read(f"{catalog}.{silver_schema}.systems")
    assessments = dlt.read(f"{catalog}.{silver_schema}.assessments")
    evidence = dlt.read(f"{catalog}.{silver_schema}.evidence")

    # Aggregate assessments by system
    system_assessments = (
        assessments
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
        evidence
        .filter(col("status") == "Accepted")
        .filter(col("is_current") == True)
        .groupBy("system_id")
        .agg(
            countDistinct("control_id").alias("controls_with_evidence"),
            count("*").alias("total_evidence_count")
        )
    )

    return (
        systems
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
        .withColumn("compliance_status",
            when(col("compliance_percentage") >= 90, "Compliant")
            .when(col("compliance_percentage") >= 70, "Partially Compliant")
            .otherwise("Non-Compliant"))
        .withColumn("snapshot_date", current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Framework Control Mapping
# MAGIC NIST 800-53 to SOC2 mapping with compliance status

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{gold_schema}.cross_framework_mapping",
    comment="NIST to SOC2 control mapping with compliance posture",
    table_properties={
        "quality": "gold"
    }
)
def cross_framework_mapping():
    nist = dlt.read(f"{catalog}.{silver_schema}.nist_controls")
    soc2 = dlt.read(f"{catalog}.{silver_schema}.soc2_criteria")
    mapping = dlt.read(f"{catalog}.{bronze_schema}.control_mapping")
    assessments = dlt.read(f"{catalog}.{silver_schema}.assessments")

    # Get average compliance per control
    control_compliance = (
        assessments
        .groupBy("control_id")
        .agg(
            avg("compliance_score").alias("avg_compliance_pct"),
            countDistinct("system_id").alias("systems_assessed"),
            sum(when(col("implementation_status") == "Implemented", 1).otherwise(0)).alias("implemented_count"),
            count("*").alias("total_assessments")
        )
    )

    return (
        nist.alias("n")
        .join(
            mapping.alias("m"),
            col("n.control_id") == col("m.source_control_id"),
            "left"
        )
        .join(
            soc2.alias("s"),
            col("m.target_control_id") == col("s.tsc_id"),
            "left"
        )
        .join(
            control_compliance.alias("c"),
            col("n.control_id") == col("c.control_id"),
            "left"
        )
        .select(
            col("n.control_id").alias("nist_control_id"),
            col("n.control_name").alias("nist_control_name"),
            col("n.control_family").alias("nist_family"),
            col("n.control_family_code").alias("nist_family_code"),
            col("n.baseline_low"),
            col("n.baseline_moderate"),
            col("n.baseline_high"),
            col("m.target_control_id").alias("soc2_tsc_id"),
            col("s.tsc_name").alias("soc2_tsc_name"),
            col("s.tsc_category").alias("soc2_category"),
            col("m.mapping_type"),
            coalesce(col("c.systems_assessed"), lit(0)).alias("systems_in_scope"),
            spark_round(coalesce(col("c.avg_compliance_pct"), lit(0)), 1).alias("avg_compliance_pct"),
            coalesce(col("c.total_assessments"), lit(0)).alias("total_assessments")
        )
        .withColumn("mapping_coverage",
            when(col("soc2_tsc_id").isNotNull(), "Mapped")
            .otherwise("Gap - No SOC2 Mapping"))
        .withColumn("snapshot_date", current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence Gap Analysis
# MAGIC Identify controls lacking current evidence

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{gold_schema}.evidence_gap_analysis",
    comment="Controls with missing or expiring evidence",
    table_properties={
        "quality": "gold"
    }
)
def evidence_gap_analysis():
    nist = dlt.read(f"{catalog}.{silver_schema}.nist_controls")
    systems = dlt.read(f"{catalog}.{silver_schema}.systems").filter(col("in_soc2_scope") | col("in_iso_scope"))
    evidence = dlt.read(f"{catalog}.{silver_schema}.evidence")

    # Get current evidence status per control/system
    evidence_status = (
        evidence
        .groupBy("control_id", "system_id")
        .agg(
            count("*").alias("total_evidence_count"),
            sum(when((col("status") == "Accepted") & (col("is_current") == True), 1).otherwise(0)).alias("current_evidence_count"),
            max(when(col("is_current") == True, col("days_until_expiry"))).alias("days_until_expiry"),
            max("upload_timestamp").alias("last_evidence_date")
        )
    )

    # Cross join controls with in-scope systems to get expected evidence matrix
    expected_evidence = (
        nist.filter(col("baseline_moderate") == True)
        .select(col("control_id"), col("control_name"), col("control_family"))
        .crossJoin(systems.select(col("system_id"), col("system_name")))
    )

    return (
        expected_evidence
        .join(evidence_status, ["control_id", "system_id"], "left")
        .withColumn("has_current_evidence",
            coalesce(col("current_evidence_count"), lit(0)) > 0)
        .withColumn("gap_severity",
            when(coalesce(col("current_evidence_count"), lit(0)) == 0, "Critical")
            .when(col("days_until_expiry") < 30, "High")
            .when(col("days_until_expiry") < 90, "Medium")
            .otherwise("Low"))
        .withColumn("evidence_status",
            when(col("has_current_evidence") == False, "Missing")
            .when(col("days_until_expiry") < 30, "Expiring Soon")
            .otherwise("Current"))
        .withColumn("snapshot_date", current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Readiness Dashboard
# MAGIC Executive summary metrics for audit preparation

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{gold_schema}.audit_readiness_metrics",
    comment="Executive audit readiness metrics",
    table_properties={
        "quality": "gold"
    }
)
def audit_readiness_metrics():
    assessments = dlt.read(f"{catalog}.{silver_schema}.assessments")
    evidence = dlt.read(f"{catalog}.{silver_schema}.evidence")
    systems = dlt.read(f"{catalog}.{silver_schema}.systems")

    # Overall assessment metrics
    assessment_metrics = (
        assessments
        .agg(
            count("*").alias("total_assessments"),
            countDistinct("control_id").alias("unique_controls_assessed"),
            countDistinct("system_id").alias("systems_assessed"),
            avg("compliance_score").alias("overall_compliance_pct"),
            sum(when(col("implementation_status") == "Implemented", 1).otherwise(0)).alias("implemented_count"),
            sum(when(col("implementation_status") == "Partially Implemented", 1).otherwise(0)).alias("partial_count"),
            sum(when(col("implementation_status") == "Not Implemented", 1).otherwise(0)).alias("gap_count"),
            sum(when(col("is_overdue") == True, 1).otherwise(0)).alias("overdue_remediation_count")
        )
    )

    # Evidence metrics
    evidence_metrics = (
        evidence
        .agg(
            count("*").alias("total_evidence_items"),
            sum(when(col("status") == "Accepted", 1).otherwise(0)).alias("accepted_evidence"),
            sum(when(col("status") == "Pending Review", 1).otherwise(0)).alias("pending_review"),
            sum(when(col("is_expired") == True, 1).otherwise(0)).alias("expired_evidence"),
            countDistinct(when(col("is_current") == True, col("control_id"))).alias("controls_with_current_evidence")
        )
    )

    # Systems metrics
    systems_metrics = (
        systems.filter(col("in_soc2_scope") | col("in_iso_scope"))
        .agg(
            count("*").alias("systems_in_scope"),
            sum(when(col("assessment_overdue") == True, 1).otherwise(0)).alias("systems_assessment_overdue")
        )
    )

    return (
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Gold layer tables for compliance dashboards:
# MAGIC
# MAGIC | Table | Purpose | Key Metrics |
# MAGIC |-------|---------|-------------|
# MAGIC | control_compliance_summary | Family-level compliance | Compliance %, status counts |
# MAGIC | system_compliance_scorecard | Per-system posture | System compliance %, overdue items |
# MAGIC | cross_framework_mapping | NIST-SOC2 mapping | Mapping coverage, compliance by control |
# MAGIC | evidence_gap_analysis | Evidence coverage gaps | Missing/expiring evidence |
# MAGIC | audit_readiness_metrics | Executive dashboard | Overall readiness score |
