# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Lakehouse - Silver Layer
# MAGIC
# MAGIC Transform Bronze data into cleaned, validated Silver tables:
# MAGIC - Data type conversions
# MAGIC - Data quality validations
# MAGIC - Computed columns for analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, to_timestamp, upper, trim, initcap,
    regexp_extract, when, datediff, current_date, current_timestamp,
    lit
)

CATALOG = "grc_compliance_dev"
BRONZE = "01_bronze"
SILVER = "02_silver"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform NIST Controls

# COMMAND ----------

df_nist = spark.table(f"{CATALOG}.{BRONZE}.nist_800_53_controls")

df_nist_silver = (
    df_nist
    .withColumn("control_id", upper(trim(col("control_id"))))
    .withColumn("control_family_code", regexp_extract(col("control_id"), "^([A-Z]+)-", 1))
    .withColumn("control_number", regexp_extract(col("control_id"), "^[A-Z]+-([0-9]+)", 1).cast("int"))
    .withColumn("is_enhancement", col("control_id").contains("("))
    .withColumn("baseline_low", col("baseline_low") == "TRUE")
    .withColumn("baseline_moderate", col("baseline_moderate") == "TRUE")
    .withColumn("baseline_high", col("baseline_high") == "TRUE")
)

# Data quality check
invalid_controls = df_nist_silver.filter("control_id IS NULL OR NOT control_id RLIKE '^[A-Z]{2}-[0-9]+'").count()
print(f"Invalid control IDs: {invalid_controls}")

df_nist_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.nist_controls")
print(f"Created silver.nist_controls with {df_nist_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform SOC2 Criteria

# COMMAND ----------

df_soc2 = spark.table(f"{CATALOG}.{BRONZE}.soc2_trust_criteria")

df_soc2_silver = (
    df_soc2
    .withColumn("tsc_id", upper(trim(col("tsc_id"))))
    .withColumn("category_code", regexp_extract(col("tsc_id"), "^([A-Z]+)", 1))
    .withColumn("category_full",
        when(col("category_code") == "CC", "Common Criteria")
        .when(col("category_code") == "A", "Availability")
        .when(col("category_code") == "C", "Confidentiality")
        .when(col("category_code") == "PI", "Processing Integrity")
        .when(col("category_code") == "P", "Privacy")
        .otherwise(col("tsc_category"))
    )
)

df_soc2_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.soc2_criteria")
print(f"Created silver.soc2_criteria with {df_soc2_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Systems Inventory

# COMMAND ----------

df_systems = spark.table(f"{CATALOG}.{BRONZE}.systems_inventory")

df_systems_silver = (
    df_systems
    .withColumn("criticality", initcap(trim(col("criticality"))))
    .withColumn("data_classification", initcap(trim(col("data_classification"))))
    .withColumn("certification_scope", upper(trim(col("certification_scope"))))
    .withColumn("last_assessment_date", to_date(col("last_assessment_date"), "yyyy-MM-dd"))
    .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd"))
    .withColumn("days_since_assessment", datediff(current_date(), col("last_assessment_date")))
    .withColumn("assessment_overdue", when(col("days_since_assessment") > 365, True).otherwise(False))
    .withColumn("in_soc2_scope", col("certification_scope").isin("SOC2", "BOTH"))
    .withColumn("in_iso_scope", col("certification_scope").isin("ISO27001", "BOTH"))
)

# Data quality check
invalid_systems = df_systems_silver.filter("system_id IS NULL").count()
print(f"Invalid systems (no ID): {invalid_systems}")

df_systems_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.systems")
print(f"Created silver.systems with {df_systems_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Assessments

# COMMAND ----------

df_assessments = spark.table(f"{CATALOG}.{BRONZE}.control_assessments")

df_assessments_silver = (
    df_assessments
    .withColumn("assessment_date", to_date(col("assessment_date"), "yyyy-MM-dd"))
    .withColumn("remediation_due_date", to_date(col("remediation_due_date"), "yyyy-MM-dd"))
    .withColumn("control_id", upper(trim(col("control_id"))))
    .withColumn("control_family_code", regexp_extract(col("control_id"), "^([A-Z]+)-", 1))
    .withColumn("implementation_status", initcap(trim(col("implementation_status"))))
    .withColumn("is_overdue",
        when(
            (col("implementation_status") != "Implemented") &
            (col("implementation_status") != "Not Applicable") &
            (col("remediation_due_date") < current_date()),
            True
        ).otherwise(False))
    .withColumn("days_to_remediation",
        when(col("remediation_due_date").isNotNull(),
            datediff(col("remediation_due_date"), current_date())
        ).otherwise(None))
    .withColumn("compliance_score",
        when(col("implementation_status") == "Implemented", 100)
        .when(col("implementation_status") == "Partially Implemented", 50)
        .when(col("implementation_status") == "Not Applicable", lit(None).cast("int"))
        .otherwise(0))
)

# Data quality check
valid_statuses = ["Implemented", "Partially Implemented", "Not Implemented", "Not Applicable"]
invalid_status = df_assessments_silver.filter(~col("implementation_status").isin(valid_statuses)).count()
print(f"Invalid implementation statuses: {invalid_status}")

df_assessments_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.assessments")
print(f"Created silver.assessments with {df_assessments_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Evidence Records

# COMMAND ----------

df_evidence = spark.table(f"{CATALOG}.{BRONZE}.evidence_records")

df_evidence_silver = (
    df_evidence
    .withColumn("upload_timestamp", to_timestamp(col("upload_timestamp")))
    .withColumn("review_timestamp", to_timestamp(col("review_timestamp")))
    .withColumn("assessment_period_start", to_date(col("assessment_period_start"), "yyyy-MM-dd"))
    .withColumn("assessment_period_end", to_date(col("assessment_period_end"), "yyyy-MM-dd"))
    .withColumn("control_id", upper(trim(col("control_id"))))
    .withColumn("is_expired", col("assessment_period_end") < current_date())
    .withColumn("days_until_expiry", datediff(col("assessment_period_end"), current_date()))
    .withColumn("is_current",
        (col("assessment_period_start") <= current_date()) &
        (col("assessment_period_end") >= current_date()))
    .withColumn("days_pending_review",
        when(col("status") == "Pending Review",
            datediff(current_date(), to_date(col("upload_timestamp")))
        ).otherwise(None))
)

# Data quality check
invalid_dates = df_evidence_silver.filter("assessment_period_start > assessment_period_end").count()
print(f"Invalid date ranges: {invalid_dates}")

df_evidence_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.evidence")
print(f"Created silver.evidence with {df_evidence_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN grc_compliance_dev.02_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Summary statistics
print("=== Silver Layer Summary ===\n")

for table in ["nist_controls", "soc2_criteria", "systems", "assessments", "evidence"]:
    count = spark.table(f"{CATALOG}.{SILVER}.{table}").count()
    print(f"{table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Complete!
# MAGIC
# MAGIC Transformations applied:
# MAGIC - Standardized IDs (uppercase, trimmed)
# MAGIC - Computed columns (compliance_score, is_overdue, days_since_assessment)
# MAGIC - Data quality validations logged
# MAGIC - Type conversions (dates, timestamps)
