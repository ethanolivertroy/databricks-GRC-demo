# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Bronze to Silver Transformation
# MAGIC
# MAGIC This notebook transforms raw Bronze data into cleaned, validated Silver tables:
# MAGIC - Data type conversions
# MAGIC - Data quality validations
# MAGIC - Computed columns for analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, to_timestamp, upper, trim, initcap,
    regexp_extract, when, datediff, current_date, current_timestamp,
    split, concat, lit
)

catalog = "grc_compliance_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean NIST 800-53 Controls

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{silver_schema}.nist_controls",
    comment="Cleaned and enriched NIST 800-53 Rev 5 controls",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_control_id": "control_id IS NOT NULL AND control_id RLIKE '^[A-Z]{2}-[0-9]+'",
    "has_description": "control_description IS NOT NULL"
})
def nist_controls():
    return (
        dlt.read(f"{catalog}.{bronze_schema}.nist_800_53_controls")
        .withColumn("control_id", upper(trim(col("control_id"))))
        .withColumn("control_family_code", regexp_extract(col("control_id"), "^([A-Z]+)-", 1))
        .withColumn("control_number", regexp_extract(col("control_id"), "^[A-Z]+-([0-9]+)", 1).cast("int"))
        .withColumn("is_enhancement", col("control_id").contains("("))
        .withColumn("baseline_low", col("baseline_low") == "TRUE")
        .withColumn("baseline_moderate", col("baseline_moderate") == "TRUE")
        .withColumn("baseline_high", col("baseline_high") == "TRUE")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean SOC2 Trust Service Criteria

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{silver_schema}.soc2_criteria",
    comment="Cleaned SOC2 Trust Service Criteria",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect("valid_tsc_id", "tsc_id IS NOT NULL")
def soc2_criteria():
    return (
        dlt.read(f"{catalog}.{bronze_schema}.soc2_trust_criteria")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Systems Inventory

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{silver_schema}.systems",
    comment="Cleaned systems inventory with calculated fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_system_id": "system_id IS NOT NULL",
    "valid_classification": "data_classification IN ('Public', 'Internal', 'Confidential', 'Restricted')",
    "valid_criticality": "criticality IN ('Critical', 'High', 'Medium', 'Low')"
})
def systems():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.systems_inventory")
        .withColumn("criticality", initcap(trim(col("criticality"))))
        .withColumn("data_classification", initcap(trim(col("data_classification"))))
        .withColumn("certification_scope", upper(trim(col("certification_scope"))))
        .withColumn("last_assessment_date", to_date(col("last_assessment_date"), "yyyy-MM-dd"))
        .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd"))
        .withColumn("days_since_assessment",
            datediff(current_date(), col("last_assessment_date")))
        .withColumn("assessment_overdue",
            when(col("days_since_assessment") > 365, True).otherwise(False))
        .withColumn("in_soc2_scope",
            col("certification_scope").isin("SOC2", "BOTH"))
        .withColumn("in_iso_scope",
            col("certification_scope").isin("ISO27001", "BOTH"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Control Assessments

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{silver_schema}.assessments",
    comment="Validated control assessment results",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_assessment_id": "assessment_id IS NOT NULL",
    "valid_status": "implementation_status IN ('Implemented', 'Partially Implemented', 'Not Implemented', 'Not Applicable')",
    "valid_assessment_date": "assessment_date IS NOT NULL"
})
def assessments():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.control_assessments")
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
            .when(col("implementation_status") == "Not Applicable", None)
            .otherwise(0))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Evidence Records

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{silver_schema}.evidence",
    comment="Validated evidence records with expiration tracking",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_evidence_id": "evidence_id IS NOT NULL",
    "has_control_reference": "control_id IS NOT NULL",
    "valid_dates": "assessment_period_start <= assessment_period_end"
})
def evidence():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.evidence_records")
        .withColumn("upload_timestamp", to_timestamp(col("upload_timestamp")))
        .withColumn("review_timestamp", to_timestamp(col("review_timestamp")))
        .withColumn("assessment_period_start", to_date(col("assessment_period_start"), "yyyy-MM-dd"))
        .withColumn("assessment_period_end", to_date(col("assessment_period_end"), "yyyy-MM-dd"))
        .withColumn("control_id", upper(trim(col("control_id"))))
        .withColumn("is_expired",
            col("assessment_period_end") < current_date())
        .withColumn("days_until_expiry",
            datediff(col("assessment_period_end"), current_date()))
        .withColumn("is_current",
            (col("assessment_period_start") <= current_date()) &
            (col("assessment_period_end") >= current_date()))
        .withColumn("days_to_review",
            when(col("status") == "Pending Review",
                datediff(current_date(), to_date(col("upload_timestamp")))
            ).otherwise(None))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Silver layer tables created with:
# MAGIC - Data quality expectations enforced
# MAGIC - Computed columns for analysis
# MAGIC - Standardized formats and values
# MAGIC
# MAGIC | Table | Key Validations | Key Computed Fields |
# MAGIC |-------|-----------------|---------------------|
# MAGIC | nist_controls | Valid control ID format | control_family_code, is_enhancement |
# MAGIC | soc2_criteria | Valid TSC ID | category_code, category_full |
# MAGIC | systems | Valid classification/criticality | days_since_assessment, in_soc2_scope |
# MAGIC | assessments | Valid implementation status | is_overdue, compliance_score |
# MAGIC | evidence | Valid dates, has control ref | is_expired, days_until_expiry |
