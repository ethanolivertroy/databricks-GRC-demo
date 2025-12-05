# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Framework Data Ingestion - Bronze Layer
# MAGIC
# MAGIC This notebook loads compliance framework data into the Bronze layer:
# MAGIC - NIST 800-53 Rev 5 Controls
# MAGIC - SOC2 Trust Service Criteria
# MAGIC - Cross-framework control mapping

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, input_file_name

catalog = "grc_compliance_dev"
bronze_schema = "01_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load NIST 800-53 Rev 5 Controls

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{bronze_schema}.nist_800_53_controls",
    comment="NIST 800-53 Rev 5 control catalog from NIST CSRC",
    table_properties={
        "quality": "bronze",
        "source": "NIST CSRC",
        "framework_version": "Rev 5"
    }
)
def nist_controls():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/grc_compliance_dev/00_landing/frameworks/nist_800_53_rev5_controls.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load SOC2 Trust Service Criteria

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{bronze_schema}.soc2_trust_criteria",
    comment="SOC2 2017 Trust Service Criteria from AICPA",
    table_properties={
        "quality": "bronze",
        "source": "AICPA",
        "framework_version": "2017"
    }
)
def soc2_criteria():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/grc_compliance_dev/00_landing/frameworks/soc2_tsc_2017.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Cross-Framework Control Mapping

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{bronze_schema}.control_mapping",
    comment="NIST 800-53 to SOC2 TSC control mapping crosswalk",
    table_properties={
        "quality": "bronze",
        "source": "AICPA-NIST Mapping"
    }
)
def control_mapping():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/grc_compliance_dev/00_landing/frameworks/nist_to_soc2_mapping.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Framework data loaded to Bronze layer:
# MAGIC - `nist_800_53_controls`: Complete NIST 800-53 Rev 5 control catalog
# MAGIC - `soc2_trust_criteria`: SOC2 Trust Service Criteria (2017)
# MAGIC - `control_mapping`: Cross-framework mapping between NIST and SOC2
