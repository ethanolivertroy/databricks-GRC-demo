# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Operational Data Ingestion - Bronze Layer
# MAGIC
# MAGIC This notebook loads operational compliance data into the Bronze layer:
# MAGIC - Systems inventory (from CMDB/ServiceNow)
# MAGIC - Control assessments (from GRC tool/audits)
# MAGIC - Evidence records (streaming uploads)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

catalog = "grc_compliance_dev"
bronze_schema = "01_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Systems Inventory
# MAGIC Simulates CMDB/ServiceNow export of systems in certification scope

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{bronze_schema}.systems_inventory",
    comment="Systems and assets in compliance certification scope",
    table_properties={
        "quality": "bronze",
        "source": "CMDB Export"
    }
)
def systems_inventory():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/grc_compliance_dev/_schemas/systems")
        .option("header", "true")
        .load("/Volumes/grc_compliance_dev/00_landing/systems/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Control Assessments
# MAGIC Assessment results from internal audits, self-assessments, and external audits

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{bronze_schema}.control_assessments",
    comment="Control assessment results and findings",
    table_properties={
        "quality": "bronze",
        "source": "GRC Tool Export"
    }
)
def control_assessments():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/grc_compliance_dev/_schemas/assessments")
        .option("header", "true")
        .load("/Volumes/grc_compliance_dev/00_landing/assessments/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Evidence Records (Streaming)
# MAGIC Evidence uploads from system owners and compliance team

# COMMAND ----------

# Define schema for evidence JSON records
evidence_schema = StructType([
    StructField("evidence_id", StringType(), False),
    StructField("control_id", StringType(), False),
    StructField("system_id", StringType(), False),
    StructField("system_name", StringType(), True),
    StructField("evidence_type", StringType(), True),
    StructField("evidence_description", StringType(), True),
    StructField("evidence_path", StringType(), True),
    StructField("uploaded_by", StringType(), True),
    StructField("upload_timestamp", StringType(), True),
    StructField("assessment_period_start", StringType(), True),
    StructField("assessment_period_end", StringType(), True),
    StructField("status", StringType(), True),
    StructField("reviewer", StringType(), True),
    StructField("review_timestamp", StringType(), True),
    StructField("review_notes", StringType(), True)
])

@dlt.table(
    name=f"{catalog}.{bronze_schema}.evidence_records",
    comment="Compliance evidence uploads and documentation",
    table_properties={
        "quality": "bronze",
        "source": "Evidence Upload System"
    }
)
def evidence_records():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/grc_compliance_dev/_schemas/evidence")
        .schema(evidence_schema)
        .load("/Volumes/grc_compliance_dev/00_landing/evidence/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Operational data loaded to Bronze layer:
# MAGIC - `systems_inventory`: Systems in compliance scope from CMDB
# MAGIC - `control_assessments`: Assessment results and findings
# MAGIC - `evidence_records`: Evidence uploads (streaming)
