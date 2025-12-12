# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Lakehouse - Bronze Layer
# MAGIC
# MAGIC Load raw data into Bronze tables from Unity Catalog Volumes.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

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
    BRONZE_SCHEMA,
    FRAMEWORKS_PATH,
    SYSTEMS_PATH,
    ASSESSMENTS_PATH,
    EVIDENCE_PATH,
)

BRONZE = BRONZE_SCHEMA

spark.sql(f"USE CATALOG {CATALOG}")

# Data paths in Volumes (uploaded via API)
frameworks_path = FRAMEWORKS_PATH
systems_path = SYSTEMS_PATH
assessments_path = ASSESSMENTS_PATH
evidence_path = EVIDENCE_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Framework Data

# COMMAND ----------

# NIST 800-53 Controls
df_nist = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/nist_800_53_rev5_controls.csv")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("NIST_CSRC"))
)

df_nist.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.nist_800_53_controls")
print(f"✓ Loaded {df_nist.count()} NIST controls")

# COMMAND ----------

# SOC2 Trust Service Criteria
df_soc2 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/soc2_tsc_2017.csv")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("AICPA"))
)

df_soc2.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.soc2_trust_criteria")
print(f"✓ Loaded {df_soc2.count()} SOC2 criteria")

# COMMAND ----------

# Control Mapping (NIST to SOC2)
df_mapping = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/nist_to_soc2_mapping.csv")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_mapping.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.control_mapping")
print(f"✓ Loaded {df_mapping.count()} control mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Operational Data

# COMMAND ----------

# Systems Inventory
df_systems = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{systems_path}/systems_inventory.csv")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_systems.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.systems_inventory")
print(f"✓ Loaded {df_systems.count()} systems")

# COMMAND ----------

# Control Assessments
df_assessments = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{assessments_path}/control_assessments.csv")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_assessments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.control_assessments")
print(f"✓ Loaded {df_assessments.count()} assessments")

# COMMAND ----------

# Evidence Records
df_evidence = (
    spark.read
    .option("multiline", "false")
    .json(f"{evidence_path}/evidence_records.json")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_evidence.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.evidence_records")
print(f"✓ Loaded {df_evidence.count()} evidence records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN grc_compliance_dev.01_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Complete!
# MAGIC
# MAGIC Next: Run `02_Silver_Layer/transform_silver_tables`
