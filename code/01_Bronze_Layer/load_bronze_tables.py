# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Lakehouse - Bronze Layer
# MAGIC
# MAGIC Load raw data into Bronze tables from Unity Catalog Volumes.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, row_number, concat, lpad
from pyspark.sql.window import Window

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
    FRAMEWORK_FILES,
    FRAMEWORK_IDS,
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
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['nist_controls']}")
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
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['soc2_criteria']}")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("AICPA"))
)

df_soc2.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.soc2_trust_criteria")
print(f"✓ Loaded {df_soc2.count()} SOC2 criteria")

# COMMAND ----------

# ISO 27001 Controls (simplified catalog)
df_iso = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['iso_controls']}")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("ISO_27001"))
)

df_iso.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.iso_27001_controls")
print(f"✓ Loaded {df_iso.count()} ISO 27001 controls")

# COMMAND ----------

# PCI-DSS v4 Controls (simplified catalog)
df_pci = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['pci_controls']}")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("PCI_DSS_V4"))
)

df_pci.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.pci_dss_v4_controls")
print(f"✓ Loaded {df_pci.count()} PCI-DSS controls")

# COMMAND ----------

# Control Mappings (NIST → SOC2 / ISO / PCI)
df_soc2_mapping = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['nist_soc2_mapping']}")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

# NIST → ISO mapping file uses a compact schema; normalize to generic mapping columns.
df_iso_mapping_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['nist_iso_mapping']}")
)
iso_window = Window.orderBy("nist_control_id", "iso_control_id")
df_iso_mapping = (
    df_iso_mapping_raw
    .withColumn(
        "mapping_id",
        concat(lit("MAPISO"), lpad(row_number().over(iso_window).cast("string"), 3, "0")),
    )
    .withColumn("source_framework", lit(FRAMEWORK_IDS["nist"]))
    .withColumnRenamed("nist_control_id", "source_control_id")
    .withColumn("target_framework", lit(FRAMEWORK_IDS["iso"]))
    .withColumnRenamed("iso_control_id", "target_control_id")
    .withColumn("mapping_type", lit("related"))
    .withColumnRenamed("mapping_rationale", "mapping_notes")
    .select(
        "mapping_id",
        "source_framework",
        "source_control_id",
        "target_framework",
        "target_control_id",
        "mapping_type",
        "mapping_notes",
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_pci_mapping = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{frameworks_path}/{FRAMEWORK_FILES['nist_pci_mapping']}")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_mapping = (
    df_soc2_mapping
    .select(
        "mapping_id",
        "source_framework",
        "source_control_id",
        "target_framework",
        "target_control_id",
        "mapping_type",
        "mapping_notes",
        "_ingestion_timestamp",
    )
    .unionByName(df_iso_mapping)
    .unionByName(df_pci_mapping)
)

df_mapping.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE}.control_mapping")
print(f"✓ Loaded {df_mapping.count()} control mappings across SOC2/ISO/PCI")

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
