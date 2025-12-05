# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Lakehouse Setup
# MAGIC
# MAGIC This notebook sets up the complete GRC Compliance Lakehouse:
# MAGIC 1. Creates Unity Catalog structure (catalog, schemas, volumes)
# MAGIC 2. Copies data files from the cloned repo to Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the catalog
# MAGIC CREATE CATALOG IF NOT EXISTS grc_compliance_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG grc_compliance_dev;
# MAGIC
# MAGIC -- Create schemas (medallion layers)
# MAGIC CREATE SCHEMA IF NOT EXISTS 00_landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS 01_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS 02_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS 03_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volumes for data storage
# MAGIC CREATE VOLUME IF NOT EXISTS grc_compliance_dev.00_landing.frameworks;
# MAGIC CREATE VOLUME IF NOT EXISTS grc_compliance_dev.00_landing.systems;
# MAGIC CREATE VOLUME IF NOT EXISTS grc_compliance_dev.00_landing.assessments;
# MAGIC CREATE VOLUME IF NOT EXISTS grc_compliance_dev.00_landing.evidence;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Copy Data from Repo to Volumes
# MAGIC
# MAGIC The data files are already in your workspace from the Git clone.
# MAGIC We'll copy them to Volumes for the pipeline to use.

# COMMAND ----------

import os

# Get the repo root path (this notebook is in code/00_Setup/)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Extract the repo root from the notebook path
repo_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])  # Go up 2 levels from 00_Setup

print(f"Repo root: {repo_root}")

# COMMAND ----------

# Define source and destination paths
CATALOG = "grc_compliance_dev"

# Source paths (from cloned repo in workspace)
source_frameworks = f"{repo_root}/data/frameworks"
source_mock = f"{repo_root}/data/mock"

# Destination paths (volumes)
dest_frameworks = f"/Volumes/{CATALOG}/00_landing/frameworks"
dest_systems = f"/Volumes/{CATALOG}/00_landing/systems"
dest_assessments = f"/Volumes/{CATALOG}/00_landing/assessments"
dest_evidence = f"/Volumes/{CATALOG}/00_landing/evidence"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy Framework Data

# COMMAND ----------

# Copy framework files
framework_files = [
    "nist_800_53_rev5_controls.csv",
    "soc2_tsc_2017.csv",
    "nist_to_soc2_mapping.csv"
]

for filename in framework_files:
    source = f"{source_frameworks}/{filename}"
    dest = f"{dest_frameworks}/{filename}"
    try:
        dbutils.fs.cp(f"file:{source}", dest)
        print(f"✓ Copied {filename}")
    except Exception as e:
        print(f"✗ Error copying {filename}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy Operational Data

# COMMAND ----------

# Copy systems inventory
try:
    dbutils.fs.cp(f"file:{source_mock}/systems_inventory.csv", f"{dest_systems}/systems_inventory.csv")
    print("✓ Copied systems_inventory.csv")
except Exception as e:
    print(f"✗ Error: {e}")

# Copy assessments
try:
    dbutils.fs.cp(f"file:{source_mock}/control_assessments.csv", f"{dest_assessments}/control_assessments.csv")
    print("✓ Copied control_assessments.csv")
except Exception as e:
    print(f"✗ Error: {e}")

# Copy evidence
try:
    dbutils.fs.cp(f"file:{source_mock}/evidence_records.json", f"{dest_evidence}/evidence_records.json")
    print("✓ Copied evidence_records.json")
except Exception as e:
    print(f"✗ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Setup

# COMMAND ----------

print("=== Volumes Created ===")
display(spark.sql("SHOW VOLUMES IN grc_compliance_dev.00_landing"))

# COMMAND ----------

print("=== Framework Data ===")
try:
    files = dbutils.fs.ls(dest_frameworks)
    for f in files:
        print(f"  {f.name} ({f.size} bytes)")
except:
    print("  (empty or not accessible)")

print("\n=== Systems Data ===")
try:
    files = dbutils.fs.ls(dest_systems)
    for f in files:
        print(f"  {f.name} ({f.size} bytes)")
except:
    print("  (empty or not accessible)")

print("\n=== Assessments Data ===")
try:
    files = dbutils.fs.ls(dest_assessments)
    for f in files:
        print(f"  {f.name} ({f.size} bytes)")
except:
    print("  (empty or not accessible)")

print("\n=== Evidence Data ===")
try:
    files = dbutils.fs.ls(dest_evidence)
    for f in files:
        print(f"  {f.name} ({f.size} bytes)")
except:
    print("  (empty or not accessible)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview Data

# COMMAND ----------

# Preview NIST controls
print("NIST 800-53 Controls:")
df_nist = spark.read.option("header", "true").csv(f"{dest_frameworks}/nist_800_53_rev5_controls.csv")
print(f"  {df_nist.count()} controls loaded")
display(df_nist.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!
# MAGIC
# MAGIC Next steps:
# MAGIC 1. Run `01_Bronze_Layer/load_bronze_tables.py`
# MAGIC 2. Run `02_Silver_Layer/transform_silver_tables.py`
# MAGIC 3. Run `03_Gold_Layer/create_gold_tables.py`
# MAGIC 4. Import the dashboard from `04_Consumption/Dashboard/`
