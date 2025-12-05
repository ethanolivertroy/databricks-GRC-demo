# Databricks notebook source
# MAGIC %md
# MAGIC # GRC Control Risk Prediction Model
# MAGIC
# MAGIC ML model to predict which controls are likely to fail next assessment.
# MAGIC Uses historical assessment patterns, evidence gaps, and system criticality.

# COMMAND ----------

# MAGIC %pip install mlflow scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from pyspark.sql.functions import col, when, lit, avg, count, max as spark_max, datediff, current_date
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

CATALOG = "grc_compliance_dev"
SILVER = "02_silver"
GOLD = "03_gold"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Training Data
# MAGIC
# MAGIC Features:
# MAGIC - days_since_assessment
# MAGIC - historical_compliance_score
# MAGIC - evidence_count
# MAGIC - evidence_gap_severity
# MAGIC - system_criticality
# MAGIC - control_family
# MAGIC
# MAGIC Target: is_at_risk (1 if Not Implemented or Partially Implemented, 0 if Implemented)

# COMMAND ----------

# Load and join data
df_assessments = spark.table(f"{CATALOG}.{SILVER}.assessments")
df_systems = spark.table(f"{CATALOG}.{SILVER}.systems")
df_evidence = spark.table(f"{CATALOG}.{SILVER}.evidence")
df_gaps = spark.table(f"{CATALOG}.{GOLD}.evidence_gap_analysis")

# Aggregate evidence per control/system
evidence_counts = (
    df_evidence
    .filter(col("status") == "Accepted")
    .groupBy("control_id", "system_id")
    .agg(count("*").alias("evidence_count"))
)

# Get gap severity per control/system
gap_severity = (
    df_gaps
    .select("control_id", "system_id", "gap_severity")
)

# COMMAND ----------

# Build training dataset
df_training = (
    df_assessments
    .join(df_systems.select("system_id", "criticality", "data_classification"), "system_id", "left")
    .join(evidence_counts, ["control_id", "system_id"], "left")
    .join(gap_severity, ["control_id", "system_id"], "left")
    .withColumn("evidence_count", when(col("evidence_count").isNull(), 0).otherwise(col("evidence_count")))
    .withColumn("gap_severity_score",
        when(col("gap_severity") == "Critical", 4)
        .when(col("gap_severity") == "High", 3)
        .when(col("gap_severity") == "Medium", 2)
        .when(col("gap_severity") == "Low", 1)
        .otherwise(0))
    .withColumn("criticality_score",
        when(col("criticality") == "Critical", 4)
        .when(col("criticality") == "High", 3)
        .when(col("criticality") == "Medium", 2)
        .otherwise(1))
    .withColumn("is_at_risk",
        when(col("implementation_status").isin("Not Implemented", "Partially Implemented"), 1.0)
        .otherwise(0.0))
    .select(
        "assessment_id",
        "control_id",
        "system_id",
        "control_family_code",
        col("compliance_score").alias("historical_compliance"),
        "days_to_remediation",
        "evidence_count",
        "gap_severity_score",
        "criticality_score",
        "is_at_risk"
    )
    .na.fill(0)
)

print(f"Training samples: {df_training.count()}")
display(df_training.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

# Index control family
family_indexer = StringIndexer(inputCol="control_family_code", outputCol="family_index", handleInvalid="keep")

# Assemble features
feature_cols = ["historical_compliance", "days_to_remediation", "evidence_count", "gap_severity_score", "criticality_score", "family_index"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Random Forest classifier
rf = RandomForestClassifier(
    labelCol="is_at_risk",
    featuresCol="features",
    numTrees=50,
    maxDepth=5,
    seed=42
)

# Build pipeline
pipeline = Pipeline(stages=[family_indexer, assembler, rf])

# COMMAND ----------

# Split data
train_df, test_df = df_training.randomSplit([0.8, 0.2], seed=42)

print(f"Train: {train_df.count()}, Test: {test_df.count()}")

# COMMAND ----------

# Train with MLflow tracking
mlflow.set_experiment(f"/Users/{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId', 'default')}/grc-risk-prediction")

with mlflow.start_run(run_name="risk_prediction_rf"):
    # Train model
    model = pipeline.fit(train_df)

    # Predict on test set
    predictions = model.transform(test_df)

    # Evaluate
    evaluator = BinaryClassificationEvaluator(labelCol="is_at_risk", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    # Log metrics
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_param("num_trees", 50)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("features", str(feature_cols))

    # Log model
    mlflow.spark.log_model(model, "model")

    print(f"Model AUC-ROC: {auc:.4f}")

    run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model to Unity Catalog

# COMMAND ----------

model_name = f"{CATALOG}.{GOLD}.control_risk_model"

# Register model
mlflow.set_registry_uri("databricks-uc")
model_uri = f"runs:/{run_id}/model"

try:
    registered = mlflow.register_model(model_uri, model_name)
    print(f"Registered model: {model_name} v{registered.version}")

    # Set alias
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    client.set_registered_model_alias(model_name, "prod", registered.version)
    print(f"Set alias 'prod' to version {registered.version}")
except Exception as e:
    print(f"Model registration skipped (may need ML Runtime): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Risk Predictions

# COMMAND ----------

# Score all current assessments
df_all = df_training.drop("is_at_risk")
df_scored = model.transform(df_all)

# Extract risk score
df_risk = (
    df_scored
    .select(
        "assessment_id",
        "control_id",
        "system_id",
        "control_family_code",
        col("probability").getItem(1).alias("risk_score"),
        col("prediction").alias("predicted_at_risk")
    )
    .withColumn("risk_level",
        when(col("risk_score") >= 0.7, "High")
        .when(col("risk_score") >= 0.4, "Medium")
        .otherwise("Low"))
    .withColumn("prediction_date", current_date())
)

# Write predictions
df_risk.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.control_risk_predictions")
print(f"Wrote {df_risk.count()} risk predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_level,
# MAGIC   COUNT(*) as control_count,
# MAGIC   ROUND(AVG(risk_score) * 100, 1) as avg_risk_pct
# MAGIC FROM grc_compliance_dev.03_gold.control_risk_predictions
# MAGIC GROUP BY risk_level
# MAGIC ORDER BY
# MAGIC   CASE risk_level WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High risk controls to prioritize
# MAGIC SELECT
# MAGIC   p.control_id,
# MAGIC   p.system_id,
# MAGIC   s.system_name,
# MAGIC   p.control_family_code,
# MAGIC   ROUND(p.risk_score * 100, 1) as risk_pct,
# MAGIC   p.risk_level
# MAGIC FROM grc_compliance_dev.03_gold.control_risk_predictions p
# MAGIC JOIN grc_compliance_dev.02_silver.systems s ON p.system_id = s.system_id
# MAGIC WHERE p.risk_level = 'High'
# MAGIC ORDER BY p.risk_score DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## ML Pipeline Complete
# MAGIC
# MAGIC - Model trained and logged to MLflow
# MAGIC - Predictions in `03_gold.control_risk_predictions`
# MAGIC - High-risk controls identified for proactive remediation
