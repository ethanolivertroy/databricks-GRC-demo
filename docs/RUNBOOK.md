# Operations Runbook

## Deployment

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Compute cluster or SQL Warehouse
- Personal access token for API calls

### Initial Setup

1. **Clone Repository**
   ```
   Workspace > Repos > Add Repo
   URL: https://github.com/ethanolivertroy/databricks-GRC-demo
   ```

2. **Create Catalog Structure**
   ```
   Run: code/00_Setup/setup_grc_lakehouse.py
   ```
   Creates:
   - Catalog: `grc_compliance_dev`
   - Schemas: `00_landing`, `01_bronze`, `02_silver`, `03_gold`
   - Volumes for data landing

3. **Upload Data Files**
   ```bash
   # Using REST API
   curl -X PUT "https://<workspace>/api/2.0/fs/files/Volumes/grc_compliance_dev/00_landing/frameworks/nist_800_53_rev5_controls.csv" \
     -H "Authorization: Bearer <token>" \
     --data-binary @data/frameworks/nist_800_53_rev5_controls.csv

   # Repeat for all files in data/frameworks/ and data/mock/
   ```

4. **Run Pipeline**
   ```
   code/01_Bronze_Layer/load_bronze_tables.py
   code/02_Silver_Layer/transform_silver_tables.py
   code/03_Gold_Layer/create_gold_tables.py
   code/05_Machine_Learning/compliance_rules.py
   code/05_Machine_Learning/risk_prediction.py
   ```

5. **Import Dashboard**
   ```
   SQL > Dashboards > Import
   Upload: code/04_Consumption/Dashboard/GRC_Compliance_Trust_Center.lvdash.json
   ```

---

## Daily Operations

### Refresh Data
1. Upload new data files to Volumes
2. Run Bronze layer notebook
3. Run Silver layer notebook
4. Run Gold layer notebook
5. Run compliance rules (generates new alerts)
6. Dashboard auto-refreshes from Gold tables

### Check Alerts
```sql
SELECT * FROM grc_compliance_dev.03_gold.compliance_alerts
WHERE alert_date = current_date()
ORDER BY severity DESC
```

### Check High-Risk Controls
```sql
SELECT * FROM grc_compliance_dev.03_gold.control_risk_predictions
WHERE risk_level = 'High'
ORDER BY risk_score DESC
```

---

## Troubleshooting

### Issue: Bronze layer fails with PATH_NOT_FOUND

**Cause:** Data files not uploaded to Volumes

**Solution:**
1. Verify files exist in Volumes:
   ```python
   dbutils.fs.ls("/Volumes/grc_compliance_dev/00_landing/frameworks/")
   ```
2. Upload missing files via REST API or UI

### Issue: Silver layer fails with column not found

**Cause:** Schema mismatch between Bronze and expected schema

**Solution:**
1. Check Bronze table schema:
   ```sql
   DESCRIBE grc_compliance_dev.01_bronze.control_assessments
   ```
2. Verify CSV headers match expected columns
3. Re-upload corrected data files

### Issue: ML model registration fails

**Cause:** MLflow registry URI not set or permissions issue

**Solution:**
1. Verify MLflow is configured:
   ```python
   import mlflow
   mlflow.set_registry_uri("databricks-uc")
   ```
2. Check Unity Catalog permissions for model creation

### Issue: Dashboard shows no data

**Cause:** Gold tables empty or query errors

**Solution:**
1. Verify Gold tables have data:
   ```sql
   SELECT COUNT(*) FROM grc_compliance_dev.03_gold.system_compliance_scorecard
   ```
2. Check dashboard dataset queries for errors
3. Re-run Gold layer notebook

### Issue: Workspace file access forbidden

**Cause:** Security policy blocks local filesystem access from Spark

**Solution:**
Upload files directly to Volumes via REST API instead of using workspace files:
```bash
curl -X PUT "https://<workspace>/api/2.0/fs/files/Volumes/..." \
  -H "Authorization: Bearer <token>" \
  --data-binary @<local-file>
```

---

## Adding New Data

### Add New Control Framework

1. Create CSV with control data in `data/frameworks/`
2. Create mapping CSV (e.g., `nist_to_<framework>_mapping.csv`)
3. Upload to Volumes
4. Modify `load_bronze_tables.py` to load new framework
5. Modify Silver/Gold layers to process new framework

### Add New Systems

1. Add rows to `systems_inventory.csv`
2. Upload to Volumes
3. Re-run Bronze layer

### Add New Assessments

1. Add rows to `control_assessments.csv`
2. Upload to Volumes
3. Re-run Bronze, Silver, Gold layers

---

## Performance Tuning

### Large Datasets

For >100K assessments:
1. Enable Delta Lake auto-optimize:
   ```sql
   ALTER TABLE grc_compliance_dev.01_bronze.control_assessments
   SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)
   ```

2. Add partitioning by assessment_date:
   ```python
   df.write.partitionBy("assessment_date").saveAsTable(...)
   ```

3. Use incremental merge instead of full overwrite:
   ```python
   from delta.tables import DeltaTable
   target = DeltaTable.forName(spark, "...")
   target.merge(source, "target.id = source.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```

### Dashboard Performance

1. Use SQL Warehouse instead of cluster
2. Create materialized views for complex aggregations
3. Add indexes on frequently filtered columns

---

## Monitoring

### Key Metrics to Track

| Metric | Query | Alert Threshold |
|--------|-------|-----------------|
| Overall Compliance | `SELECT overall_compliance_pct FROM audit_readiness_metrics` | < 70% |
| Overdue Items | `SELECT COUNT(*) FROM assessments WHERE is_overdue` | > 50 |
| High-Risk Controls | `SELECT COUNT(*) FROM control_risk_predictions WHERE risk_level = 'High'` | > 100 |
| Expired Evidence | `SELECT COUNT(*) FROM evidence WHERE is_expired` | > 20 |

### Setting Up Alerts

Use Databricks SQL Alerts:
1. SQL > Alerts > Create Alert
2. Define query and threshold
3. Set notification channel (email, Slack)

---

## Backup & Recovery

### Backup Tables
Delta Lake maintains history by default. To restore:
```sql
RESTORE TABLE grc_compliance_dev.03_gold.compliance_alerts
TO VERSION AS OF <version_number>
```

### Export Data
```python
spark.table("grc_compliance_dev.03_gold.system_compliance_scorecard") \
  .write.mode("overwrite") \
  .csv("/Volumes/grc_compliance_dev/00_landing/exports/scorecard.csv")
```

---

## Contact

For issues with this demo project:
- GitHub Issues: https://github.com/ethanolivertroy/databricks-GRC-demo/issues
