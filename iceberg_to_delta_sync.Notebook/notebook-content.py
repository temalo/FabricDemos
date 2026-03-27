# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "3f04b076-176a-4eeb-877b-ae66d27c2c59",
# META       "known_warehouses": [
# META         {
# META           "id": "3f04b076-176a-4eeb-877b-ae66d27c2c59",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Iceberg → Delta Lake Sync
# Reads Iceberg tables from OneLake shortcut paths (Files section) and upserts them into Delta tables in the Lakehouse Tables section.
# 
# **Usage:**
# 1. Update `LAKEHOUSE_NAME` and `WORKSPACE_ID` in the Config cell
# 2. Update `ICEBERG_TABLE_CONFIG` with your shortcut paths and merge keys
# 3. Run all cells, or schedule via a Fabric Data Pipeline

# MARKDOWN ********************

# ## 1. Config — Update This Section

# CELL ********************

# ── REQUIRED: Auto-generated table mappings from Lakehouse Files ───────────────

WORKSPACE_ID   = "18c93f05-44a2-4a59-8a64-785ccdbd7725"       # Found in the URL of your Fabric workspace
LAKEHOUSE_NAME = "IndustrialSupply"      # Name of your Fabric Lakehouse

# ── TABLE CONFIG ───────────────────────────────────────────────────────────────
# Each entry defines one Iceberg shortcut → Delta table mapping.
#
# shortcut_path : path inside the Files section (relative to the Files root)
# delta_table   : name of the Delta table to create/upsert in the Tables section
# merge_keys    : list of columns that uniquely identify a row (used for upsert)
#                 Set to [] to do a full overwrite instead of a merge
#
ICEBERG_TABLE_CONFIG = [
    {"shortcut_path": "dim_customer", "delta_table":   "dim_customer", "merge_keys": []},
    {"shortcut_path": "dim_date", "delta_table":   "dim_date", "merge_keys": []},
    {"shortcut_path": "dim_employee", "delta_table":   "dim_employee", "merge_keys": []},
    {"shortcut_path": "dim_location", "delta_table":   "dim_location", "merge_keys": []},
    {"shortcut_path": "dim_product", "delta_table":   "dim_product", "merge_keys": []},
    {"shortcut_path": "dim_supplier", "delta_table":   "dim_supplier", "merge_keys": []},
    {"shortcut_path": "fact_inventory_snapshot", "delta_table":   "fact_inventory_snapshot", "merge_keys": []},
    {"shortcut_path": "fact_purchase_order", "delta_table":   "fact_purchase_order", "merge_keys": []},
    {"shortcut_path": "fact_returns", "delta_table":   "fact_returns", "merge_keys": []},
    {"shortcut_path": "fact_sales_order", "delta_table":   "fact_sales_order", "merge_keys": []},
    {"shortcut_path": "mart_customer_360", "delta_table":   "mart_customer_360", "merge_keys": []},
    {"shortcut_path": "mart_product_velocity", "delta_table":   "mart_product_velocity", "merge_keys": []},
    {"shortcut_path": "mart_sales_summary_daily", "delta_table":   "mart_sales_summary_daily", "merge_keys": []},
]

# ── OPTIONAL SETTINGS ──────────────────────────────────────────────────────────
WRITE_MODE        = "merge"     # "merge" | "overwrite" — default for tables with merge_keys
PARTITION_COLUMNS = []          # e.g. ["year", "month"] — applies to newly created tables only
LOG_LEVEL         = "INFO"      # "INFO" | "DEBUG"

print("Config loaded. Tables to sync:", [t["delta_table"] for t in ICEBERG_TABLE_CONFIG])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Imports & Setup

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import traceback

# Base OneLake ABFSS path for this lakehouse
ONELAKE_BASE = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_NAME}.Lakehouse"
)

FILES_ROOT  = f"{ONELAKE_BASE}/Files"
TABLES_ROOT = f"{ONELAKE_BASE}/Tables"

def log(msg: str, level: str = "INFO"):
    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")

log(f"OneLake base path: {ONELAKE_BASE}")
log(f"Spark version: {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Core Functions

# CELL ********************

def read_iceberg(shortcut_path: str) -> DataFrame:
    """Read an Iceberg table from a OneLake Files shortcut path."""
    full_path = f"{FILES_ROOT}/{shortcut_path}"
    log(f"Reading Iceberg from: {full_path}", "DEBUG")
    try:
        df = spark.read.format("iceberg").load(full_path)
        log(f"  Read {df.count():,} rows | {len(df.columns)} columns")
        return df
    except Exception as e:
        # Fallback: try reading raw Parquet if Iceberg metadata isn't recognised
        log(f"  Iceberg read failed, falling back to Parquet: {e}", "DEBUG")
        df = spark.read.format("parquet").load(f"{full_path}/data")
        log(f"  Parquet fallback — read {df.count():,} rows | {len(df.columns)} columns")
        return df


def table_exists(table_name: str) -> bool:
    """Check whether a Delta table already exists in the Lakehouse."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except AnalysisException:
        return False


def create_delta_table(df: DataFrame, table_name: str, partition_cols: list):
    """Create a new Delta table (first-time load)."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_name)
    log(f"  Created new Delta table: {table_name}")


def merge_into_delta(df: DataFrame, table_name: str, merge_keys: list):
    """Upsert (merge) source data into an existing Delta table."""
    delta_table = DeltaTable.forName(spark, table_name)

    # Build the join condition from merge keys
    join_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    (
        delta_table.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log(f"  Merged into: {table_name} (keys: {merge_keys})")


def overwrite_delta(df: DataFrame, table_name: str):
    """Full overwrite of a Delta table."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    log(f"  Overwritten: {table_name}")


def sync_table(config: dict) -> dict:
    """
    Sync one Iceberg shortcut to a Delta table.
    Returns a result dict for the summary report.
    """
    shortcut_path = config["shortcut_path"]
    delta_table   = config["delta_table"]
    merge_keys    = config.get("merge_keys", [])

    result = {
        "table":   delta_table,
        "status":  "success",
        "action":  None,
        "rows":    None,
        "error":   None,
    }

    log(f"\n{'='*60}")
    log(f"Syncing: {shortcut_path} → {delta_table}")

    try:
        df = read_iceberg(shortcut_path)
        result["rows"] = df.count()

        exists = table_exists(delta_table)

        if not exists:
            # First run — create the table
            create_delta_table(df, delta_table, PARTITION_COLUMNS)
            result["action"] = "created"

        elif not merge_keys:
            # No merge keys defined — full overwrite
            overwrite_delta(df, delta_table)
            result["action"] = "overwritten"

        else:
            # Merge (upsert) into existing table
            merge_into_delta(df, delta_table, merge_keys)
            result["action"] = "merged"

    except Exception as e:
        result["status"] = "failed"
        result["error"]  = str(e)
        log(f"  ERROR: {e}", "DEBUG")
        traceback.print_exc()

    return result

log("Functions defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Run the Sync

# CELL ********************

run_start = datetime.now()
log(f"Starting sync run at {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
log(f"Tables to process: {len(ICEBERG_TABLE_CONFIG)}")

results = []
for config in ICEBERG_TABLE_CONFIG:
    result = sync_table(config)
    results.append(result)

run_end = datetime.now()
elapsed = (run_end - run_start).total_seconds()
log(f"\nSync complete in {elapsed:.1f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Summary Report

# CELL ********************

print("\n" + "="*65)
print(f"{'SYNC SUMMARY':^65}")
print("="*65)
print(f"{'Table':<25} {'Status':<10} {'Action':<12} {'Rows':>10}")
print("-"*65)

success_count = 0
fail_count    = 0

for r in results:
    rows_str = f"{r['rows']:,}" if r['rows'] is not None else "—"
    print(f"{r['table']:<25} {r['status']:<10} {str(r['action']):<12} {rows_str:>10}")
    if r['status'] == 'success':
        success_count += 1
    else:
        fail_count += 1
        print(f"  └─ Error: {r['error']}")

print("-"*65)
print(f"Total: {len(results)} | Success: {success_count} | Failed: {fail_count}")
print(f"Duration: {elapsed:.1f}s")
print("="*65)

# Raise an exception if any table failed — this will mark a pipeline run as failed
if fail_count > 0:
    failed_tables = [r['table'] for r in results if r['status'] == 'failed']
    raise RuntimeError(f"Sync failed for tables: {failed_tables}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Optional — Inspect a Table After Sync

# CELL ********************

# Change this to any table name from your config to preview it
PREVIEW_TABLE = ICEBERG_TABLE_CONFIG[0]["delta_table"]

print(f"Preview: {PREVIEW_TABLE}")
spark.sql(f"SELECT * FROM {PREVIEW_TABLE} LIMIT 10").display()

print(f"\nDelta history: {PREVIEW_TABLE}")
spark.sql(f"DESCRIBE HISTORY {PREVIEW_TABLE}").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
