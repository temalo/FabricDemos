# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f346f4f6-d6df-49b7-8089-421e1f94ca35",
# META       "default_lakehouse_name": "IndustrialSupply",
# META       "default_lakehouse_workspace_id": "18c93f05-44a2-4a59-8a64-785ccdbd7725",
# META       "known_lakehouses": [
# META         {
# META           "id": "f346f4f6-d6df-49b7-8089-421e1f94ca35"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Lakehouse Table/Files Sync Process with Delta Merge (no overwrite)
import os
import json
import re
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# Manifest Table name
MANIFEST_TABLE = "sync_manifest"
LAKEHOUSE_SCHEMA = "dbo"
FILES_ROOT = "Files"  # where table folders are stored

# Compile regex to extract primary key from table name
key_pattern = re.compile(r"(?:dim|fact|mart)_([a-z0-9]+)")

def get_primary_key(table):
    match = key_pattern.match(table)
    if match:
        return f"{match.group(1)}_key"
    else:
        # Fallback: try to find *_key column, or default to 'id'
        return "id"

# Load previous manifest, if it exists
try:
    manifest_df = spark.read.table(f"{LAKEHOUSE_SCHEMA}.{MANIFEST_TABLE}")
    manifest = {row['folder']: json.loads(row['files']) for row in manifest_df.collect()}
except Exception:
    manifest = {}

current_state = {}
folders = notebookutils.fs.ls(FILES_ROOT)
for folder in folders:
    if not folder.isDir:  # Only process directories
        continue
    folder_path = os.path.join(FILES_ROOT, folder.name)
    files = notebookutils.fs.ls(folder_path)
    file_info = []
    for f in files:
        if f.isDir:
            continue
        file_info.append({
            'name': f.name,
            'size': f.size,
            'last_modified': f.modificationTime  # epoch ms
        })
    # Sort for consistent comparison
    file_info.sort(key=lambda x: x['name'])
    current_state[folder.name] = file_info

# Sync logic: use merge instead of overwrite
updated = False
for table_folder, files in current_state.items():
    manifest_files = manifest.get(table_folder, [])
    # Compare by file metadata (name, size, last_modified)
    if json.dumps(files) != json.dumps(manifest_files):
        if not files:
            continue  # Skip empty folders
        # Accept Parquet and CSV; extend easily
        suffixes = set(f['name'].split('.')[-1].lower() for f in files if '.' in f['name'])
        file_paths = [os.path.join(FILES_ROOT, table_folder, f['name']) for f in files]
        if 'parquet' in suffixes:
            df = spark.read.parquet(*file_paths)
        elif 'csv' in suffixes:
            df = spark.read.option("header", True).csv(*file_paths)
        else:
            print(f"Unsupported file types in {table_folder}, skipping.")
            continue
        # Get primary key from table name
        pk = get_primary_key(table_folder)
        delta_table = f'{LAKEHOUSE_SCHEMA}.{table_folder}'
        print(f"MERGE into table '{table_folder}' on key '{pk}' from new/changed files...", flush=True)
        # Create temp view
        temp_view = f"staging_merge_{table_folder}"
        df.createOrReplaceTempView(temp_view)
        # Delta merge SQL: update if match, insert if not (no delete here)
        merge_sql = f"""
        MERGE INTO {delta_table} AS target
        USING {temp_view} AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
        # Update manifest
        manifest[table_folder] = files
        updated = True
    else:
        print(f"No change in '{table_folder}'.")

# Drop tables for deleted folders (optional, comment if you want to retain tables)
table_names = [t.name for t in spark.catalog.listTables(LAKEHOUSE_SCHEMA)]
for tab in table_names:
    if tab == MANIFEST_TABLE:
        continue
    if tab not in current_state:
        print(f"Dropping table '{tab}' as its folder was removed...")
        spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_SCHEMA}.{tab}")
        manifest.pop(tab, None)
        updated = True

# Save manifest as a hidden (non-user) table
schema = StructType([
    StructField("folder", StringType()),
    StructField("files", StringType())
])
rows = [(folder, json.dumps(files)) for folder, files in manifest.items()]
manifest_sync_df = spark.createDataFrame(rows, schema)
manifest_sync_df.write.mode('overwrite').saveAsTable(f'{LAKEHOUSE_SCHEMA}.{MANIFEST_TABLE}')

if updated:
    print("Lakehouse tables Delta-synced successfully.")
else:
    print("Lakehouse tables already up to date.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
