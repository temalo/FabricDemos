# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9614ed5e-db9b-452e-9345-2eae070eed91",
# META       "default_lakehouse_name": "ChicagoPublicSafety",
# META       "default_lakehouse_workspace_id": "b231a8b4-2076-4e0b-87eb-8fab9887e3d7",
# META       "known_lakehouses": [
# META         {
# META           "id": "9614ed5e-db9b-452e-9345-2eae070eed91"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

maxDate = "2001-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This code retrieves the last date from the ChicagoReports table and uses that to set the MaxDate exit variable
# If there are no records in the table, it sets a default date of 2001-01-01 (which is the earliest that Chicago has on file)
from pyspark.sql.functions import coalesce,lit
PoliceReports = spark.sql("SELECT MAX(DateOnly) AS Date FROM ChicagoPublicSafety.chicagoreports")
PoliceReports = PoliceReports.withColumn("MaxDate", coalesce(PoliceReports.Date,lit("2001-01-01")))
maxDate = PoliceReports.select('MaxDate').collect()[0][0]
mssparkutils.notebook.exit(maxDate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
