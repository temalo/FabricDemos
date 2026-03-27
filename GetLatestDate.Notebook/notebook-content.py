# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "27593f2c-5eaf-4ae7-90d5-e51949c286db",
# META       "default_lakehouse_name": "ChicagoPublicSafety",
# META       "default_lakehouse_workspace_id": "18c93f05-44a2-4a59-8a64-785ccdbd7725",
# META       "known_lakehouses": [
# META         {
# META           "id": "27593f2c-5eaf-4ae7-90d5-e51949c286db"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# This is a parameter cell to set the default date

maxDate = '2001-01-01'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This code retrieves the last date from the ChicagoReports table and uses that to set the MaxDate exit variable
# If there are no records in the table, it sets a default date of 2001-01-01 (which is the earliest that Chicago has on file)
from pyspark.sql.functions import coalesce,lit
PoliceReports = spark.sql("SELECT MAX(DateOnly) AS Date FROM ChicagoPublicSafety.ChicagoReports")
PoliceReports = PoliceReports.withColumn("MaxDate", coalesce(PoliceReports.Date,lit("2001-01-01")))
maxDate = PoliceReports.select('MaxDate').collect()[0][0]
mssparkutils.notebook.exit(maxDate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
