# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "91b87b80-a5dd-472f-8f46-55f966f037e7",
# META       "default_lakehouse_name": "FabricLakehouse",
# META       "default_lakehouse_workspace_id": "18c93f05-44a2-4a59-8a64-785ccdbd7725",
# META       "known_lakehouses": [
# META         {
# META           "id": "91b87b80-a5dd-472f-8f46-55f966f037e7"
# META         },
# META         {
# META           "id": "27593f2c-5eaf-4ae7-90d5-e51949c286db"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# This is a parameter cell to set the application token if it is not found in the KeyVault. This is a generic token
appToken="WxJpjZeMUc8upPyRMz1Zdz28j"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This code extracts the app token from Azure KeyVault

from trident_token_library_wrapper import PyTridentTokenLibrary as tl
access_token = mssparkutils.credentials.getToken("keyvault")
appToken=tl.get_secret_with_token("https://tmAKV.vault.azure.net/","ChicagoCrimeAppToken",access_token)
mssparkutils.notebook.exit(appToken)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
