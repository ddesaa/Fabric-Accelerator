# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b79c000-057b-46f9-9c4e-6a9196b969cb",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "b8d12831-e5df-4a49-aba5-84e328c00ec0",
# META       "known_lakehouses": [
# META         {
# META           "id": "2b79c000-057b-46f9-9c4e-6a9196b969cb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Landing to bronze
# This notebook upsert an entity from the bronze layer. with a file from the landing zone

# PARAMETERS CELL ********************

## Parameters
par_source_folder='landing/Employee/2025/07/27/1211'                          ## Source folder
par_source_workspaceid='b8d12831-e5df-4a49-aba5-84e328c00ec0'                   ## Source Workspace ID
par_config_workspaceid='b8d12831-e5df-4a49-aba5-84e328c00ec0'                   ## Config Workspace ID 
par_config_folder='cfg/mapping'                                                 ## Folder mapping
par_LH_Source='LH_Bronze'                                                       ## Bronze Lakehouse 
par_LH_Config='LH_Bronze'                                                       ## Config Lakehouse
par_entity="Employee"                                                         ## Entity Name
par_file='Employee.csv'                                                       ## Source File
par_Header='True'                                                               ## first row Is header (True/False)
par_business_key="EmployeeID"                                                 ## Business Keys (separated with |)
par_target_table="Employee"                                                    ## Bronze Table Name
par_global_run_id ="0"                                                           ## Global Run ID 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common_delta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common_transform

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Mapping

# CELL ********************

## Mapping reading
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
data=[]
schema=None
mapping_file=f"{par_entity}_Map.json"
if file_exists(folder=par_config_folder,workspaceid=par_config_workspaceid,LHName=par_LH_Config,file=mapping_file):
    df= readFile(folder=par_config_folder,workspaceid=par_config_workspaceid,LHName=par_LH_Config,file=f"{par_entity}_Map.json")
    for row in df.collect():
        if row.data_type=="IntegerType":
            data.append(StructField(row.column_name,IntegerType(), False))
        if row.data_type=="StringType":
            data.append(StructField(row.column_name,StringType(), False))
        if row.data_type=="DateType":
            data.append(StructField(row.column_name,DateType(), False))
    schema = StructType(data)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


dfRaw= readFile(folder=par_source_folder,workspaceid=par_source_workspaceid,LHName=par_LH_Source,file=par_file,headerFlag=par_Header,schema=schema)

## Add common transform functions
dfCT=CommonTransforms(dfRaw)

# Remove duplicates
if par_business_key is not None:
    df=dfCT.deDuplicate(par_business_key.split("|"))
else:
    df=dfCT.deDuplicate()

df= df.withColumn("SYS_Run_ID",lit(par_global_run_id))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upsertDelta(df,par_target_table,par_business_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
