# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6eb3eb5-f425-492a-9965-c387d55d629c",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "b8d12831-e5df-4a49-aba5-84e328c00ec0",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6eb3eb5-f425-492a-9965-c387d55d629c"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************


par_process_full ="Yes"
par_run_ID ="005b5b7e-3fc8-41e4-8303-5cebd928eb73"


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

from pyspark.sql.functions import lit
source_query = """select 
    DateID, DepartmentID, ShiftID, 
    count(EmployeeID) Employee ,  count(SalesPersonID) SalesPerson  
from dbo.Headcount
group by
    DateID, DepartmentID, ShiftID   
    """
    
df=spark.sql(source_query)

df= df.withColumn("SYS_Run_ID",lit(par_run_ID))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upsertDelta(df,"Headcount_by_Month","DateID|DepartmentID|ShiftID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
