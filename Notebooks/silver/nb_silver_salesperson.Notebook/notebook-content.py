# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1cbc6a84-c5f0-4c9a-8219-aea1d16d2fe5",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "b8d12831-e5df-4a49-aba5-84e328c00ec0",
# META       "known_lakehouses": [
# META         {
# META           "id": "1cbc6a84-c5f0-4c9a-8219-aea1d16d2fe5"
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
    sp.BusinessEntityID SalesPersonID,
    concat(p.LastName,', ', p.FirstName ) SalesPerson,
    coalesce(st.Name,'without Territory') as SalesTerritory,
    coalesce(st.Group,'without Group') as SalesTerritoryGroup,
    coalesce(st.CountryRegionCode,'') SalesRegion
from 
    Bronze.salesperson sp
left join 
    Bronze.people p
    on p.PeopleID=sp.BusinessEntityID
LEFT JOIN 
    Bronze.salesterritory as st 
    on st.TerritoryID=sp.TerritoryID
    """
if par_process_full in ( "False","No"): 
    source_query=f"{source_query} WHERE sp.SYS_Run_ID='{par_run_ID}' or p.SYS_Run_ID ='{par_run_ID}' or st.SYS_Run_ID ='{par_run_ID}' "
    
df=spark.sql(source_query)

df= df.withColumn("SYS_Run_ID",lit(par_run_ID))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upsertDelta(df,"SalesPerson","SalesPersonID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
