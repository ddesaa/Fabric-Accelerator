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


par_process_full ="False"


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

if par_process_full=="False":  
    df= spark.sql("select add_months(current_date,-1) as FromDate,to_date(concat(year(current_date)+1,'-12-31')) as ToDate")
else:
    df= spark.sql("select to_date(concat(year(current_date)-5,'-01-01')) as FromDate,to_date(concat(year(current_date)+1,'-12-31')) as ToDate")

row = df.collect()[0]
from datetime import datetime, timedelta


var_fromdate = row["FromDate"]
var_todate = row["ToDate"]

data=[]
while True:
    var_DateID=int(var_fromdate.strftime("%Y%m%d"))
    var_Date = var_fromdate
    var_Year = int(var_fromdate.strftime("%Y"))
    var_Month = int(var_fromdate.strftime("%m"))
    var_MonthName = var_fromdate.strftime("%B")
    var_WeekDay = int(var_fromdate.strftime("%w"))
    var_WeekDayName = var_fromdate.strftime("%A")
    var_YearMonth = int(var_fromdate.strftime("%Y%m"))
    var_MonthYear = var_fromdate.strftime("%b %y")
    
    
    data.append([var_DateID,var_Date,var_Year,var_Month,var_MonthName,var_WeekDay,var_WeekDayName,var_YearMonth,var_MonthYear])
    var_fromdate = var_fromdate + timedelta(days=1)

    if var_fromdate > var_todate :
        break 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
    StructField("DateID", IntegerType(), False),
    StructField("Date", DateType(), False),
    StructField("Year", IntegerType(), False),
    StructField("Month", IntegerType(), False),
    StructField("MonthName", StringType(), False),
    StructField("WeekDay", IntegerType(), False),
    StructField("WeekDayName", StringType(), False),
    StructField("YearMonth", IntegerType(), False),
    StructField("MonthYear", StringType(), False),
    
    ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(data,schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upsertDelta(df,"calendar","DateID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
