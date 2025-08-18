# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def file_exists(file_path_dest: str)->bool:
    """
    Function that check if a file exists or not
    Args:
        file_path_dest (str): Destination path.
    
    Returns:
        bool    
    """
    try:
        # Try to read the file to check if it exists
        df = spark.read.format("binaryFile").load(file_path_dest)
        return df.count() > 0
    except Exception as e:
        # If an error occurs while reading the file, it may not exist
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_delta_table_exists(file_path_raw: str)->bool:
    """
    Function that checks the existence of a delta table.

    Args:
        A path to the table
    Returns:
        Bool
    """

    try:
        # Intentar leer la tabla Delta
        df = spark.read.format("delta").load(file_path_raw)
        print("The Delta table exists at the specified path.")
        return True
    except Exception as e:
        print("Delta table does not exist in the specified path. Will be created below")
        print(f"Error: {e}")
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_FKey_columns(
    df: DataFrame,
    fk_entity_name: str,
    cols_alternative_key: tuple) -> DataFrame:

    df = df.withColumn(
        fk_entity_name + "_Key", xxhash64(concat_ws("||", *cols_alternative_key))
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_system_columns(
    df: DataFrame,
    entity_name: str,
    cols_alternative_key: tuple,
    cols_scd1: tuple,
    cols_scd2: tuple,
    is_full_load: bool,
    current_date: datetime.date,
    execution_id: int
) -> DataFrame:

    df = df.withColumn(
        entity_name + "_Key", xxhash64(concat_ws("||", *cols_alternative_key))
    )
    col="Sys_ExecutionID"
    if col not in df.columns:
        df = df.withColumn("Sys_ExecutionID", lit(execution_id))
    if not cols_scd1 is None or len(cols_scd1)>0:
      df = df.withColumn("Sys_SCD1Hash", sha2(concat_ws("||", *cols_scd1), 256))
    else:
      df = df.withColumn("Sys_SCD1Hash", lit(None))
    
    if not cols_scd2 is None or len(cols_scd2)>0:
      df = df.withColumn("Sys_SCD2Hash", sha2(concat_ws("||", *cols_scd2), 256))
    else:
      df = df.withColumn("Sys_SCD2Hash", lit(None))

    df = df.withColumn("Sys_IsDeleted", lit(0))\
           .withColumn("Sys_IsCurrent", lit(1))\
           .withColumn("Sys_ToDate",lit('2099-12-31').cast(DateType()))
    
    if is_full_load:
      df = df.withColumn("Sys_FromDate", lit("1900-01-01").cast(DateType()))
    else:
      df = df.withColumn("Sys_FromDate", lit(current_date).cast(DateType()))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def SQLSCD2Genetator(table_name :str,tbNews :str, SCD1 :tuple, SCD2 :tuple)->str:
    # Genera el script de merge SCD2 
    if spark.catalog._jcatalog.tableExists(table_name):
        df_schema = spark.sql(f"describe {table_name}")
    else:
        df_schema = spark.sql(f"describe {tbNews}")

    sqlinsert1=f'INSERT (  '
    sqlinsert2=f'( '
    for row in df_schema.collect():
        sqlinsert1=sqlinsert1+row["col_name"]+','
        sqlinsert2=sqlinsert2+"so."+row["col_name"]+','
    sqlinsert1=sqlinsert1[:-1]+')'
    sqlinsert2=sqlinsert2[:-1]+')'

    # Si aplica SCD2
    sqlSCD2=''
    for campo in SCD2:
        x=f"""ta.{campo}=so.{campo},"""
        sqlSCD2=sqlSCD2+x
    if sqlSCD2!='':
        sqlSCD2='''WHEN MATCHED AND ta.Sys_SCD2Hash != so.Sys_SCD2Hash THEN 
                        UPDATE SET ta.Sys_ToDate= current_date() , ta.SYS_IsCurrent=0  '''
    
    # Si aplica SCD1    
    sqlSCD1=''
    for campo in SCD1:
        x=f"""ta.{campo}=so.{campo},"""
        sqlSCD1=sqlSCD1+x
    sqlSCD1=sqlSCD1[:-1] 
    if sqlSCD1!='':
        sqlSCD1=f'''WHEN MATCHED AND 
            ta.Sys_SCD1Hash != so.Sys_SCD1Hash THEN 
            UPDATE SET {sqlSCD1} '''

    # Script final

    dinSQL=f"""MERGE into {table_name} as ta 
        USING tbNews as so
        on ta.{entity_name}_Key=so.{entity_name}_key 
        and ta.SYS_IsCurrent=1 
        {sqlSCD2}    
        {sqlSCD1}
        WHEN NOT MATCHED by TARGET THEN
            {sqlinsert1}
            VALUES {sqlinsert2};
        """  
    return dinSQL
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
