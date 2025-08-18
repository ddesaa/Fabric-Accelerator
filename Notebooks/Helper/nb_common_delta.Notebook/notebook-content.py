# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from delta.tables import *
import json

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")
spark.conf.set('spark.ms.autotune.queryTuning.enabled', 'true')

#Low shuffle for untouched rows during MERGE
spark.conf.set("spark.microsoft.delta.merge.lowShuffle.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getAbfsPath(workspaceid,LHName):  
    lh= notebookutils.lakehouse.getWithProperties(LHName,workspaceid)
    abfsPath = lh.properties['abfsPath']

    return abfsPath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def file_exists(folder, file, LHName, workspaceid, container='Files')->bool:
    ##################################################################
    # Function that check if a file exists or not
    # Args:
    #    file_path_dest (str): Destination path.
    #
    # Returns:
    #    bool    
    ###################################################################
    abfsPath = getAbfsPath(workspaceid,LHName)
    relativePath =  container +'/' + folder +'/' + file
    filePath = abfsPath + '/' + relativePath
    try:
        # Try to read the file to check if it exists
        df = spark.read.format("binaryFile").load(filePath)
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

def readFile(folder, file, LHName, workspaceid, container='Files',colSeparator=None, headerFlag=None, schema=None):
  # ##########################################################################################################################  
  # Function: readFile
  # Reads a data file from Lakehouse and returns as spark dataframe
  # 
  # Parameters:
  # folder = Folder within container where data file resides. E.g 'raw-bronze/wwi/Sales/Orders/2013-01'
  # file = File name of data file including and file extension. E.g 'Sales_Orders_2013-01-01_000000.parquet'
  # colSeparator = Column separator for text files. Default value None
  # headerFlag = boolean flag to indicate whether the text file has a header or not. Default value None
  # 
  # Returns:
  # A dataframe of the data file
  # ##########################################################################################################################    
    assert folder is not None, "folder not provided"
    assert file is not None, "file not provided"

    abfsPath = getAbfsPath(workspaceid,LHName)
    relativePath =  container +'/' + folder +'/' + file
    filePath = abfsPath + '/' + relativePath

    if ".csv" in file or ".txt" in file:
        if schema==None:
            df = spark.read.csv(path=filePath, sep=colSeparator, header=headerFlag, inferSchema="true")
        else:
            df  = spark.read.csv(path=filePath, sep=colSeparator, header=headerFlag, schema=schema)
    elif ".parquet" in file:
        df = spark.read.parquet(filePath)
    elif ".json" in file:
        df = spark.read.json(filePath, multiLine= True)
    elif ".orc" in file:
        df = spark.read.orc(filePath)
    else:
        df = spark.read.format("csv").load(filePath)
    df.dropDuplicates()
   
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsertDelta(df,tableName,keyColumns,watermarkColumn=None):
  # ##########################################################################################################################################   
  # Function: upsertDelta
  # Upserts a dataframe to delta lake table. Inserts record if they don't exist, updates existing if the version is newer than existing
  # 
  # Parameters:
  # df = Input dataframe
  # tableName = Target tableName in current lakehouse
  # mode = write mode. Valid values are "append", "overwrite". Default value "append"
  # 
  # Returns:
  # Json containing operational metrics.This is useful to get information like number of records inserted/updated.
  # E.g payload
  #     {'numOutputRows': '4432', 'numTargetRowsInserted': '0', 'numTargetFilesAdded': '1', 
  #     'numTargetFilesRemoved': '1', 'executionTimeMs': '2898', 'unmodifiedRewriteTimeMs': '606',
  #      'numTargetRowsCopied': '0', 'rewriteTimeMs': '921', 'numTargetRowsUpdated': '4432', 'numTargetRowsDeleted': '0',
  #      'scanTimeMs': '1615', 'numSourceRows': '4432', 'numTargetChangeFilesAdded': '0'}
  # ##########################################################################################################################################   

    # Creating a delta table with schema name not supported at the time of writing this code, so replacing schema name with "_". To be commented out when this
    #feature is available
 

    #Get target table reference
    DeltaTable.createIfNotExists(spark).tableName(tableName).addColumns(df.schema).execute()
    target = DeltaTable.forName(spark,tableName)
    assert target is not None, "Target delta lake table does not exist"

    keyColumnsList = keyColumns.split("|")
   
    #Merge Condition
    joinCond=""
    for keyCol in keyColumnsList:
        joinCond = joinCond + "target." + keyCol + " = source." + keyCol +" and "

    remove = "and"
    reverse_remove = remove[::-1]
    joinCond = joinCond[::-1].replace(reverse_remove,"",1)[::-1]

    if watermarkColumn is not None:
        joinCond = joinCond + " and target." + watermarkColumn + " <= source." + watermarkColumn
    max_retries = 10

    # Column mappings for insert and update
    mapCols =""
    for col in df.columns:
        mapCols = mapCols + '"' + col + '":' + ' "' + 'source.' + col + '" ,'

    remove = ","
    reverse_remove = remove[::-1]
    mapCols = mapCols[::-1].replace(reverse_remove,"",1)[::-1]

    #Convert Insert and Update Expression in to Dict
    updateStatement = json.loads("{"+mapCols+"}")
    insertStatement = json.loads("{"+mapCols+"}")
    for attempt in range(max_retries):
        try:
            target.alias("target") \
                .merge( df.alias('source'), joinCond) \
                .whenMatchedUpdate(set = updateStatement) \
                .whenNotMatchedInsert(values = insertStatement) \
                .execute()
               
            print("MERGE completado con éxito")
            break
        except Exception as e:
            print(attempt)
            if attempt < max_retries:
                wait_time = 5
                print(f"Conflicto de concurrencia. Reintentando en {wait_time}s...")
            else:
                raise
    


    # print(updateStatement)
    stats = target.history(1).select("OperationMetrics").first()[0]

    return stats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def readMedallionLHTable(LHName, workspaceid,tableRelativePath, filterCond=None, colList=None,schema=None):
  # ##########################################################################################################################  
  # Function: readMedallionLHTable
  # Retrieves a Lakehouse Table from the medallion layers, allowing for table filtering and specific column selection
  # 
  # Parameters:
  # tableRelativePath = Relative path of the LH table in format Tables/Schema/TableName
  # filterCond = A valid filter condition for the table, passed as string. E.g "ColorName == 'Salmon'". Default value is None. 
  #              If filterCond is None, the full table will be returned.
  # colList = Columns to be selected, passed as list. E.g. ["ColorID","ColorName"]. Default value is None.
  #           If colList is None, all columns in the table will be returned.
  # 
  # Returns:
  # A dataframe containing the Lakehouse table.
  # ##########################################################################################################################    
    abfsPath = getAbfsPath(workspaceid,LHName)
    tablePath = abfsPath + '/' + tableRelativePath

    # check if table exists
    table = DeltaTable.forPath(spark,tablePath)
    assert table is not None, "Lakehouse table does not exist"

    df = spark.read.format("delta").load(tablePath)
    
    # Apply filter condition
    if filterCond is not None:
        df = df.filter(filterCond)

    # Select columns
    if colList is not None:
        df = df.select(colList)

    df =df.dropDuplicates

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
