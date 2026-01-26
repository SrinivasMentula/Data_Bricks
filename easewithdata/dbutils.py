# Databricks notebook source
# MAGIC %md 
# MAGIC #Employees Information

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,IntegerType
from delta import DeltaTable
from pyspark.sql.functions import col,count,rank
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col,count,current_timestamp
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Log Table Creation
# MAGIC %run /Workspace/Users/sarathazurelearning@gmail.com/Data_Bricks/easewithdata/logTableCreation

# COMMAND ----------

# DBTITLE 1,Common Notebook
# MAGIC %run /Workspace/Users/sarathazurelearning@gmail.com/Data_Bricks/easewithdata/common

# COMMAND ----------

validation = dbutils.notebook.run('employeesTableCreation',600)
if validation  == 'Success':
    employeesobj = DeltaTable.forName(spark,'employees')
else:
    raise exception("Table not created")
dbutils.notebook.run('logTableCreation',600)



# COMMAND ----------

# DBTITLE 1,Login Parameters
# creating an inbound parameter 
# The default value  should be passed on as a string only and it is not possible to pass it as number
try:
    startTime  = datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
    dbutils.widgets.text("departmentid",defaultValue="10")
    noteBookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    noteBookName = noteBookPath.split('/')[-1].split('.')[0]
    userName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception as e:
    msg = str(e)
    logError( notebookName=noteBookName,notebookPath= noteBookPath ,error_msg=e,startTime=startTime,userName=userName,status='Failure')
    raise Exception("File not found")


# COMMAND ----------

# DBTITLE 1,SQL Configuration
# MAGIC %sql
# MAGIC use catalog hands_on_catlog;
# MAGIC use database default;
# MAGIC -- describe extended employees;
# MAGIC DROP DATABASE IF EXISTS database_one; 
# MAGIC DROP DATABASE  IF EXISTS EMPLOYEES;
# MAGIC DROP VOLUME  if exists volumne_exampleone;
# MAGIC DROP TABLE IF exists employee_records_2

# COMMAND ----------

# DBTITLE 1,FileLocation
# file location from the volume
volume_location = '/Volumes/hands_on_catlog/default/raw_data/employees_01042026.csv'

# COMMAND ----------

# DBTITLE 1,Schema Definition
#Creating the user defined schema 
try:
    _schema = StructType([
                            StructField("firstName",StringType(),nullable=True),
                            StructField("lastName",StringType(),nullable=True),
                            StructField("jobTitle",StringType(),nullable=True),
                            StructField("dob",DateType(),nullable=True),
                            StructField("email",StringType(),nullable=True),
                            StructField("phone",StringType(),nullable=True),
                            StructField("salary",IntegerType(),nullable=True),
                            StructField("departmentId",IntegerType(),nullable=True),
                            StructField("_corrupt_record",StringType(),True)
    ])
except Exception as e:
    msg = str(e)
    logError( notebookName=noteBookName,notebookPath= noteBookPath ,error_msg=e,startTime=startTime,userName=userName,status='Failure')
    raise Exception (e)
    

# COMMAND ----------

# DBTITLE 1,Data  loading
# Reading the data from the volume 
# the mode has been set to Permissive to make sure the 
try :
    employeesDf = ( spark.
                    read.
                    format("csv").
                    option("mode","PERMISSIVE").
                    option("sep",',').
                    schema(_schema).
                    option("header",True).load(volume_location)
    )
    employeesDf = employeesDf.dropDuplicates(['firstName','lastName','email'])
except Exception as e:
    msg = str(e)
    logError( notebookName=noteBookName,notebookPath= noteBookPath ,error_msg=e,startTime=startTime,userName=userName,status='Failure')
    raise Exception("File not found")
    

# COMMAND ----------

_corruptedRecordsDF = employeesDf.filter('_corrupt_record is not null ')

# COMMAND ----------

# loading the data of the corrupted records into the tab
_corruptedRecordsDF.write.format("delta").mode("append").saveAsTable('hands_on_catlog.default.corruptedrecords')

# COMMAND ----------

# DBTITLE 1,Droping the Duplicates
# Droping the duplicates from the  main table and rewrite them
try:
    tableDF = spark.read.table("hands_on_catlog.default.employees")
    windoow_spec = Window.partitionBy('firstName','lastName','email').orderBy('id')
    tableDF = tableDF.withColumn('ranking',rank().over(windoow_spec)).filter('ranking <=1').drop('ranking','id')
    tableDF.write.format("delta").mode("overwrite").saveAsTable('hands_on_catlog.default.employees')
except Exception as e:
    msg = str(e)
    logError( notebookName=noteBookName,notebookPath= noteBookPath ,error_msg=e,startTime=startTime,userName=userName,status='Failure')
    raise Exception("File not found")


# COMMAND ----------

# DBTITLE 1,Merge Operation
#Merge operation using the delta table object 
try:
    employeesobj.alias("t1").merge(
                employeesDf.alias("t2"),
                    (col("t1.firstName") == col("t2.firstName")) &
                    (col("t1.lastName")  == col('t2.lastName')) &
                    (col("t1.email")   ==  col('t2.email') ) 
    ).whenMatchedUpdate(
        set={
                "firstName":col("t2.firstName"),
                "lastName":col("t2.lastName"),
                "jobTitle":col("t2.jobTitle"),
                "dob":col("t2.dob"),    
                "email":col("t2.email"),
                "phone":col("t2.phone"),
                "salary":col("t2.salary"),
                "departmentId":col("t2.departmentId"),
                "updatedDate":current_timestamp()
        }
    ).whenNotMatchedInsert(
                                values= {
                                            "firstName":col("t2.firstName"),
                                            "lastName": col("t2.lastName"),
                                            "jobTitle": col("t2.jobTitle"),
                                            "dob": col("t2.dob"),
                                            "email":col("t2.email"),
                                            "phone": col("t2.phone"),
                                            "salary": col("t2.salary"),                                        
                                            "departmentId":col("t2.departmentId")
                                }
    ).execute()
except Exception as e:
    msg = str(e)
    logError( notebookName=noteBookName,notebookPath= noteBookPath ,error_msg=e,startTime=startTime,userName=userName,status='Failure')
    raise Exception (e)

# COMMAND ----------

# DBTITLE 1,Archiving
# Archiving the file 
listOfFiles = dbutils.fs.ls("/Volumes/hands_on_catlog/default/raw_data")
listOfFiles = dbutils.fs.ls("/Volumes/hands_on_catlog/default/raw_data")
for i in listOfFiles:
    if i.size > 0:
        path = i.path
        name  = i.name 
        name = name +'_'+datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d-%H-%M-%S") 
        dbutils.fs.mv(path,f'/Volumes/hands_on_catlog/default/raw_data/employeesArchive/{name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe  history hands_on_catlog.default.employees
