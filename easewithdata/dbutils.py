# Databricks notebook source
# MAGIC %md 
# MAGIC #Understanding the dbutils

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,IntegerType
from delta import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------

# creating an inbound parameter 
# The default value  should be passed on as a string only and it is not possible to pass it as number
dbutils.widgets.text("departmentid",defaultValue="10")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hands_on_catlog;
# MAGIC use database default;
# MAGIC -- describe extended employees;
# MAGIC DROP DATABASE IF EXISTS database_one; 
# MAGIC DROP DATABASE  IF EXISTS EMPLOYEES;
# MAGIC DROP VOLUME  if exists volumne_exampleone;
# MAGIC DROP TABLE IF exists employee_records_2

# COMMAND ----------

# file location from the volume
volume_location = '/Volumes/hands_on_catlog/default/raw_data/employee_records.csv'


# COMMAND ----------

#Creating the user defined schema 
_schema = StructType([
                        StructField("firstName",StringType(),nullable=True),
                        StructField("lastName",StringType(),nullable=True),
                        StructField("jobTitle",StringType(),nullable=True),
                        StructField("dob",DateType(),nullable=True),
                        StructField("email",StringType(),nullable=True),
                        StructField("phone",StringType(),nullable=True),
                        StructField("salary",IntegerType(),nullable=True),
                        StructField("departmentId",IntegerType(),nullable=True)
])

# COMMAND ----------

# Reading the data from the volume 
employeesDf = ( spark.
                read.
                format("csv").
                option("mode","DROPMALFORMED").
                option("sep",',').
                schema(_schema).
                option("header",True).load(volume_location)
)

# COMMAND ----------

validation = dbutils.notebook.run('employeesTableCreation',600)
if validation  == 'Success':
    employeesobj = DeltaTable.forName(spark,'employees')
else:
    raise exception("Table not created")


# COMMAND ----------

employeesObj  =  DeltaTable.forName(spark,'employees')

# COMMAND ----------

#Merge operation using the delta table object 
employeesObj.alias("t1").merge(
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
            "departmentId":col("t2.departmentId")
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

# COMMAND ----------

# Archiving the file 
dbutils.fs.mv('/Volumes/hands_on_catlog/default/raw_data/employeesArchive','/Volumes/hands_on_catlog/default/raw_data/employeesArchive/employyes.csv')


# COMMAND ----------

# MAGIC %sql 
# MAGIC desc extended employees

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hands_on_catlog;
# MAGIC use database default;
# MAGIC create table if not exists hands_on_catlog.default.volumeExample (
# MAGIC  sno int,
# MAGIC  sname varchar(456)
# MAGIC )
# MAGIC using delta
# MAGIC location '/Volumes/hands_on_catlog/default/raw_data/employeesExample'
