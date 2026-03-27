# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.types import *
from pyspark.sql import   functions as F
from delta import DeltaTable

# COMMAND ----------

pip install  pandas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATING A  CATLOG IF NOT EXISTS 
# MAGIC USE CATALOG nyctaxi;
# MAGIC CREATE DATABASE IF NOT EXISTS nyctaxi.testDB; 
# MAGIC CREATE VOLUME IF NOT EXISTS nyctaxi.testDB.inboundData;
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi.testDB.employees
# MAGIC (
# MAGIC    employeeId  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC    firstName varchar(20), 
# MAGIC    gender  varchar(20),
# MAGIC    startdate date,
# MAGIC    lastLogin VARCHAR(20),
# MAGIC    salary   long , 
# MAGIC    bonus     DECIMAL(34,2),
# MAGIC    seniorManager VARCHAR(20),
# MAGIC    team varchar(20)
# MAGIC )

# COMMAND ----------

_userSchema = StructType(
                         [
                              StructField("firstName" , StringType() ),
                              StructField("gender" , StringType() ),
                              StructField("startdate", StringType()),
                              StructField("lastLogin",StringType()),
                              StructField("salary" , LongType()),
                              StructField("bonus" , DecimalType(34,2)),
                              StructField("seniorManager" , StringType()),
                              StructField("team",StringType())
                      ]
)

# COMMAND ----------

# Reading the data from the  volume and loading it into the delta table 
employeesDF = spark.read.csv(path='/Volumes/nyctaxi/testdb/inbounddata/employees.csv',
                             header= True,
                             schema=_userSchema,
                             sep = ',')
employeesDF = employeesDF.withColumn("startdate",F.to_date(F.col("startdate"),'M/d/yyyy'))
employeesDF = employeesDF.fillna({"Gender":"Unkown",
                    "Team":"Undeclared"})


# COMMAND ----------

# understaning the merge schema 
employeesDF.write.format("delta").mode("overWrite").option("mergeSchema",True).saveAsTable("nyctaxi.testDB.employees")
#  aggregating the data  to see how it will be shown in the UI 
employeesDF.groupBy(F.col("Team")).agg(F.sum("Salary").alias("groupedSalary")).show()
