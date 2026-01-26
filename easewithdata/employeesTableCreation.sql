-- Databricks notebook source
-- MAGIC %md
-- MAGIC # employees table creation 
-- MAGIC

-- COMMAND ----------

USE CATALOG hands_on_catlog;
use database default;

-- COMMAND ----------

-- employees table creation  
CREATE TABLE IF NOT EXISTS default.employees (
    id bigint GENERATED ALWAYS as IDENTITY,
    firstName string,
    lastName string,
    jobTitle  string,
    dob DATE,
    email STRING,
    phone  string ,
    salary  bigint,
    departmentId int,
    created_date TIMESTAMP DEFAULT current_timestamp(),
    updateddate TIMESTAMP 
)
TBLPROPERTIES(
  'delta.enableDeletionVectors' = true,
  'delta.feature.allowColumnDefaults' = 'supported'
)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta import DeltaTable
-- MAGIC try :
-- MAGIC   result =DeltaTable.forName(spark, "hands_on_catlog.default.corruptedRecords").toDF().limit(1).count()
-- MAGIC   dbutils.notebook.exit("Success")
-- MAGIC except Exception as e :
-- MAGIC   DeltaTable.create(spark).tableName('hands_on_catlog.default.corruptedRecords')\
-- MAGIC     .addColumn("firstName","STRING")\
-- MAGIC     .addColumn("lastName","STRING")\
-- MAGIC     .addColumn("jobTitle","STRING")\
-- MAGIC     .addColumn("dob","DATE")\
-- MAGIC     .addColumn("email","STRING")\
-- MAGIC     .addColumn("phone","STRING")\
-- MAGIC     .addColumn("salary","INTEGER")\
-- MAGIC     .addColumn("departmentId","INTEGER")\
-- MAGIC     .addColumn("_corrupt_record","STRING")\
-- MAGIC     .addColumn("createdDate","TIMESTAMP").execute()

-- COMMAND ----------

-- ALTER TABLE default.corruptedrecords SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")

-- COMMAND ----------

SELECT * FROM default.employees
