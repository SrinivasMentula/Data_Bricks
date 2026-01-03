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
-- MAGIC dbutils.notebook.exit("Success")
