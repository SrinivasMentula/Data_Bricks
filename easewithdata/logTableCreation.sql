-- Databricks notebook source
use catalog hands_on_catlog;
use database default

-- COMMAND ----------

use catalog hands_on_catlog;
use  database default;
create table if not exists default.notebookLogs(
     ID BIGINT generated always AS identity,
     notebookName  varchar(200),
     startTime timestamp,
     endTime timestamp,
     totalDurration decimal,
     executedBy varchar(200),
     status varchar(200),
     error_msg  string,
     notebookPath varchar(200)
)
TBLPROPERTIES 
(
     'delta.enableDeletionVectors' = true
)

