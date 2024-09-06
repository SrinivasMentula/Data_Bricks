# Databricks notebook source
# MAGIC %md
# MAGIC  we have table job_id(delta_table) which is having columns job_id,date_of_insertation,unique_number, the incoming data will have  only 
# MAGIC job_id and date_of_insertation we are generating the new unique column which is unique_number

# COMMAND ----------

# creating an temporary  data for the incremental load
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, desc, col
from datetime import datetime, date

current_date = date.today()

data = [
    ("12345672", current_date),
    ("12345674", current_date),
    ("12345675", current_date),
    ("12345677", current_date),
    ("12345679", current_date),
    ("12345678", current_date),
    ("12345676", current_date),
    ("12345673", current_date),
    ("12345671", current_date),
]
schema = StructType(
    [
        StructField("Job_id", StringType(), True),
        StructField("date_of_insertation", DateType(), True),
    ]
)

staging_data_frame = spark.createDataFrame(data, schema=schema)

staging_data_frame.createOrReplaceTempView("staging_data_frame")

# COMMAND ----------

# make the string dynamic by keeping the columns in  a a string
df_job_id = spark.read.table("job_id")
columns_in_job_id = df_job_id.columns
columns_in_job_id_with_out_unique_number = []
for column in columns_in_job_id:
    if column != "unique_number":
        columns_in_job_id_with_out_unique_number.append(column)
string_select_statement = (
    ",".join(
        f"stage.{column_name}"
        for column_name in columns_in_job_id_with_out_unique_number
    )
    + ",main.unique_number"
)

# COMMAND ----------

# using the left join updating the  incremental load and  by maintaing the unique value form the main table only
combinational_data = spark.sql(
    f"""
select {string_select_statement}
from staging_data_frame as stage left join job_id as main 
on stage.job_id = main.Job_id """
).orderBy(desc("unique_number"))

combinational_data.createOrReplaceTempView("view_combinational_data")
combinational_data.write.mode("overWrite").saveAsTable("job_id")

# loading the maximum value from the  main table to an variable

max_data_in_list = combinational_data.select("unique_number").collect()
maximum_unique_value = max_data_in_list[0][0] if max_data_in_list else 0

# updating the  unique column data for the  dauily incremental load using the maximum value from  the old table
combinational_data_new = combinational_data.filter(
    col("unique_number").isNull()
).withColumn(
    "unique_number",
    row_number().over(Window.orderBy(col("unique_number"))) + maximum_unique_value,
)
combinational_data_new.createOrReplaceTempView("view_updated_combinational_data")

# COMMAND ----------

string_update_statement = ",".join(
    f"target.{column_name} = source.{column_name}" for column_name in columns_in_job_id
)
merge_query = f"""
                 merge into job_id as target 
                 using view_updated_combinational_data as source 
                 on  target.job_id = source.job_id 
                 when matched  then update set {string_update_statement}      
                                    
                                    """
spark.sql(merge_query)
