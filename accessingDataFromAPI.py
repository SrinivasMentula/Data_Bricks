# Databricks notebook source
# MAGIC %md 
# MAGIC reading the data from  the API and   doing some  necessary changes 

# COMMAND ----------

import requests
import json
from pyspark.sql import DataFrame

# COMMAND ----------

## reading the data  from  the API
url = "https://restcountries.com/v3.1/all"
response = requests.get(url)
jason_data = response.json();
#string_data =  json.dumps(jason_data)
#print(len(jason_data))
##dbutils.fs.put("dbfs:/FileStore/rest_countries_data/result.json",string_data);

# COMMAND ----------

print(len(jason_data))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType,ArrayType,BooleanType,IntegerType,FloatType

# Define the schema correctly using MapType
schema_correct = StructType([
                             StructField("name", StructType([
                                    StructField("common", StringType(), True),
                                    StructField("official", StringType(), True),
                                    StructField("nativeName", MapType(
                                        StringType(),  # Key type (dynamic)
                                        StructType([   # Value type (fixed structure for values)
                                            StructField("official", StringType(), True),
                                            StructField("common", StringType(), True)
                                        ]), True
                                    ), True)
                                ])),
    StructField("tld",ArrayType(StringType(),True)),
    StructField("cca2",StringType(),True),
    StructField("ccn3",StringType(),True),
    StructField("cca3",StringType(),True),
    StructField("independent",BooleanType(),True),
    StructField("status",StringType(),True),
    StructField("unMember",BooleanType(),True),
    StructField("currencies",
                MapType(StringType(),StructType([StructField("name",StringType(),True),StructField("Symbol",StringType(),True)]))),
   StructField("idd", StructType([
        StructField("root", StringType(), nullable=True),
        StructField("suffixes", ArrayType(StringType()), nullable=True)
    ])),
   StructField("capital",ArrayType(StringType()),nullable=True),
   StructField("altSpellings",ArrayType(StringType()),nullable=True),
   StructField('translations',
               MapType(StringType(),
                       StructType([StructField("official",StringType(),nullable=True),
                                    StructField("common",StringType(),nullable=True)])),nullable=True),
 StructField("latlng",ArrayType(FloatType(),True),nullable=True),
 StructField("landlocked",BooleanType(),nullable=True),
StructField("area",FloatType(),True),
StructField("demonyms",MapType(StringType(),
  StructType([StructField("f",StringType(),True),StructField("m",StringType(),True)]))),
  StructField("flag",StringType(),True),
  StructField("maps",StructType([StructField("googlemaps",StringType(),True),StructField("openStreetMaps",StringType(),True)])),
  StructField("population",IntegerType(),True),StructField("car",StructType([(StructField("signs",ArrayType(StringType()),True)),
                                                                             StructField("side",StringType(),True)])),
  StructField("timezones",ArrayType(StringType(),True),nullable=True),
  StructField("continents",ArrayType(StringType(),True),nullable=True),
  StructField("flags",MapType(StringType(),StringType(),True),nullable=True),
  StructField("coatOfArms",MapType(StringType(),StringType()),True),
  StructField("startOfWeek",StringType(),nullable=True),
  StructField("capitalInfo",MapType(StringType(),ArrayType(FloatType(),True)),nullable=True)
                                                                           
])


# COMMAND ----------

data_frame = spark.createDataFrame(jason_data,schema_correct)
limited_data = data_frame.limit(2)
##display(limited_data)
##print(data)

# COMMAND ----------

from pyspark.sql.functions import *
flatened_data_frame = data_frame.select(data_frame.name.common.alias("common_name"),data_frame.name.official.alias("official_name"),
                                      data_frame.name.nativeName.alias("Native_name"),explode(data_frame.tld).alias("tld"),col("cca2"),
                                      col("ccn3"),col("cca3"),col("independent"),col("status"),"currencies",
                                      col("idd").root.alias('root_idd'),
                                      explode(col("idd").suffixes).alias("suffixes_idd"),
                                      explode(col("capital")).alias("captial"),
                                      explode(col("capital")).alias("capital"),
                                      col("altSpellings"),
                                      col("translations"),
                                      col("latlng")[0].alias("latitude"),
                                      col("latlng")[1].alias("longitude"),
                                      col("landlocked"),
                                      col("area"),
                                      col("demonyms"),
                                      col("flag"),
                                      col("maps"),
                                      col("population"),
                                      col("car").signs.alias('sighn'),
                                      col("car").side.alias("side_of_driving"),
                                      explode(col("timezones")).alias("timeZones"),
                                      explode(col("continents")).alias("continents"),
                                      col("flags").png.alias("flag_in_png"),
                                      col("flags").svg.alias("flag_in_svg"),
                                      col("coatOfarms").png.alias("coatOfArms_png"),
                                      col("coatOfarms").svg.alias("coatOfArms_svg"),
                                      col("startOfWeek"),
                                      col("capitalInfo").latlng[0].alias("capital_lat"),
                                      col("capitalInfo").latlng[1].alias("capital_long")
                                      )
limited_data = flatened_data_frame.limit(2)
display(limited_data)

# COMMAND ----------

total_number_of_rows = flatened_data_frame.count();
print(total_number_of_rows)

# COMMAND ----------

flatened_data_frame.createOrReplaceTempView("rest_countries");
countries_with_more_than_50million_population = spark.sql("select distinct common_name from rest_countries where population > 50000000")
display(countries_with_more_than_50million_population)

# COMMAND ----------

## countrie with maximum population 
country_with_maximum_population = spark.sql("select distinct common_name , population \
                                             from rest_countries where population = (select max(population) from rest_countries)\
                                            ")

# COMMAND ----------

pip install num2words

# COMMAND ----------

from num2words import num2words
number = 1402112000;
number_in_words = num2words(1402112000)
display(number_in_words)

# COMMAND ----------

## Finding the solution for the querys using  the pyspark instead of the sql 
## 1. count the number of  distinct countries in  the  data frame 
##List Unique continents: Identify all the unique regions represented in the data.
##Countries by Region: Group the countries by their respective regions and count how many countries are in each region.
##Top 5 Most Populous Countries: Find the top 5 countries with the highest population.

# COMMAND ----------

## 1. count the number of  distinct countries in  the  data frame 
from pyspark.sql.functions import *
number_of_unique_countries = flatened_data_frame.select("common_name").distinct().count()
print(number_of_unique_countries)


# COMMAND ----------

##List Unique Continents: Identify all the unique regions represented in the data.lis
list_of_continents = flatened_data_frame.select("continents").distinct()
##display(list_of_continents)


# COMMAND ----------

##Countries by Region: Group the countries by their respective regions and count how many countries are in each region.
continent_and_count_of_countries =flatened_data_frame.select("common_name", "continents").distinct().groupBy("continents").count().alias("number_of_countries")

# COMMAND ----------

##Top 5 Most Populous Countries: Find the top 5 countries with the highest population.
flatened_data_frame.createOrReplaceTempView("rest_countries_flaten")

top_five_populated_countries = spark.sql( " select * from \
                               (select  distinct common_name , population,dense_rank() over ( order by population desc) as rank from rest_countries_flaten) where rank <= 5 ")
##display(top_five_populated_countries)
                                                        
