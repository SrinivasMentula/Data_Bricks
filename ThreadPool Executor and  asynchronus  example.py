# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor
import time
import requests
import aiohttp
import asyncio

def data_from_the_api(url):
    response = requests.get(url)
    RDD = spark.sparkContext.parallelize(response.json())
    return RDD

start_time_two = time.time()
end_time_two = time.time()
urls = ["https://restcountries.com/v3.1/all"]
with ThreadPoolExecutor(max_workers=2) as executor:
    start_time_two = time.time()
    results = [executor.submit(data_from_the_api, url) for url in urls]
    end_time_two = time
print(type(results))

for future in results:
    print(future.result())
end_time_two = time.time()
print(end_time_two - start_time_two)

    

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
                MapType(StringType(),StructType([StructField("name",StringType(),True),StructField("symbol",StringType(),True)]))),
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
for i in results:
    data_in_rdd = i.result()
rest_countries_data_frame = spark.createDataFrame(data_in_rdd,schema_correct)
print(rest_countries_data_frame.count())

# COMMAND ----------

# DBTITLE 1,Example demonistrating the asynchronus programing with the asyncio library

import asyncio
import aiohttp

urls = ["https://restcountries.com/v3.1/all"]

import asyncio
import aiohttp
import time
start_time_three = time.time()
end_time_three = time.time()
urls = ["https://restcountries.com/v3.1/all"]

semaphore = asyncio.Semaphore(10) 

async def get_the_data_from_the_api(session, url):
    async with semaphore:
        async with session.get(url) as response:
            return await response.json()
start_time = time.time()
async def main_data_from_the_api():
    start_time_three= time.time()
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[get_the_data_from_the_api(session, url) for url in urls])
        end_time_three = time.time()
        return results

# Execute the async function directly
data = await main_data_from_the_api()
result = end_time_three-start_time_three
print(f"{result:.10f}")
#print(data)




# COMMAND ----------

# DBTITLE 1,Asynch and Multi Threading

from concurrent.futures import ThreadPoolExecutor

urls = ["https://restcountries.com/v3.1/all"]

semaphore = asyncio.Semaphore(10)  # Limits concurrency to 10

async def get_the_data_from_the_api(session, url):
    async with semaphore:
        async with session.get(url) as response:
            return await response.json()

async def main_data_from_the_api():
    
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[get_the_data_from_the_api(session, url) for url in urls])
       
        # print(f"Async Time taken: {end_time_three - start_time_three} seconds")
        return results

def run_async_code():
    return asyncio.run(main_data_from_the_api())

# Use ThreadPoolExecutor to run the async code
with ThreadPoolExecutor(max_workers=1) as executor:
    start_time_four = time.time()
    future = executor.submit(run_async_code)
    end_time_four = time.time()
    print(end_time_four - start_time_four)
    result = future.result()

print(type(result))  # To see the type of the result
print(len(result[0]))

