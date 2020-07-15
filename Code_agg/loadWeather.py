from pyspark import SparkConf, SparkContext
import sys
import re, string
import operator
import json
import requests
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, functions, types
from io import *
import csv
import pandas as pd
from urllib.request import *
import getCodeSets as codesets
from pyspark.sql.functions import input_file_name
spark = SparkSession.builder.appName('Load Weather Data').getOrCreate()

weather_schema = types.StructType([
types.StructField('REF_DATE', types.StringType(), True),
types.StructField('Year', types.StringType(), True),
types.StructField('Month', types.StringType(), True),
types.StructField('Mean_Max_Temp', types.StringType(), True),
types.StructField('Mean_Max_Temp_Flag', types.StringType(), True),
types.StructField('Mean_Min_Temp', types.StringType(), True),
types.StructField('Mean_Min_Temp_Flag', types.StringType(), True),
types.StructField('Mean_Temp', types.StringType(), True),
types.StructField('Mean_Temp_Flag', types.StringType(), True),
types.StructField('Extr_Max_Temp', types.StringType(), True),
types.StructField('Extr_Max_Temp_Flag', types.StringType(), True),
types.StructField('Extr_Min_Temp', types.StringType(), True),
types.StructField('Extr_Min_Temp_Flag', types.StringType(), True),
types.StructField('Total_Rain', types.StringType(), True),
types.StructField('Total_Rain_Flag', types.StringType(), True),
types.StructField('Total_Snow', types.StringType(), True),
types.StructField('Total_Snow_Flag', types.StringType(), True),
types.StructField('Total_Precip', types.StringType(), True),
types.StructField('Total_Precip_Flag', types.StringType(), True),
types.StructField('Snow_Grnd_Last_Day', types.StringType(), True),
types.StructField('Snow_Grnd_Last_Day_Flag', types.StringType(), True),
types.StructField('Dir_of_Max_Gust', types.StringType(), True),
types.StructField('Dir_of_Max_Gust_Flag', types.StringType(), True),
types.StructField('Spd_of_Max_Gust', types.StringType(), True),
types.StructField('Spd of Max Gust_Flag', types.StringType(), True),])


def loadWeatherData():
    weather = spark.read.csv("Other_sources/weather", schema=weather_schema)
    weather.withColumn("input_file", input_file_name()).createOrReplaceTempView("weather_info")

    weather_info = spark.sql("SELECT REF_DATE, Mean_Max_Temp,Mean_Min_Temp, Mean_Temp,Total_Rain,Total_Snow,SUBSTR(substr(input_file, - instr(reverse(input_file), '/') + 1) , 0, INSTR(substr(input_file, - instr(reverse(input_file), '/') + 1) , '_')-1) as province FROM weather_info")#.createOrReplaceTempView("weather_t")
    return weather_info
