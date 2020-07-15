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
spark = SparkSession.builder.appName('Load Mortage Data').getOrCreate()

mortage_schema = types.StructType([
types.StructField('date', types.StringType(), True),
types.StructField('1y_fixed_posted', types.StringType(), True),
types.StructField('2y_bond', types.StringType(), True),
types.StructField('3y_bond', types.StringType(), True),
types.StructField('3y_fixed_posted', types.StringType(), True),
types.StructField('5y_bond', types.StringType(), True),
types.StructField('5y_fixed_posted', types.StringType(), True),
types.StructField('7y_bond', types.StringType(), True),
types.StructField('10y_bond', types.StringType(), True),
types.StructField('bank', types.StringType(), True),
types.StructField('overnight', types.StringType(), True),
types.StructField('overnight_target', types.StringType(), True),
types.StructField('prime', types.StringType(), True),])


def loadMortageInfo():
    mortage = spark.read.csv("Other_sources/mortgage rate since 1935.csv", schema=mortage_schema).createOrReplaceTempView("mortage")
    transf_year_month = spark.sql("SELECT *, substr(m.date, 1, instr(m.date, '-') +2) as year_month FROM mortage m ").createOrReplaceTempView("mortage_transformed")
    group_by_yearmonth = spark.sql("SELECT year_month as REF_DATE, AVG(1y_fixed_posted) AS avg_1y_fixed_posted, AVG(2y_bond) as avg_2y_bond, AVG(3y_bond) AS avg_3y_bond,\
     AVG(3y_fixed_posted) as avg_3y_fixed_posted, AVG(5y_bond) as avg_5y_bond, AVG(5y_fixed_posted) as avg_5y_fixed_posted, AVG(7y_bond) as avg_7y_bond, \
     AVG(10y_bond) as avg_10y_bond, AVG(bank) as avg_bank,AVG(overnight) AS avg_overnight, AVG(overnight_target) as avg_overnight_target, AVG(prime) as avg_prime\
     FROM mortage_transformed GROUP BY year_month")
    return group_by_yearmonth
