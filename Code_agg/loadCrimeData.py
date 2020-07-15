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
spark = SparkSession.builder.appName('Load Tourism Data').getOrCreate()

crime_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('Violations', types.StringType(), True),
    types.StructField('Statistics', types.StringType(), True),
    types.StructField('UOM', types.StringType(), True),
    types.StructField('UOM_ID', types.StringType(), True),
    types.StructField('SCALAR_FACTOR', types.StringType(), True),
    types.StructField('SCALAR_ID', types.StringType(), True),
    types.StructField('VECTOR', types.StringType(), True),
    types.StructField('COORDINATE', types.StringType(), True),
    types.StructField('VALUE', types.StringType(), True),
    types.StructField('STATUS', types.StringType(), True),
    types.StructField('SYMBOL', types.StringType(), True),
    types.StructField('TERMINATE', types.StringType(), True),
    types.StructField('DECIMALS', types.StringType(), True),])

def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with ZipFile(BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
            	df = pd.read_csv(thefile)
            	return (df)


def loadCrimeData():
	#PRODUCT ID FOR CRIME INFO.
    productId = "35100177"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    violations_df= pdDF.loc[pdDF['Violations'].isin(['Total, all violations'])]
    alltypesviol_df = violations_df.loc[pdDF['Statistics'].isin(['Actual incidents'])]
    
    crime_df = spark.createDataFrame(alltypesviol_df, schema  = crime_schema).createOrReplaceTempView("crime_info")

    province_temp = spark.sql("SELECT *,substr(GEO, - instr(reverse(GEO), ',') + 1) as province_temp FROM crime_info").createOrReplaceTempView("province_info")
    province = spark.sql("SELECT *, SUBSTR(province_temp, 0, INSTR(province_temp, '[')-2) as province FROM province_info ").createOrReplaceTempView("province_transformed")

    avg_crime_rates = spark.sql("SELECT province, REF_DATE, 'Number' as uom_crime, 'unit' as scalar_crime,'Total of all violations-Actual incidents' as statistic_crime,AVG(VALUE) as avg_crime_incidents FROM province_transformed GROUP BY province,REF_DATE")
    return avg_crime_rates
    #return avg_tourist_data
    #return avg_per_province
