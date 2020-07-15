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

touristinfo_schema = types.StructType([
types.StructField('REF_DATE', types.StringType(), True),
types.StructField('GEO', types.StringType(), True),
types.StructField('Total_international_travellers', types.StringType(), True),
types.StructField('Total_Canadian_residents', types.StringType(), True),])

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


def loadTouristInfo():
	#PRODUCT ID FOR TOURSIM INFO.
    productId = "24100041"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    new_df = pdDF.loc[pdDF['Traveller characteristics'].isin(['Total international travellers','Total Canadian residents'])]
    
    transposeDF = new_df.pivot_table(index = ['REF_DATE','GEO'], columns='Traveller characteristics', values='VALUE').reset_index(['REF_DATE','GEO'])
    tourism_df = spark.createDataFrame(transposeDF, schema  = touristinfo_schema).createOrReplaceTempView("tourist_data")

    tourist_data_with_province = spark.sql("SELECT *,substr(GEO, - instr(reverse(GEO), ',') + 1) as province FROM tourist_data").createOrReplaceTempView("province_info")
    avg_tourist_data = spark.sql("SELECT province, REF_DATE,'Persons' as uom_tourist,'unit' as scalar_tourist, AVG(Total_international_travellers) as avg_international_tourism, AVG(Total_Canadian_residents) as avg_domestic_tourism FROM province_info GROUP BY province,REF_DATE")
    return avg_tourist_data
    #return avg_per_province
