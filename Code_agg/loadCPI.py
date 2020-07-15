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
spark = SparkSession.builder.appName('Load Consumer Price Index Data').getOrCreate()

cpi_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('VALUE', types.StringType(), True),])

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


def loadCPI():
	#PRODUCT ID FOR CPI INFORMATION
    productId = "18100256"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    cpi_info= pdDF.loc[pdDF['Alternative measures'].isin(['Consumer Price Index (CPI), all-items excluding eight of the most volatile components as defined by the Bank of Canada and excluding the effect of changes in indirect taxes, seasonally adjusted'])]
    
    transposeDF = cpi_info.pivot_table(index = ['REF_DATE','GEO'], columns='Alternative measures', values='VALUE').reset_index(['REF_DATE','GEO'])
    cpi_df = spark.createDataFrame(transposeDF,schema=cpi_schema).createOrReplaceTempView("cpi_info")

    avg_cpi_index = spark.sql("SELECT GEO as province, REF_DATE, '2002=100' as uom_cpi, 'unit' as scalar_cpi, AVG(VALUE) as avg_cpi_index FROM cpi_info GROUP BY GEO,REF_DATE")
    return avg_cpi_index
    #return avg_tourist_data
    #return avg_per_province

    
