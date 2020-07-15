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
spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",True).config("spark.memory.offHeap.size","32g").config("spark.driver.maxResultSize","10g").appName("Load Labour Force Data").getOrCreate()
#conf = SparkConf().setAppName('reddit etl')
#sc = SparkContext(conf=conf)

immigration_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType(), True),
	types.StructField('GEO', types.StringType(), True),
	types.StructField('In_migrants', types.IntegerType(), True),
    types.StructField('Out_migrants', types.IntegerType(), True),])

# dtype={"REF_DATE": str, "GEO": str, "DGUID":str , "Labour force characteristics":str, "Sex":str, "Age group":str, \
                    #"Statistics":str, "Data type":str, "UOM":str, "UOM_ID":int, "SCALAR_FACTOR":str, "SCALAR_ID":int, "VECTOR":str, "COORDINATE":str, "VALUE":str, "STATUS":str, \
                    #"SYMBOL":str, "TERMINATE":str, "DECIMALS":int}
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

def loadImmigrationData():
	#PRODUCT ID FOR .
    productId = "17100020"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #print(pdDF)
    
    transposeDF = pdDF.pivot_table(index = ['REF_DATE','GEO'], columns='Interprovincial migration', values='VALUE').reset_index(['REF_DATE','GEO'])
    immigration_df = spark.createDataFrame(transposeDF,schema=immigration_schema).createOrReplaceTempView("immigration_info")

    avg_per_province = spark.sql("SELECT GEO as province, REF_DATE, 'Persons' as uom_imm, 'units' as scalar_imm, \
        AVG(In_migrants) as avg_in_migrants, AVG(Out_migrants) AS avg_out_migrants\
        FROM immigration_info GROUP BY GEO,REF_DATE")
    
    return avg_per_province

