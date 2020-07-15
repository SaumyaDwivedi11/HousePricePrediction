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

oil_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType(), True),
	types.StructField('GEO', types.StringType(), True),
	types.StructField('Diesel_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Diesel_self_service_filling_stations', types.FloatType(), True),
    types.StructField('Premium_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Premium_self_service_filling_stations', types.FloatType(), True),
    types.StructField('Regular_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Regular_self_service_filling_stations', types.FloatType(), True),])

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


def loadOilData():
	#PRODUCT ID FOR OIL DATA.
    productId = "18100001"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #print(pdDF)
    new_df = pdDF.loc[pdDF['Type of fuel'].isin(['Diesel fuel at full service filling stations','Diesel fuel at self service filling stations','Premium unleaded gasoline at full service filling stations','Premium unleaded gasoline at self service filling stations','Regular unleaded gasoline at full service filling stations','Regular unleaded gasoline at self service filling stations'])]

    transposeDF = new_df.pivot_table(index = ['REF_DATE','GEO'], columns='Type of fuel', values='VALUE').reset_index(['REF_DATE','GEO'])
    oil_df = spark.createDataFrame(transposeDF,schema=oil_schema).createOrReplaceTempView("oil_info")

    transform_ontario_exception = spark.sql("SELECT *, replace(GEO,'Ottawa-Gatineau, Ontario part, Ontario/Quebec','Ottawa,Ontario') as NEW_GEO FROM oil_info").createOrReplaceTempView("ontario_transform")
    transform_quebec_exception = spark.sql("SELECT *, replace(NEW_GEO,'Ottawa-Gatineau, Quebec part, Ontario/Quebec','Ottawa,Quebec') as NEW_GEO_ALL FROM ontario_transform").createOrReplaceTempView("province_transformed")

    province = spark.sql("SELECT *,substr(NEW_GEO_ALL, - instr(reverse(NEW_GEO_ALL), ',') + 1) as province FROM province_transformed").createOrReplaceTempView("province_info")
    
    avg_per_province = spark.sql("SELECT province, REF_DATE, 'Cents per litre' as uom_oil, 'units ' as scalar_oil, \
        AVG(Diesel_full_service_filling_stations) as avg_diesel_fillingstations, AVG(Diesel_self_service_filling_stations) AS avg_diesel_selfservstations, \
        AVG(Premium_full_service_filling_stations) as avg_premium_fillingstations, AVG(Premium_self_service_filling_stations) as avg_premium_selfservstations , \
        AVG(Regular_full_service_filling_stations) AS avg_regular_fillingstations,AVG(Regular_self_service_filling_stations) AS avg_regular_selfservstations \
        FROM province_info GROUP BY province,REF_DATE")
    
    return avg_per_province

