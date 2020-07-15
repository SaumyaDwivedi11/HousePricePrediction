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
spark = SparkSession.builder.appName('Load Income Data').getOrCreate()
#conf = SparkConf().setAppName('reddit etl')
#sc = SparkContext(conf=conf)


income_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType(), True),
	types.StructField('GEO', types.StringType(), True),
	types.StructField('DGUID', types.StringType(), True),
	types.StructField('Sex', types.StringType(), True),
	types.StructField('Age_group', types.StringType(), True),
	types.StructField('Persons_with_income', types.StringType(), True),
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

def loadIncomeData():
	#PRODUCT ID FOR HOUSE PRICE INDEX.
    productId = "11100008"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #print(pdDF)
    income_df = spark.createDataFrame(pdDF,schema=income_schema).createOrReplaceTempView("income")
    transform_income = spark.sql("SELECT * FROM income WHERE Sex = 'Both sexes' AND Age_group = 'All age groups' AND Persons_with_income = 'Median total income' ").createOrReplaceTempView("income_transformed")
    
    uom_df = codesets.getCodeDescription("uom").createOrReplaceTempView("uom")
    scalar_df = codesets.getCodeDescription("scalar").createOrReplaceTempView("scalar")
    
    join_income_uom_scalar = spark.sql("SELECT it.REF_DATE, it.GEO,  uom.memberUomEn, scalar.scalarFactorDescEn, it.VALUE \
      FROM income_transformed it \
    	INNER JOIN uom ON it.uom_id = uom.memberUomCode \
    	INNER JOIN scalar on it.scalar_id = scalar.scalarFactorCode ").createOrReplaceTempView("income_info")

    province = spark.sql("SELECT *,substr(GEO, - instr(reverse(GEO), ',') + 1) as province FROM income_info").createOrReplaceTempView("province_info")
    avg_per_province = spark.sql("SELECT province,REF_DATE,memberUomEn as uom_income,scalarFactorDescEn as scalar_income, 'Median total income,Both sexes,All age groups' as statistic_income, AVG(VALUE) as avg_income \
    FROM province_info GROUP BY province,REF_DATE,memberUomEn,scalarFactorDescEn ")#.createOrReplaceTempView("avg_province")
    
    return avg_per_province
    

    