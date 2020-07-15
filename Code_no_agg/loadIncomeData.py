import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types
from io import *
import pandas as pd
from urllib.request import *
import getCodeSets as codesets

spark = SparkSession.builder.appName('Load Income Data').getOrCreate()
#Schema for income information
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
    types.StructField('DECIMALS', types.StringType(), True), ])

'''
	 * Description: This method is used to download and extract the zip file contents in memory.
	 * input: String -> url of response.
	 * output:  -> Panda DataFrame -> file contents.
'''
def download_extract_zip(url):
    response = requests.get(url)
    with ZipFile(BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                df = pd.read_csv(thefile)
                return (df)

'''
	 * Description: This method is used to request income information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with income info per province and year-month
'''
def loadIncomeData():
    # PRODUCT ID FOR HOUSE PRICE INDEX.
    productId = "11100008"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    income_df = spark.createDataFrame(pdDF, schema=income_schema).createOrReplaceTempView("income")
    #Filter only features that we need.
    transform_income = spark.sql(
        "SELECT * FROM income WHERE Sex = 'Both sexes' AND Age_group = 'All age groups' AND Persons_with_income = '5-year percent change of median income' ").createOrReplaceTempView(
        "income_transformed")
    #Get codesets description for uom and scalar units
    uom_df = codesets.getCodeDescription("uom").createOrReplaceTempView("uom")
    scalar_df = codesets.getCodeDescription("scalar").createOrReplaceTempView("scalar")
    join_income_uom_scalar = spark.sql("SELECT it.REF_DATE, it.GEO,  it.DGUID, uom.memberUomEn, scalar.scalarFactorDescEn, it.VALUE \
      FROM income_transformed it \
        INNER JOIN uom ON it.uom_id = uom.memberUomCode \
        INNER JOIN scalar on it.scalar_id = scalar.scalarFactorCode ").createOrReplaceTempView("income_info")
    avg_per_province = spark.sql("SELECT GEO, DGUID, REF_DATE, memberUomEn as uom_income, scalarFactorDescEn as scalar_income, '5-year percent change of median income, Both sexes, All age groups' as statistic_income, VALUE as income \
    FROM income_info")
    return avg_per_province
