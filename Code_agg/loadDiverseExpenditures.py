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
spark = SparkSession.builder.appName('Load Diverse Expenditures').getOrCreate()

expeditures_schema = types.StructType([
types.StructField('REF_DATE', types.StringType(), True),
types.StructField('GEO', types.StringType(), True),
types.StructField('Food_expenditures', types.StringType(), True),
types.StructField('income_taxes', types.StringType(), True),
types.StructField('Mortgage_insurance', types.StringType(), True),
types.StructField('Mortgage_paid', types.StringType(), True),
types.StructField('Principal_accommodation', types.StringType(), True),
types.StructField('Rent', types.StringType(), True),
types.StructField('Shelter', types.StringType(), True),
types.StructField('Total_expenditure', types.StringType(), True),
types.StructField('Taxes_transfer_landregistration_fees', types.StringType(), True),])
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


def loadExpenditures():
	#PRODUCT ID FOR DIVERSE EXPENDITURES.
    productId = "11100222"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    new_df = pdDF.loc[pdDF['Household expenditures, summary-level categories'].isin(['Total expenditure','Food expenditures','Shelter','Principal accommodation','Rent','Mortgage paid for owned living quarters','Mortgage insurance premiums for owned living quarters','Transfer taxes and land registration fees for owned living quarters','Income taxes'])]
    
    transposeDF = new_df.pivot_table(index = ['REF_DATE','GEO'], columns='Household expenditures, summary-level categories', values='VALUE').reset_index(['REF_DATE','GEO'])
    expediture_df = spark.createDataFrame(transposeDF, schema  = expeditures_schema).createOrReplaceTempView("expeditures")

    avg_per_province = spark.sql("SELECT GEO as province,REF_DATE,'Dollars' as uom_expenditure, 'unit' as scalar_expenditure,'Average expenditure per household' as statistic_expenditure, AVG(Food_expenditures) as avg_food_expenditures, AVG(income_taxes) as avg_income_taxes, \
    AVG(Mortgage_insurance) as avg_mortageinsurance, AVG(Mortgage_paid) as avg_mortagePaid, AVG(Principal_accommodation) AS avg_accomodation, AVG(Rent) as avg_rent , \
    AVG(Shelter) AS avg_shelter, AVG(Total_expenditure) as avg_total_expenditure, AVG(Taxes_transfer_landregistration_fees) as avg_taxes_landregfees FROM expeditures GROUP BY province,REF_DATE ")#.createOrReplaceTempView("avg_province")
    
    return avg_per_province
