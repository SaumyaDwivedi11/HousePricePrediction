import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types
from io import *
import pandas as pd
from urllib.request import *

spark = SparkSession.builder.appName('Load Diverse Expenditures').getOrCreate()

#Schema for Diverse Expenditures
expeditures_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('Food_expenditures', types.StringType(), True),
    types.StructField('income_taxes', types.StringType(), True),
    types.StructField('Mortgage_insurance', types.StringType(), True),
    types.StructField('Mortgage_paid', types.StringType(), True),
    types.StructField('Principal_accommodation', types.StringType(), True),
    types.StructField('Rent', types.StringType(), True),
    types.StructField('Shelter', types.StringType(), True),
    types.StructField('Total_expenditure', types.StringType(), True),
    types.StructField('Taxes_transfer_landregistration_fees', types.StringType(), True),
    types.StructField('Education', types.StringType(), True)])

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
	 * Description: This method is used to request diverse expenditures information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with diverse expenditures info per province and year-month
'''
def loadExpenditures():
    # PRODUCT ID FOR DIVERSE EXPENDITURES.
    productId = "11100222"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #Filter data only with features we needed.
    new_df = pdDF.loc[pdDF['Household expenditures, summary-level categories'].isin(
        ['Total expenditure', 'Food expenditures', 'Shelter', 'Principal accommodation', 'Rent',
         'Mortgage paid for owned living quarters', 'Mortgage insurance premiums for owned living quarters',
         'Transfer taxes and land registration fees for owned living quarters', 'Income taxes', 'Education'])]
    #Transpose DF to have features as columns.
    transposeDF = new_df.pivot_table(index=['REF_DATE', 'GEO', 'DGUID'],
                                     columns='Household expenditures, summary-level categories',
                                     values='VALUE').reset_index(['REF_DATE', 'GEO', 'DGUID'])
    expediture_df = spark.createDataFrame(transposeDF, schema=expeditures_schema).createOrReplaceTempView("expeditures")
    avg_per_province = spark.sql("SELECT GEO, REF_DATE, DGUID, 'Dollars' as uom_expenditure, 'unit' as scalar_expenditure,\
    'Average expenditure per household' as statistic_expenditure,  Food_expenditures, income_taxes, \
    Mortgage_insurance, Mortgage_paid, Principal_accommodation, Rent, Shelter, Total_expenditure, \
    Taxes_transfer_landregistration_fees, 'Education' FROM expeditures")
    return avg_per_province
