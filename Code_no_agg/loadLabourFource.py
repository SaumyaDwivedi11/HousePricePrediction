import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types
from io import *
import pandas as pd
from urllib.request import *

spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "70g").config("spark.driver.memory",
                                                                                              "50g").config(
    "spark.memory.offHeap.enabled", True).config("spark.memory.offHeap.size", "32g").config(
    "spark.driver.maxResultSize", "10g").appName("Load Labour Force Data").getOrCreate()

#Schema for labour information
labour_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('Labour_force_characteristics', types.StringType(), True),
    types.StructField('Sex', types.StringType(), True),
    types.StructField('Age_group', types.StringType(), True),
    types.StructField('Statistics', types.StringType(), True),
    types.StructField('data_type', types.StringType(), True),
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
#Schema for special labour characteristics information
labource_charact_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('Employment', types.FloatType(), True),
    types.StructField('Employment_rate', types.FloatType(), True),
    types.StructField('Full_time_employment', types.FloatType(), True),
    types.StructField('Labour_force', types.FloatType(), True),
    types.StructField('Part_time_employment', types.FloatType(), True),
    types.StructField('Participation_rate', types.FloatType(), True),
    types.StructField('Population', types.FloatType(), True),
    types.StructField('Unemployment', types.FloatType(), True),
    types.StructField('Unemployment_rate', types.FloatType(), True), ])

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
	 * Description: This method is used to request labour information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with labour info per province and year-month
'''
def loadLabourForceData():
    # PRODUCT ID FOR LABOUR FORCE.
    productId = "14100287"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #Filter only needed features
    new_df = pdDF.loc[pdDF['Sex'].isin(['Both sexes'])]
    seasonally_adjusted = new_df.loc[pdDF['Data type'].isin(['Seasonally adjusted'])]
    statistics = seasonally_adjusted.loc[pdDF['Statistics'].isin(['Estimate'])]
    #Transpose DF to have features as column headers
    transposeDF = statistics.pivot_table(index=['REF_DATE', 'GEO', 'DGUID'], columns='Labour force characteristics',
                                         values='VALUE').reset_index(['REF_DATE', 'GEO', 'DGUID'])
    labour_ch_df = spark.createDataFrame(transposeDF, schema=labource_charact_schema).createOrReplaceTempView("labour_info")
    labourforce = spark.sql("SELECT GEO, REF_DATE, DGUID, 'Both sexes, All age groups, Seasonally adjusted' as statistic_labourforce, 'Persons' as uom_labourforce, 'thousands' as scalar_labourforce, Employment as employment, Full_time_employment as fulltime, \
        Labour_force as labourforce, Part_time_employment as parttime , Population AS population, \
         Unemployment AS unemployment, 'Both sexes, All age groups, Seasonally adjusted' as statistic_labourforceperc, 'Percentage' as uom_lfperc, 'unit' as scalar_lfperc, Participation_rate as participationrate, Employment_rate employment_rate, Unemployment_rate as unemployment_rate FROM labour_info")
    return labourforce
