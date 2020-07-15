import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types
from io import *
import pandas as pd
from urllib.request import *

spark = SparkSession.builder.appName('Load Crime Data').getOrCreate()

#Schema for Consumer Price Index
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
	 * Description: This method is used to request crime information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with crime info per province and year-month
'''
def loadCrimeData():
    # PRODUCT ID FOR CRIME INFO.
    productId = "35100177"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    # filter out only crime features that are going to be used in the HPI analysis.
    violations_df = pdDF.loc[pdDF['Violations'].isin(['Total, all violations'])]
    alltypesviol_df = violations_df.loc[pdDF['Statistics'].isin(['Rate per 100,000 population'])]
    crime_df = spark.createDataFrame(alltypesviol_df, schema=crime_schema).createOrReplaceTempView("crime_info")
    crime_rates = spark.sql("SELECT GEO, REF_DATE, DGUID, 'Number' as uom_crime, 'unit' as scalar_crime, 'Total of all violations-Rate per 100,000 population' as statistic_crime, VALUE as crime_incidents FROM crime_info")
    return crime_rates
