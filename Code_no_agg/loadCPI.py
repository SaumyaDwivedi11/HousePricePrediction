import json
import requests
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types
from io import *
import pandas as pd
from urllib.request import *
spark = SparkSession.builder.appName('Load Consumer Price Index Data').getOrCreate()

#Schema for Consumer Price Index
cpi_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('VALUE', types.StringType(), True),])

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
	 * Description: This method is used to request consumer price index information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with CPI info per province and year-month
'''
def loadCPI():
	#PRODUCT ID FOR CPI INFORMATION
    productId = "18100256"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #filter out only cpi features that are going to be used in the HPI analysis.
    cpi_info= pdDF.loc[pdDF['Alternative measures'].isin(['Measure of core inflation based on a factor model, CPI-common (year-over-year percent change)'])]
    #transpose table to have the features as columns.
    transposeDF = cpi_info.pivot_table(index = ['REF_DATE','GEO'], columns='Alternative measures', values='VALUE').reset_index(['REF_DATE','GEO'])
    cpi_df = spark.createDataFrame(transposeDF,schema=cpi_schema).createOrReplaceTempView("cpi_info")
    avg_cpi_index = spark.sql("SELECT GEO, REF_DATE, 'Percent' as uom_cpi, 'unit' as scalar_cpi, VALUE as cpi_index FROM cpi_info")
    return avg_cpi_index

    
