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
    types.StructField('DECIMALS', types.StringType(), True),])

labource_charact_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType(), True),
	types.StructField('GEO', types.StringType(), True),
	types.StructField('Employment', types.FloatType(), True),
    types.StructField('Employment_rate', types.FloatType(), True),
    types.StructField('Full_time_employment', types.FloatType(), True),
    types.StructField('Labour_force', types.FloatType(), True),
    types.StructField('Part_time_employment', types.FloatType(), True),
    types.StructField('Participation_rate', types.FloatType(), True),
    types.StructField('Population', types.FloatType(), True),
    types.StructField('Unemployment', types.FloatType(), True),
    types.StructField('Unemployment_rate', types.FloatType(), True),])
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


def loadLabourForceData():
	#PRODUCT ID FOR LABOUR FORCE.
    productId = "14100287"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #print(pdDF)
    new_df = pdDF.loc[pdDF['Sex'].isin(['Both sexes'])]
    seasonally_adjusted = new_df.loc[pdDF['Data type'].isin(['Seasonally adjusted'])]
    statistics = seasonally_adjusted.loc[pdDF['Statistics'].isin(['Estimate'])]

    transposeDF = statistics.pivot_table(index = ['REF_DATE','GEO'], columns='Labour force characteristics', values='VALUE').reset_index(['REF_DATE','GEO'])
    labour_ch_df = spark.createDataFrame(transposeDF,schema=labource_charact_schema).createOrReplaceTempView("labour_info")

    avg_labourforce = spark.sql("SELECT GEO as province, REF_DATE,'Both sexes, All age groups, Seasonally adjusted' as statistic_labourforce, 'Persons' as uom_labourforce, 'thousands' as scalar_labourforce, AVG(Employment) as avg_employment, AVG(Full_time_employment) AS avg_fulltime, \
        AVG(Labour_force) as avg_labourforce, AVG(Part_time_employment) as avg_parttime , AVG(Population) AS avg_population, \
         AVG(Unemployment) AS avg_unemployment, 'Both sexes, All age groups, Seasonally adjusted' as statistic_labourforceperc,'Percentage' as uom_lfperc, 'unit' as scalar_lfperc,AVG(Participation_rate) as avg_participationrate, AVG(Employment_rate) as avg_employment_rate,AVG(Unemployment_rate) as avg_unemploymentrate FROM labour_info GROUP BY GEO,REF_DATE")
    return avg_labourforce
