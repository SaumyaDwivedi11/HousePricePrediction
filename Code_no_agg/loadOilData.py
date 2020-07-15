import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, types, functions as F
from io import *
import pandas as pd
from urllib.request import *

spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "70g").config("spark.driver.memory",
                                                                                              "50g").config(
    "spark.memory.offHeap.enabled", True).config("spark.memory.offHeap.size", "32g").config(
    "spark.driver.maxResultSize", "10g").appName("Load Labour Force Data").getOrCreate()

#Schema for Oil information
oil_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('Diesel_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Diesel_self_service_filling_stations', types.FloatType(), True),
    types.StructField('Premium_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Premium_self_service_filling_stations', types.FloatType(), True),
    types.StructField('Regular_full_service_filling_stations', types.FloatType(), True),
    types.StructField('Regular_self_service_filling_stations', types.FloatType(), True), ])

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
	 * Description: This method is used to request house prince index information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with HPI info per province and year-month
'''
def get_dguid():
    productId = "18100205"
    response = requests.get( "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #Transpose to have features as columns.
    transposeDF = pdDF.pivot_table(index=['REF_DATE', 'GEO', 'DGUID'], columns='New housing price indexes', values = 'VALUE').reset_index(['REF_DATE', 'GEO', 'DGUID'])
    land_schema = types.StructType([
        types.StructField('REF_DATE', types.StringType(), True),
        types.StructField('GEO', types.StringType(), True),
        types.StructField('DGUID', types.StringType(), True),
        types.StructField('House_only', types.StringType(), True),
        types.StructField('Land_only', types.StringType(), True),
        types.StructField('Total_house_land', types.StringType(), True)])
    return spark.createDataFrame(transposeDF, schema=land_schema).select('GEO', 'DGUID').drop_duplicates()

'''
	 * Description: This method is used to request oil information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with oil info per province and year-month
'''
def loadOilData():
    # PRODUCT ID FOR OIL DATA.
    productId = "18100001"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #Filter the features needed.
    new_df = pdDF.loc[pdDF['Type of fuel'].isin(
        ['Diesel fuel at full service filling stations', 'Diesel fuel at self service filling stations', \
         'Premium unleaded gasoline at full service filling stations',
         'Premium unleaded gasoline at self service filling stations', \
         'Regular unleaded gasoline at full service filling stations',
         'Regular unleaded gasoline at self service filling stations'])]
    #Transpose df to have features as column headers
    transposeDF = new_df.pivot_table(index=['REF_DATE', 'GEO', 'DGUID'], columns='Type of fuel', values='VALUE').reset_index(['REF_DATE', 'GEO', 'DGUID'])
    oil_df = spark.createDataFrame(transposeDF, schema=oil_schema).createOrReplaceTempView("oil_info")
    #Catch exceptions with cities so they are taking into account in the aggregation by province.
    transform_ontario_exception = spark.sql("SELECT *, replace(GEO,'Ottawa-Gatineau, Ontario part, Ontario/Quebec','Ottawa,Ontario') as NEW_GEO FROM oil_info").createOrReplaceTempView("ontario_transform")
    transform_quebec_exception = spark.sql("SELECT *, replace(NEW_GEO,'Ottawa-Gatineau, Quebec part, Ontario/Quebec','Ottawa,Quebec') as NEW_GEO_ALL FROM ontario_transform").createOrReplaceTempView("province_transformed")
    province = spark.sql("SELECT *, substr(NEW_GEO_ALL, - instr(reverse(NEW_GEO_ALL), ',') + 1) as province FROM province_transformed").createOrReplaceTempView("province_info")
    avg_per_province = spark.sql("SELECT TRIM(province) as GEO, REF_DATE, 'Cents per litre' as uom_oil, 'units' as scalar_oil, \
    AVG(Diesel_full_service_filling_stations) as Diesel_full_service_filling_stations, AVG(Diesel_self_service_filling_stations) as Diesel_self_service_filling_stations, \
    AVG(Premium_full_service_filling_stations) as Premium_full_service_filling_stations, AVG(Premium_self_service_filling_stations) as Premium_self_service_filling_stations , \
    AVG(Regular_full_service_filling_stations) as Regular_full_service_filling_stations, AVG(Regular_self_service_filling_stations) as Regular_self_service_filling_stations \
    FROM province_info GROUP BY TRIM(province), REF_DATE")
    dguid = get_dguid()
    oil_df_1 = spark.sql("select *, 'Cents per litre' as uom_oil, 'units' as scalar_oil from oil_info")
    tmp = avg_per_province.join(dguid, ['GEO'], 'inner').select(*oil_df_1.columns)
    oil_df_2 = oil_df_1.union(tmp).select('GEO', 'REF_DATE', 'DGUID', F.lit('Cents per litre').alias('uom_oil'), \
                                          F.lit('units').alias('scalar_oil'), F.col('Diesel_full_service_filling_stations').alias('diesel_fillingstations'), F.col('Diesel_self_service_filling_stations').alias('diesel_selfservstations'), \
                                          F.col('Premium_full_service_filling_stations').alias('premium_fillingstations'), F.col('Premium_self_service_filling_stations').alias('premium_selfservstations'), \
                                          F.col('Regular_full_service_filling_stations').alias('regular_fillingstations'), F.col('Regular_self_service_filling_stations').alias('regular_selfservstations'))
    return oil_df_2

