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

spark = SparkSession.builder.appName('Load Housing Price').getOrCreate()

#Schema for House information
housing_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('New_housing_price_indexes', types.StringType(), True),
    types.StructField('UOM', types.StringType(), True),
    types.StructField('UOM_ID', types.StringType(), True),
    types.StructField('SCALAR_FACTOR', types.StringType(), True),
    types.StructField('SCALAR_ID', types.StringType(), True),
    types.StructField('VECTOR', types.StringType(), True),
    types.StructField('COORDINATE', types.StringType(), True),
    types.StructField('VALUE', types.StringType(), True),
    types.StructField('STATUS', types.StringType(), True),
    types.StructField('SYMBOL', types.StringType(), True),
    types.StructField('TERMINATED', types.StringType(), True),
    types.StructField('DECIMALS', types.StringType(), True), ])

#Schema for Feature information only
land_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType(), True),
    types.StructField('GEO', types.StringType(), True),
    types.StructField('DGUID', types.StringType(), True),
    types.StructField('House_only', types.StringType(), True),
    types.StructField('Land_only', types.StringType(), True),
    types.StructField('Total_house_land', types.StringType(), True), ])

'''
	 * Description: This method is used to download and extract the zip file contents in memory.
	 * input: String -> url of response.
	 * output:  -> Panda DataFrame -> file contents.
'''
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

'''
	 * Description: This method is used to request house price index information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with house price info per province and year-month
'''
def loadPriceIndex():
    # PRODUCT ID FOR HOUSE PRICE INDEX.
    productId = "18100205"
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/" + productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)

    #Transpose df to have features as columns
    transposeDF = pdDF.pivot_table(index=['REF_DATE', 'GEO', 'DGUID'], columns='New housing price indexes',
                                   values='VALUE').reset_index(['REF_DATE', 'GEO', 'DGUID'])
    land_df = spark.createDataFrame(transposeDF, schema=land_schema).createOrReplaceTempView("land_info")
    housing_df = spark.createDataFrame(pdDF, schema=housing_schema).createOrReplaceTempView("housing")

    #Get uom and scalar description
    get_uom = spark.sql("SELECT uom_id FROM housing GROUP BY uom_id").collect()[0][0]
    get_scalar = spark.sql("SELECT scalar_id FROM housing GROUP BY scalar_id").collect()[0][0]
    uom_df = codesets.getCodeDescription("uom").createOrReplaceTempView("uom")
    scalar_df = codesets.getCodeDescription("scalar").createOrReplaceTempView("scalar")
    get_uom_description = spark.sql(
        "SELECT memberUomCode, memberUomEn FROM uom WHERE memberUomCode = " + get_uom).createOrReplaceTempView(
        "uom_desc")
    get_scalar_description = spark.sql(
        "SELECT scalarFactorCode, scalarFactorDescEn FROM scalar WHERE scalarFactorCode = " + get_scalar).createOrReplaceTempView(
        "scalar_desc")
    join_land_housing = spark.sql("SELECT li.REF_DATE, li.GEO, li.DGUID, uom.memberUomEn, scalar.scalarFactorDescEn, li.House_only, \
    li.Land_only, li.Total_house_land, li.House_only*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as house_only_uom,\
     li.Land_only * substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as land_only_uom, \
     li.Total_house_land * substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as totalhouseland_uom \
      FROM land_info li \
        INNER JOIN housing h ON li.REF_DATE = h.REF_DATE AND li.GEO = h.GEO \
        INNER JOIN uom_desc uom ON h.uom_id = uom.memberUomCode \
        INNER JOIN scalar_desc scalar on h.scalar_id = scalar.scalarFactorCode \
     ORDER BY li.REF_DATE, li.GEO").createOrReplaceTempView("land_housing_info")
    avg_per_province = spark.sql("SELECT TRIM(GEO) as GEO, TRIM(DGUID) AS DGUID, TRIM(REF_DATE) AS REF_DATE,TRIM(memberUomEn) as uom_houseindex, \
    TRIM(scalarFactorDescEn) as scalar_houseindex, AVG(House_only) as house_only, AVG(Land_only) as land_only, AVG(Total_house_land) as total_house_land \
    FROM land_housing_info GROUP BY TRIM(GEO),TRIM(REF_DATE), TRIM(DGUID),TRIM(memberUomEn),TRIM(scalarFactorDescEn)").coalesce(1)
    return avg_per_province

