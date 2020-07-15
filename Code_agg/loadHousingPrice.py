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
spark = SparkSession.builder.appName('Load Housing Price').getOrCreate()
#conf = SparkConf().setAppName('reddit etl')
#sc = SparkContext(conf=conf)


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
	types.StructField('DECIMALS', types.StringType(), True),])

land_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType(), True),
	types.StructField('GEO', types.StringType(), True),
	types.StructField('House_only', types.StringType(), True),
	types.StructField('Land_only', types.StringType(), True),
	types.StructField('Total_house_land', types.StringType(), True),])

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

def loadPriceIndex():
	#PRODUCT ID FOR HOUSE PRICE INDEX.
    productId = "18100205"

    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"+productId + "/en")
    jdata = json.loads(response.text)
    zipUrl = jdata['object']
    pdDF = download_extract_zip(zipUrl)
    #print(pdDF)
    transposeDF = pdDF.pivot_table(index = ['REF_DATE','GEO'], columns='New housing price indexes', values='VALUE').reset_index(['REF_DATE','GEO'])
    land_df = spark.createDataFrame(transposeDF,schema=land_schema).createOrReplaceTempView("land_info")
    housing_df = spark.createDataFrame(pdDF,schema=housing_schema).createOrReplaceTempView("housing")
    get_uom = spark.sql("SELECT uom_id FROM housing GROUP BY uom_id").collect()[0][0]
    get_scalar = spark.sql("SELECT scalar_id FROM housing GROUP BY scalar_id").collect()[0][0]
    uom_df = codesets.getCodeDescription("uom").createOrReplaceTempView("uom")
    scalar_df = codesets.getCodeDescription("scalar").createOrReplaceTempView("scalar")
    get_uom_description = spark.sql("SELECT memberUomCode,memberUomEn FROM uom WHERE memberUomCode = " + get_uom).createOrReplaceTempView("uom_desc")
    get_scalar_description = spark.sql("SELECT scalarFactorCode,scalarFactorDescEn FROM scalar WHERE scalarFactorCode = " + get_scalar).createOrReplaceTempView("scalar_desc")
    
    join_land_housing = spark.sql("SELECT li.REF_DATE, li.GEO,uom.memberUomEn,scalar.scalarFactorDescEn, li.House_only,li.Land_only, li.Total_house_land, li.House_only*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as house_only_uom,\
     li.Land_only*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as land_only_uom, \
     li.Total_house_land*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) as totalhouseland_uom \
      FROM land_info li \
    	INNER JOIN housing h ON li.REF_DATE = h.REF_DATE AND li.GEO = h.GEO \
    	INNER JOIN uom_desc uom ON h.uom_id = uom.memberUomCode \
    	INNER JOIN scalar_desc scalar on h.scalar_id = scalar.scalarFactorCode \
    	GROUP BY li.REF_DATE, li.GEO,uom.memberUomEn, scalar.scalarFactorDescEn,li.House_only,li.Land_only, li.Total_house_land, li.House_only*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1),\
     li.Land_only*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1), \
     li.Total_house_land*substr(uom.memberUomEn, instr(uom.memberUomEn, '=') + 1) \
     ORDER BY li.REF_DATE, li.GEO").createOrReplaceTempView("land_housing_info")

    
    transform_ontario_exception = spark.sql("SELECT *, replace(GEO,'Ottawa-Gatineau, Ontario part, Ontario/Quebec','Ottawa,Ontario') as NEW_GEO FROM land_housing_info").createOrReplaceTempView("ontario_transform")
    transform_quebec_exception = spark.sql("SELECT *, replace(NEW_GEO,'Ottawa-Gatineau, Quebec part, Ontario/Quebec','Ottawa,Quebec') as NEW_GEO_ALL FROM ontario_transform").createOrReplaceTempView("province_transformed")
    
    province = spark.sql("SELECT *,substr(NEW_GEO_ALL, - instr(reverse(NEW_GEO_ALL), ',') + 1) as province FROM province_transformed").createOrReplaceTempView("province_info")
    
    avg_per_province = spark.sql("SELECT TRIM(province) as province,TRIM(REF_DATE) AS REF_DATE,TRIM(memberUomEn) as uom_houseindex,TRIM(scalarFactorDescEn) as scalar_houseindex,AVG(House_only) as avg_house_only, AVG(Land_only) as avg_land_only, AVG(Total_house_land) as avg_totalhouseland\
    FROM province_info GROUP BY TRIM(province),TRIM(REF_DATE),TRIM(memberUomEn),TRIM(scalarFactorDescEn)").coalesce(1)#.createOrReplaceTempView("avg_province")
    #t_province = spark.sql("SELECT DISTINCT(province) as transformed_province FROM province_info")
    #join_all_data = spark.sql("SELECT p.province,p.REF_DATE, p.House_only, p.Land_only, p.Total_house_land, a.House_only, a.Land_only, a.Total_house_land  FROM province_info p LEFT JOIN avg_province a ON a.REF_DATE = p.REF_DATE ")

    #avg_per_province.write.csv("house_only_data",mode='overwrite',header = 'true') 
    return avg_per_province
    
    #d.show()
    

if __name__ == '__main__':
    #type_ = sys.argv[1]
    #output = sys.argv[2]
    main()