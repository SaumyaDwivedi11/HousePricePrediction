import requests
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pandas.io.json import json_normalize
import pandas as pd
import json
spark = SparkSession.builder.appName('Get Code_agg Sets').getOrCreate()



scalar_schema = types.StructType([
	types.StructField('scalarFactorCode', types.IntegerType(), True),
	types.StructField('scalarFactorDescEn', types.StringType(), True),
	types.StructField('scalarFactorDescFr', types.StringType(), True),])

frequency_schema = types.StructType([
	types.StructField('frequencyCode', types.IntegerType(), True),
	types.StructField('frequencyDescEn', types.StringType(), True),
	types.StructField('frequencyDescFr', types.StringType(), True),])

symbol_schema = types.StructType([
	types.StructField('symbolCode', types.StringType(), False),
	types.StructField('symbolRepresentationEn', types.StringType(), False),
	types.StructField('symbolRepresentationFr', types.StringType(), False ),
	types.StructField('symbolDescEn', types.StringType(), True),
	types.StructField('symbolDescFr', types.StringType(), True),])


uom_schema = types.StructType([
	types.StructField('memberUomCode', types.IntegerType(), True),
	types.StructField('memberUomEn', types.StringType(), True),
	types.StructField('memberUomFr', types.StringType(), True),])


classification_schema = types.StructType([
	types.StructField('classificationTypeCode', types.IntegerType(), True),
	types.StructField('classificationTypeEn', types.StringType(), True),
	types.StructField('classificationTypeFr', types.StringType(), True),])

def options(type_):
	options = {"scalar" : scalar_schema,
           "frequency" : frequency_schema,
           "symbol" : symbol_schema,
           "uom" : uom_schema,
           "classificationType" : classification_schema,
	}
	return options[type_]

def getCodeDescription(type_):
    #data = {"productId":18100205}
    #response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getCodeSets", json=data)
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getCodeSets")
    jdata = json.loads(response.text)
    keys = jdata['object']
    rddData = spark.sparkContext.parallelize(keys[type_])
    schema_to_use = options(type_)
    resultDF = spark.read.json(rddData, schema = schema_to_use)
    return resultDF
