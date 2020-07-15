import requests
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import json
spark = SparkSession.builder.appName('Get Code Sets').getOrCreate()



#Schema structure for all unit of measures used.

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

'''
	 * Description: This method is used to decode based on the unit to retrieve which schema to use.
	 * input: String -> type of codeset (i.e.  scalar, uom, etc)
	 * output: String -> Schema to use.
'''
def options(type_):
	options = {"scalar" : scalar_schema,
           "frequency" : frequency_schema,
           "symbol" : symbol_schema,
           "uom" : uom_schema,
           "classificationType" : classification_schema,
	}
	return options[type_]


'''
	 * Description: This method is used to get all the codesets. We used this method to retrieve the unit of measure and scalar unit descriptions.
	 * input: String -> type of codeset (i.e.  scalar, uom, etc)
	 * output: DataFrame -> with all the specified unit descriptions.
'''
def getCodeDescription(type_):
	#Gather all code sets using the next webservice
    response = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getCodeSets")
    jdata = json.loads(response.text)
    keys = jdata['object']
    rddData = spark.sparkContext.parallelize(keys[type_])
    schema_to_use = options(type_)
    resultDF = spark.read.json(rddData, schema = schema_to_use)
    return resultDF
