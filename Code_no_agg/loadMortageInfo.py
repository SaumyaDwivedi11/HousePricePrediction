import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types
from urllib.request import *

spark = SparkSession.builder.appName('Load Mortage Data').getOrCreate()
#Schema for Mortage Information
mortage_schema = types.StructType([
    types.StructField('date', types.StringType(), True),
    types.StructField('1y_fixed_posted', types.StringType(), True),
    types.StructField('2y_bond', types.StringType(), True),
    types.StructField('3y_bond', types.StringType(), True),
    types.StructField('3y_fixed_posted', types.StringType(), True),
    types.StructField('5y_bond', types.StringType(), True),
    types.StructField('5y_fixed_posted', types.StringType(), True),
    types.StructField('7y_bond', types.StringType(), True),
    types.StructField('10y_bond', types.StringType(), True),
    types.StructField('bank', types.StringType(), True),
    types.StructField('overnight', types.StringType(), True),
    types.StructField('overnight_target', types.StringType(), True),
    types.StructField('prime', types.StringType(), True)])

'''
	 * Description: This method is used to request mortage information, perform transformations and generate an output dataframe 
	 * input: -
	 * output:  DataFrame-> with mortage info per province and year-month
'''
def loadMortageInfo():
    mortage = spark.read.csv("Other_sources/mortgage rate since 1935.csv", schema=mortage_schema).createOrReplaceTempView("mortage")
    transf_year_month = spark.sql(
        "SELECT *, substr(m.date, 1, instr(m.date, '-') + 2) as year_month FROM mortage m").createOrReplaceTempView(
        "mortage_transformed")
    group_by_yearmonth = spark.sql("SELECT year_month as REF_DATE, AVG(1y_fixed_posted) AS 1y_fixed_posted, AVG(2y_bond) as 2y_bond, AVG(3y_bond) AS 3y_bond,\
     AVG(3y_fixed_posted) as 3y_fixed_posted, AVG(5y_bond) as 5y_bond, AVG(5y_fixed_posted) as 5y_fixed_posted, AVG(7y_bond) as 7y_bond, \
     AVG(10y_bond) as 10y_bond, AVG(bank) as bank, AVG(overnight) AS overnight, AVG(overnight_target) as overnight_target, AVG(prime) as prime \
     FROM mortage_transformed GROUP BY year_month")
    return group_by_yearmonth

