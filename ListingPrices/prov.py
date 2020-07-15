import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def main(inputs):
    # main logic starts here
    listing_schema = types.StructType([
    types.StructField('province', types.StringType(), False),
    types.StructField('listprice', types.StringType(), False),
    types.StructField('date_added', types.StringType(), False),
    types.StructField('locality', types.StringType(), False),
    types.StructField('postal_code', types.StringType(), False),
    types.StructField('year_built', types.StringType(), True),
    types.StructField('taxes', types.StringType(), True),
    types.StructField('Basement', types.StringType(), True),
    types.StructField('Lot_size', types.StringType(), True),
    types.StructField('Bed', types.StringType(), True),
    types.StructField('Baths', types.StringType(), True),
    types.StructField('Area', types.StringType(), True),
    types.StructField('monthyr', types.StringType(), True),
    types.StructField('PriceperSqft', types.StringType(), True),
    types.StructField('Age', types.StringType(), True)
    ])
    listingdata = spark.read.csv(inputs, schema=listing_schema).createOrReplaceTempView('listingdata')
    listingdata1 = spark.sql('select cast(listprice as double) as listprice, province from listingdata').createOrReplaceTempView('listingdata1')
    provdf = spark.sql('select province, max(listprice) as maxprice, min(listprice) as minprice, avg(listprice) as avgprice from listingdata1 group by province').show()
    maxdf = spark.sql('select province, max(listprice) as maxprice from listingdata1 group by province').createOrReplaceTempView('maxdf')
    maxdf = spark.sql('select listingdata.*, maxdf.maxprice from listingdata, maxdf where listingdata.listprice=maxdf.maxprice').show()
    mindf = spark.sql('select province, min(listprice) as minprice from listingdata1 group by province').createOrReplaceTempView('mindf')
    mindf = spark.sql('select listingdata.*, mindf.minprice from listingdata, mindf where listingdata.listprice=mindf.minprice').show()


if __name__ == '__main__':
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)



   