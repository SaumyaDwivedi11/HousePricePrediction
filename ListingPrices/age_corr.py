#code for finding correlation between age and of real estate and price
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import math
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
    types.StructField('Age', types.StringType(), True),
    types.StructField('DateAdd', types.StringType(), True),
    ])
    listingdata = spark.read.csv(inputs, schema=listing_schema).createOrReplaceTempView('listingdata')
    listingdata1 = spark.sql('select (cast(Age as double)) as x, (cast(listprice as double)) as y, 1 as cnt from listingdata where Age is not null').createOrReplaceTempView('listingdata1')
    listingdata2 = spark.sql('select x, y, cnt, x*x as x2, y*y as y2, x*y as xy from listingdata1 ').createOrReplaceTempView('listingdata2')
    listingdata3 = spark.sql('select sum(x), sum(y), sum(cnt), sum(x2), sum(y2), sum(xy) from listingdata2')#.createOrReplaceTempView('listingdata3')
    
    lst = listingdata3.collect()
    p = math.sqrt((lst[0][2]*lst[0][3]) - (lst[0][0]**2))
    q = math.sqrt((lst[0][2]*lst[0][4]) - (lst[0][1]**2))
    r = ((lst[0][2]*lst[0][5])-(lst[0][0]*lst[0][1])) / (p*q)
    print("r=",r)
    print("r^2=", (r*r))
    #listingdata6.write.format("csv").save(output)
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)



   
