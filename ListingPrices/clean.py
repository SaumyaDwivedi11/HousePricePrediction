import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StringType,DateType
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def main(inputs,output):
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
    types.StructField('Area', types.StringType(), True)
    ])
    listingdata = spark.read.csv(inputs, schema=listing_schema).createOrReplaceTempView('listingdata')
    listingdata_price = spark.sql('select * from listingdata where province is not null and listprice is not null and province is not null and date_added is not null')
    udf1 = udf(lambda x:x[2:],StringType()) 
    udf2 = udf(lambda x: x.split(','))
    udf3 = udf(lambda x: ''.join(x))
    listingdata_price.withColumn('list_price',udf1('listprice')).withColumn('list_price1', udf2('list_price')).withColumn('list_price2', udf3('list_price1')).createOrReplaceTempView('listingdata1')
    listingdata1 = spark.sql('select province, list_price2, date_added, locality,postal_code, year_built, taxes, Basement, Lot_size, Bed, Baths, Area from listingdata1')
    udf4 = udf(lambda x:x.split('-'))
    udf5 = udf(lambda x: ''.join(x[1:3]))
    udf6 = udf(lambda x: x[::-1])
    udf7 = udf(lambda x: '-'.join(x[0:3]))
    udf8 = udf(lambda x: month_convert(x))
    udf9 = udf(lambda x: year(x))
    listingdata2 = listingdata1.withColumn('month-yr1', udf4('date_added')).withColumn('monthyr', udf5('month-yr1')).drop('month-yr1')
    listingdata3 = listingdata2.fillna({'year_built':'2018'})
    listingdata3 = listingdata3.withColumn('year_built',udf9('year_built')).createOrReplaceTempView('listingdata3')
    
    avg_area = spark.sql('select avg(cast(Area as int)) from listingdata3').collect()[0][0]
    listingdata4 = spark.sql('select * from listingdata3')
    listingdata5 = listingdata4.fillna({'Area':avg_area}).withColumn('Area2', udf2('Area')).withColumn('area', udf3('Area2'))
    listingdata6 = listingdata5.drop(listingdata5['Area2'])
    listingdata6 = listingdata6.withColumn('Price/SqFt',listingdata5['list_price2']/listingdata5['Area'])
    listingdata7 = listingdata6.withColumn('Age',(2018 - listingdata6['year_built']))
    listingdata8 = listingdata7.withColumn('DateAdd',udf8('date_added'))
    listingdata8.write.format("csv").save(output)
    
def month_convert(dt):
    dt = dt.split('-')
    
    dict_month = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
    
    for key,val in dict_month.items():
        if key == dt[1]:
            mm = val

    return '20'+dt[2]+'-'+mm+'-'+dt[0]


def year(yr):
    if(int(yr) > 2018):
        yr = 2018
    return yr

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)



   