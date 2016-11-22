# spark 2.0.0

from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext
from pyspark.sql.types import *

# this is spark 2x syntax, 1.6x needs a spark and sql context defined?
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.load("iris.csv",
                      format ="csv",
                      header = "true",
                      inferSchema = "true")

df.printSchema()

# customise schema:
# types are here (may be a little outdated for 2x:
# http://spark.apache.org/docs/1.6.1/api/python/_modules/pyspark/sql/types.html

customSchema = StructType([ \
    StructField("petal_length", DoubleType(), nullable = False), \
    StructField("petal_width", DoubleType(), nullable = False), \
    StructField("sepal_length", DoubleType(), nullable = False), \
    StructField("sepal_width", DoubleType(), nullable = False), \
    StructField("species", DoubleType(), nullable = False)])

df2 = spark.read.load("iris.csv",
                      format ="csv",
                      header = "true",
                      schema = customSchema)

# all equivilant syntax
df2.select(df2.petal_length, df2.petal_width)
df2.select(['petal_length', 'petal_width'])
df2.select('petal_length', 'petal_width')

print(df.show())
print(df.take(10))

print(df.count())

print(df.dtypes)

#df.collect() can be dangerous as returns all to driver- can cause memory error!


# write back:
(df
  .select('petal_length', 'petal_width', 'species')
  .write
  .format('csv')
  .save('newiris'))
