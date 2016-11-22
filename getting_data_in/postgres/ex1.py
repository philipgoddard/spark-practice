from pyspark.sql import SparkSession

'''
to get the jdbc driver:
it was easy. i did this at CL

spark-shell --packages org.postgresql:postgresql:9.4.1211

installs in ~/.ivy2/jars/

have to point to the jdbc driver
spark-submit --driver-class-path ~/.ivy2/jars/org.postgresql_postgresql-9.4.1211.jar ex1.py
do the same if using pyspark
'''

# need to do some digging to understand setup options
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


'''
options go in here- such as schema, user and password. my password is hats
note that postgres defaults to 5432
'''
url = 'jdbc:postgresql://localhost:5432/project_001?currentSchema=build_0_0_1&user=phil&password=hats'

df = spark.read.load(format="jdbc", url=url, dbtable = 'l2_001_iris')

print(df.show())
