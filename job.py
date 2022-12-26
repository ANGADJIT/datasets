from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('emr').getOrCreate()

numbers = [ i for i in range(100)]

rdd = spark.sparkContext.parallelize(numbers)

print(rdd.collect())