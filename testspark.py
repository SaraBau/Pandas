from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('demo1').getOrCreate()

df = spark.createDataFrame([("joe", 34), ("luisa", 22)], ["first_name", "age"])
df.show()