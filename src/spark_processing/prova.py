import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestJava").master("local[1]").getOrCreate()
print("Spark avviato con successo")
spark.stop()
spark.sparkContext.stop()


