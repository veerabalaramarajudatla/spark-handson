from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkSession").getOrCreate()
print("Successfully Launched the Spark Session")
