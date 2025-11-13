from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkSession").getOrCreate()
print("Successfully Launched the Spark Session")

data = [("Alice", 25, "NY"), ("Bob", 30, "SF"), ("Cathy", 28, "LA")]
cols = ["name", "age", "city"]
df = spark.createDataFrame(data, cols)

df.show()
df.printSchema()
