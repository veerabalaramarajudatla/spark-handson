from pyspark.sql import SparkSession 
#Here we are importing the Spark Session to create a session in that we can import spark features like Dataframe, RDD , etc.. and execute our Job
#Starting the Spark Session App name = Spark Job name get or create is used to create a session or use the existing session 
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()
list1 = [1, 2, 3, 4, 5] # Sample List
rdd = spark.sparkContext.parallelize(list1) # We are assigning the list into RDD named rdd
rdd_squared = rdd.map(lambda x: x ** 2) # Transformation method - Map() and storing the output to the new RDD named rdd_squared
result = rdd_squared.collect() # Action Method - collect()
print("Squared Numbers:", result)
spark.stop()
