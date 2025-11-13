from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkSession").getOrCreate()
print ("Spark Session Conected")

df_csv = spark.read.csv("/Volumes/workspace/mystorageschema/mydata/Machine_Raw(Sheet1).csv",header=True)
print("copied")
#print(df_csv)
#display(df_csv)
df_csv.show()
