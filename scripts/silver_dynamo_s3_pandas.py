from pyspark.sql import SparkSession
import sys
import io
import boto3
import decimal
import pandas
import pyarrow as pa
import pyarrow.parquet as pq

spark = SparkSession.builder.appName("Silver Job").getOrCreate()

AWS_REGION = "ap-south-1"
AWS_ACCESS_KEY = "####"
AWS_SECRET_KEY = "####"

BUCKET_NAME = "####-datla"
BRONZE = "####/" 
SILVER = "####/"

# ------- Boto3 Session for S3 ------- 
#Used Boto3 Connection to connect Python code with AWS Resource
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
bronze_session = session.resource('s3')
print("Connected to S3")

# ------- Reading the Data from S3 Bucket -------
table_name = "sampledata"
print("Table name:", table_name)
directory = f"{BRONZE}{table_name}.parquet"
df = bronze_session.Object(BUCKET_NAME, directory ).get()['Body'].read() #Fecthing the data
df = pandas.read_parquet(io.BytesIO(df)) #Reading the parquet file

# --------- Converting the Data to Spark Dataframe -------
df = spark.createDataFrame(df)
print("Data in Bronze layer") 
df.show(10)

# --------- Dropping the null values -------
df = df.dropna()
print("Droped the null values")
print("Data Stroing in the Silver layer")
df.show(10)

# --------- Writing the Data to S3 Bucket -------
pdf = df.toPandas() # Convert to Pandas
table = pa.Table.from_pandas(pdf) # Convert to Arrow Table
buf = io.BytesIO() # Create a buffer
pq.write_table(table, buf) # Write to Parquet
buf.seek(0) # Seek to the beginning of the buffer
# Upload to BRONZE S3 Path using boto3
directory = f"{SILVER}{table_name}.parquet" #Path
s3_client = session.client("s3")
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key=directory,
    Body=buf.getvalue()
)
print("Silver layer parquet file is stored in S3")
