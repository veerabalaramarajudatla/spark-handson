# Import libraries
from pyspark.sql import SparkSession
import boto3
import io
import pandas
import pyarrow as pa
import pyarrow.parquet as pq

# Create Spark Session
spark = SparkSession.builder.appName("Gold Layer Job").getOrCreate()

# Set AWS Credentials
AWS_REGION = "ap-south-1"
AWS_ACCESS_KEY = "####"
AWS_SECRET_KEY = "####"

# Set S3 Bucket
BUCKET_NAME = "####-datla" 
SILVER = "####/"
GOLD = "####/"

# --------- Reading the Data from S3 Bucket -------
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
bronze_session = session.resource('s3')
print("Connected to S3")
table_name = "sampledata"
print("Table name:", table_name)
directory = f"{SILVER}{table_name}.parquet"
df = bronze_session.Object(BUCKET_NAME, directory ).get()['Body'].read() #Fecthing the data
df = pandas.read_parquet(io.BytesIO(df)) #Reading the parquet file

# --------- Converting the Data to Spark Dataframe -------
df = spark.createDataFrame(df)
print("Data in Silver layer") 
df.show(10)

df = df.select('id', 'name')
df = df.filter(df.id < 1215)

print("Data in Gold layer") 
df.show(10)

# --------- Writing the Data to S3 Bucket -------
pdf = df.toPandas() # Convert to Pandas
table = pa.Table.from_pandas(pdf) # Convert to Arrow Table
buf = io.BytesIO() # Create a buffer
pq.write_table(table, buf) # Write to Parquet
buf.seek(0) # Seek to the beginning of the buffer
# Upload to BRONZE S3 Path using boto3
directory = f"{GOLD}{table_name}.parquet" #Path
s3_client = session.client("s3")
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key=directory,
    Body=buf.getvalue()
)
print("Gold layer parquet file is stored in S3")
