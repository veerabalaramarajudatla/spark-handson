#------- Library -------
from pyspark.sql import SparkSession
import sys
import boto3
import time
import decimal
from pyspark.sql.types import *
from pyspark.sql import Row
import json
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ------- Spark Session -------
Spark = SparkSession.builder.appName("Data Retriving Session new").getOrCreate() #Spark Session

# ------- AWS Credentials -------
AWS_REGION = "ap-south-1" #Resource Region
AWS_ACCESS_KEY = "####" #Access Key IAM
AWS_SECRET_KEY = "####" #Secret Key IAM

# ------- DynamoDB & S3 -------
DDB_TABLES = ["sampledata"] #DynamoDB Tables

BUCKET_NAME = "####-datla"
BRONZE = "####/"


# ------- Boto3 Session for Dynamo DB -------
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
) #Used Boto3 Connection to connect Python code with AWS Resources

dynamodb = session.resource('dynamodb') #DynamoDB Resource
print("boto3 DynamoDB session created.")

# -------  Main Function -------
#By Default Dynamo DB Returns us the Decimal Values here we are converting the decimal values into int or float
def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(v) for v in obj] 
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()} #Json
    if isinstance(obj, decimal.Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def dynamo_table_scan(table_name):  #Scanning the full table in the Dynamo DB
    table = dynamodb.Table(table_name)
    items = []
    params = {}
    
    while True:
        response = table.scan(**params)
        batch = response.get("Items", [])
        for it in batch:
            items.append(convert_decimals(it))
        
        # Check for more results
        if "LastEvaluatedKey" not in response:
            break
        params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    
    return items

# ------- Testing of retriving the data from the Dynamo DB -------
table_name = DDB_TABLES[0]
print(f"Reading {table_name}...")

items = dynamo_table_scan(table_name)
print("Items Retrieved:", len(items))

df = Spark.createDataFrame(items)
df.show(5)
df.printSchema()

# ------- Boto3 Session for S3 Bucket -------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
print("boto3 S3 session created.")

# --------- Uploading the Data to S3 Bucket -------
# csv_buffer = io.StringIO()
# pdf = df.toPandas() # Convert to Pandas
# pdf.to_csv(csv_buffer, index=False) # Convert to CSV
# # Upload CSV
# s3.put_object(
#     Bucket=BUCKET_NAME,
#     Key=BRONZE,
#     Body=csv_buffer.getvalue()
# )
# print("CSV uploaded to S3 Bronze")

# --------- Writing the Data to S3 Bucket -------
pdf = df.toPandas() # Convert to Pandas
table = pa.Table.from_pandas(pdf) # Convert to Arrow Table
buf = io.BytesIO() # Create a buffer
pq.write_table(table, buf) # Write to Parquet
buf.seek(0) # Seek to the beginning of the buffer
# Upload to BRONZE S3 Path using boto3
directory = f"{BRONZE}{table_name}.parquet" #Path
s3_client = session.client("s3")
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key=directory,
    Body=buf.getvalue()
)
print("Parquet Converted to S3")
