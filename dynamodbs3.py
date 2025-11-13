from pyspark.sql import SparkSession
import sys
import boto3
import time
import decimal

Spark = SparkSession.builder.appName("Data Retriving Session new").getOrCreate() #Spark Session

AWS_REGION = "ap-south-1" #Resource Region
AWS_ACCESS_KEY = "#####" #Access Key IAM
AWS_SECRET_KEY = "#####" #Secret Key IAM

DDB_TABLES = ["SampleData1", "SampleData2"] #DynamoDB Tables

S3_OUTPUT_BASE = "s3://####/bronzedata/" #Bronze Location

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
) #Used Boto3 Connection to connect Python code with AWS Resources

dynamodb = session.resource('dynamodb') #DynamoDB Resource
print("boto3 DynamoDB session created.")

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

table_name = DDB_TABLES[1]
print(f"Reading {table_name}...")

items = dynamo_table_scan(table_name)
print("Items Retrieved:", len(items))

df = Spark.createDataFrame(items)
df.show(5)
df.printSchema()

s3 = session.client("s3")
out_path = f"{S3_OUTPUT_BASE}"

print("Writing DataFrame to:", out_path)
df.write.mode("overwrite").parquet(out_path)
print("✔ Write finished.")
