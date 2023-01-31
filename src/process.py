import argparse
import botocore
import boto3
from datetime import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.0.0 pyspark-shell'

BUCKET = 'inves-dw'
RAW_PATH = 'raw'
TRUST_PATH = 'trust'
DW_PATH = 'datawarehouse'

spark = SparkSession.builder.appName('Example1').getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "com.amazonaws.auth.profile.ProfileCredentialsProvider")
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

TABLES = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "category_name"
]


def get_tables():
    try:
        dw = s3_resource.Bucket(BUCKET).objects.filter(Prefix="raw/")
        tables = set()
        for e in dw:
            arr = e.key.split("/")
            if (arr[1] != ''):
                tables.add(arr[1])
        if len(tables) > 0:
            return tables
        return TABLES
    except Exception as e:
        return TABLES


def from_s3(time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]
        for table in get_tables():
            df = spark.read.format('csv').options(header='true', inferSchema='true').load(
                f"s3a://{BUCKET}/{RAW_PATH}/{table}/{year}/{month}/10/")
            df.coalesce(1).write.mode("append").option("compression", "snappy").parquet(
                f"s3a://{BUCKET}/{TRUST_PATH}/{table}/{year}/{month}/{day}/")
        print('Process success!')
    except Exception as e:
        print(e)


def from_hdfs(time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]

        for table in get_tables():
            df = spark.read.format('csv').options(header='true', inferSchema='true').load(
                f"data/{table}/{year}/{month}/{day}/")
            my_schema = df.schema()
            columns = []
            for field in my_schema.fields():
                columns.append(field.name()+" "+field.dataType().typeName())
            columns_string = ",".join(columns)
            print(columns_string + "\n")
            outputDf = df.withColumn("year", lit(year)).withColumn(
                "month", lit(month)).withColumn("day", lit(day))
            outputDf.write.mode("overite").partitionBy("year", "month", "day").parquet(
                f"hdfs://namenode:9000/user/hive/warehouse/revenue/{table}")
        print('Process success!')
    except Exception as e:
        print(e)


def execute(type, time):
    match type:
        case "lc":
            from_hdfs(time)
        case _:
            from_s3(time)


if __name__ == '__main__':
    app_parser = argparse.ArgumentParser(allow_abbrev=False)
    app_parser.add_argument('type',
                            type=str,
                            help='lc - HDFS Local / s3 - S3')

    app_parser.add_argument('time',
                            type=str,
                            help='Time format YYYY-mm-dd')

    args = app_parser.parse_args()
    execute(args.type, args.time)
