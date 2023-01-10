import argparse
import botocore
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.0.0 pyspark-shell'

BUCKET='inves-dw'
RAW_PATH='raw'
TRUST_PATH='trust'
DW_PATH='datawarehouse'

spark = SparkSession.builder.appName('Example1').getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
spark.conf.set("spark.debug.maxToStringFields", 10000)

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


def execute(time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]

        df = spark.read.format('parquet').options(header='true', inferSchema='true').option("recursiveFileLookup", "true").load(f"s3a://{BUCKET}/{DW_PATH}/").createOrReplaceTempView('dwh')
        query = spark.sql('select * from dwh')
        print(query.count())
    except Exception as e:
        print(e)


if __name__ == '__main__':
    app_parser = argparse.ArgumentParser(allow_abbrev=False)

    app_parser.add_argument('time',
                            action='store',
                            type=str,
                            help='Time format YYYY-mm-dd')

    args = app_parser.parse_args()
    execute(args.time)
