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

def get_tables():
    try:
        dw = s3_resource.Bucket(BUCKET).objects.filter(Prefix="raw/")
        tables = set()
        for e in dw:
            arr = e.key.split("/")
            if(arr[1] != ''):
                tables.add(arr[1])
        if len(tables) > 0:
            return tables
        return TABLES
    except Exception as e:
        return TABLES

def execute(time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]
        for table in get_tables():
            spark.read.format('parquet').options(header='true', inferSchema='true').load(f"s3a://{BUCKET}/{TRUST_PATH}/{table}/{year}/{month}/{day}/").createOrReplaceTempView(table)
        query = spark.sql("""
            select
                oi.*,
                o.customer_id,
                o.order_status,
                o.order_purchase_timestamp,
                o.order_approved_at,
                o.order_delivered_carrier_date,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                s.seller_city,
                s.seller_state,
                p.product_category_name,
                p.product_name_lenght,
                p.product_description_lenght,
                p.product_photos_qty,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                c.customer_unique_id,
                c.customer_city,
                c.customer_state,
                op.payment_sequential,
                op.payment_type,
                op.payment_installments,
                op.payment_value
            from orders o
            join order_items oi on o.order_id=oi.order_id
            join customers c on o.customer_id=c.customer_id
            join products p on p.product_id=oi.product_id
            join sellers s on oi.seller_id=s.seller_id
            join order_payments op on op.order_id=o.order_id
        """)
        # print(query.count())
        query.coalesce(1).write.mode("append").option("compression", "snappy").parquet(f"s3a://{BUCKET}/{DW_PATH}/{year}/{month}/{day}/")
        print("Transformation success")
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
