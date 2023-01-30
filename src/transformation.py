import argparse
import botocore
import boto3
from pyspark.sql.functions import col,lit
from datetime import datetime
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.0.0 pyspark-shell'

BUCKET='inves-dw'
RAW_PATH='raw'
TRUST_PATH='trust'
DW_PATH='datawarehouse'

spark = SparkSession.builder.config("spark.jars", "./postgresql-42.5.1.jar").appName('Tranformation').getOrCreate()
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

def get_path_of_datalake(type):
    match type:
        case "lc":
            return f"hdfs://namenode:9000/datalake"
        case _:
            return f"s3a://{BUCKET}/{TRUST_PATH}"
        
def get_path_of_datawarehouse(type):
    match type:
        case "lc":
            return f"hdfs://namenode:9000/dw"
        case _:
            return f"s3a://{BUCKET}/{DW_PATH}"
        
def execute(type, time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]
        
        path = get_path_of_datalake(type)
        spark.read.options(header='true', inferSchema='true').csv("dim_time.csv").createOrReplaceTempView("dim_time")
        for table in get_tables():
            spark.read.options(header='true', inferSchema='true').parquet(f"{path}/{table}").drop("year", "month", "day").createOrReplaceTempView(table)
        query = spark.sql("""
            select
                oi.*,
                di.key,
                di.year,
                di.quater,
                di.month,
                di.week,
                di.day_of_week,
                di.day_of_month,
                di.day_of_year,
                di.hour_of_day,
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
            join dim_time di on di.key=TO_NUMBER(date_format(o.order_purchase_timestamp, 'yyyymmddHH'), '9999999999')
            join order_items oi on o.order_id=oi.order_id
            join customers c on o.customer_id=c.customer_id
            join products p on p.product_id=oi.product_id
            join sellers s on oi.seller_id=s.seller_id
            join order_payments op on op.order_id=o.order_id
        """)
        
        query.write.mode("overwrite").format("jdbc").option("url", "jdbc:postgresql://db:5432/metastore")\
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "fact") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .save()
        print("Transformation success")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    app_parser = argparse.ArgumentParser(allow_abbrev=False)

    app_parser.add_argument('type',
                            type=str,
                            help='lc - HDFS Local / s3 - S3')
    
    app_parser.add_argument('time',
                            action='store',
                            type=str,
                            help='Time format YYYY-mm-dd')

    args = app_parser.parse_args()
    execute(args.type, args.time)
