import argparse
import botocore
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

def execute(type, time):
    try:
        arrTime = time.split('-')
        year = arrTime[0]
        month = arrTime[1]
        day = arrTime[2]

        df = spark.read.format('csv').options(header='true', inferSchema='true').load(f"s3a://{BUCKET}/{RAW_PATH}/")
        df.coalesce(1).write.mode("append").option("compression", "snappy").parquet(f"s3a://{BUCKET}/{TRUST_PATH}/{year}/{month}/{day}/")
        print('Process success!')
    except Exception as e:
        print(e)

if __name__ == '__main__':
    app_parser = argparse.ArgumentParser(allow_abbrev=False)
    app_parser.add_argument('--type', ## Possible values: zone_lookup, green, yellow
                            action='store',
                            type=str,
                            required=True,
                            dest='type_opt',
                            help='Set the taxi type that will be extracted data from AWS Open Data.')
    app_parser.add_argument('--time',
                            action='store',
                            type=str,
                            required=True,
                            dest='time_opt',
                            help='Time format YYYY-mm-dd')

    args = app_parser.parse_args()
    execute(args.type_opt, args.time_opt)