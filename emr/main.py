import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
from botocore.exceptions import NoCredentialsError
from os import listdir
from os.path import join as path_join


ENVIRON=os.environ.get('ENVIRON')
BUCKET=os.environ.get('BUCKET')
access_key=os.environ.get('aws_access_key_id')
secret_key=os.environ.get('aws_secret_access_key')

driver_jars_path = path_join("..","emr","jar")
all_jars = listdir(driver_jars_path)
all_pathed_jars = [path_join(driver_jars_path, jar)
                           for jar in all_jars]
jars_paths = ",".join(all_pathed_jars)
print(jars_paths)

## Environment
if ENVIRON == 'DEV':
    print(f'The environment is {ENVIRON}')
    ##os.environ.setdefault('AWS_PROFILE', 'itvgithub')
    print("access_key: "+access_key)
    print("secret_key: "+secret_key)


spark = SparkSession.builder.appName("Data Transformation").config('spark.jars', jars_paths).config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.3.6').config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
                .config('spark.hadoop.fs.s3a.access.key', access_key) \
                .config('spark.hadoop.fs.s3a.secret.key', secret_key).getOrCreate()

df=spark.read.json("s3a://gh-data-project/bronze")
df.printSchema()




