import os
from os import listdir
from os.path import join as path_join
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth

def main():
    ENVIRON = os.environ.get("ENVIRON")
    driver_jars_path = path_join(".","jar")
    print(driver_jars_path)
    all_jars = listdir(driver_jars_path)
    print(all_jars)
    all_pathed_jars = [path_join(driver_jars_path, jar)
                                   for jar in all_jars]
    jars_paths = ",".join(all_pathed_jars)
        
       
    ## Environment
    if ENVIRON == 'DEV':
        access_key = os.environ.get("aws_access_key_id")
        secret_key = os.environ.get("aws_secret_access_key")
        print(f'The environment is {ENVIRON}')
        ##os.environ.setdefault('AWS_PROFILE', 'itvgithub')
        print("access_key: "+access_key)
        print("secret_key: "+secret_key)
        spark = SparkSession.builder.master('local').appName("Data Transformation").config('spark.jars', jars_paths).config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.3.6').config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
                    .config('spark.hadoop.fs.s3a.access.key', access_key) \
                    .config('spark.hadoop.fs.s3a.secret.key', secret_key).getOrCreate()
    else:
        spark = SparkSession.builder.master('yarn').appName("Data Transformation").getOrCreate()

    BUCKET=os.environ.get('BUCKET')
    #Preparing JSON RAW DATA
    BRONZE=os.environ.get('BRONZE')
    
    df=spark.read.json(f"s3a://{BUCKET}/{BRONZE}")
    df.printSchema()
    
    #Write RAM Data to Parquet Files
    SILVER=os.environ.get('SILVER')
    
    dfSilver = df.withColumn("year", year("created_at")).withColumn("month",month("created_at")).withColumn("dayofmonth",dayofmonth("created_at"))
    
    dfWrite = dfSilver
    dfWrite.printSchema()
    
    dfWrite.write.mode("overwrite").format("parquet").save(f's3a://{BUCKET}/{SILVER}/')
    spark.stop()

if __name__=='__main__':
    main()



