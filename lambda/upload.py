import os
import boto3
import requests

os.environ.setdefault('AWS_PROFILE', 'itvgithub')

s3_client = boto3.client('s3')

file = '2021-01-29-0.json.gz'
res = requests.get(f'https://data.gharchive.org/{file}')

upload_res = s3_client.put_object(
    Bucket='gh-data-project',
    Key=file,
    Body=res.content
)

##print(s3_objects['Contents'][0])
print(upload_res)

