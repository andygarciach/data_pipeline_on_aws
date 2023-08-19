import os
from download import download_file
from upload import upload_s3
 
def lambda_handler(event, context):
    file = '2021-01-29-2.json.gz'
    download_res = download_file(file)
    bucket = os.environ.get('BUCKET_NAME')
    environ = os.environ.get('ENVIRON')
    file_prefix = os.environ.get('FILE_PREFIX') 
    if environ == 'DEV':
        print(f'Running in {environ} environment')
        os.environ.setdefault('AWS_PROFILE', 'itvgithub')
    upload_res = upload_s3(
        download_res.content,
        bucket,
        f'{file_prefix}/{file}'
    )
    return upload_res