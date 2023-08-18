import json
from download import download

def lambda_handler(event, context):
    # TODO implement
    donwload_res = download('2021-01-29-0.json.gz')
    
    return {
        'statusCode': donwload_res.status_code,
        'body': json.dumps('Download status code!')
    }