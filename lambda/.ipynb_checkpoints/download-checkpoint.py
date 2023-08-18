import requests

def download(file):
    res = requests.get(f'https://data.gharchive.org/{file}')
    return res
