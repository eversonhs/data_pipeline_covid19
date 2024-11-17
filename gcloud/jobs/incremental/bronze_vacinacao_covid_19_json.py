import argparse
import requests
from google.cloud import storage
import logging
import json
from datetime import datetime

# Execution Date
parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str, required=True)
known_args, args_cli = parser.parse_known_args()
ingest_date = datetime.strptime(known_args.date, "%Y-%m-%d")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info('Initializing application')
# Request Config
base_url = 'https://imunizacao-es.saude.gov.br/'
# Não é boa prática, mas as credencias estão disponível no link da api de forma pública
username = "imunizacao_public"
password = "qlto5t&7r_@+#Tlstigi"

headers = {
    'Content-Type': 'application/json',
    'Connection': 'keep-alive'
}

query = {
    "size": 10000,
    "query": {
        "match": {
            "vacina_dataAplicacao": ingest_date.strftime('%Y-%m-%dT00:00:00.000Z')
        }
    },
    "sort": [
        {"vacina_dataAplicacao": {"order": "asc", "format": "strict_date_optional_time_nanos"}},
        {"document_id": "asc"}
    ]
}

# GCS Bucket
bronze_bucket = "pgii-bronze"
storage_client = storage.Client()
bucket = storage_client.bucket(bronze_bucket)
partitioning_folders='{0:4d}/{1:02d}/{2:02d}'.format(ingest_date.year, ingest_date.month, ingest_date.day)
file_directory = "/".join(["json", partitioning_folders])

# Making requests
getting_data = True
iterator = 1
while getting_data:
    filename = f"{ingest_date.strftime('%Y-%m-%d')}_{iterator}.json"
    url = '/'.join([base_url, 'desc-imunizacao', '_search'])
    logger.info(f'Iteration {iterator}')
    logger.info(f'Requesting data to {url}')
    logger.info(f'Requesting data that matches vacina_dataAplicacao={query["query"]["match"]["vacina_dataAplicacao"]}')
    req = requests.request(method='get', url=url, data=json.dumps(query), headers=headers, auth=(username, password))
    logger.info(f'Receive response')
    response = req.json()
    try:
        data = response["hits"]["hits"]
        latest_sort = data[-1]['sort']
        query.update({
            'search_after': latest_sort
        })
        blob = bucket.blob("/".join([file_directory, filename]))
        with blob.open("w") as file:
            logger.info(f'Writing to file {filename}')
            file.write(json.dumps(data))
            logger.info(f'Written to file {filename}')
        iterator = iterator + 1
    except IndexError:
        logger.info(f'No more data to request')
        getting_data = False
    
logger.info(f'Finish Application')
