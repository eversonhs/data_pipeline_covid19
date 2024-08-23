import requests
from pathlib import Path
import os
import sys
from datetime import datetime, UTC, timedelta
import logging
import json

# Execution Date
today = datetime.now(UTC)
date_d_1 = today - timedelta(days=30)
data_directory = Path('./data')
# Logging config
log_path = Path(os.environ["INGEST_API_LOG_PATH"])
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(name)s - %(levelname)s: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            filename=log_path.joinpath(f'ingest_api_{today.strftime('%Y-%m-%dT%H_%M_%SZ')}.log'),
            encoding='utf-8',
            mode='a+'
        ),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info('Initializing application')
# Request Config
base_url = 'https://imunizacao-es.saude.gov.br/'
username = os.environ['API_USERNAME']
password = os.environ['API_PASSWORD']

headers = {
    'Content-Type': 'application/json',
    'Connection': 'keep-alive'
}

query = {
    "size": 10000,
    "query": {
        "match": {
            "vacina_dataAplicacao": date_d_1.strftime('%Y-%m-%dT00:00:00.000Z')
        }
    },
    "sort": [
        {"vacina_dataAplicacao": {"order": "asc", "format": "strict_date_optional_time_nanos"}},
        {"document_id": "asc"}
    ]
}

# Data directory
partitioning_folders='{0:4d}/{1:02d}/{2:02d}'.format(date_d_1.year, date_d_1.month, date_d_1.day)
file_directory = data_directory.joinpath(partitioning_folders)
file_directory.mkdir(parents=True, exist_ok=True)

# Making requests
getting_data = True
iterator = 1
while getting_data:
    filename = f'{date_d_1.strftime('%Y-%m-%d')}_{iterator}.json'
    
    filepath = file_directory.joinpath(filename)
    with open(filepath, 'w+') as file:
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
            logger.info(f'Writing to file {filename}')
            file.write(json.dumps(data))
            logger.info(f'Written to file {filename}')
            iterator = iterator + 1
        except IndexError:
            logger.info(f'No more data to request')
            getting_data = False
logger.info(f'Finish Application')
