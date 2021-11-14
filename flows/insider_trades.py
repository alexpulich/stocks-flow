import json
from datetime import date, datetime, timedelta
from typing import Dict

import pandas as pd  # type: ignore
import prefect  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from prefect import Flow, Parameter, task  # type: ignore
from prefect.executors import LocalDaskExecutor  # type: ignore
from prefect.schedules import CronSchedule  # type: ignore
from requests_html import HTMLSession  # type: ignore

PROJECT_NAME = 'stocks'

INSIDER_TRADES = 'data/insider_trades/'

DATA_URL_BUY = 'https://finviz.com/insidertrading.ashx?tc=1'
DATA_URL_SELL = 'https://finviz.com/insidertrading.ashx?tc=2'

BUY = 'buy'
SELL = 'sell'

TRADE_SIDES = {
    BUY: DATA_URL_BUY,
    SELL: DATA_URL_SELL,
}


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_insider_trades(trade_side: str) -> Dict[str, str]:
    """Downloads content of the corresponding html page with data needed"""
    logger = prefect.context.get('logger')
    filename = f'{INSIDER_TRADES}{str(date.today())}.{trade_side}.raw.html'

    logger.info(
        f'Downloading insider {trade_side} trades data into {filename}'
    )

    session = HTMLSession()
    response = session.get(TRADE_SIDES[trade_side])
    with open(filename, 'w') as f:
        f.write(response.text)

    logger.info(f'Insider trades data is stored as {filename}')
    return {'filename': filename, 'trade_side': trade_side}


@task
def create_insder_trades_csv(file_dict: Dict[str, str]) -> Dict[str, str]:
    """Parses raw html data, transforms it and saves as csv"""
    logger = prefect.context.get('logger')
    filename = file_dict['filename']
    trade_side = file_dict['trade_side']
    logger.info(f'Transforming {filename}')

    with open(filename, 'r') as f:
        content = f.read()

    df = pd.read_html(content, header=0)[-1]
    csv_filename = f'{INSIDER_TRADES}{str(date.today())}.{trade_side}.data.csv'
    df.to_csv(csv_filename)

    logger.info(f'Saved transformed csv file as {csv_filename}')
    return {'filename': csv_filename, 'trade_side': trade_side}


@task
def create_metadata(file_dict: Dict[str, str]) -> Dict[str, str]:
    """Creates json metadata file"""
    logger = prefect.context.get('logger')
    logger.info('Creating metadata')

    data_filename = file_dict['filename']
    trade_side = file_dict['trade_side']

    filename = (
        f'{INSIDER_TRADES}{str(date.today())}.{trade_side}.metadata.json'
    )
    metadata = {
        'name': 'Unusual Volume',
        'description': 'Dataset contains unusual volumes for stocks ',
        'effective_date': datetime.today().strftime('%Y-%m-%d'),
        'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_date': datetime.today().strftime('%Y-%m-%d'),
        'data_location': data_filename,
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f)

    logger.info(f'Saved metadata as {filename}')
    return {'filename': filename, 'trade_side': trade_side}


schedule = CronSchedule('0 20 * * MON-FRI')

with Flow('insider_trades', schedule=schedule) as flow:
    sides = [BUY, SELL]
    file_dict = download_insider_trades.map(sides)
    csv_file_dict = create_insder_trades_csv.map(file_dict)
    create_metadata.map(csv_file_dict)

flow.executor = LocalDaskExecutor()
flow.register(project_name=PROJECT_NAME)
