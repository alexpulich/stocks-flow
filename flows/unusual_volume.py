from datetime import date, datetime, timedelta
import json
import requests

import pandas as pd

from bs4 import BeautifulSoup
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.schedules import IntervalSchedule

UNUSUAL_VOLUME = 'data/unusual_volume/'
DATA_URL = 'https://stockbeep.com/table-data/unusual-volume-stocks' \
           '?sort-column=ssrvol&sort-order=desc&country=us&time-zone=-180'


@task
def download_unusual_volume() -> str:
    filename = UNUSUAL_VOLUME + str(date.today()) + '.raw.html'

    response = requests.get(DATA_URL)

    with open(filename, 'w') as f:
        f.write(response.text)

    return filename


@task
def create_unusual_volume_csv(filename: str):
    def transform_number(x: str) -> float:
        mult = x[-1]
        if mult == 'B':
            mult = 1000000000
        elif mult == 'M':
            mult = 1000000
        elif mult == 'K':
            mult = 1000
        else:
            raise ValueError(f'Unsupported number {x}')
        number = float(x[:-1])
        return mult * number

    def get_ticker_from_href(x: str) -> str:
        soup = BeautifulSoup(x, 'html.parser')
        return soup.a['href'].split('/')[-1].split('-')[-1]

    def get_exchange_from_href(x: str) -> str:
        soup = BeautifulSoup(x, 'html.parser')
        return soup.a['href'].split('/')[-1].split('-')[0]

    with open(filename, 'r') as f:
        content = f.read()

    json_data = json.loads(content)['data']
    df = pd.DataFrame(json_data)
    df['ticker'] = df['sscode'].apply(get_ticker_from_href)
    df['exchange'] = df['sscode'].apply(get_exchange_from_href)
    df = df[[
        'ticker', 'exchange', 'ssname', 'sslast', 'sshigh', 'sschg', 'sschgp', 'ssvol', 'ssrvol', 'ss5mvol', 'sscap'
    ]].rename({
        'ssname': 'company',
        'sslast': 'last',
        'sshigh': 'high',
        'sschg': 'change',
        'sschgp': 'change_percents',
        'ssvol': 'volume',
        'ssrvol': 'relative_volume',
        'ss5mvol': '5min_volume',
        'sscap': 'capitalization',
    }, axis=1)
    df['volume'] = df['volume'].apply(transform_number)
    df['5min_volume'] = df['5min_volume'].apply(transform_number)
    df['capitalization'] = df['capitalization'].apply(transform_number)

    csv_filename = UNUSUAL_VOLUME + str(date.today()) + '.data.csv'
    df.to_csv(csv_filename)


@task
def create_metadata() -> str:
    filename = UNUSUAL_VOLUME + str(date.today()) + '.metadata.json'
    metadata = {
        'name': 'Unusual Volume',
        'description': 'Dataset contains unusual volumes for stocks ',
        'effective_date': datetime.today().strftime('%Y-%m-%d'),
        'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_date': datetime.today().strftime('%Y-%m-%d'),
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f)

    return filename


schedule = IntervalSchedule(
    start_date=datetime(2021, 11, 14, 20, 0, 0),
    interval=timedelta(days=1),
)

with Flow("unusual_volume", schedule=schedule) as flow:
    raw_data_filename = download_unusual_volume()
    create_unusual_volume_csv(raw_data_filename)
    create_metadata()

flow.executor = LocalDaskExecutor()
flow.register(project_name="stocks")
