import json
from datetime import date, datetime, timedelta

import pandas as pd  # type: ignore
import prefect  # type: ignore
import requests
from bs4 import BeautifulSoup  # type: ignore
from prefect import Flow, task  # type: ignore
from prefect.executors import LocalDaskExecutor  # type: ignore
from prefect.schedules import CronSchedule  # type: ignore

PROJECT_NAME = 'stocks'
UNUSUAL_VOLUME = 'data/unusual_volume/'
DATA_URL = (
    'https://stockbeep.com/table-data/unusual-volume-stocks'
    '?sort-column=ssrvol&sort-order=desc&country=us&time-zone=-180'
)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_unusual_volume() -> str:
    """Downloads content of the corresponding html page with data needed"""

    logger = prefect.context.get('logger')

    filename = UNUSUAL_VOLUME + str(date.today()) + '.raw.html'
    logger.info(f'Downloading unusual volume data into {filename}')

    response = requests.get(DATA_URL)

    with open(filename, 'w') as f:
        f.write(response.text)

    logger.info(f'Unusual volume data is stored as {filename}')
    return filename


@task
def create_unusual_volume_csv(filename: str) -> str:
    """Parses raw html data, transforms it and saves as csv"""

    def transform_number(x: str) -> float:
        mult_symb = x[-1]
        if mult_symb == 'B':
            mult = 1000000000
        elif mult_symb == 'M':
            mult = 1000000
        elif mult_symb == 'K':
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

    logger = prefect.context.get('logger')
    logger.info(f'Transforming {filename}')

    with open(filename, 'r') as f:
        content = f.read()

    json_data = json.loads(content)['data']
    df = pd.DataFrame(json_data)
    df['ticker'] = df['sscode'].apply(get_ticker_from_href)
    df['exchange'] = df['sscode'].apply(get_exchange_from_href)
    df = df[
        [
            'ticker',
            'exchange',
            'ssname',
            'sslast',
            'sshigh',
            'sschg',
            'sschgp',
            'ssvol',
            'ssrvol',
            'ss5mvol',
            'sscap',
        ]
    ].rename(
        {
            'ssname': 'company',
            'sslast': 'last',
            'sshigh': 'high',
            'sschg': 'change',
            'sschgp': 'change_percents',
            'ssvol': 'volume',
            'ssrvol': 'relative_volume',
            'ss5mvol': '5min_volume',
            'sscap': 'capitalization',
        },
        axis=1,
    )
    df['volume'] = df['volume'].apply(transform_number)
    df['5min_volume'] = df['5min_volume'].apply(transform_number)
    df['capitalization'] = df['capitalization'].apply(transform_number)

    csv_filename = UNUSUAL_VOLUME + str(date.today()) + '.data.csv'
    df.to_csv(csv_filename)

    logger.info(f'Saved transformed csv file as {csv_filename}')
    return csv_filename


@task
def create_metadata(data_path) -> str:
    """Creates json metadata file"""

    logger = prefect.context.get('logger')
    logger.info('Creating metadata')
    filename = UNUSUAL_VOLUME + str(date.today()) + '.metadata.json'
    metadata = {
        'name': 'Unusual Volume',
        'description': 'Dataset contains unusual volumes for stocks ',
        'effective_date': datetime.today().strftime('%Y-%m-%d'),
        'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_date': datetime.today().strftime('%Y-%m-%d'),
        'data_location': data_path,
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f)

    logger.info(f'Saved metadata as {filename}')
    return filename


schedule = CronSchedule('0 20 * * MON-FRI')

with Flow('unusual_volume', schedule=schedule) as flow:
    raw_data_filename = download_unusual_volume()
    data_path = create_unusual_volume_csv(raw_data_filename)
    create_metadata(data_path)

flow.executor = LocalDaskExecutor()
flow.register(project_name=PROJECT_NAME)
