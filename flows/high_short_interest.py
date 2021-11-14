import json
import re
from datetime import date, datetime, timedelta

import pandas as pd  # type: ignore
import prefect  # type: ignore
import requests
from bs4 import BeautifulSoup  # type: ignore
from prefect import Flow, task  # type: ignore
from prefect.executors import LocalDaskExecutor  # type: ignore
from prefect.schedules import CronSchedule  # type: ignore

PROJECT_NAME = 'stocks'
HIGH_SHORT_INTEREST_PATH = 'data/high_short_interest/'

DATE_REGEXP = (
    r'('
    r'(January|February|March|April|May|June|July|August|September|October|November|December)'
    r'\s\d{1,2},\s20\d{2})'
)


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_high_short_interest() -> str:
    """Downloads content of the corresponding html page with data needed"""

    logger = prefect.context.get('logger')

    filename = HIGH_SHORT_INTEREST_PATH + str(date.today()) + '.raw.html'

    logger.info(f'Downloading high short interest data into {filename}')

    response = requests.get('https://www.highshortinterest.com/')

    with open(filename, 'w') as f:
        f.write(response.text)

    logger.info(f'High short interest data is stored as {filename}')

    return filename


@task
def parse_date(filename: str) -> str:
    """Parses raw html data and gets date when data was uploaded to the source"""

    logger = prefect.context.get('logger')
    logger.info(f'Parsing date from {filename}')

    with open(filename, 'r') as f:
        content = f.read()

    soup = BeautifulSoup(content, 'html.parser')
    table = soup.find('table')
    date_line = table('tr')[-1]('td')[0].text.strip()

    regexp = re.compile(DATE_REGEXP)
    matches = regexp.search(date_line)
    if matches is None:
        raise ValueError(f'Did not find date in {date_line}')

    datetime_obj = datetime.strptime(matches[0], '%B %d, %Y')
    parsed_date = datetime_obj.date().strftime('%Y-%m-%d')
    logger.info(f'Found date {matches[0]}, which is parsed as {parsed_date}')
    return parsed_date


@task
def create_high_short_interest_csv(filename: str) -> str:
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

    logger = prefect.context.get('logger')
    logger.info(f'Transforming {filename}')

    with open(filename, 'r') as f:
        content = f.read()

    soup = BeautifulSoup(content, 'html.parser')
    table = soup.find('table')('tr')[2]('table')[1]
    df = pd.read_html(str(table), header=0)[0]
    df = df.dropna()
    df = df[df['Ticker'].str.match('[A-Z]{1,4}')]
    df['ShortInt'] = df['ShortInt'].str.replace('%', '').astype('float')
    df['Float'] = df['Float'].apply(transform_number)
    df['Outstd'] = df['Outstd'].apply(transform_number)

    csv_filename = HIGH_SHORT_INTEREST_PATH + str(date.today()) + '.data.csv'
    df.to_csv(csv_filename)
    logger.info(f'Saved transformed csv file as {csv_filename}')
    return csv_filename


@task
def create_metadata(data_date: str, data_path: str) -> str:
    """Creates json metadata file"""

    logger = prefect.context.get('logger')
    logger.info(f'Creating metadata for {data_date} data')

    filename = HIGH_SHORT_INTEREST_PATH + str(date.today()) + '.metadata.json'
    metadata = {
        'name': 'High Short Interest',
        'description': 'Dataset contains short interest for stocks where it is high',
        'effective_date': datetime.today().strftime('%Y-%m-%d'),
        'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_date': data_date,
        'data_location': data_path,
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f)

    logger.info(f'Saved metadata as {filename}')
    return filename


schedule = CronSchedule('0 20 * * MON-FRI')

with Flow('high_short_interest', schedule=schedule) as flow:
    raw_data_filename = download_high_short_interest()
    data_date = parse_date(raw_data_filename)
    data_path = create_high_short_interest_csv(raw_data_filename)
    create_metadata(data_date, data_path)

flow.executor = LocalDaskExecutor()
flow.register(project_name=PROJECT_NAME)
