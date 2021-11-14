import json
import re
from datetime import date, datetime, timedelta

import pandas as pd  # type: ignore
import requests
from bs4 import BeautifulSoup  # type: ignore
from common import transform_number
from prefect import Flow, task  # type: ignore
from prefect.executors import LocalDaskExecutor  # type: ignore
from prefect.schedules import IntervalSchedule  # type: ignore

HIGH_SHORT_INTEREST_PATH = 'data/high_short_interest/'

DATE_REGEXP = (
    r'('
    r'(January|February|March|April|May|June|July|August|September|October|November|December)'
    r'\s\d{1,2},\s20\d{2})'
)


@task
def download_high_short_interest() -> str:
    filename = HIGH_SHORT_INTEREST_PATH + str(date.today()) + '.raw.html'

    response = requests.get('https://www.highshortinterest.com/')

    with open(filename, 'w') as f:
        f.write(response.text)

    return filename


@task
def parse_date(filename: str) -> str:
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
    return datetime_obj.date().strftime('%Y-%m-%d')


@task
def create_high_short_interest_csv(filename: str) -> str:
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
    return csv_filename


@task
def create_metadata(data_date: str) -> str:
    filename = HIGH_SHORT_INTEREST_PATH + str(date.today()) + '.metadata.json'
    metadata = {
        'name': 'High Short Interest',
        'description': 'Dataset contains short interest for stocks where it is high',
        'effective_date': datetime.today().strftime('%Y-%m-%d'),
        'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_date': data_date,
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f)

    return filename


schedule = IntervalSchedule(
    start_date=datetime(2021, 11, 14, 20, 0, 0),
    interval=timedelta(days=1),
)

with Flow('high_short_interest', schedule=schedule) as flow:
    raw_data_filename = download_high_short_interest()
    data_date = parse_date(raw_data_filename)
    create_high_short_interest_csv(raw_data_filename)
    create_metadata(data_date)

flow.executor = LocalDaskExecutor()
flow.register(project_name='stocks')
