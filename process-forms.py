#! /usr/bin/python

import argparse
from bs4 import BeautifulSoup
import datetime
import os
import pandas as pd
import threading
import sys

import config
import processors
import utils

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

parser = argparse.ArgumentParser('Collect executives\' movements of securities into pandas dataframe')
parser.add_argument('ticker', help='Ticker for the company to check')
parser.add_argument('-l', '--load', action='store_true', help='Attempt to load previously-saved data')
parser.add_argument('-t', '--threads', type=int, default=config.NUM_THREADS, help='Number of threads to use')
args = parser.parse_args()

TICKER = args.ticker.upper()
COMPANY_PATH = os.path.join(os.getcwd(),config.STORAGE, TICKER)
edgar_df = pd.DataFrame(
    columns=['owner', 'url', 'total', 'isDirector', 'isOfficer',
             'isTenPercentOwner', 'isOther', 'date'])

def load_company_data():
    global edgar_df
    print('Continuing with previously-saved data to collect new datapoints.')
    edgar_df = pd.read_pickle(COMPANY_PATH)

if os.path.exists(COMPANY_PATH):
    if args.load:
        load_company_data()
    else:
        choice = input(
            ('Company data exists already for {}.\n'
            'Overwrite original data? [y,N] ').format(TICKER))
        if choice.lower() == 'y':
            print('Creating new data. Previous data will be overwritten ')
        else:
            load_company_data()

# Find the CIK from the ticker
soup = BeautifulSoup(
    utils.get_and_check_URL('https://sec.report/Ticker/' + TICKER).content,
    'html.parser'
)
page_header = soup.find(class_='jumbotron')
cik = str.lstrip(page_header.find('h2').text.split()[2], '0')
company_name = page_header.find('h1').text

print('Looking at {}'.format(company_name))

# Get the list of documents for this company
base_url = 'https://www.sec.gov/Archives/edgar/data/{}/'.format(cik)

def make_url(components):
    if isinstance(components,str):
        components = [components]
    return base_url + '/'.join(components)

# Get the filings and request that they be decoded into a dict
content = utils.get_and_check_URL(make_url('index.json'), to_json=True)

checked_urls = []
if len(edgar_df):
    checked_urls = edgar_df['url'].to_list()

# These two objects will be accessed by the threads as input and output, respecively
leads_packet = utils.Leads([x['name'] for x in content['directory']['item']])
edgar_df_ts = utils.ThreadSafeDataFrame(edgar_df, COMPANY_PATH, config.SAVE_PERIOD)

def edgar_reader(cabinet, df):

    while True:
        filing_number = leads_packet.get()

        if filing_number == -1:
            break

        # get the list of documents we can read for this item
        index =  utils.get_and_check_URL(make_url([filing_number, 'index.json']), to_json=True)

        # Find the xml
        xml_filename = next( (item['name'] for item in index['directory']['item']
                                        if item['name'].endswith('xml')), None)

        # There must be an xml to parse xml
        if xml_filename is None:
            continue

        url = make_url([filing_number, xml_filename])

        # skip urls that we've already parsed
        if url in checked_urls:
            print('+0 (ALREADY IN DATAFRAME) for {}'.format(url))
            continue

        try:
            edgar = processors.Edgar(url)
        except processors.EdgarException:
            print('+0 (UNKNOWN SCHEMA, LIKELY NOT FORM 4) for {}'.format(url))
            continue

        new_data = edgar.build_updates_list()
        data_count = len(new_data)
        print('+{} for {}'.format(data_count,url))

        if data_count != 0:
            df.add(new_data)

threads = []

for index in range(args.threads):
    print("Main    : create and start thread {}.".format(index))
    x = threading.Thread(target=edgar_reader, args=(leads_packet, edgar_df_ts,))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    print("Main    : before joining thread {}.".format(index))
    thread.join()
    print("Main    : thread {} done".format(index))

# save whatever has yet to be saved
edgar_df_ts.save()

print('Complete')
