#! /usr/bin/python
import sys
if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

import argparse
from bs4 import BeautifulSoup
import datetime
import os
import pandas as pd
import threading

import config
import processors
import utils

parser = argparse.ArgumentParser('Collect executives\' movements of securities into pandas dataframe')
parser.add_argument('ticker', help='Ticker for the company to check')
parser.add_argument('-l', '--load', action='store_true', help='Attempt to load previously-saved data')
args = parser.parse_args()

TICKER = args.ticker.upper()
COMPANY_PATH = os.path.join(os.getcwd(),config.STORAGE, TICKER)
edgar_df = pd.DataFrame(
    columns=['owner', 'url', 'total', 'isDirector', 'isOfficer',
             'isTenPercentOwner', 'isOther', 'date'])

# The SEC has a limit to the number of requests one can make per second. Use
# this value and a safety factor to not go over the limit
eggtimer = util.EggTimer(
    (1.0 + config.SAFETY_FACTOR) / config.REQUESTS_PER_SECOND)

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

leads_packet = [x['name'] for x in content['directory']['item']]
edgar_df_ts = utils.ThreadSafeDataFrame(edgar_df, COMPANY_PATH, config.SAVE_PERIOD)

def edgar_gofer(filing_number):

    # get the list of documents we can read for this item
    index = utils.get_and_check_URL(
        make_url([filing_number, 'index.json']), to_json=True)

    # Find the xml
    xml_filename = next( (item['name'] for item in index['directory']['item']
                                    if item['name'].endswith('xml')), None)

    # There must be an xml to parse xml
    if xml_filename is None:
        return

    url = make_url([filing_number, xml_filename])

    # skip urls that we've already parsed
    if url in checked_urls:
        print('+0 (ALREADY IN DATAFRAME) for {}'.format(url))
        return

    response = url_getter.get(url)

    try:
        edgar = processors.Edgar(response.content)
    except processors.EdgarException:
        print('+0 (UNKNOWN SCHEMA, LIKELY NOT FORM 4) for {}'.format(url))
        return

    new_data = edgar.build_updates_list()
    data_count = len(new_data)
    print('+{} for {}'.format(data_count,url))

    if data_count != 0:
        edgar_df_ts.add(new_data)

threads = []

while len(leads_packet) > 0:
    import pdb; pdb.set_trace()

    # check to see if we can send another thread out
    for index, thread in enumerate(threads):
        thread.join(eggtimer.time_left())
        if thread.isAlive():
            # We timed out, which means we can go send off another thread
            break
        # we can get rid of the thread now
        del a[0]
    if eggtimer.time_left() == 0.0:
        # start a new thread and add it to the list
        x = threading.Thread(target=edgar_gofer, args=(leads_packet.pop(0),))
        threads.append(x)
        # And (re) start the eggtimers
        eggtimer.start()
    elif len(threads) == 0:
        # We have no more threads to join, so we just wait
        eggtimer.wait()

# save whatever has yet to be saved
edgar_df_ts.save()

print('Complete')
