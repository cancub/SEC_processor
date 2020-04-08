from collections import deque
import json
import pandas as pd
import requests
import threading

import config

def get_and_check_URL(url, to_json=False):
    r = requests.get(url, headers=config.HEADERS)
    r.raise_for_status()
    if r.status_code != 200:
        raise Exception('Got HTTP code {}'.format(res.status_code))
    if to_json:
        r = json.loads(r.content)
    return r

class Leads(object):

    def __init__(self, urls = []):
        self._urls = deque(urls)
        self._lock = threading.Lock()

    def get(self):
        self._lock.acquire()
        if len(self._urls) == 0:
            # Signify that we're empty
            value = -1
        else:
            value = self._urls.popleft()
        self._lock.release()
        return value

class ThreadSafeDataFrame(object):
    def __init__(self, df, filename = None, period = 1):
        self._df = df
        self._lock = threading.Lock()
        self._filename = filename
        if not isinstance(period, int):
            raise TypeError(
                'period must be an int, got {}'.format(period.__class__.__name__))
        self._period = period
        self._countdown = period

    def add(self, rows):
        self._lock.acquire()
        self._df = pd.concat(
            [self._df, pd.DataFrame(data=rows)],ignore_index=True,)

        # Save if we're set up to do so and we've also waited long enough
        if self._filename is not None:
            self._countdown -= 1
            if self._countdown == 0:
                self.save(locked = True)
                self._countdown = self._period
            print('Saving in {}'.format(self._countdown))

        self._lock.release()

    def save(self, locked = False):
        if self._filename is None:
            return

        if not locked:
            self._lock.acquire()
        self._df.sort_values(by=['date','owner'], inplace=True, ignore_index=True)
        self._df.to_pickle(self._filename)
        if not locked:
            self._lock.release()


