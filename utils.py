import json
import requests

import config

def get_and_check_URL(url, to_json=False):
    r = requests.get(url, headers=config.HEADERS)
    r.raise_for_status()
    if r.status_code != 200:
        raise Exception('Got HTTP code {}'.format(res.status_code))
    if to_json:
        r = json.loads(r.content)
    return r
