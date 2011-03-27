#!/usr/bin/env python

import gevent
from gevent.pool import Pool
from gevent import monkey; monkey.patch_all()

import cPickle as pickle
import datetime
import logging

import redis

from restkit import request, RequestError
from restkit.globals import set_manager
from restkit.manager.mgevent import GeventManager

# configure logging
logging.basicConfig(format="%(asctime)-15s %(message)s",
                    level=logging.INFO)

# connect to redis server
r = redis.Redis(host='localhost')

# change restkit manager to enable connection reuse
set_manager(GeventManager())
# limit ourselves to max 25 simultaneous outstanding requests
pool = Pool(1024)

def fetch(url):
    "Fetches `url` and writes the result to redis"
    #logging.info('start %s', url)
    data = dict(url=url, fetched_at=datetime.datetime.now().isoformat())
    try:
        req = request(url)
    except RequestError:
        # invalid url
        pass
    else:
        data.update(headers=dict(req.headers),
                    body=req.body_string().strip(),
                    status=req.status_int)
    r.lpush('fetched', pickle.dumps(data))
    r.lrem('new_backup', url)
    logging.info('fetched %s', url)
    return data

def main():
    logging.info('* Fetcher started')

    # check backup queue
    backup_count = r.llen('new_backup')
    if backup_count:
        logging.info('* Re-fetching canceled entries')
        urls = [r.rpoplpush('new_backup', 'new_backup') for n in range(backup_count)]
        with gevent.Timeout(3, False):
            for url in urls:
                pool.spawn(fetch, url)
            pool.join()
    
    while 1:
        url = r.brpoplpush('new', 'new_backup')
        with gevent.Timeout(3, False):
            pool.spawn(fetch, url)

if __name__ == '__main__':
    main()
