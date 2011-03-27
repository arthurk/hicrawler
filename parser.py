#!/usr/bin/env python

import cPickle as pickle
from fnmatch import fnmatch
import logging
from urlparse import urlparse, urldefrag

from lxml.html import fromstring, tostring

import psycopg2
from psycopg2 import IntegrityError

import redis
import urlnorm
from BeautifulSoup import UnicodeDammit

"""
todo:
- ADD parsed_at field to db
- check content-type for text/html
- mark *.jpg etc. as invalid
- 

"""

# configure logging
logging.basicConfig(format="%(asctime)-15s %(message)s",
                    level=logging.INFO)

# connect to redis server
r = redis.Redis(host='localhost')

# connect to postgres server and create cursor for db operations
con = psycopg2.connect("dbname=crawler user=arthur")
cur = con.cursor()

def is_valid_url(url, blacklist=[]):
    """
    Checks if `url` is valid. For example:

    >>> is_valid_url('http://www.google.com/')
    True
    >>> is_valid_url('ftp://192.168.1.1')
    False

    The blacklist parameter specifies urls which are always invalid. The
    rules can be written with fnmatch expressions (see 
    http://docs.python.org/library/fnmatch.html).

    >>> is_valid_url('http://www.example.org/oo.tar.gz', ['*.tar.gz'])
    False
    """
    url_parsed = urlparse(url)

    # only http links
    if not url_parsed.scheme in ('http', 'https',):
        return False
    # only german domains
    #if not url_parsed.hostname.endswith('.de'):
    #    return False

    # check if url is in blacklist
    for pattern in blacklist:
        if fnmatch(url, pattern):
            return False
    return True

def extract_links(base_url, html):
    "Returns links in `html`"
    doc = fromstring(html)
    doc.make_links_absolute(base_url)

    urls = []
    for link in doc.cssselect('a'):
        url = link.get('href')
        # href could be empty e.g. <a href="">
        if url:
            # remove fragments from url
            url = urldefrag(url)[0]
            # normalize url
            try:
                url = urlnorm.norm(url)
            except urlnorm.InvalidUrl:
                continue
            except UnicodeDecodeError:
                pass
            #if not is_valid_url(url, [base_url,]):
            #    logging.info('invalid url: %s', url)
            if is_valid_url(url, [base_url,]) and url not in urls:
                urls.append(url)
    return urls

def decode_html(html, charset='ascii'):
    "Decode html_string to unicode"
    try:
        body = unicode(html, charset)
    except (UnicodeDecodeError, LookupError,):
        body = UnicodeDammit(html, isHTML=True).unicode
    return body

def update_link(data):
    "Update link entry in database with `data`"
    # check if content-type is text/html and update body when true

    # decode body to unicode
    if data.get('body'):
        try:
            charset = data['headers']['Content-Type'].split('charset=')[1]
        except (IndexError, KeyError,):
            charset = 'ascii'
        body = decode_html(data['body'], charset)
    else:
        body = ''

    cur.execute("""UPDATE links
                   SET headers = %s, body = %s, status = %s, fetched_at = %s
                   WHERE url = %s""", (
                       pickle.dumps(data.get('headers')),
                       body,
                       data.get('status'),
                       data['fetched_at'],
                       data['url']))
    con.commit()

def update_host(data):
    "Update hosts entry in database with `data`"
    hostname = urlparse(data['url']).hostname
    cur.execute("""UPDATE hosts
                   SET fetched_at = %s
                   WHERE hostname = %s""", (data['fetched_at'],
                                            hostname))
    con.commit()

def parse(pickled_data):
    data = pickle.loads(pickled_data)

    # update database entries
    update_host(data)
    update_link(data)

    if data.get('body'):
        links = extract_links(data['url'], data['body'])
        for link in links:
            # create new entry in hosts table
            hostname = urlparse(link).hostname
            try:
                cur.execute("""INSERT INTO hosts (hostname)
                               VALUES (%s)""", (hostname,))
            except IntegrityError:
                # host already exists
                pass
            con.commit()

            # create new entry in links table
            try:
                cur.execute("""INSERT INTO links (url, hostname)
                               VALUES (%s, %s)""", (link, hostname,))
            except IntegrityError:
                # link already in db
                pass
            con.commit()
    r.lrem('fetched_backup', pickled_data)
    logging.info('parsed %s', data['url'])

def main():
    logging.info('* Parser started')

    # check backup queue
    backup_count = r.llen('fetched_backup')
    if backup_count:
        logging.info('* Re-parsing canceled items')
        for n in range(backup_count):
            parse(r.rpoplpush('fetched_backup', 'fetched_backup'))
    
    while 1:
        parse(r.brpoplpush('fetched', 'fetched_backup'))

if __name__ == '__main__':
    main()

