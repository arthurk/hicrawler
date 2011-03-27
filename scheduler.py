#!/usr/bin/env python

import datetime
import logging
import robotparser
import time

import psycopg2
import redis

# configure logging
logging.basicConfig(format="%(asctime)-15s %(message)s",
                    level=logging.INFO)

# connect to redis server
r = redis.Redis(host='localhost')

# connect to postgres server and create cursor for db operations
con = psycopg2.connect("dbname=crawler user=arthur")
cur = con.cursor()

def schedule(url):
    "Schedule to fetch `url`"
    r.lpush('new', url)
    cur.execute("""
        UPDATE links 
        SET scheduled_at = %s
        WHERE url = %s
    """, (datetime.datetime.now().isoformat(), url,))
    con.commit()
    logging.info('scheduled %s', url)

def main():
    logging.info('* Scheduler started')

    while 1:
        cur.execute("""
            SELECT hostname
            FROM hosts
            WHERE NOW() >= fetched_at + interval '15 seconds' 
                  OR fetched_at IS NULL
            LIMIT 1000""")
        hosts = cur.fetchall()
        for host in hosts:
            hostname = host[0]
            robots_url = 'http://%s/robots.txt' % hostname

            '''
            # check if a robots.txt file was downloaded for this host
            cur.execute("""
                SELECT url, body, fetched_at
                FROM links
                WHERE url = %s
            """, (robots_url,))
            # AND fetched_at IS NOT NULL
            robots = cur.fetchone()
            print robots
            if robots:
                print 'found an entry'
                # if fetched_at + no body then empty robots.txt
                rp = robotparser.RobotFileParser()
                #rp.parse()
                print robots
                # can fetch?
            else:
                # schedule to download the robots.txt file
                cur.execute("""
                    INSERT INTO links (url, hostname)
                    VALUES (%s, %s)
                """, (robots_url, hostname,))
                # integrity error: in table but not fetched yet: continue
                con.commit()
                print 'schedule(%s)' % robots_url
                #schedule(robots_url)
            '''
            cur.execute("""
                SELECT url
                FROM links
                WHERE hostname = %s
                      AND fetched_at IS NULL 
                      AND scheduled_at IS NULL
                LIMIT 1""", (hostname,))
            url = cur.fetchone()
            if url:
                url = url[0]
                logging.info('scheduled %s', url)
                cur.execute('UPDATE links SET scheduled_at = %s WHERE url = %s',
                            (datetime.datetime.now().isoformat(), url,))
                con.commit()
                r.lpush('new', url)
        time.sleep(1)

if __name__ == '__main__':
    main()
