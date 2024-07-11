import click
import sqlite3
import requests
import re

from crawl.queue import Queue


@click.group()
def c():
    pass


@c.command()
@click.option(
    '--db',
    default='index.db',
    help='where to create the SQLite database file',
    type=click.Path()
)
@click.option(
    '--sql',
    default='crawler.sql',
    help='SQL to initialize database tables',
    type=click.File()
)
def init_db(db, sql):
    """
    Create database file and initialize tables with SQL script
    """
    # https://stackoverflow.com/a/54290631
    sql_script = sql.read()
    db = sqlite3.connect(db)
    cursor = db.cursor()
    cursor.executescript(sql_script)
    db.commit()
    db.close()


@c.command()
@click.option(
    '--url',
    default='https://www.uni-tuebingen.de/',
    help='URL to request',
    required=True
)
def url_request(url):
    headers = {"Accept-Language": "en-US,en,en-GB"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
    print(response.text)
    print(response.headers)
    lang = response.headers.get('Content-Language', 'unknown')
    type = response.headers.get('Content-Type', 'unknown')
    print(f'Language: {lang}')
    print(f'Type: {type}')

    return response.text, response.url, response.headers, lang, type


@c.command()
@click.option(
    '--db',
    default='index.db',
    help='location of the SQLite database file',
    type=click.Path()
)
@click.option(
    '--urls',
    default='seed.urls',
    help='newline-separated list of URLs',
    type=click.File()
)
def load_urls(db, urls):
    '''
    Load URLs from file and insert into the frontier
    '''
    queue = Queue(db)
    for i, url in enumerate(urls):
        queue.push(url.strip())


if __name__ == '__main__':
    c()
