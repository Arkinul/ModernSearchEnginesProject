import time
import click
import apsw
import requests

from crawl import DEFAULT_CRAWLER_DB
from crawl.queue import Queue
from crawl.request import Request, Status
from crawl.robots import can_crawl


@click.group()
def c():
    #print("SQLite version", apsw.sqlite_lib_version())
    pass


@c.command()
@click.option(
    '--db',
    default=DEFAULT_CRAWLER_DB,
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
    db = apsw.Connection(db)
    db.execute(sql_script)
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
    default='crawler.db',
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
    for url in urls:
        queue.push(url.strip())


@c.command()
@click.option(
    '--db',
    default=DEFAULT_CRAWLER_DB,
    help='location of the SQLite database file',
    type=click.Path()
)
def crawl_next(db):
    queue = Queue(db)
    url = queue.pop()
    if not url:
        exit(-1)

    req = Request(url)
    match req.check_status(db):
        case Status.PROHIBITED | Status.TIMEOUT | Status.FAILED as s:
            print(f"{url} previously not fetched ({s.name})")
            exit(0)
        case status if type(status) == Status:
            print(f"{url} already fetched with status {status}")
            exit(0)
        case limited if type(limited) == float and limited > time.time():
            print(f"{url} throttled for another {limited - time.time()}s")
            queue.push(url)
            exit(0)

    res = can_crawl(url)
    if type(res) == float:
        print(f"host rate-limited for {res}s")
        Request.rate_limited(url, res).save(db)
        queue.push(url)
        exit(0)
    elif res != True:
        Request.prohibited(url).save(db)
        print(f"crawling prohibited for {url}")
        exit(0)
    print(f"fetching {url}")
    succeeded = req.make()
    #TODO: where to check for content-type?
    req.save(db)
    if not succeeded:
        exit(0)
    if doc := req.document():
        doc.parse()
        print(f"parsed document, relevance score is {doc.relevance()}")
        if doc.check_for_duplicates():
            # TODO: save these also? as reference to the duplicate?
            exit(0)
        doc.save(db)
        if doc.is_relevant():
            links = doc.links()
            print(f"extracted {len(links)} links")
            # TODO: implemented batched queuing
            for link in links:
                # TODO: check whether to queue here
                queue.push(link)
        else:
            print("document is irrelevant, ignoring links")




if __name__ == '__main__':
    c()
