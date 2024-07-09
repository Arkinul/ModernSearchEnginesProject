import click
import sqlite3
import requests
from bs4 import BeautifulSoup
import hashlib
import re  # regex
import warnings
from contextlib import contextmanager
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from url_normalize import url_normalize
from datetime import datetime, timedelta
from collections import Counter
from nltk.stem import PorterStemmer


#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
def index(doc, index):
    #TODO: Implement me
    pass


#Crawl the web. You need (at least) two parameters:
#frontier: The frontier of known URLs to crawl. You will initially populate this with your seed set of URLs and later maintain all discovered (but not yet crawled) URLs here.
#index: The location of the local index storing the discovered documents.
def crawl(frontier, index):
    #TODO: Implement me

    # seed set
    # open database & fill frontier
    # make requests
    # database layout
    # language detection
        # HTML attribute
    # determine if document is relevant
        # to Tübingen
        # for a search engine (ignore javascript, css?)
        # detect duplicate
    # save document to database
        # last modified?
    # extract URLs & add to frontier
        # check whether URLs have been visited already

    pass


class Index:
    def __init__(self, db):
        # https://docs.python.org/3/library/sqlite3.html#transaction-control
        self.con = sqlite3.connect(db, isolation_level = None)
        self.con.execute("PRAGMA foreign_keys = 1")


    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    # credit: https://github.com/litements/litequeue/blob/main/litequeue.py#L568
    @contextmanager
    def transaction(self, mode="DEFERRED"):
        if mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            raise ValueError(f"Transaction mode '{mode}' is not valid")
        # We must issue a "BEGIN" explicitly when running in auto-commit mode.
        self.con.execute(f"BEGIN {mode}")
        try:
            # Yield control back to the caller.
            yield
        except BaseException as e:
            self.con.rollback()  # Roll back all changes if an exception occurs.
            raise e
        else:
            self.con.commit()


class Queue(Index):
    def __iter__(self):
        return self


    def __next__(self):
        if entry := self.pop():
            return entry
        else:
            raise StopIteration


    def shift(self, cur, position, amount = 1):
        '''
        Move all rows in the frontier >= `position` back by `amount`

        Negative `amount`s shift them forward.
        '''
        # push forward or back
        # can't be done in one statement due to this limitation:
        # https://stackoverflow.com/a/7703239
        cur.execute(
            "UPDATE frontier \
            SET position = -(position + ?2) \
            WHERE position >= ?1",
            (position, amount)
        )
        cur.execute(
            "UPDATE frontier \
            SET position = abs(position) \
            WHERE position < 0"
        )


    def push(self, url):
        '''
        Add an URL to the end of the frontier & create `url` entry if necessary

        Does nothing if the URL is already queued.
        '''
        # TODO: normalize URL
        with self.transaction():
            cur = self.con.cursor()
            # check if URL is already in the URL table, also return position if already queued
            id_and_position = cur.execute(
                "SELECT id, frontier.position FROM url \
                FULL OUTER JOIN frontier ON url.id = frontier.url_id \
                WHERE url.url LIKE ?1",
                (url, )
            ).fetchone()
            if id_and_position:
                #print(f"URL already stored with id {url_id}")
                url_id, prev_pos = id_and_position
                if prev_pos is not None:
                    #print(f"URL already queued at {prev_pos}")
                    return
            else:
                # insert URL into url table
                res = cur.execute(
                    "INSERT OR IGNORE INTO url (url) \
                    VALUES (?1) \
                    RETURNING url.id",
                    (url, )
                ).fetchone()
                assert cur.rowcount == 1, f"failed to store URL {url} in table"
                (url_id, ) = res

            # insert frontier entry at the end
            cur.execute(
                "INSERT INTO frontier (position, url_id) \
                VALUES ( \
                    IFNULL((SELECT max(position) + 1 FROM frontier), 0), \
                    ?1 \
                )",
                (url_id, )
            )


    def pop(self):
        with self.transaction():
            cur = self.con.cursor()
            pos_and_url = cur.execute(
                "DELETE FROM frontier \
                WHERE position = (SELECT min(position) FROM frontier) \
                RETURNING position, (SELECT url FROM url WHERE id = url_id)"
            ).fetchone()
            if pos_and_url:
                pos, url = pos_and_url
                if pos >= 0:
                    #TODO: optimize by not shifting down after every single pop
                    self.shift(cur, pos, -1)
                else:
                    warnings.warn(f"queue entry with negative position {pos}")
                return url
            else:
                return None


    def insert(self, url, position):
        '''
        Insert an URL into the `url` table and add it to the frontier at the given position, without creating gaps
        '''
        # TODO: normalize URL
        with self.transaction():
            cur = self.con.cursor()
            # check if URL is already in the URL table, also return position if already queued
            id_and_position = cur.execute(
                "SELECT id, frontier.position FROM url \
                FULL OUTER JOIN frontier ON url.id = frontier.url_id \
                WHERE url.url LIKE ?1",
                (url, )
            ).fetchone()
            if id_and_position:
                url_id, prev_pos = id_and_position
                #print(f"URL already stored with id {url_id}")
                if prev_pos == position:
                    return
                elif prev_pos is not None:
                    #print(f"URL already queued at {prev_pos}")
                    # take it out of the frontier
                    cur.execute(
                        "DELETE FROM frontier WHERE position = ?1",
                        (prev_pos, )
                    )
                    # close the gap
                    self.shift(cur, prev_pos, -1)
                    # TODO: optimize by checking if prev_pos < position, only shift once
            else:
                # insert URL into url table
                res = cur.execute(
                    "INSERT OR IGNORE INTO url (url) \
                    VALUES (?1) \
                    RETURNING url.id",
                    (url, )
                ).fetchone()
                assert cur.rowcount == 1, f"failed to store URL {url} in table"
                (url_id, ) = res

            # make space for the frontier entry
            self.shift(cur, position)
            # insert frontier entry into the gap or at the end, but not after
            cur.execute(
                "INSERT INTO frontier (position, url_id) \
                VALUES ( \
                    MAX(0, MIN( \
                        ?1, \
                        IFNULL((SELECT max(position) + 1 FROM frontier), 0) \
                    )), \
                    ?2 \
                )",
                (position, url_id)
            )


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
    try:
        response = requests.get(url, headers={"Accept-Language": "en-US,en,en-GB;q=0.5"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        return None
    print(response.text)
    isenglish = re.search('lang="en"', response.text) is not None
    print(isenglish)
    return response.text, isenglish



def html_cleaner(response, url):
    soup = BeautifulSoup(response.content, 'html.parser')
    title = soup.title.string if soup.title else 'No Title'
    meta_description = ''
    if soup.find("meta", attrs={"name": "description"}):
        meta_description = soup.find("meta", attrs={"name": "description"}).get("content", "")

    irrelevant_tags = [
        "script", "style", "link", "meta", "header", "nav", "aside", "footer", "form", 
        "iframe", "template", "button", "input", "select", "textarea", "label",
        "img", "picture", "svg", "canvas", "audio", "video", "object",
        "param", "source", "track", "noscript", "map", "area", "figure", "figcaption", 
        "details", "summary", "dialog", "menu", "menuitem", "applet", "embed"
    ]

    for tag in soup(irrelevant_tags):
        tag.extract()
    text = soup.get_text(separator=' ')
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text_content = ' '.join(chunk for chunk in chunks if chunk)
    
    combined_text = ' '.join([title, meta_description, text_content])
    if is_relevant(combined_text):
        #TODO:instead of dictonary put it into the database
        page_data = {
            'url': url,
            'title': title,
            'meta_description': meta_description,
            'text_content': text_content
        }
        return page_data
    else:
        return None

def is_relevant(content):
    keywords = [
        "tübingen", "hölderlin", "hohenzollern",
        "neckar", "schwaben", "schwäbisch", "tübinger",
        "bebenhausen", "tubingen", "tuebingen", "tuebinger",
        "swabian", "schwaebisch", "schwabisch"
    ]

    # Initialize stemmer, probably not needed yet for the current keyword list,
    # but possibly in the future (transforms e.g. meeting -> meet)
    # https://www.nltk.org/howto/stem.html
    stemmer = PorterStemmer()

    content_lower = content.lower()
    words = re.findall(r'\b\w+\b', content_lower)  # Regex to tokenize individual words on a site
    stemmed_words = [stemmer.stem(word) for word in words]  # Stem words on site

    stemmed_keywords = {stemmer.stem(keyword) for keyword in keywords}  # Stem keywords as well

    # Count how often each word appears on a site and the number of total words
    word_counts = Counter(stemmed_words)
    total_words = len(stemmed_words)

    # Count the number of relevant words on a site
    relevant_count = 0
    for word in stemmed_keywords:
        if word in word_counts:
            relevant_count += word_counts[word]

    # Keyword density = out of all words, how many of them are keywords
    # We can use this instead of a binary relevancy check and adjust the threshold below
    # Probably (?) better than only relevant or not
    if total_words > 0:
        keyword_density = relevant_count / total_words
    else:
        keyword_density = 0

    # Adjust threshold here
    keyword_density_threshold = 0.01  # e.g. if 1 out of 100 words is a keyword, site is relevant

    is_relevant_content = keyword_density >= keyword_density_threshold

    return is_relevant_content

def shingle(text, k=5):
    return set(text[i:i+k] for i in range(len(text) - k + 1))

def hash_shingle(shingle):
    return int(hashlib.md5(shingle.encode('utf-8')).hexdigest(), 16)

def compute_simhash(texts, k=5):
    shingles = set()
    for text in texts:
        shingles.update(shingle(text, k))
    v = [0] * 128
    for sh in shingles:
        h = hash_shingle(sh)
        for i in range(128):
            bitmask = 1 << i
            if h & bitmask:
                v[i] += 1
            else:
                v[i] -= 1
    fingerprint = 0
    for i in range(128):
        if v[i] >= 0:
            fingerprint |= 1 << i
    return fingerprint

def hamming_distance(x, y):
    return (x ^ y).bit_count()

#TODO:set appropiate treshold (might need some more testing)
def is_near_duplicate_simhash(simhash1, simhash2, threshold=15):
    return hamming_distance(simhash1, simhash2) <= threshold

def check_duplicate(page_data):
    #TODO:fetch existing simhases from database. Do we compare a new simhash with every other existing simhash from the database or can we optimize?
    global existing_simhashes
    combined_texts = [page_data['title'], page_data['meta_description'], page_data['text_content']]
    simhash = compute_simhash(combined_texts)
    for existing_simhash in existing_simhashes:
        if is_near_duplicate_simhash(simhash, existing_simhash):
            return False
        
    #TODO:insert page with new simhash into database
    existing_simhashes.append(simhash)
    return True


#TODO: Instead of robots_cache dictonary use database to save and fetch robots.txt. We probably need an additional field for the robots.txt in the database
robots_cache = {}

def get_host(url):
    '''
    Extracts the host (scheme and netloc) from the given URL.

    Parameters:
    url (str): The URL from which the host is to be extracted.

    Returns:
    str: The host part of the URL, including the scheme.
    '''
    parsed_url = urlparse(url)
    return parsed_url.scheme + "://" + parsed_url.netloc


def is_allowed_by_robots(url):
    '''
    Determines if the given URL is allowed to be crawled according to the robots.txt rules of the host.

    Parameters:
    url (str): The URL to check against the robots.txt rules.

    Returns:
    bool: True if the URL is allowed to be crawled, False otherwise.
    '''
    host = get_host(url)
    if host not in robots_cache:
        robots_txt_url = host + "/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_txt_url)
        rp.read()
        robots_cache[host] = rp

    rp = robots_cache[host]
    user_agent = 'MSE_Crawler' 
    if rp.can_fetch(user_agent, url):
        return True
    else:
        return False
    

def normalize_url(url):
    '''
    Normalize the given URL using the url-normalize library.

    Parameters:
    url (str): The URL to be normalized.

    Returns:
    str: The normalized URL.
    '''
    return url_normalize(url)

#WARNING: Hasn't been tested yet 
def should_crawl(con, url, recrawl_interval_days=30):
    '''
    Determines if the given URL should be crawled based on whether it has been previously crawled
    and the time since it was last modified or fetched.

    Parameters:
    con (sqlite3.Connection): Active SQLite database connection.
    url (str): The URL to be checked.
    recrawl_interval_days (int): The interval in days after which a URL should be recrawled. Default is 30 days.

    Returns:
    bool: True if the URL should be crawled, False otherwise.
    '''
    cur = con.cursor()
    cur.execute("SELECT document_id, (SELECT last_modified FROM document WHERE id = document_id) FROM url WHERE url = ?", (url,))
    result = cur.fetchone()
    
    if result:
        document_id, last_modified = result
        if document_id:
            if last_modified:
                last_modified_date = datetime.fromtimestamp(last_modified)
                if datetime.now() - last_modified_date > timedelta(days=recrawl_interval_days):
                    return True
            else:
                cur.execute("SELECT fetched FROM document WHERE id = ?", (document_id,))
                fetched = cur.fetchone()[0]
                fetched_date = datetime.fromtimestamp(fetched)
                if datetime.now() - fetched_date > timedelta(days=recrawl_interval_days):
                    return True
            return False
    return True





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
