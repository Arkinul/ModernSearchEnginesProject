import click
import sqlite3
import requests
import beautifulsoup4
import hashlib
import re  # regex


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
        "img", "picture", "svg", "canvas", "audio", "video", "embed", "object", 
        "param", "source", "track", "noscript", "map", "area", "figure", "figcaption", 
        "details", "summary", "dialog", "menu", "menuitem", "applet", "embed", "object"
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
    "bebenhausen"]
    for keyword in keywords:
        if keyword in content:
            return True
    return False

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
    return bin(x ^ y).count('1')

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


if __name__ == '__main__':
    c()
