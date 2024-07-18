import hashlib
from collections import Counter
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
nltk.download('stopwords')
from url_normalize import url_normalize
import re

def html_cleaner(response, url):
    soup = BeautifulSoup(response.content, 'html.parser')
    lang = soup.html.get('lang', 'unknown') if soup.html else 'unknown'
    title = soup.title.string if soup.title else 'No Title'
    meta_description = ''
    if soup.find("meta", attrs={"name": "description"}):
        meta_description = soup.find("meta", attrs={"name": "description"}).get("content", "")
    links = [link.get('href') for link in soup.find_all('a', href=True)]
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
            'lang': lang,
            'title': title,
            'meta_description': meta_description,
            'links': links,
            'text_content': text_content
        }
        return page_data

def is_relevant(content):
    keywords = {
        "tübingen": 1.0, "hölderlin": 1.0, "hohenzollern": 1.0,
        "neckar": 1.0, "schwaben": 1.0, "schwäbisch": 1.0, "tübinger": 1.0,
        "bebenhausen": 1.0, "tubingen": 1.0, "tuebingen": 1.0, "tuebinger": 1.0,
        "swabian": 1.0, "schwaebisch": 1.0, "schwabisch": 1.0
    }

    # Initialize stemmer, probably not needed yet for the current keyword list,
    # but possibly in the future (transforms e.g. meeting -> meet)
    # https://www.nltk.org/howto/stem.html
    stemmer = PorterStemmer()

    content_lower = content.lower()
    words = re.findall(r'\b\w+\b', content_lower)  # Regex to tokenize individual words on a site
    stemmed_words = [stemmer.stem(word) for word in words]  # Stem words on site

    stemmed_keywords = {stemmer.stem(keyword): weight for keyword, weight in keywords.items()}  # Stem keywords as well

    # Count how often each word appears on a site and the number of total words
    word_counts = Counter(stemmed_words)
    total_words = len(stemmed_words)

    # Count the number of relevant words on a site
    relevant_count = 0
    for word, weight in stemmed_keywords.items():
        if word in word_counts:
            relevant_count += word_counts[word] * weight

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

def preprocess_text(text):
    lemmatizer = WordNetLemmatizer()
    low = text.lower()
    words = re.findall(r'\b\w+\b', low)
    stemmed_words = [lemmatizer.lemmatize(word) for word in words]
    #remove stopwords
    filtered_words = [word for word in stemmed_words if word not in stopwords.words('english')]
    return filtered_words


