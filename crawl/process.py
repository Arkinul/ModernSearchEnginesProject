import hashlib
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.chunk import ne_chunk
from nltk.tokenize import word_tokenize
from nltk.tree import Tree
from url_normalize import url_normalize
import re
import apsw
import os

# Absoluter Pfad zur Index-Datenbank
DEFAULT_INDEX_DB = os.path.abspath('../index.db')  # Angenommen, die Index-Datenbank befindet sich eine Ebene höher im Verzeichnisbaum

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

    stemmed_words = preprocess_text(content)  # Stem words on site

    stemmed_keywords = {preprocess_text(keyword).pop(): weight for keyword, weight in
                        keywords.items()}  # Stem keywords as well

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
    return set(text[i:i + k] for i in range(len(text) - k + 1))


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
    cur.execute(
        "SELECT document_id, (SELECT last_modified FROM document WHERE id = document_id) FROM url WHERE url = ?",
        (url,))
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


lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

def preprocess_text(text):
    low = text.lower()
    words = re.findall(r'\b\w+\b', low)
    stemmed_words = [lemmatizer.lemmatize(word) for word in words]
    filtered_words = [word for word in stemmed_words if word not in stop_words]
    return filtered_words

def find_synonyms(word, max_terms_per_token=3):
    synonyms = set()
    for syn in wordnet.synsets(word, lang='eng'):
        for lemma in syn.lemmas(lang='eng'):
            processed_synonym = preprocess_text(lemma.name().replace('_', ' '))
            synonyms.update(processed_synonym)
            if len(synonyms) >= max_terms_per_token:
                break
        if len(synonyms) >= max_terms_per_token:
            break
    return list(synonyms)

def named_entities_nltk(text):
    tokens = word_tokenize(text)
    tagged_tokens = nltk.pos_tag(tokens)
    chunked_tokens = ne_chunk(tagged_tokens)
    entities = set()
    for chunk in chunked_tokens:
        if isinstance(chunk, Tree):
            entity = " ".join([token for token, pos in chunk.leaves()])
            entities.add(entity.lower())
    return entities

def term_priority(term, term_freq, named_entities):
    pos = term[1]
    length = len(term[0])
    frequency = term_freq[term[0]]
    is_named_entity = term[0] in named_entities
    return (frequency, is_named_entity, pos in ('NN', 'NNS', 'NNP', 'NNPS'), pos in ('JJ', 'VB'), length)

def truncate_query(preprocessed_query, max_terms=20):
    tagged_tokens = nltk.pos_tag(preprocessed_query)
    term_freq = Counter(preprocessed_query)
    
    named_entities = named_entities_nltk(' '.join(preprocessed_query))

    prioritized_tokens = sorted(tagged_tokens, key=lambda term: term_priority(term, term_freq, named_entities), reverse=True)
    unique_terms = set()
    most_common_terms = []
    
    for term, pos in prioritized_tokens:
        if term not in unique_terms:
            most_common_terms.append(term)
            unique_terms.add(term)
        if len(most_common_terms) >= max_terms:
            break

    return most_common_terms

def enrich_query(preprocessed_query, max_total_terms=15, max_terms_per_token=3, truncation_threshold=30):
    if len(preprocessed_query) > truncation_threshold:
        preprocessed_query = truncate_query(preprocessed_query, max_terms=truncation_threshold)
    
    enriched_query = set(preprocessed_query)
    
    for token in preprocessed_query:
        synonyms = find_synonyms(token, max_terms_per_token)
        enriched_query.update(synonyms)
        if len(enriched_query) >= max_total_terms:
            break

    return list(enriched_query)[:max_total_terms]

# BM25 parameters
k1 = 1.5
b = 0.75

def calculate_bm25_score(query_terms, conn, original_query_terms, weight=2.0, title_weight=1.5):
    query_term_freq = {term: query_terms.count(term) for term in set(query_terms)}

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM document")
    doc_count = cursor.fetchone()[0]

    cursor.execute("SELECT document_id, COUNT(*) as length FROM inverted_index GROUP BY document_id")
    doc_lengths = {row[0]: row[1] for row in cursor.fetchall()}
    avg_doc_length = sum(doc_lengths.values()) / doc_count

    term_doc_freq = {}
    term_doc_positions = {}

    for term in query_terms:
        cursor.execute("SELECT id FROM word WHERE word=?", (term,))
        word_id_row = cursor.fetchone()
        if not word_id_row:
            continue
        word_id = word_id_row[0]

        cursor.execute("SELECT COUNT(*) FROM inverted_index WHERE word_id=?", (word_id,))
        term_doc_freq[term] = cursor.fetchone()[0]

        cursor.execute("SELECT document_id, COUNT(*) FROM inverted_index WHERE word_id=? GROUP BY document_id", (word_id,))
        term_doc_positions[term] = {row[0]: row[1] for row in cursor.fetchall()}

    scores = {}
    for term, freq in query_term_freq.items():
        if term not in term_doc_freq:
            continue
        idf = math.log((doc_count - term_doc_freq[term] + 0.5) / (term_doc_freq[term] + 0.5) + 1)
        term_weight = weight if term in original_query_terms else 1.0
        for doc_id, term_freq in term_doc_positions[term].items():
            if doc_id not in scores:
                scores[doc_id] = 0
            doc_len = doc_lengths.get(doc_id, 0)
            tf = term_freq
            score = term_weight * idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_length))))
            scores[doc_id] += score

    doc_ids = list(scores.keys())
    if doc_ids:
        placeholders = ','.join('?' for _ in doc_ids)
        cursor.execute(f"SELECT id, title FROM document WHERE id IN ({placeholders})", doc_ids)
        titles = {row[0]: row[1] for row in cursor.fetchall()}

    for doc_id in scores.keys():
        title = titles.get(doc_id, "")
        if title is None:
            title = ""
        title_terms = preprocess_text(title)
        if any(term in title_terms for term in original_query_terms):
            scores[doc_id] *= title_weight

    return sorted(scores.items(), key=lambda item: item[1], reverse=True)[:12]

@dataclass
class Result:
    url: str
    title: str | None
    score: float

    def normalize_score(self, min_score, max_score):
        if min_score == max_score:
            self.score = 100
            return
        self.score = 100 * (self.score - min_score) / (max_score - min_score)

def result_from_id(doc_id, score, conn) -> Result:
    row = conn.execute("SELECT url, title FROM document WHERE id = ?1", [doc_id]).fetchone()
    if row:
        url, title = row
        return Result(url, title, score)
    else:
        raise Exception(f"no document with id {doc_id} in index")

def get_document_url(doc_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM document WHERE id=?", (doc_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def get_top_12_results(query, max_query_terms=50):
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.db')
    conn = apsw.Connection(db_path)

    cursor = conn.cursor()
    cursor.execute("PRAGMA busy_timeout = 30000;")

    original_query_terms = preprocess_text(query)
    if len(original_query_terms) > max_query_terms:
        original_query_terms = truncate_query(original_query_terms, max_terms=max_query_terms)

    enriched_query_terms = enrich_query(original_query_terms)

    print("Original Query Terms:", original_query_terms)
    print("Enriched Query Terms:", enriched_query_terms)

    top_documents = calculate_bm25_score(enriched_query_terms, conn, original_query_terms)

    results = [result_from_id(doc_id, score, conn) for doc_id, score in top_documents]

    scores = [result.score for result in results if result]
    min_score, max_score = min(scores), max(scores)
    for result in results:
        result.normalize_score(min_score, max_score)

    return results