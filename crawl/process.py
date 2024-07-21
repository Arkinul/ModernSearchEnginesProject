import hashlib
import math
from collections import Counter
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.chunk import ne_chunk
from nltk import pos_tag
from nltk.tokenize import word_tokenize
from nltk.tree import Tree
from url_normalize import url_normalize
import re
import apsw
import os

# Absoluter Pfad zur Index-Datenbank
DEFAULT_INDEX_DB = os.path.abspath('../index.db')  # Angenommen, die Index-Datenbank befindet sich eine Ebene höher im Verzeichnisbaum


def preprocess_text(text):
    lemmatizer = WordNetLemmatizer()
    low = text.lower()
    words = re.findall(r'\b\w+\b', low)
    stemmed_words = [lemmatizer.lemmatize(word) for word in words]
    filtered_words = [word for word in stemmed_words if word not in stopwords.words('english')]
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
    tagged_tokens = pos_tag(tokens)
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
    
    # Use NLTK to identify named entities
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

def enrich_query(preprocessed_query, max_total_terms=15, max_terms_per_token=3, truncation_threshold=20):
    truncated_query = None
    if len(preprocessed_query) > truncation_threshold:
        truncated_query = truncate_query(preprocessed_query, max_terms=truncation_threshold)
        preprocessed_query = truncated_query
    
    enriched_query = set(preprocessed_query)
    
    for token in preprocessed_query:
        synonyms = find_synonyms(token, max_terms_per_token)
        for term in synonyms:
            if len(enriched_query) >= max_total_terms:
                break
            enriched_query.add(term)
        if len(enriched_query) >= max_total_terms:
            break

    if len(enriched_query) > max_total_terms:
        enriched_query = list(enriched_query)[:max_total_terms]
    
    return list(enriched_query), truncated_query

# BM25 parameters
k1 = 1.5
b = 0.75

# Function to calculate BM25 score
def calculate_bm25_score(query_terms, conn):
    query_term_freq = {term: query_terms.count(term) for term in set(query_terms)}

    # Get document frequencies and term frequencies
    doc_freqs = {}
    term_freqs = {}
    doc_lengths = {}
    doc_count = 0
    avg_doc_length = 0

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM document")
    doc_count = cursor.fetchone()[0]

    cursor.execute("SELECT document_id, COUNT(*) as length FROM inverted_index GROUP BY document_id")
    for row in cursor.fetchall():
        doc_lengths[row[0]] = row[1]
        avg_doc_length += row[1]
    
    avg_doc_length /= doc_count

    for term in query_terms:
        cursor.execute("SELECT id FROM word WHERE word=?", (term,))
        word_id_row = cursor.fetchone()
        if not word_id_row:
            continue
        word_id = word_id_row[0]
        
        cursor.execute("SELECT COUNT(*) FROM inverted_index WHERE word_id=?", (word_id,))
        doc_freqs[term] = cursor.fetchone()[0]

        cursor.execute("SELECT document_id, COUNT(*) FROM inverted_index WHERE word_id=? GROUP BY document_id", (word_id,))
        term_freqs[term] = {row[0]: row[1] for row in cursor.fetchall()}

    scores = {}
    for term, freq in query_term_freq.items():
        if term not in doc_freqs:
            continue
        idf = math.log((doc_count - doc_freqs[term] + 0.5) / (doc_freqs[term] + 0.5) + 1)
        for doc_id, term_freq in term_freqs[term].items():
            if doc_id not in scores:
                scores[doc_id] = 0
            doc_len = doc_lengths.get(doc_id, 0)
            tf = term_freq
            score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_length))))
            scores[doc_id] += score

    return sorted(scores.items(), key=lambda item: item[1], reverse=True)[:100]

# Function to get the URL for a given document ID
def get_document_url(doc_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM document WHERE id=?", (doc_id,))
    result = cursor.fetchone()
    return result[0] if result else None

# Main function to get top 100 documents based on BM25
def get_top_100_documents(query):
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.db')
    conn = apsw.Connection(db_path)
    
    # Optionally set busy timeout
    cursor = conn.cursor()
    cursor.execute("PRAGMA busy_timeout = 30000;")  # 30 seconds
    
    top_documents = calculate_bm25_score(query, conn)
    
    # Get URLs for the top documents
    top_documents_with_urls = [(get_document_url(doc_id, conn), score) for doc_id, score in top_documents]
    
    conn.close()
    return top_documents_with_urls

def get_connection(db_path):
    con = apsw.Connection(db_path)
    return con

# Hauptfunktion zur Durchführung der Abfrage und Ausgabe der Ergebnisse
def main(query_text, n=100):
    preprocessed_query = preprocess_text(query_text)
    enriched_query, truncated_query = enrich_query(preprocessed_query)

    print("Original Query:", preprocessed_query)
    print("Enriched Query:", enriched_query)
    
    top_docs = get_top_100_documents(enriched_query)
    
    for url, score in top_docs:
        print(f"URL: {url}\nScore: {score}\n{'-'*80}")

# Beispielabfrage
query_text = "food and drink"
main(query_text)
