import sqlite3
import numpy as np
import math
from collections import Counter


# Placeholder for the preprocess function
def preprocess(text):
    # This function will be implemented later or imported (depending on preprocessing of documents)
    return text.split()  

# Fetch relevant documents from inverted index
def fetch_relevant_documents_from_index(db_path, query_tokens):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(query_tokens))
    query = f"SELECT DISTINCT document_id FROM invertedIndex WHERE word IN ({placeholders})"
    cursor.execute(query, query_tokens)
    document_ids = [row[0] for row in cursor.fetchall()]
    
    documents = {}
    for doc_id in document_ids:
        cursor.execute("SELECT content FROM document WHERE id = ?", (doc_id,))
        row = cursor.fetchone()
        if row:
            documents[doc_id] = row[0].split()  # Assuming the content is already preprocessed and tokenized
    
    conn.close()
    return documents

# Calculate term frequencies (tf) for each document
def calculate_tf(doc):
    return Counter(doc)

# Calculate document frequencies (df) for all documents
def calculate_df(docs):
    df = Counter()
    for doc in docs:
        unique_terms = set(doc)
        for term in unique_terms:
            df[term] += 1
    return df

# Calculate inverse document frequencies (idf) for all terms
def calculate_idf(docs, df):
    N = len(docs)
    idf = {}
    for term, freq in df.items():
        idf[term] = math.log((N - freq + 0.5) / (freq + 0.5) + 1)
    return idf

# BM25 scoring function
def bm25_score(query, doc, tf, idf, avg_doc_len, k1=1.5, b=0.75):
    score = 0
    doc_len = len(doc)
    for term in query:
        if term in tf:
            term_freq = tf[term]
            term_idf = idf.get(term, 0)
            score += term_idf * ((term_freq * (k1 + 1)) / (term_freq + k1 * (1 - b + b * (doc_len / avg_doc_len))))
    return score

# Prepare the BM25 model
def prepare_bm25(docs):
    avg_doc_len = sum(len(doc) for doc in docs.values()) / len(docs)
    df = calculate_df(docs.values())
    idf = calculate_idf(docs.values(), df)
    tf_docs = {doc_id: calculate_tf(doc) for doc_id, doc in docs.items()}
    return avg_doc_len, idf, tf_docs

# BM25 retrieval function
def bm25_retrieve(query, docs, avg_doc_len, idf, tf_docs, top_k=5):
    query_tokens = preprocess(query)
    scores = []
    for doc_id, doc in docs.items():
        score = bm25_score(query_tokens, doc, tf_docs[doc_id], idf, avg_doc_len)
        scores.append((doc_id, score))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_k]

# Main function to run the retrieval for a single query
def main_single_query(db_path, query):
    query_tokens = preprocess(query)
    docs = fetch_relevant_documents_from_index(db_path, query_tokens)
    if not docs:
        print("No relevant documents found.")
        return
    
    avg_doc_len, idf, tf_docs = prepare_bm25(docs)
    top_docs = bm25_retrieve(query, docs, avg_doc_len, idf, tf_docs)
    
    print("Original Query:", query)
    print("\nTop matching documents:")
    for doc_id, score in top_docs:
        print(f"Document ID: {doc_id}, Score: {score}")

# Function to run the retrieval for multiple queries from a batch file
def main_batch_queries(db_path, batch_file_path):
    with open(batch_file_path, 'r') as file:
        queries = file.readlines()

    for line in queries:
        query_id, query_text = line.strip().split('\t')
        print(f"\nQuery ID: {query_id}")
        main_single_query(db_path, query_text)

db_path = './index.sql'

# Single query
query = "food and drinks"
main_single_query(db_path, query)

# Batch query example
batch_file_path = './queries.txt'
main_batch_queries(db_path, batch_file_path)