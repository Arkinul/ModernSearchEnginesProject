import re
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import spacy

# Load the spaCy NER model
nlp = spacy.load("en_core_web_sm")

def preprocess(text):
    """
    Preliminary preprocessing: tokenization, stopword removal, and lemmatization.
    Later, this should use the same preprocessing as document preprocessing during indexing.
    """
    tokens = word_tokenize(text.lower())
    tokens = [re.sub(r'\W+', '', token) for token in tokens if token.isalnum()]
    filtered_tokens = [word for word in tokens if word not in stopwords.words('english')]
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
    return lemmatized_tokens

def find_synonyms(word, max_terms_per_token=3):
    """
    Finds synonyms for a given word using WordNet.
    """
    synonyms = set()
    for syn in wordnet.synsets(word, lang='eng'):
        for lemma in syn.lemmas(lang='eng'):
            processed_synonym = preprocess(lemma.name().replace('_', ' '))
            synonyms.update(processed_synonym)
            if len(synonyms) >= max_terms_per_token:
                break
        if len(synonyms) >= max_terms_per_token:
            break
    return list(synonyms)

def term_priority(term, term_freq, named_entities):
    """
    Prioritizes terms based on frequency, named entity status, POS tag, and length.
    """
    pos = term[1]
    length = len(term[0])
    frequency = term_freq[term[0]]
    is_named_entity = term[0] in named_entities
    return (frequency, is_named_entity, pos in ('NN', 'NNS', 'NNP', 'NNPS'), pos in ('JJ', 'VB'), length)

def truncate_query(preprocessed_query, max_terms=20):
    """
    Truncates the query to a maximum number of terms based on prioritization.
    """
    tagged_tokens = nltk.pos_tag(preprocessed_query)
    term_freq = Counter(preprocessed_query)
    doc = nlp(" ".join(preprocessed_query))
    named_entities = {ent.text for ent in doc.ents}

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
    """
    Enriches the query with synonyms up to a maximum number of terms.
    Truncates the query first if it exceeds the truncation threshold.
    """
    truncated_query = None
    if len(preprocessed_query) > truncation_threshold:
        truncated_query = truncate_query(preprocessed_query, max_terms=truncation_threshold)
        preprocessed_query = truncated_query
    
    enriched_query = set(preprocessed_query)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(find_synonyms, token, max_terms_per_token) for token in preprocessed_query]
        for future in as_completed(futures):
            synonyms = future.result()
            for term in synonyms:
                if len(enriched_query) >= max_total_terms:
                    break
                enriched_query.add(term)
            if len(enriched_query) >= max_total_terms:
                break

    if len(enriched_query) > max_total_terms:
        enriched_query = list(enriched_query)[:max_total_terms]
    
    return list(enriched_query), truncated_query

def main():
    """
    Tests multiple queries related to Tübingen.
    Preprocesses, potentially truncates, and enriches each query.
    """
    queries = [
        "What are the best tourist attractions in Tübingen?",
        "Find top-rated restaurants in Tübingen that offer vegetarian options.",
        "Historical sites and museums to visit in Tübingen.",
        "Events and festivals happening in Tübingen this month.",
        "Outdoor activities and parks in Tübingen.",
        "Tübingen public transportation information and schedules.",
        "Best cafes in Tübingen with free Wi-Fi.",
        "Shopping centers and local markets in Tübingen.",
        "Accommodation options in Tübingen for a weekend trip.",
        "University of Tübingen notable alumni and academic programs."
    ]

    for query in queries:
        print(f"Testing query: {query}")
        preprocessed_query = preprocess(query)
        print("Preprocessed Query:", preprocessed_query)
        final_enriched_query, truncated_query = enrich_query(preprocessed_query, max_total_terms=15, max_terms_per_token=2, truncation_threshold=20)
        if truncated_query:
            print("Truncated Query:", truncated_query)
        print("Enriched Query:", final_enriched_query)
        print("-" * 50)

if __name__ == "__main__":
    main()
