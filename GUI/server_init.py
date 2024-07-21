from flask import Flask, request, render_template, jsonify
import sys
import os
import logging

# Füge den Pfad zu `crawl` hinzu, um `process.py` zu importieren
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../crawl')))
from process import preprocess_text, enrich_query, get_top_12_documents

app = Flask(__name__)

# Logging konfigurieren
logging.basicConfig(level=logging.DEBUG)

# Define route for the homepage
@app.route('/')
def index():
    return render_template('index2.html')

# Define route for search endpoint that accepts POST requests
@app.route('/generate_word_cloud', methods=['POST'])
def search():
    query = request.json.get('query', '')  # Extract search query from JSON body of POST request
    app.logger.debug(f'Received query: {query}')
    top_docs = get_top_12_documents(query)
    
    results = [{"text": url, "value": score} for url, score in top_docs if url is not None]
    
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
