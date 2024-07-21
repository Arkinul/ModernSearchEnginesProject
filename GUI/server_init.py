from flask import Flask, request, render_template, jsonify
import sys
import os
import logging

# Füge den Pfad zu `crawl` hinzu, um `process.py` zu importieren
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../crawl')))
from process import preprocess_text, enrich_query, get_top_12_results

app = Flask(__name__)

# Logging konfigurieren
logging.basicConfig(level=logging.DEBUG)

# Define route for the homepage
@app.route('/')
def index():
    return render_template('index.html')

# Define route for search endpoint that accepts POST requests
@app.route('/generate_word_cloud', methods=['POST'])
def search():
    query = request.json.get('query', '')  # Extract search query from JSON body of POST request
    app.logger.debug(f'Received query: {query}')

    results = [
        {
            "text": result.title if result.title else result.url,
            "value": result.score,
            "url": result.url
        }
        for result
        in get_top_12_results(query)
    ]

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
