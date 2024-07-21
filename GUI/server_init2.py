from flask import Flask, request, render_template, jsonify

# TODO: Use this later instead of temporary generate_word_cloud()
# from some_python_file import get_retrieved_documents

app = Flask(__name__)

# Define route for the homepage
@app.route('/')
def index():
    return render_template('index2.html')

# Define route for search endpoint that accepts POST requests
@app.route('/generate_word_cloud', methods=['POST'])
def search():
    query = request.json.get('query', '')  # Extract search query from JSON body of POST request
    # TODO: Change results to our fetched results (results <- our retrieved documents)
    # TODO: Use import (see import at the top) instead of the temporary generate_word_cloud() function below
    results = generate_word_cloud(query)
    return jsonify(results)

def generate_word_cloud(query):
    # Simulate search result data
    # TODO: This needs to be replaced with our actual search results
    results = [
        {"text": "Tübingen Concerts", "value": 100},
        {"text": "University of Tübingen", "value": 80},
        {"text": "Hölderlin tower", "value": 60},
        {"text": "Neckar river", "value": 50},
        {"text": "Top 10 Restaurants in Tübingen", "value": 40},
        {"text": "University Hospital Tübingen", "value": 30},
        {"text": "Epplehaus", "value": 20},
        {"text": "Castle Hohentübingen", "value": 20},
        {"text": "Cyber Valley", "value": 20},
        {"text": "Neptune fountain", "value": 35},
        {"text": "Botanical garden", "value": 20},
        {"text": "Neckarmüller", "value": 20},
    ]
    return results

if __name__ == '__main__':
    app.run(debug=True)
