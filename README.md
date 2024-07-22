# Usage

Crawler CLI:
```
python -m crawl.cli
```

Install requirements:
```
pip install -r requirements.txt
```

Download NLTK Corpora:
```
python -m crawl.cli download-corpora
```

Initialize crawler database & start crawling from the seed set:
```
python -m crawl.cli init-db
python -m crawl.cli load-urls
python -m crawl.cli crawl
```

Create the index from the crawler database:
```
python -m crawl.cli index-all
```

Query the index with a batch file:
```
python -m crawl.cli query
```

Serve the web interface locally:
```
python GUI/server_init.py
```
