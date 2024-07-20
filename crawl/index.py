#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
import apsw
from collections import Counter

from crawl import DEFAULT_CRAWLER_DB, DEFAULT_INDEX_DB
from crawl.document import Document
from crawl.process import preprocess_text


def index_all_db(crawl_db=DEFAULT_CRAWLER_DB, index_db=DEFAULT_INDEX_DB):
    con = apsw.Connection(crawl_db)

    max_doc_id = con.execute(
        "SELECT MAX(id) FROM document"
    ).fetchone()[0]
    for doc_id in range(1, max_doc_id + 1):
        doc = Document.load(doc_id, db=crawl_db)
        index(doc, index_db)


def index(doc: Document, db=DEFAULT_INDEX_DB):
    con = apsw.Connection(db)
    #preprocess text
    words = preprocess_text(doc.text_content)

    #insert document
    con.execute(
        "INSERT INTO document (id, content, url) VALUES (?1, ?2, ?3)",
        (doc.id, doc.text_content, doc.url)
    )

    for word_index, word in enumerate(words):
        con.execute("INSERT OR IGNORE INTO word (word) VALUES (?1)", (word, ))
        con.execute(
            "INSERT INTO inverted_index (word_id, document_id, position) \
            VALUES ((SELECT id FROM word WHERE word = ?1), ?2, ?3)",
            (word, doc.id, word_index)
        )


