#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
import apsw
from collections import Counter

from crawl import DEFAULT_CRAWLER_DB, DEFAULT_INDEX_DB
from crawl.document import Document


def index_all_db(crawl_db=DEFAULT_CRAWLER_DB, index_db=DEFAULT_INDEX_DB):
    con = apsw.Connection(crawl_db)

    max_doc_id = con.execute(
        "SELECT MAX(id) FROM document"
    ).fetchone()[0]
    for doc_id in range(1, max_doc_id + 1):
        doc = Document.load(doc_id, db)
        index(doc, index_db)


def index(doc, db=DEFAULT_INDEX_DB):

    con = apsw.Connection(db)
    cur = con.cursor()

    #preprocess text
    words = preprocess_text(doc.text_content)
    word_counts = Counter(words)

    #insert document
    con.execute(
        "INSERT INTO document (id,content,url) VALUES (?,?,?)",
        (doc.id, doc.text_content, doc.url)
    )

    for word in words:
        con.execute(
            "INSERT INTO word (word) VALUES (?) ON CONFLICT DO NOTHING",
            word
        )
        con.execute(
            "INSERT INTO inverted_index (word_id, document_id, frequency, position) VALUES ( \
                (SELECT id FROM word WHERE word = ?), \
                ?, \
                ? \
                ?)",
            (word, doc.id, word_counts[word], words.index(word))
        )


