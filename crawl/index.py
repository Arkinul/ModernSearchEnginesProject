#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
import apsw
from collections import Counter

from tqdm import tqdm

from crawl import DEFAULT_CRAWLER_DB, DEFAULT_INDEX_DB
from crawl.document import Document
from crawl.process import preprocess_text


def index_all_db(crawl_db=DEFAULT_CRAWLER_DB, index_db=DEFAULT_INDEX_DB):
    crawl_con = apsw.Connection(crawl_db)
    index_con = apsw.Connection(index_db)

    (total, ) = crawl_con.execute("SELECT COUNT() FROM document").fetchone()

    for doc in tqdm(Document.load_all(crawl_con), total=total):
        index(doc, index_con)


def index(doc: Document, con: apsw.Connection):
    #preprocess text
    words = preprocess_text(doc.text_content)

    with con:
        try:
            #insert document
            con.execute(
                "INSERT INTO document (id, content, title, url) \
                VALUES (?1, ?2, ?3, ?4)",
                (doc.id, doc.text_content, doc.title, doc.url)
            )
        except apsw.ConstraintError:
            # assumes that the document is fully hashed if it exists
            return

        for word_index, word in enumerate(words):
            con.execute(
                "INSERT OR IGNORE INTO word (word) VALUES (?1)",
                (word, )
            )
            con.execute(
                "INSERT INTO inverted_index (word_id, document_id, position) \
                VALUES ((SELECT id FROM word WHERE word = ?1), ?2, ?3)",
                (word, doc.id, word_index)
            )


