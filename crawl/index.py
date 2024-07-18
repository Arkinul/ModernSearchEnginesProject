#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
import apsw


def index(doc, db=DEFAULT_INDEX_DB):

    con = apsw.Connection(db)
    cur = con.cursor()

    #preprocess text
    words = preprocess_text(doc.text_content)
    total_words = len(words)

    for word in words:
        con.execute(
            "INSERT INTO word (word) VALUES (?) ON CONFLICT DO NOTHING",
            word
        )



