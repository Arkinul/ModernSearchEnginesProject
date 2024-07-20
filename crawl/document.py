from collections import Counter
import json
import re
import apsw
from urllib.parse import urljoin

from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup

from crawl import DEFAULT_CRAWLER_DB
from crawl.process import compute_simhash, is_near_duplicate_simhash, normalize_url, preprocess_text

# e.g. if 1 out of 100 words is a keyword, site is relevant
KEYWORD_DENSITY_THRESHOLD = 0.01


KEYWORD_WEIGHTS = {
    "tübingen": 1.0,
    "hölderlin": 1.0,
    "hohenzollern": 1.0,
    "neckar": 1.0,
    "schwaben": 1.0,
    "schwäbisch": 1.0,
    "tübinger": 1.0,
    "bebenhausen": 1.0,
    "tubingen": 1.0,
    "tuebingen": 1.0,
    "tuebinger": 1.0,
    "swabian": 1.0,
    "schwaebisch": 1.0,
    "schwabisch": 1.0
}

IRRELEVANT_TAGS = [
    "script",
    "style",
    "link",
    "meta",
    "header",
    "nav",
    "aside",
    "footer",
    "form",
    "iframe",
    "template",
    "button",
    "input",
    "select",
    "textarea",
    "label",
    "img",
    "picture",
    "svg",
    "canvas",
    "audio",
    "video",
    "object",
    "param",
    "source",
    "track",
    "noscript",
    "map",
    "area",
    "figure",
    "figcaption",
    "details",
    "summary",
    "dialog",
    "menu",
    "menuitem",
    "applet",
    "embed"
]


class Document:
    def __init__(self, request_id, url, headers, data: bytes):
        self.text_content = None
        self.request_id = request_id
        self.url = url
        self.headers = headers
        self.data = data
        self.relevance_score = None
        self.simhash_value = None
        self.id = None

    def parse(self) -> bool:
        try:
            self.soup = BeautifulSoup(self.data, 'html.parser')
            self.lang = self.soup.html.get('lang') if self.soup.html else None
            self.title = self.soup.title.string if self.soup.title else None
            meta_description = self.soup.find(
                "meta",
                attrs={"name": "description"}
            )
            if meta_description:
                self.meta_description = meta_description.get("content")
            else:
                self.meta_description = None

            for tag in self.soup(IRRELEVANT_TAGS):
                tag.extract()

            text = self.soup.get_text(separator=' ')
            lines = (line.strip() for line in text.splitlines())
            chunks = (
                phrase.strip()
                for line in lines
                for phrase in line.split()
            )
            self.text_content = ' '.join(chunk for chunk in chunks if chunk)
        except Exception as e:
            print(f"failed to parse {self.url}: {e}")
            return False
        else:
            return True


    def is_english(self) -> bool:
        if type(self.lang) == str and self.lang.lower().startswith("en"):
            return True
        if type(self.headers) == dict:
            if lang := self.headers.get("Content-Language"):
                return lang.lower().startswith("en")
        return False


    def relevance(self) -> float:
        if self.relevance_score:
            return self.relevance_score

        stemmed_words = preprocess_text(self.text_content)  # Stem words on site

        # Stem keywords as well
        stemmed_keywords = {
            preprocess_text(keyword).pop(): weight
            for keyword, weight
            in KEYWORD_WEIGHTS.items()
        }

        # Count how often each word appears on a site and the number of total words
        word_counts = Counter(stemmed_words)
        total_words = len(stemmed_words)

        # Count the number of relevant words on a site
        relevant_count = 0
        for word, weight in stemmed_keywords.items():
            if word in word_counts:
                relevant_count += word_counts[word] * weight

        # Keyword density = out of all words, how many of them are keywords
        # We can use this instead of a binary relevancy check and adjust the threshold below
        # Probably (?) better than only relevant or not
        if total_words > 0:
            keyword_density = relevant_count / total_words
        else:
            keyword_density = 0
        self.relevance_score = keyword_density
        return keyword_density

    def is_relevant(self) -> bool:
        return self.relevance() >= KEYWORD_DENSITY_THRESHOLD

    def simhash(self) -> int:
        if self.simhash_value:
            return self.simhash_value
        texts = [self.text_content, self.title, self.meta_description]
        self.simhash_value = compute_simhash([t for t in texts if t])
        return self.simhash_value

    def check_for_duplicates(self, db=DEFAULT_CRAWLER_DB) -> bool:
        con = apsw.Connection(db)
        hashes = con.execute("SELECT id, simhash FROM document").fetchall()
        for doc_id, simhash_bytes in hashes:
            assert type(simhash_bytes) == bytes, "invalid simhash type"
            simhash = int.from_bytes(simhash_bytes, byteorder='big')
            if is_near_duplicate_simhash(self.simhash(), simhash):
                print(f"document is near duplicate of {doc_id}")
                return True
        return False

    def links(self):
        for link_tag in self.soup.find_all('a', href=True):
            if link := link_tag.get('href'):
                if link[0] != "#": continue
                absolute = urljoin(self.url, link)
                if not absolute.startswith("http"): continue
                yield normalize_url(absolute)


    def save(self, db=DEFAULT_CRAWLER_DB):
        """
        Store the document in the database.
        """
        if not self.request_id:
            raise Exception("cannot store document without request id")
        con = apsw.Connection(db)
        res = con.execute(
            "INSERT INTO document ( \
                request_id, \
                simhash, \
                relevance, \
                content \
            ) \
            VALUES (?1, ?2, ?3, ?4) \
            RETURNING id",
            (
                self.request_id,
                self.simhash().to_bytes(16, byteorder='big'),
                self.relevance(),
                self.text_content
            )
        ).fetchone()
        if res:
            (self.id,) = res
            return self.id
        else:
            raise Exception("failed to store document")


    @staticmethod
    def load(doc_id, db):
        doc = Document(None, None, None, None)
        con = apsw.Connection(db)
        row = con.execute(
            "SELECT id, request_id, simhash, relevance, content \
            FROM document \
            WHERE id = ?1",
            (doc_id, )
        ).fetchone()
        if row:
            (
                doc.id,
                doc.request_id,
                simhash_bytes,
                doc.relevance_score,
                doc.text_content
            ) = row
            doc.simhash_value = int.from_bytes(simhash_bytes, byteorder='big')
            return doc
        else:
            raise Exception("document not found")


    @staticmethod
    def load_request(request_id, db):
        con = apsw.Connection(db)
        row = con.execute(
            "SELECT url.url, JSON(headers), data \
            FROM request \
            JOIN url ON url_id = url.id \
            WHERE request.id = ?1",
            (request_id, )
        ).fetchone()
        if row:
            url, headers_json, data = row
            if not headers_json:
                return None
            headers = json.loads(headers_json)
            doc = Document(request_id, url, headers, data)
            return doc
        return None
