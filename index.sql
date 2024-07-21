BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS "document" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"content"	TEXT,
    "url" TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS "word" (
    "id"    INTEGER NOT NULL PRIMARY KEY,
    "word"  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS "inverted_index" (
    "word_id" INTEGER NOT NULL,
    "document_id" INTEGER NOT NULL,
    "position" INTEGER NOT NULL,
    UNIQUE("document_id", "position"),
    FOREIGN KEY("word_id") REFERENCES "word",
    FOREIGN KEY("document_id") REFERENCES "document"
);

COMMIT;
