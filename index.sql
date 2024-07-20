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

CREATE TABLE IF NOT EXISTS "invertedIndex" (
    "word_id" INTEGER NOT NULL,
    "document_id" INTEGER NOT NULL,
    "frequency" INTEGER NOT NULL,
    "position" INTEGER NOT NULL,
    PRIMARY KEY("word_id", "document_id"),
    FOREIGN KEY("word_id") REFERENCES "word",
    FOREIGN KEY("document_id") REFERENCES "document"
    --"next_pointer" INTEGER,
    --"skip_pointer" INTEGER
);

COMMIT;
