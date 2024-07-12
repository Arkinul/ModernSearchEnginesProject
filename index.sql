BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS "document" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"fetched"		INTEGER	NOT NULL,
	"last_modified"	INTEGER,
	"content"	TEXT
);

CREATE TABLE IF NOT EXISTS "invertedIndex" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "word" TEXT NOT NULL,
    "document_id" INTEGER,
    FOREIGN KEY("document_id") REFERENCES "document",
    "next_pointer" INTEGER,
    "skip_pointer" INTEGER
);

COMMIT;
