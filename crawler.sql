BEGIN TRANSACTION;


CREATE TABLE IF NOT EXISTS "document" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"fetched"		INTEGER	NOT NULL,
	"last_modified"	INTEGER,
	"content"	TEXT
)


CREATE TABLE IF NOT EXISTS "url" (
	"id"	INTEGER	PRIMARY KEY,
	"url"	TEXT NOT NULL,
	"document_id" INTEGER,
	FOREIGN KEY("document_id") REFERENCES "document"
);

CREATE TABLE IF NOT EXISTS "frontier" (
	"url_id"	INTEGER PRIMARY KEY,
	FOREIGN KEY("url_id") REFERENCES "url"
);


COMMIT;

