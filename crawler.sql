BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS "document" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"fetched"		INTEGER	NOT NULL,
	"last_modified"	INTEGER,
	"content"	TEXT
);

CREATE TABLE IF NOT EXISTS "request" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"url_id"	INTEGER NOT NULL,
	"fetched"	REAL NOT NULL,
	"duration"	REAL NOT NULL,
	"last_modified"	INTEGER,
	"status"	INTEGER,
	"headers"	TEXT,
	"data"	BLOB,
	FOREIGN KEY("url_id") REFERENCES "url"
);

CREATE TABLE IF NOT EXISTS "url" (
	"id"	INTEGER	PRIMARY KEY,
	"url"	TEXT NOT NULL UNIQUE,
	"document_id" INTEGER,
	FOREIGN KEY("document_id") REFERENCES "document"
);

CREATE TABLE IF NOT EXISTS "frontier" (
	"position"	INTEGER	UNIQUE,
	"url_id"	INTEGER PRIMARY KEY,
	FOREIGN KEY("url_id") REFERENCES "url"
);

CREATE VIEW IF NOT EXISTS "frontier_urls" AS
	SELECT
		frontier.position AS 'position',
		url.url AS 'URL'
	FROM frontier
	JOIN url ON url_id = url.id
	ORDER BY frontier.position;

COMMIT;

