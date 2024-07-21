BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS "document" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"request_id"	INTEGER NOT NULL,
	"last_modified"	INTEGER,
	"simhash"	BLOB NOT NULL,
	"relevance"	REAL,
	"language"	TEXT,
	"title"	TEXT,
	"content"	TEXT,
	FOREIGN KEY ("request_id") REFERENCES "request"
);

-- Use "status" either as INTEGER status code or REAL timestamp
-- ANY + STRICT to ensure these don't get mixed up
-- https://www.sqlite.org/stricttables.html#strict_tables
CREATE TABLE IF NOT EXISTS "request" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"url_id"	INTEGER NOT NULL,
	"time"	REAL NOT NULL,
	"duration"	REAL,
	"status"	ANY,
	"headers"	TEXT,
	"data"	BLOB,
	FOREIGN KEY("url_id") REFERENCES "url"
) STRICT;

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

CREATE VIEW IF NOT EXISTS "request_urls" AS
	SELECT
		request.id,
		url.url AS 'URL',
		time,
		duration,
		status,
		headers,
		data
	FROM request
	JOIN url ON url_id = url.id
	ORDER BY time;

CREATE VIEW IF NOT EXISTS "size_by_extension" AS
	WITH extension AS (
		SELECT
			id,
			CASE
				WHEN substr(url, -4) LIKE '%/%' THEN NULL
				WHEN url LIKE '%.___'  THEN substr(url, -3)
				WHEN url LIKE '%.____' THEN substr(url, -4)
			END AS extension
		FROM url
	)
	SELECT
		extension,
		COUNT() AS 'count',
		SUM(length(data)) / COUNT() AS avg_size,
		SUM(length(data)) AS total
	FROM request
	JOIN extension on url_id = extension.id
	JOIN url on url_id = url.id
	WHERE extension IS NOT NULL
	GROUP BY extension
	ORDER BY total DESC;

COMMIT;

