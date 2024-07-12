BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS "host" (
	"id"	INTEGER NOT NULL PRIMARY KEY,
	"origin"		TEXT NOT NULL UNIQUE,
	"global_policy"	BOOL,
	"robots_txt"	TEXT,
	"refill_delay"	REAL,
	"refill_cap"	REAL CHECK(refill_cap >= 0),
	"updated"	REAL,
	"tokens"	REAL CHECK(tokens >= 0 AND tokens <= refill_cap)
);

COMMIT;

