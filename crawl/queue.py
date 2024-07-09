import sqlite3
from contextlib import contextmanager
import warnings

class Index:
    def __init__(self, db):
        # https://docs.python.org/3/library/sqlite3.html#transaction-control
        self.con = sqlite3.connect(db, isolation_level = None)
        self.con.execute("PRAGMA foreign_keys = 1")


    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    # credit: https://github.com/litements/litequeue/blob/main/litequeue.py#L568
    @contextmanager
    def transaction(self, mode="DEFERRED"):
        if mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            raise ValueError(f"Transaction mode '{mode}' is not valid")
        # We must issue a "BEGIN" explicitly when running in auto-commit mode.
        self.con.execute(f"BEGIN {mode}")
        try:
            # Yield control back to the caller.
            yield
        except BaseException as e:
            self.con.rollback()  # Roll back all changes if an exception occurs.
            raise e
        else:
            self.con.commit()


class Queue(Index):
    def __iter__(self):
        return self


    def __next__(self):
        if entry := self.pop():
            return entry
        else:
            raise StopIteration


    def shift(self, cur, position, amount = 1):
        '''
        Move all rows in the frontier >= `position` back by `amount`

        Negative `amount`s shift them forward.
        '''
        # push forward or back
        # can't be done in one statement due to this limitation:
        # https://stackoverflow.com/a/7703239
        cur.execute(
            "UPDATE frontier \
            SET position = -(position + ?2) \
            WHERE position >= ?1",
            (position, amount)
        )
        cur.execute(
            "UPDATE frontier \
            SET position = abs(position) \
            WHERE position < 0"
        )


    def push(self, url):
        '''
        Add an URL to the end of the frontier & create `url` entry if necessary

        Does nothing if the URL is already queued.
        '''
        # TODO: normalize URL
        with self.transaction():
            cur = self.con.cursor()
            # check if URL is already in the URL table, also return position if already queued
            id_and_position = cur.execute(
                "SELECT id, frontier.position FROM url \
                FULL OUTER JOIN frontier ON url.id = frontier.url_id \
                WHERE url.url LIKE ?1",
                (url, )
            ).fetchone()
            if id_and_position:
                #print(f"URL already stored with id {url_id}")
                url_id, prev_pos = id_and_position
                if prev_pos is not None:
                    #print(f"URL already queued at {prev_pos}")
                    return
            else:
                # insert URL into url table
                res = cur.execute(
                    "INSERT OR IGNORE INTO url (url) \
                    VALUES (?1) \
                    RETURNING url.id",
                    (url, )
                ).fetchone()
                assert cur.rowcount == 1, f"failed to store URL {url} in table"
                (url_id, ) = res

            # insert frontier entry at the end
            cur.execute(
                "INSERT INTO frontier (position, url_id) \
                VALUES ( \
                    IFNULL((SELECT max(position) + 1 FROM frontier), 0), \
                    ?1 \
                )",
                (url_id, )
            )


    def pop(self):
        with self.transaction():
            cur = self.con.cursor()
            pos_and_url = cur.execute(
                "DELETE FROM frontier \
                WHERE position = (SELECT min(position) FROM frontier) \
                RETURNING position, (SELECT url FROM url WHERE id = url_id)"
            ).fetchone()
            if pos_and_url:
                pos, url = pos_and_url
                if pos >= 0:
                    #TODO: optimize by not shifting down after every single pop
                    self.shift(cur, pos, -1)
                else:
                    warnings.warn(f"queue entry with negative position {pos}")
                return url
            else:
                return None


    def insert(self, url, position):
        '''
        Insert an URL into the `url` table and add it to the frontier at the given position, without creating gaps
        '''
        # TODO: normalize URL
        with self.transaction():
            cur = self.con.cursor()
            # check if URL is already in the URL table, also return position if already queued
            id_and_position = cur.execute(
                "SELECT id, frontier.position FROM url \
                FULL OUTER JOIN frontier ON url.id = frontier.url_id \
                WHERE url.url LIKE ?1",
                (url, )
            ).fetchone()
            if id_and_position:
                url_id, prev_pos = id_and_position
                #print(f"URL already stored with id {url_id}")
                if prev_pos == position:
                    return
                elif prev_pos is not None:
                    #print(f"URL already queued at {prev_pos}")
                    # take it out of the frontier
                    cur.execute(
                        "DELETE FROM frontier WHERE position = ?1",
                        (prev_pos, )
                    )
                    # close the gap
                    self.shift(cur, prev_pos, -1)
                    # TODO: optimize by checking if prev_pos < position, only shift once
            else:
                # insert URL into url table
                res = cur.execute(
                    "INSERT OR IGNORE INTO url (url) \
                    VALUES (?1) \
                    RETURNING url.id",
                    (url, )
                ).fetchone()
                assert cur.rowcount == 1, f"failed to store URL {url} in table"
                (url_id, ) = res

            # make space for the frontier entry
            self.shift(cur, position)
            # insert frontier entry into the gap or at the end, but not after
            cur.execute(
                "INSERT INTO frontier (position, url_id) \
                VALUES ( \
                    MAX(0, MIN( \
                        ?1, \
                        IFNULL((SELECT max(position) + 1 FROM frontier), 0) \
                    )), \
                    ?2 \
                )",
                (position, url_id)
            )

