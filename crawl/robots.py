import os
import apsw
import time
from http.client import RemoteDisconnected
from urllib import error, request
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from crawl import DEFAULT_HOSTS_DB


# TODO: actually use this when making requests
USER_AGENT = 'MSE_Crawler'
DEFAULT_REFILL_CAP = 60
DEFAULT_REFILL_RATE = DEFAULT_REFILL_CAP / 30

# TODO: make this configurable / robust
HOSTS_DB_SQL = "hosts.sql"


class Host:
    @staticmethod
    def open_db(hosts_db_path=DEFAULT_HOSTS_DB) -> apsw.Connection:
        existed = os.path.exists(hosts_db_path)
        con = apsw.Connection(hosts_db_path)
        if not existed:
            with open(HOSTS_DB_SQL) as file:
                schema = file.read()
                con.execute(schema)
        return con


    def __init__(self, origin: str):
        self.origin = origin


    def fetch(self) -> None:
        """
        Copy of RobotFileParser.read(), with some modifications
        """
        try:
            self.rfp = RobotFileParser()
            f = request.urlopen(self.origin + "/robots.txt")
            raw = f.read()
            self.rfp.parse(raw.decode("utf-8").splitlines())
        except error.HTTPError as err:
            if err.code in (401, 403):
                self.global_policy = False
            if err.code in range(300, 400):
                self.global_policy = False
            elif err.code >= 400 and err.code < 500:
                self.global_policy = True
            elif err.code >= 500:
                self.global_policy = False
            else:
                # TODO make robust (ignore if they can't even serve robots.txt?)
                raise Exception(
                    f"failed to fetch robots for {self.origin}: {err}"
                )
            self.refill_cap = DEFAULT_REFILL_CAP
            self.refill_rate = DEFAULT_REFILL_RATE
            self.tokens = DEFAULT_REFILL_CAP
            self.robots_txt = None
        except (error.URLError, UnicodeDecodeError, RemoteDisconnected):
            #TODO: log
            # ignore hosts that have robots.txt with invalid unicode
            # ignore hosts that we can't fetch robots.txt from
            self.global_policy = False
            self.refill_cap = DEFAULT_REFILL_CAP
            self.refill_rate = DEFAULT_REFILL_RATE
            self.tokens = DEFAULT_REFILL_CAP
            self.robots_txt = None
        else:
            # TODO: log this instead of printing
            #print(f"fetched robots.txt for {self.origin}")

            # No global allow/disallow policy, store the robots.txt content
            # don't store the raw file, re-serialize the parsed RFP
            self.robots_txt = str(self.rfp)
            # TODO: technically the robots.txt could boil down to a global policy as well
            self.global_policy = None

            if rate := self.rfp.request_rate(USER_AGENT):
                self.refill_cap = rate.requests
                self.refill_rate = rate.seconds
            elif delay := self.rfp.crawl_delay(USER_AGENT):
                self.refill_cap = 1
                self.refill_rate = 1 / float(delay)
            else:
                self.refill_cap = DEFAULT_REFILL_CAP
                self.refill_rate = DEFAULT_REFILL_RATE

        self.updated = time.time()
        # start with full token bucket
        # TODO: seems to overshoot the intended rate in the very first period
        self.tokens = self.refill_cap
        # TODO: keep track of how old the robots.txt / host entry is


    def store(self, con: apsw.Connection):
        """
        Stores the Host & robots.txt in the hosts database
        """
        con.execute(
            "INSERT OR REPLACE INTO host ( \
                origin, \
                global_policy, \
                robots_txt, \
                refill_rate, \
                refill_cap, \
                updated, \
                tokens \
            ) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                self.origin,
                self.global_policy,
                self.robots_txt,
                self.refill_rate,
                self.refill_cap,
                self.updated,
                self.tokens
            )
        )


    def try_take_token(self, con: apsw.Connection, url: str) -> bool | float:
        if self.global_policy == False:
            return False
        if self.global_policy is None:
            if not self.rfp.can_fetch(USER_AGENT, url):
                return False
        now = time.time()
        # TODO: ensure this is actually atomic (disable journal?, share conn?)
        try:
            # TODO: this breaks if the clock shifts backward
            con.execute(
                "UPDATE host \
                SET tokens = MIN( \
                        tokens + ((?2 - updated) * refill_rate), \
                        refill_cap \
                    ) - 1, \
                    updated = ?2 \
                WHERE origin = ?1",
                (self.origin, now)
            )
            # TODO: logging
        except apsw.ConstraintError:
            # TODO: logging
            # constraint violated because bucket is empty
            # calculate remaining time
            needed = (1 - self.tokens) / self.refill_rate
            waited = now - self.updated
            # TODO: should only happen due to concurrent update, retry instantly
            assert needed > waited, f"{self.origin}, should have tokens"
            return needed - waited
        else:
            return True


    def try_load(self, con: apsw.Connection) -> bool:
        res = con.execute(
            "SELECT \
                origin, \
                global_policy, \
                robots_txt, \
                refill_rate, \
                refill_cap, \
                updated, \
                tokens \
            FROM host \
            WHERE origin = ?1",
            (self.origin, )
        ).fetchone()
        if res:
            (
                self.origin,
                self.global_policy,
                robots_txt,
                self.refill_rate,
                self.refill_cap,
                self.updated,
                self.tokens
            ) = res
            if self.global_policy is None:
                self.rfp = RobotFileParser()
                self.rfp.parse(robots_txt.splitlines())
            return True
        else:
            return False



def get_host(url):
    '''
    Extracts the host (scheme and netloc) from the given URL.

    Parameters:
    url (str): The URL from which the host is to be extracted.

    Returns:
    str: The host part of the URL, including the scheme.
    '''
    parsed_url = urlparse(url)
    return parsed_url.scheme + "://" + parsed_url.netloc


def can_crawl(
    url: str,
    con: apsw.Connection | None = None,
    hosts_db_path=DEFAULT_HOSTS_DB
) -> bool | float:
    '''
    Determines if the given URL is allowed to be crawled according to the robots.txt rules of the host and the rate-limiter.
    Automatically counts a request against the rate-limiter: assumes that a request will be made if this function returns `True`.

    Parameters:
    url (str): The URL to check against the robots.txt rules.
    hosts_db_path: path of the hosts database file (created if nonexistent).

    Returns:
    bool: `True` if the URL is allowed to be crawled now, `False` if crawling the URL is prohibited.
    float: Time in seconds until this function is expected to return `True`.
    '''
    return Host(get_host(url), con, hosts_db_path).try_take_token(url)
