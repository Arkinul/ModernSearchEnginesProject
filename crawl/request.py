from enum import IntEnum
import sqlite3
import time
import requests

from crawl import DEFAULT_CRAWLER_DB
from crawl.robots import USER_AGENT


REQUEST_TIMEOUT = 3.0
HEADERS = {
    "Accept-Language": "en-US,en,en-GB",
    "Accept": "text/html,application/xhtml+xml,application/xml,text/*",
    "User-Agent": USER_AGENT,
    # TODO: check if requests takes care of this
    #"Accept-Encoding": "gzip, deflate, br"
}


class Status(IntEnum):
    # Custom non-HTTP statuses
    FAILED = 10
    TIMEOUT = 11
    PROHIBITED = 20

    # 1xx Informational
    CONTINUE = 100
    SWITCHING_PROTOCOLS = 101
    PROCESSING = 102
    EARLY_HINTS = 103

    # 2xx Success
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NON_AUTHORITATIVE_INFORMATION = 203
    NO_CONTENT = 204
    RESET_CONTENT = 205
    PARTIAL_CONTENT = 206
    MULTI_STATUS = 207
    ALREADY_REPORTED = 208
    IM_USED = 226

    # 3xx Redirection
    MULTIPLE_CHOICES = 300
    MOVED_PERMANENTLY = 301
    FOUND = 302
    SEE_OTHER = 303
    NOT_MODIFIED = 304
    USE_PROXY = 305
    TEMPORARY_REDIRECT = 307
    PERMANENT_REDIRECT = 308

    # 4xx Client Error
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    PAYMENT_REQUIRED = 402
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    NOT_ACCEPTABLE = 406
    PROXY_AUTHENTICATION_REQUIRED = 407
    REQUEST_TIMEOUT = 408
    CONFLICT = 409
    GONE = 410
    LENGTH_REQUIRED = 411
    PRECONDITION_FAILED = 412
    PAYLOAD_TOO_LARGE = 413
    URI_TOO_LONG = 414
    UNSUPPORTED_MEDIA_TYPE = 415
    RANGE_NOT_SATISFIABLE = 416
    EXPECTATION_FAILED = 417
    IM_A_TEAPOT = 418
    MISDIRECTED_REQUEST = 421
    UNPROCESSABLE_ENTITY = 422
    LOCKED = 423
    FAILED_DEPENDENCY = 424
    TOO_EARLY = 425
    UPGRADE_REQUIRED = 426
    PRECONDITION_REQUIRED = 428
    TOO_MANY_REQUESTS = 429
    REQUEST_HEADER_FIELDS_TOO_LARGE = 431
    UNAVAILABLE_FOR_LEGAL_REASONS = 451

    # 5xx Server Error
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    HTTP_VERSION_NOT_SUPPORTED = 505
    VARIANT_ALSO_NEGOTIATES = 506
    INSUFFICIENT_STORAGE = 507
    LOOP_DETECTED = 508
    NOT_EXTENDED = 510
    NETWORK_AUTHENTICATION_REQUIRED = 511


# Separate dictionary for status code descriptions
STATUS_DESCRIPTIONS = {
    Status.FAILED: "Request failed",
    Status.TIMEOUT: "Timed out",
    Status.PROHIBITED: "Crawling prohibited",

    Status.CONTINUE: "Continue",
    Status.SWITCHING_PROTOCOLS: "Switching Protocols",
    Status.PROCESSING: "Processing",
    Status.EARLY_HINTS: "Early Hints",
    Status.OK: "OK",
    Status.CREATED: "Created",
    Status.ACCEPTED: "Accepted",
    Status.NON_AUTHORITATIVE_INFORMATION: "Non-Authoritative Information",
    Status.NO_CONTENT: "No Content",
    Status.RESET_CONTENT: "Reset Content",
    Status.PARTIAL_CONTENT: "Partial Content",
    Status.MULTI_STATUS: "Multi-Status",
    Status.ALREADY_REPORTED: "Already Reported",
    Status.IM_USED: "IM Used",
    Status.MULTIPLE_CHOICES: "Multiple Choices",
    Status.MOVED_PERMANENTLY: "Moved Permanently",
    Status.FOUND: "Found",
    Status.SEE_OTHER: "See Other",
    Status.NOT_MODIFIED: "Not Modified",
    Status.USE_PROXY: "Use Proxy",
    Status.TEMPORARY_REDIRECT: "Temporary Redirect",
    Status.PERMANENT_REDIRECT: "Permanent Redirect",
    Status.BAD_REQUEST: "Bad Request",
    Status.UNAUTHORIZED: "Unauthorized",
    Status.PAYMENT_REQUIRED: "Payment Required",
    Status.FORBIDDEN: "Forbidden",
    Status.NOT_FOUND: "Not Found",
    Status.METHOD_NOT_ALLOWED: "Method Not Allowed",
    Status.NOT_ACCEPTABLE: "Not Acceptable",
    Status.PROXY_AUTHENTICATION_REQUIRED: "Proxy Authentication Required",
    Status.REQUEST_TIMEOUT: "Request Timeout",
    Status.CONFLICT: "Conflict",
    Status.GONE: "Gone",
    Status.LENGTH_REQUIRED: "Length Required",
    Status.PRECONDITION_FAILED: "Precondition Failed",
    Status.PAYLOAD_TOO_LARGE: "Payload Too Large",
    Status.URI_TOO_LONG: "URI Too Long",
    Status.UNSUPPORTED_MEDIA_TYPE: "Unsupported Media Type",
    Status.RANGE_NOT_SATISFIABLE: "Range Not Satisfiable",
    Status.EXPECTATION_FAILED: "Expectation Failed",
    Status.IM_A_TEAPOT: "I'm a teapot",
    Status.MISDIRECTED_REQUEST: "Misdirected Request",
    Status.UNPROCESSABLE_ENTITY: "Unprocessable Entity",
    Status.LOCKED: "Locked",
    Status.FAILED_DEPENDENCY: "Failed Dependency",
    Status.TOO_EARLY: "Too Early",
    Status.UPGRADE_REQUIRED: "Upgrade Required",
    Status.PRECONDITION_REQUIRED: "Precondition Required",
    Status.TOO_MANY_REQUESTS: "Too Many Requests",
    Status.REQUEST_HEADER_FIELDS_TOO_LARGE: "Request Header Fields Too Large",
    Status.UNAVAILABLE_FOR_LEGAL_REASONS: "Unavailable For Legal Reasons",
    Status.INTERNAL_SERVER_ERROR: "Internal Server Error",
    Status.NOT_IMPLEMENTED: "Not Implemented",
    Status.BAD_GATEWAY: "Bad Gateway",
    Status.SERVICE_UNAVAILABLE: "Service Unavailable",
    Status.GATEWAY_TIMEOUT: "Gateway Timeout",
    Status.HTTP_VERSION_NOT_SUPPORTED: "HTTP Version Not Supported",
    Status.VARIANT_ALSO_NEGOTIATES: "Variant Also Negotiates",
    Status.INSUFFICIENT_STORAGE: "Insufficient Storage",
    Status.LOOP_DETECTED: "Loop Detected",
    Status.NOT_EXTENDED: "Not Extended",
    Status.NETWORK_AUTHENTICATION_REQUIRED: "Network Authentication Required",
}


class Request:
    def __init__(self, url: str) -> None:
        self.time = time.time()
        self.elapsed = None
        self.headers = None
        self.data = None
        self.url = url


    @staticmethod
    def prohibited(url: str):
        req = Request(url)
        req.status = Status.PROHIBITED
        return req


    @staticmethod
    def rate_limited(url: str, seconds: float):
        req = Request(url)
        req.status = time.time() + seconds
        return req


    def check_status(self, db=DEFAULT_CRAWLER_DB) -> Status | float | None:
        con = sqlite3.connect(db)
        res = con.execute(
            "SELECT status FROM request \
            JOIN url on url_id = url.id \
            WHERE url = ?1 \
            ORDER BY request.time DESC \
            LIMIT 1",
            (self.url, )
        ).fetchone()
        match res:
            case None:
                return None
            case (status, ) if type(status) == int:
                return Status(status)
            case (limited_until, ) if type(limited_until) == float:
                return limited_until
            case other:
                raise Exception(f"{self.url}: invalid status {other}")


    def make(self) -> bool:
        try:
            response = requests.get(
                self.url,
                timeout=REQUEST_TIMEOUT,
                headers=HEADERS
            )
            self.elapsed = response.elapsed
            response.raise_for_status()
        except requests.Timeout:
            self.status = Status.TIMEOUT
        except requests.RequestException as e:
            print(f"request for {self.url} failed: {e}")
            self.status = Status.FAILED
        else:
            self.status = Status(response.status_code)
            self.headers = response.headers
            self.data = response.content
        return self.data is not None


    def save(self, db=DEFAULT_CRAWLER_DB):
        """
        Store the request in the database.
        Assumes the URL already exists in the `url` table
        """
        if self.elapsed:
            elapsed = self.elapsed.total_seconds()
        else:
            elapsed = None
        if self.headers:
            headers = str(self.headers)
        else:
            headers = None
        con = sqlite3.connect(db)
        cur = con.cursor()
        res = cur.execute(
            "INSERT INTO request ( \
                url_id, \
                time, \
                duration, \
                status, \
                headers, \
                data \
            ) \
            VALUES ((SELECT id FROM url WHERE url = ?1), ?2, ?3, ?4, ?5, ?6)",
            (self.url, self.time, elapsed, self.status, headers, self.data)
        )
        con.commit()
