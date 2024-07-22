import multiprocessing as mp
from multiprocessing.connection import Connection, wait
import time

import apsw

from crawl import DEFAULT_HOSTS_DB
from crawl.document import Document
from crawl.queue import Queue
from crawl.request import Request, Status
from crawl.robots import can_crawl, Host, get_host

class QueueEmpty(Exception):
    pass




# type of messages sent from crawler to workers
#   - Request & Host for which robots.txt needs to be fetched first
#   - Request that needs to be made
#   - Document that needs to be parsed
#   - Document whose links need to be extracted
#   - amount of seconds to sleep until new work
#type Work = tuple[Request, Host] | Request | Document | float

#type Result = tuple[Request, Host] | Request | Document | list[str] | None

class Crawler:
    def __init__(self, crawl_db: str, hosts_db: str) -> None:
        self.crawl_db = apsw.Connection(crawl_db)
        self.hosts_db = Host.open_db(hosts_db)
        self.queue = Queue(self.crawl_db)


    def start(self, worker_count=8):
        self.workers = []
        self.pipes = []
        for _ in range(worker_count):
            our, their = mp.Pipe()
            w = mp.Process(target=Crawler.worker, args=[their], daemon=True)
            w.start()
            self.workers.append(w)
            # TODO: this breaks if the frontier is shorter than worker_count
            self.give_work(our)
            self.pipes.append(our)


    @staticmethod
    def worker(pipe: Connection):
        try:
            while work := pipe.recv():
                result = Crawler.work(work)
                pipe.send(result)
        except KeyboardInterrupt:
            return

    @staticmethod
    def work(work):
        match work:
            case req, host if type(req) == Request and type(host) == Host:
                # fetch robots.txt
                host.fetch()
                return (req, host)
            case request if type(request) == Request:
                # make the request
                request.make()
                return request
            case document if type(document) == Document:
                if not document.parsed:
                    # parse the document & calculate relevance
                    document.parse()
                    document.simhash()
                    # TODO: could skip calculating relevance if duplicate
                    document.relevance()
                    return document
                else:
                    # extract links
                    return list(document.links())
            case idle_for if type(idle_for) == float:
                print(f"idling for {idle_for}s")
                time.sleep(idle_for)
                return None
            case other:
                raise Exception(f"unexpected work {type(other)}: {other}")


    def give_work(self, pipe: Connection):
        try:
            # TODO: count skips and give up eventually
            skips = 0
            while (work := self.next_url()) == None:
                skips += 1
            if skips > 30:
                print(f"Skipped {skips} URLs to get work")
            #print(f"got work after {skips} skips")
            pipe.send(work)
        except QueueEmpty:
            now_ish = time.time()
            rows = self.crawl_db.execute(
                "SELECT url_id, MAX(time) \
                FROM request \
                GROUP BY url_id \
                HAVING TYPEOF(status) == 'real' \
                AND status < ?1",
                [now_ish]
            ).fetchall()
            queued = 0
            for url_id, _ in rows:
                assert type(url_id) == int
                self.queue.push_id(url_id)
                queued += 1
            if queued == 0:
                # stalled, wait for next token to become available
                res = self.crawl_db.execute(
                    "SELECT status, MAX(time) \
                    FROM request \
                    GROUP BY url_id \
                    HAVING TYPEOF(status) = 'real' \
                    ORDER BY status ASC \
                    LIMIT 1"
                ).fetchone()
                if not res:
                    #print("completely done!")
                    #raise QueueEmpty
                    pipe.send(3.0)
                else:
                    (soonest, ) = res
                    assert type(soonest) == float
                    pipe.send(soonest - now_ish)
            else:
                self.give_work(pipe)



    def next_url(self) -> tuple[Request, Host] | Request | None:
        url = self.queue.pop()
        if not url:
            raise QueueEmpty
        req = Request(url)
        match req.check_status(self.crawl_db):
            case Status.PROHIBITED | Status.TIMEOUT | Status.FAILED as s:
                #print(f"{url} previously not fetched ({s.name})")
                return None
            case status if type(status) == Status:
                #print(f"{url} already fetched with status {status}")
                return None
            case limited if type(limited) == float and limited > time.time():
                #print(f"{url} throttled for another {limited - time.time()}s")
                #TOOD remove this now==
                self.queue.push(url)
                return None
        host = Host(get_host(url))
        if host.try_load(self.hosts_db):
            return self.try_request(req, host)
        else:
            return (req, host)


    def try_request(self, req: Request, host: Host) -> Request | None:
        res = host.try_take_token(self.hosts_db, req.url)
        if type(res) == float:
            #print(f"host rate-limited for {res}s")
            Request.rate_limited(req.url, res).save(self.crawl_db)
            self.queue.push(req.url)
            #return False
        elif res != True:
            Request.prohibited(req.url).save(self.crawl_db)
            #print(f"crawling prohibited for {url}")
            #return True
        else:
            return req


    def handle_result(self, pipe: Connection, result):
        match result:
            case req, host if type(req) == Request and type(host) == Host:
                # store robots.txt & get token
                host.store(self.hosts_db)
                if req := self.try_request(req, host):
                    pipe.send(req)
                    return
            case request if type(request) == Request:
                # save the request
                request.save(self.crawl_db)
                if doc := request.document():
                    pipe.send(doc)
                    return
            case document if type(document) == Document:
                if document.is_relevant():
                    # store the document & check for duplicates
                    # TODO: save dupes also? as reference to the original?
                    if not document.check_for_duplicates(self.crawl_db):
                        document.save(self.crawl_db)
                        pipe.send(document)
                        return
            case links if type(links) == list:
                # TODO: implement batched queuing
                for link in links:
                    self.queue.push_if_new(link)
            case None:
                # worker finished idling, try to give new work
                pass
            case other:
                raise Exception(f"unexpected result {type(other)}: {other}")

        self.give_work(pipe)


    def run(self):
        while self.pipes:
            for pipe in wait(self.pipes):
                q_size = len(Queue(self.crawl_db))
                avg, rate, ok, failed, timed_out, prohibited = Request.stats(
                    self.crawl_db
                )
                print(
                    f"\r{rate:.4f} req/s, {avg:.4f} s/req, {q_size} queued,",
                    f"{failed: 3} / {timed_out: 3} / {prohibited: 3} (f/t/p),",
                    f"{ok: 4} ok ",
                    flush=True,
                    end=""
                )
                assert type(pipe) == Connection
                assert pipe.poll()
                try:
                    result = pipe.recv()
                except EOFError:
                    self.pipes.remove(pipe)
                else:
                    self.handle_result(pipe, result)


