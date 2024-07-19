import time

from crawl.queue import Queue
from crawl.request import Request, Status
from crawl.robots import can_crawl

class QueueEmpty(Exception):
    pass


def crawl_next(db) -> bool:
    """
    Take an URL of the queue and crawl it.
    Returns whether progress was made, raising `QueueEmpty` if appropriate.
    """
    queue = Queue(db)
    url = queue.pop()
    if not url:
        raise QueueEmpty

    # should have been before queuing
    if not url.startswith("http"):
        return True

    req = Request(url)
    match req.check_status(db):
        case Status.PROHIBITED | Status.TIMEOUT | Status.FAILED as s:
            #print(f"{url} previously not fetched ({s.name})")
            return True
        case status if type(status) == Status:
            #print(f"{url} already fetched with status {status}")
            return True
        case limited if type(limited) == float and limited > time.time():
            #print(f"{url} throttled for another {limited - time.time()}s")
            queue.push(url)
            return False

    res = can_crawl(url)
    if type(res) == float:
        #print(f"host rate-limited for {res}s")
        Request.rate_limited(url, res).save(db)
        queue.push(url)
        return False
    elif res != True:
        Request.prohibited(url).save(db)
        #print(f"crawling prohibited for {url}")
        return True
    #print(f"fetching {url}")
    succeeded = req.make()
    #TODO: where to check for content-type?
    req.save(db)
    if not succeeded:
        return True
    if doc := req.document():
        if not doc.parse():
            return True
        #print(f"parsed document, relevance score is {doc.relevance()}")
        if doc.check_for_duplicates():
            # TODO: save these also? as reference to the duplicate?
            return True
        doc.save(db)
        if doc.is_relevant():
            #links = doc.links()
            #print(f"extracted {len(links)} links")
            # TODO: implemented batched queuing
            for link in doc.links():
                queue.push_if_new(link)
        else:
            #print("document is irrelevant, ignoring links")
            pass
    return True



def crawler_loop(db):
    try:
        queue_size = len(Queue(db))
        progress_stalled_counter = 0
        while progress_stalled_counter < queue_size:
            avg, rate, ok, failed, timed_out, prohibited = Request.stats(db)
            print(
                f"\r{rate:.4f} req/s, {avg:.4f} s/req, {queue_size} queued,",
                f"{failed: 3} / {timed_out: 3} / {prohibited: 3} (f/t/p),",
                f"{ok: 4} ok",
                flush=True,
                end=""
            )
            if crawl_next(db):
                progress_stalled_counter = 0
                queue_size = len(Queue(db))
            else:
                progress_stalled_counter += 1
        print("unable to make progress")
    except QueueEmpty:
        print("ran out of links to crawl!")



