import time
import threading
import heapq
import logging
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from vsm import getcursor, get_recent_submissions_for_all_terms, get_recent_submimssions_for_term
from scrape import scrape_submissions_to_db


"""
gets search terms from vsm module
finds recent submissions for each search term
estimates the rate at which submissions appear (submission frequency) for each term
creates a queue of scrape jobs
scheduled time for jobs is based on the term's submission frequency
on scraping, submission frequency is re-calculated and a new job is scheduled
"""


MULTIPLIER = 2  # scrape rate is multiplied by this value to create a buffer
# in case more posts suddenly appear in the interim between 2 scrapes
MIN_SCRAPES_PER_DAY = 1  # per term
MAX_SCRAPES_PER_DAY = 500  # per term
SECONDS_PER_DAY = 86400


class ScrapeScheduler:
    def __init__(self, max_workers=4):
        self.lock = threading.Lock()
        self.task_heap = []
        self.task_set = set()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.setup()

    def add_task(self, term, scrape_time):
        with self.lock:
            if term not in self.task_set:
                heapq.heappush(self.task_heap, (scrape_time, term))
                self.task_set.add(term)

    def setup(self):
        logging.info("beginning setup for ScrapeScheduler")
        now = time.time()
        with getcursor() as cur:
            terms_and_intervals = get_all_terms_and_intervals(cur)
        logging.info(f"{len(terms_and_intervals)} terms found.")
        for i, (term, interval) in enumerate(terms_and_intervals):
            # Spread out the start times within the range of the interval
            spacing_offset = (interval / len(terms_and_intervals)) * i
            next_scrape_time = now + spacing_offset
            printdate = datetime.utcfromtimestamp(
                next_scrape_time).strftime('%Y-%m-%d %H:%M')
            logging.info(f"scrape time set for {term}: {printdate}")
            self.add_task(term, next_scrape_time)

    def scrape_loop(self):
        """keep checking each task to see if time has been reached
        scrape when time is reached and then re-check db for recent results to calc the next
        scrape time and set it and re-add to tasks"""
        while True:
            with self.lock:
                if self.task_heap:
                    next_time, term = heapq.heappop(self.task_heap)
                    self.task_set.remove(term)
                    # minimum 5s between scraping initializations
                    time.sleep(5)
                else:
                    time.sleep(1)
                    continue

            now = time.time()
            if next_time <= now:
                self.executor.submit(self.scrape_and_reschedule, term)
            else:
                self.add_task(term, next_time)
                sleep_duration = next_time - now
                logging.info(f"not time yet for {
                             term}, sleeping for {sleep_duration}s")
                time.sleep(sleep_duration)

    def scrape_and_reschedule(self, term):
        logging.info(f"[{datetime.utcnow()}] Scraping: {term}")
        with getcursor() as cur:
            try:
                scrape_submissions_to_db(cur, [term])
                interval = get_interval_for_term(cur, term)
            except Exception as e:
                logging.error(f"scraping failed for term {term}: {e}")
                interval = 300
            finally:
                next_scrape = time.time() + interval
                self.add_task(term, next_scrape)


def get_interval_for_term(cur, term):
    submissions = get_recent_submimssions_for_term(cur, term)
    scrapes_per_day = calculate_scrapes_per_day(submissions)
    interval = SECONDS_PER_DAY / scrapes_per_day
    return interval


def get_all_terms_and_intervals(cur):
    terms = get_recent_submissions_for_all_terms(cur)
    search_terms_and_intervals = []
    for term, submissions in terms.items():
        scrapes_per_day = calculate_scrapes_per_day(submissions)
        interval = SECONDS_PER_DAY / scrapes_per_day
        search_terms_and_intervals.append((term, interval))
    search_terms_and_intervals.sort(key=lambda x: x[1])
    return search_terms_and_intervals


def calculate_scrapes_per_day(recent_submissions):
    """Expects list of tuples: [(submission_id, created_utc)]"""
    if len(recent_submissions) < 2:
        return MIN_SCRAPES_PER_DAY
    timestamps = sorted(s[1] for s in recent_submissions)
    time_span = timestamps[-1] - timestamps[0]

    if time_span == 0:
        return MAX_SCRAPES_PER_DAY

    avg_interval = time_span / (len(timestamps) - 1)
    submissions_per_day = SECONDS_PER_DAY / avg_interval
    scrapes_per_day = min(MAX_SCRAPES_PER_DAY, max(
        MIN_SCRAPES_PER_DAY, MULTIPLIER * (submissions_per_day / 250)))
    scrapes_per_day = math.ceil(scrapes_per_day)
    return scrapes_per_day


if __name__ == "__main__":
    scheduler = ScrapeScheduler()
    scheduler.scrape_loop()
