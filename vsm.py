from collections import defaultdict
import atexit
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv
import os

load_dotenv()

AZURE_CREDENTIALS = {
    "host": os.environ["PGHOST"],
    "user": os.environ["PGUSER"],
    "password": os.environ["PGPASSWORD"],
    "port": int(os.environ["PGPORT"]),
    "database": os.environ["PGDATABASE"],
}

pg_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    **AZURE_CREDENTIALS
)


@atexit.register
def close_pg_pool():
    if pg_pool:
        print("Closing PostgreSQL connection pool...")
        pg_pool.closeall()


@contextmanager
def getcursor(commit=True):
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        pg_pool.putconn(conn)


def get_recent_submissions_for_all_terms(cur, limit=50):
    # Step 1: Run the query and collect data
    cur.execute(f"""
        SELECT
            s.name AS search_term_name,
            r.created_utc,
            r.id AS submission_id
        FROM
            search_term s
        LEFT JOIN LATERAL (
            SELECT r.id, r.created_utc
            FROM search_term_match_reddit_submission m
            JOIN reddit_submission r ON m.submission_id = r.id
            WHERE m.search_term_id = s.id
            ORDER BY r.created_utc DESC
            LIMIT {limit}
        ) r ON true
    """)

    raw = cur.fetchall()
    data = defaultdict(list)
    for name, created_utc, sid in raw:
        if sid is not None:
            data[name.lower()].append((sid, created_utc))
        else:
            data.setdefault(name.lower(), [])

    # Step 2: Remove super-terms
    terms = sorted(data.keys())

    def is_super_term(a, b):
        """Returns True if `b` is a super-term of `a`."""
        a_words = a.split()
        b_words = b.split()
        if len(b_words) <= len(a_words):
            return False
        return all(word in b_words for word in a_words)
    good_terms = set()
    for term in terms:
        if not any(is_super_term(comparison, term) for comparison in terms if comparison != term):
            good_terms.add(term)

    # Step 3: Filter data to keep only good (non-super) terms
    filtered_data = {term: data[term] for term in good_terms}
    return filtered_data


def get_recent_submimssions_for_term(cur, search_term_name, limit=50):
    cur.execute("""
        SELECT r.id, r.created_utc
        FROM search_term s
        JOIN search_term_match_reddit_submission m ON s.id = m.search_term_id
        JOIN reddit_submission r ON m.submission_id = r.id
        WHERE s.name = %s
        ORDER BY r.created_utc DESC
        LIMIT %s
    """, (search_term_name, limit))
    return cur.fetchall()


def get_search_term_list_without_superterms(conn):
    """
    "pneu-c-13" is NOT a super-term of "pneu-c". these will return different, non-overlapping search results.
    """
    search_terms = get_full_search_term_list(conn)
    search_terms = [s.lower() for s in search_terms]
    search_terms = sorted(list(set(search_terms)))
    super_terms = []
    good_search_terms = []

    def is_super_term(a, b):
        """checks if b is a super-term of a"""
        a = a.split(" ")
        b = b.split(" ")
        if len(b) <= len(a):
            return False
        for word in a:
            if word not in b:
                return False
        return True

    for a in search_terms:
        a_is_super_term = False
        for b in search_terms:
            if is_super_term(b, a):
                a_is_super_term = True
                super_terms.append([a, b])
                # keep going over loop to find all super-terms
        if not a_is_super_term:
            good_search_terms.append(a)
    return good_search_terms


def get_full_search_term_list(cur):
    cur.execute("""SELECT name FROM search_term""")
    r = cur.fetchall()
    search_term_list = [r[0] for r in r]
    # TODO: Should i just remove apostrophes from the data in the first place?
    search_term_list = [i.replace("'", "") for i in search_term_list]
    return search_term_list
