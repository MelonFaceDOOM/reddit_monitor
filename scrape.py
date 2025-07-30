import os
import time
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import praw
import prawcore
from praw.exceptions import RedditAPIException
from psycopg2.extras import execute_values


load_dotenv()

# ───────────  Logging setup ───────────
os.makedirs("logs", exist_ok=True)

log_format = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)

# File handler
log_path = f"logs/scrape_{datetime.now():%Y%m%d_%H%M%S}.log"
file_handler = logging.FileHandler(log_path, encoding="utf-8")
file_handler.setFormatter(log_format)

# Root logger setup
logging.basicConfig(level=logging.INFO, handlers=[
                    console_handler, file_handler])


COMMENT_FIELDS = [
    "id", "parent_id", "link_id", "body", "permalink", "created_utc", "subreddit_id",
    "subreddit_type", "total_awards_received", "subreddit", "score", "gilded", "stickied",
    "is_submitter", "gildings", "all_awardings", "is_en"
]

SUBMISSION_FIELDS = [
    "id", "url", "domain", "title", "permalink", "created_utc", "url_overridden_by_dest",
    "subreddit_id", "subreddit", "upvote_ratio", "score", "gilded", "num_comments",
    "num_crossposts", "pinned", "stickied", "over_18", "is_created_from_ads_ui", "is_self",
    "is_video", "media", "gildings", "all_awardings", "is_en"
]


def scrape_submissions_to_db(cur, queries):
    reddit = make_reddit_api_interface()
    for query in queries:
        # Ensure the query exists as a valid search term
        cur.execute(
            "SELECT id FROM search_term WHERE name = %s", (query,)
        )
        result = cur.fetchone()
        if not result:
            raise ValueError(f"The query '{
                             query}' does not exist in the DB as a search term and cannot be scraped.")
        search_term_id = result[0]

        # Find existing submissions for that search term
        cur.execute("""
            SELECT r.id
            FROM reddit_submission r
            JOIN search_term_match_reddit_submission m ON r.id = m.submission_id
            WHERE m.search_term_id = %s
        """, (search_term_id,))
        existing_submission_ids = [row[0] for row in cur.fetchall()]

        if not existing_submission_ids:
            logging.info(
                f"No existing submissions found for '{query}'")

        # Scrape new submissions
        submissions_to_insert = list(get_submissions_until_duplicate(
            reddit, query, existing_submission_ids))
        logging.info(f"{len(submissions_to_insert)
                        } submissions found, inserting into db...")
        insert_submissions(cur, query, submissions_to_insert)

        logging.info(f"Scraping for query '{query}' complete.")


def scrape_comments_to_db(cur, submission_id):
    comments = scrape_comments(cur, submission_id)
    insert_comments(cur, comments)


def scrape_comments(cur, submission_id):
    reddit = make_reddit_api_interface()
    submission = reddit.submission(id=submission_id)
    submission.comments.replace_more(limit=None)
    comments = submission.comments.list()
    if not comments:
        logging.info(f"No comments found for submission_id {submission_id}")
        return
    logging.info(f"Found {len(comments)} comments for submission {
                 submission_id}")
    return comments


def insert_comments(cur, comments):
    if not comments:
        return

    # Prepare comment data for insertion
    comment_rows = [clean_comment_for_insert(
        comment) for comment in comments]
    insert_query = f"""
        INSERT INTO reddit_comment ({','.join(COMMENT_FIELDS)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, insert_query, comment_rows)

    logging.info(f"Inserted {len(comment_rows)} comments")


def insert_submissions(cur, query, submissions):
    if not submissions:
        return

    # Ensure the search term exists and fetch its ID
    cur.execute("SELECT id FROM search_term WHERE name = %s", (query,))
    search_term_row = cur.fetchone()
    if search_term_row is None:
        raise ValueError(
            f"The query '{query}' does not exist in the DB as a search term.")

    submission_rows = [clean_submission_for_insert(s) for s in submissions]
    insert_query = f"""
        INSERT INTO reddit_submission ({','.join(SUBMISSION_FIELDS)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, insert_query, submission_rows)
    logging.info(f"Inserted {len(submission_rows)
                             } submissions and match rows for query: '{query}'")

    # Insert into match table
    search_term_id = search_term_row[0]
    match_rows = [(s.id, search_term_id) for s in submissions]
    match_query = """
        INSERT INTO search_term_match_reddit_submission (submission_id, search_term_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, match_query, match_rows)
    logging.info(f"Inserted {len(match_rows)} match rows for query: '{query}'")


def clean_submission_for_insert(submission):
    return clean_reddit_obj_for_insert(submission, SUBMISSION_FIELDS)


def clean_comment_for_insert(comment):
    return clean_reddit_obj_for_insert(comment, COMMENT_FIELDS)


def clean_reddit_obj_for_insert(reddit_obj, fields):
    cleaned = []
    for field in fields:
        val = getattr(reddit_obj, field, None)

        # Fix: convert subreddit object to string
        if field == "subreddit" and hasattr(val, "display_name"):
            val = val.display_name

        elif isinstance(val, (dict, list)):
            val = json.dumps(val)

        cleaned.append(val)
    return tuple(cleaned)


def scrape_to_file(queries):
    reddit = make_reddit_api_interface()
    os.makedirs("results", exist_ok=True)
    for query in queries:
        out_file = f"results/submission_{query}.jsonl"
        scrape_and_save_submissions_to_file(reddit, query, out_file)


def scrape_and_save_submissions_to_file(reddit, query, out_file):
    logging.info(f"Preparing to scrape query: '{query}'")

    existing_submission_ids = []
    if os.path.isfile(out_file):
        existing_submissions = read_submissions_from_file(out_file)
        existing_submission_ids = [s["id"] for s in existing_submissions]
        logging.info(f"Found existing file with {
                     len(existing_submission_ids)} submissions.")

    with open(out_file, "a+", encoding="utf-8") as f:
        for submission in get_submissions_until_duplicate(
                reddit, query, existing_submission_ids):
            json.dump(vars(submission), f, default=str)
            f.write("\n")
    logging.info(f"Scraping for query {query} complete.")


def read_submissions_from_file(json_file):
    submissions = []
    with open(json_file, encoding="utf-8") as f:
        for line in f:
            try:
                submission = json.loads(line)
                submissions.append(submission)
            except json.JSONDecodeError:
                logging.warning("Skipping malformed JSON line")
    return submissions


def make_reddit_api_interface():
    try:
        logging.info("Initializing Reddit API interface")
        return praw.Reddit(
            client_id=os.environ["REDDIT_ID"],
            client_secret=os.environ["REDDIT_SECRET"],
            user_agent=os.getenv("REDDIT_UA", "debug-scraper/0.1"),
            ratelimit_seconds=60,
        )
    except KeyError as k:
        raise SystemExit(f"Missing env var: {k}. Check your .env file.")


def backoff_api_call(api_call_func, *args, max_sleep=300, **kwargs):
    """Retry praw api call with exponential back-off on transient errors."""
    delay = 2
    while True:
        try:
            return api_call_func(*args, **kwargs)
        except StopIteration:
            raise
        except RedditAPIException as e:
            for item in e.items:
                if item.error_type == "RATELIMIT":
                    logging.warning(f"Rate limit hit: {item.message}")
                    wait_minutes = 1
                    if "minute" in item.message:
                        import re
                        match = re.search(r"(\d+)\s+minute", item.message)
                        if match:
                            wait_minutes = int(match.group(1))
                    wait_seconds = wait_minutes * 60
                    logging.info(
                        f"Waiting {wait_seconds} seconds before retrying...")
                    time.sleep(wait_seconds)
                    continue
            raise
        except prawcore.exceptions.RequestException as e:
            logging.warning(f"Request exception: {e}. Retrying...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Unexpected exception: {e}")
            raise
        time.sleep(delay)
        delay = min(delay * 2, max_sleep)


def get_submissions_until_duplicate(
    reddit,
    query_str,
    existing_submission_ids=None
):
    """
    Stops when a previously seen submission ID is encountered.
    Each new submission is written to file immediately.
    """
    logging.info(f"Starting submission scrape for query: '{query_str}'")

    if existing_submission_ids is None:
        existing_submission_ids = []
    gen = reddit.subreddit("all").search(query_str, sort="new", limit=None)
    for submission in gen:
        # wait is lambda: submission even right? i mean it's been working?
        submission = backoff_api_call(lambda: submission)
        if submission.id in existing_submission_ids:
            logging.info(f"Stopping: submission ID {
                         submission.id} already exists.")
            break
        else:
            yield submission
            logging.debug(f"yielded submission ID: {submission.id}")
