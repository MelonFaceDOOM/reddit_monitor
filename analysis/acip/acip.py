import csv
import os
import traceback
import datetime
import pandas as pd
from collections import defaultdict
from cgpt import single_prompt_response
from vsm import getcursor, init_connection
from update_submissions import update_selected_submission_stats
from utils import dump_submissions, load_submissions
from analysis.analyse import save_submissions_per_day, save_num_comments_per_day, save_score_per_day, save_top_submissions, get_top_subreddits_by_total_comments, get_top_subreddits_by_submission_count


COLLECTION_START_DATE = "2025-06-23"
SUBMISSIONS_FILE = "analysis/acip/submissions.json"  # data pulled from db
# copy of data with cgpt_response column
CGPT_RESPONSE_FILE = "analysis/acip/submissions_with_responses.csv"
PROMPT_TEMPLATE = open("analysis/acip/prompt.txt",
                       "r", encoding="utf-8").read()
ACIP_TERMS = ['rfk', 'acip', 'cdc', 'hhs', 'advisory committee for immunization practices', 'vaccine panel',
              'vicky pebsworth', 'national vaccine information center', 'martin kulldorff', 'retsef levi', 'cody meissner']


def acip_analysis(df):
    save_submissions_per_day(df, xlabel="Date", ylabel="Number of Submissions",
                             title="Total Submissions Per Day", output_path="analysis/acip/results/num_submissions_per_day.png")
    save_num_comments_per_day(df, xlabel="Date", ylabel="Number of Comments",
                              title="Total Comments Per Day", output_path="analysis/acip/results/num_comments_per_day.png")
    save_score_per_day(df, xlabel="Date", ylabel="Total Score",
                       title="Total Upvotes (Score) Per Day", output_path="analysis/acip/results/num_upvotes_per_day.png")
    save_top_submissions(df, score_csv="analysis/acip/results/submissions_by_score.csv",
                         comments_csv="analysis/acip/results/submissions_by_comments.csv")

    top_comments = get_top_subreddits_by_total_comments(df, 10)
    top_comments.to_csv(
        "analysis/acip/results/top_subreddits_by_comments.csv", header=["total_comments"])

    top_submissions = get_top_subreddits_by_submission_count(df, 10)
    top_submissions.to_csv(
        "analysis/acip/results/top_subreddits_by_submissions.csv", header=["submission_count"])


def refresh_acip_analysis():
    if os.path.isfile(CGPT_RESPONSE_FILE):
        # Rescrape votes/comment counts for relevant threads
        update_votes_and_comments()
        update_submissions_in_file()
    dump_submissions_from_db()  # dumps to SUBMISSIONS_FILE setup_response_csv
    setup_response_csv()  # adds new submissions from SUBMISSIONS_FILE to CGPT_RESPONSE_FILE
    label_data()  # calls cgpt query for each new submission
    df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    df = filter_df_for_analysis(df)
    acip_analysis(df)  # saves png charts


def dump_submissions_from_db():
    try:
        with getcursor() as cur:
            submissions = get_submissions_for_other_vaccine_concepts(cur)
        dump_submissions(submissions, SUBMISSIONS_FILE)
    except Exception as e:
        print("ruh roh,", e)
        print(traceback.format_exc())


def setup_response_csv():
    """if response csv doesn't exist, create it
    if response csv exists, find new unique submissions and append them
    to existing response csv"""
    # create new_df from unique list of submissions
    submissions = load_submissions(SUBMISSIONS_FILE)
    all_submissions = []
    for sublist in submissions.values():
        all_submissions.extend(sublist)
    print(f"Total submissions from file: {len(all_submissions)}")
    new_df = pd.DataFrame(all_submissions)
    new_df.drop_duplicates(subset="id", inplace=True)
    print(f"After deduplication: {len(new_df)}")

    # if response csv already exists, subtract its ids from new_df
    if os.path.exists(CGPT_RESPONSE_FILE):
        existing_df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
        existing_ids = set(existing_df["id"])
        new_df = new_df[~new_df["id"].isin(existing_ids)]
        print(f"New unique submissions: {len(new_df)}")
    else:
        existing_df = pd.DataFrame()
        print("No existing response CSV found.")

    # add new_df (with response = NA) to existing response csv
    if not new_df.empty:
        new_df["cgpt_response"] = pd.NA
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = existing_df

    # Save response csv
    combined_df.to_csv(CGPT_RESPONSE_FILE, index=False)
    print(f"Updated CSV saved with {len(combined_df)} total rows.")


def save_results_to_file(file_path, row_dict):
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='a' if file_exists else 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row_dict.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)


def update_votes_and_comments():
    """ update db vote/comment values for submissions where cgpt_response == 1 or 2"""
    df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    df = df[df['cgpt_response'].isin([1, 2])]
    ids_to_update = df['id'].tolist()
    update_selected_submission_stats(ids_to_update)


def update_submissions_in_file():
    """updates cgpt response file with new vote/comment values (pulled from db)"""
    df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    ids = tuple(df["id"].dropna().unique())  # Ensure deduped and not null

    if not ids:
        print("No IDs found in the file.")
    else:
        with getcursor() as cur:
            cur.execute("""
                SELECT id, score, num_comments FROM reddit_submission
                WHERE id = ANY(%s)
            """, (list(ids),))
            rows = cur.fetchall()

        # Load into dicts for mapping
        score_map = {r[0]: r[1] for r in rows}
        comments_map = {r[0]: r[2] for r in rows}

        # Update DataFrame
        df["score"] = df["id"].map(score_map)
        df["num_comments"] = df["id"].map(comments_map)

        print(f"Updated scores and comment counts for {
              len(rows)} submissions.")

        # Save updated file
        df.to_csv(CGPT_RESPONSE_FILE, index=False)


def label_data():
    df = pd.read_csv(CGPT_RESPONSE_FILE)
    # filter out old submissions to save some tokens
    created_dates = pd.to_datetime(
        df["created_utc"], unit="s", utc=True).dt.date
    cutoff = datetime.datetime.strptime("2025-06-23", "%Y-%m-%d").date()

    # Identify rows to process: new + cgpt_response is NA
    mask = (created_dates >= cutoff) & (df["cgpt_response"].isna())
    rows_to_process = df[mask]

    print(f"{len(rows_to_process)} submissions to process...")

    for idx in rows_to_process.index:
        row = df.loc[idx]
        prompt = PROMPT_TEMPLATE.format(submission_title=row["title"])
        try:
            response = single_prompt_response(prompt.strip())
        except Exception as e:
            print(f"Error for ID {row['id']}: {e}")
            continue

        # Update in the full DataFrame
        df.at[idx, "cgpt_response"] = response

        # Save updated full CSV
        df.to_csv(CGPT_RESPONSE_FILE, index=False)
        print(f"{response}: {row['title']}")


def filter_df_for_analysis(df):
    """ converts date from utc to datetime
    removes data before june 23,
    keeps only rows where cgpt response is 1"""

    df["created_date"] = pd.to_datetime(
        df["created_utc"], unit="s", utc=True).dt.date
    # first full day of data collection
    df = filter_df_by_utc_date(df, COLLECTION_START_DATE)
    df = df[df["cgpt_response"].notna()]
    df = df[df['cgpt_response'].isin([1, 2])]
    return df


def filter_df_by_utc_date(df, cutoff_date_str):
    """
    Filters the DataFrame to include only rows where created_utc (in Unix timestamp)
    is on or after the given UTC date (yyyy-mm-dd).
    """
    # Parse the cutoff string into a timezone-aware datetime
    cutoff_dt = datetime.datetime.strptime(
        cutoff_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)

    return df[df["created_date"] >= cutoff_dt.date()].copy()


def get_submissions_for_other_vaccine_concepts(cur):
    # Step 1: Run the query and collect data

    cur.execute("""
        SELECT
            s.name AS search_term_name,
            r.*
        FROM
            search_term s
        LEFT JOIN LATERAL (
            SELECT r.*
            FROM search_term_match_reddit_submission m
            JOIN reddit_submission r ON m.submission_id = r.id
            WHERE m.search_term_id = s.id
            ORDER BY r.created_utc DESC
        ) r ON true
        WHERE s.name = ANY(%s)
    """, (ACIP_TERMS,))

    columns = [desc[0] for desc in cur.description]
    raw = cur.fetchall()

    data = defaultdict(list)
    for row in raw:
        row_dict = dict(zip(columns, row))
        name = row_dict["search_term_name"].lower()

        if row_dict["id"] is not None:
            submission = {k: v for k, v in row_dict.items() if k !=
                          "search_term_name"}
            data[name].append(submission)
        else:
            data.setdefault(name, [])

    return dict(data)

if __name__ == "__main__":
    init_connection()
    # do whatever