import matplotlib.pyplot as plt
import os
import logging
import pandas as pd
from datetime import datetime, timedelta


def examine_results(results_file):
    logging.info(f"Loading and plotting data from: {results_file}")
    df = pd.read_json(results_file, lines=True)
    print(f"{len(df)} results for {results_file}")
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df.set_index('created_utc', inplace=True)
    daily_counts = df.resample('1D').size()

    plt.figure(figsize=(12, 6))
    daily_counts.plot(kind='bar')
    plt.title("Reddit Posts Per Day")
    plt.xlabel("Date")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    filename = "plot_" + os.path.basename(results_file).split(".")[0] + ".png"
    outfile = os.path.join("results", filename)
    plt.savefig(outfile)
    logging.info("Saved plot to measles_results.png")


def save_daily_bart_chart(df, xlabel, ylabel, title, output_path):
    plt.figure(figsize=(12, 6))
    df.plot(kind="bar")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path)


def save_submissions_per_day(df, xlabel, ylabel, title, output_path="submissions_per_day.png"):
    daily_counts = df.groupby("created_date").size()
    save_daily_bart_chart(daily_counts, xlabel, ylabel, title, output_path)


def save_num_comments_per_day(df, xlabel, ylabel, title, output_path="num_comments_per_day.png"):
    daily_comments = df.groupby("created_date")["num_comments"].sum()
    save_daily_bart_chart(daily_comments, xlabel, ylabel, title, output_path)


def save_score_per_day(df, xlabel, ylabel, title, output_path="upvotes_per_day.png"):
    daily_score = df.groupby("created_date")["score"].sum()
    save_daily_bart_chart(daily_score, xlabel, ylabel, title, output_path)


def save_top_submissions(df, score_csv="submissions_by_score.csv", comments_csv="submissions_by_comments.csv"):
    top_score = df.sort_values("score", ascending=False).head(25)
    top_score_subset = top_score[["title", "created_date", "score"]]
    top_score_subset.to_csv(score_csv, index=False)

    # Sort by num_comments and get top 50
    top_comments = df.sort_values("num_comments", ascending=False).head(25)
    # Filter out any already in top_score by ID
    score_ids = set(top_score["id"])
    top_comments_unique = top_comments[~top_comments["id"].isin(score_ids)]
    top_comments_subset = top_comments_unique[[
        "title", "created_date", "num_comments"]]
    top_comments_subset.to_csv(comments_csv, index=False)


def print_top_submission_info(df):
    top_score = df.sort_values("score", ascending=False).head(10)
    for _, row in top_score.iterrows():
        print(f"Score: {row['score']}, URL: {row['permalink']}")


def top_submissions_for_day(df, day, limit=5):
    """
    Get top N submissions by score for a specific UTC date (e.g., '2025-07-22').
    """
    try:
        day_start = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        return []

    day_end = day_start + timedelta(days=1)
    timestamp_start = int(day_start.timestamp())
    timestamp_end = int(day_end.timestamp())
    filtered = df[
        (df["created_utc"] >= timestamp_start) &
        (df["created_utc"] < timestamp_end)
    ]

    top = filtered.sort_values("score", ascending=False).head(limit)
    return top


def get_top_subreddits_by_submission_count(df, limit=10):
    """
    Returns top subreddits by number of submissions (row count).
    """
    top_subs = df["subreddit"].value_counts().head(limit)
    return top_subs


def get_top_subreddits_by_total_comments(df, limit=10):
    """
    Returns top subreddits by cumulative number of comments across submissions.
    Assumes 'subreddit' and 'num_comments' columns exist.
    """
    agg = df.groupby("subreddit")["num_comments"].sum()
    top_subs = agg.sort_values(ascending=False).head(limit)
    return top_subs
