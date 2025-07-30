import logging
import prawcore  # for handling not-found/deleted errors
from scrape import make_reddit_api_interface
from vsm import getcursor  # now using db.py


def test():
    id = '1ll86em'
    reddit = make_reddit_api_interface()
    submission = reddit.submission(id=id)
    print(submission.num_comments)


def update_submission_stats():
    """Fetch submission list from DB, look them up via Reddit API, and update their stats."""
    reddit = make_reddit_api_interface()

    # Fetch submissions once
    with getcursor() as cur:
        cur.execute("SELECT id, title FROM reddit_submission")
        submissions = cur.fetchall()

    updated = 0
    failed = 0

    # Reuse a second cursor for updates
    with getcursor() as cur:
        for sub_id, title in submissions:
            try:
                submission = reddit.submission(id=sub_id)
                cur.execute("""
                    UPDATE reddit_submission
                    SET score = %s,
                        num_comments = %s,
                        upvote_ratio = %s,
                        num_crossposts = %s
                    WHERE id = %s
                """, (
                    submission.score,
                    submission.num_comments,
                    submission.upvote_ratio,
                    submission.num_crossposts,
                    sub_id,
                ))
                print(f"submission updated successfully - {title}")
                updated += 1
            except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
                print(f"submission couldn't be retrieved - {title}")
                failed += 1
            except Exception as e:
                logging.error(f"Unexpected error for {sub_id}: {e}")
                failed += 1

    print(f"Updated: {updated}, Failed: {failed}")


def update_selected_submission_stats(submission_ids):
    """
    Updates score, comment count, upvote ratio, and crosspost count
    for the given list of Reddit submission IDs.
    """
    if not submission_ids:
        print("No submission IDs provided.")
        return

    reddit = make_reddit_api_interface()

    # Fetch titles for those IDs to print/log meaningful messages
    with getcursor() as cur:
        cur.execute(
            "SELECT id, title FROM reddit_submission WHERE id = ANY(%s)",
            (submission_ids,)
        )
        submissions = cur.fetchall()

    updated = 0
    failed = 0

    with getcursor() as cur:
        for sub_id, title in submissions:
            try:
                submission = reddit.submission(id=sub_id)
                cur.execute("""
                    UPDATE reddit_submission
                    SET score = %s,
                        num_comments = %s,
                        upvote_ratio = %s,
                        num_crossposts = %s
                    WHERE id = %s
                """, (
                    submission.score,
                    submission.num_comments,
                    submission.upvote_ratio,
                    submission.num_crossposts,
                    sub_id,
                ))
                updated += 1
            except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
                print(f"submission couldn't be retrieved - {title}")
                failed += 1
            except Exception as e:
                logging.error(f"Unexpected error for {sub_id}: {e}")
                failed += 1

    print(f"Updated: {updated}, Failed: {failed}")


if __name__ == "__main__":
    update_submission_stats()
