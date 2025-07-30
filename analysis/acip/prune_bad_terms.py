import pandas as pd
from db import getcursor
from analysis.acip.acip import load_submissions, CGPT_RESPONSE_FILE


def response_summary(output_path="analysis/acip/search_term_relevance.csv"):
    """prints cgpt_response value_counts for each search term"""
    submissions = load_submissions()
    responses = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    print(f"{len(responses)} total responses")
    print(responses['cgpt_response'].value_counts())

    summary_rows = []

    for search_term in submissions:
        term_ids = [s['id'] for s in submissions[search_term]]
        responses_for_term = responses[responses['id'].isin(term_ids)]
        value_counts = responses_for_term['cgpt_response'].value_counts(
            dropna=False)

        print(f"{len(responses_for_term)} responses for {search_term}")
        print(value_counts)
        print(80 * "-")

        for response_value, count in value_counts.items():
            summary_rows.append({
                "search_term": search_term,
                "cgpt_response": response_value,
                "count": count
            })

    # Save summary to CSV
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_path, index=False)
    print(f"Saved summary to {output_path}")


def remove_low_pos_terms_from_db():
    with open("low_pos_terms.txt") as f:
        low_pos_terms = f.read().split("\n")
    with getcursor(commit=True) as cur:
        for term in low_pos_terms:
            delete_search_term(cur, term)


def delete_search_term(cur, search_term_name):
    """
    Deletes a single search term and all associated matches.
    """
    # Step 1: Get the search_term.id
    cur.execute("""
        SELECT id FROM search_term
        WHERE name = %s
    """, (search_term_name,))
    result = cur.fetchone()

    if not result:
        print(f"No search term found with name: {search_term_name}")
        return

    search_term_id = result[0]
    print(f"Deleting search term: {search_term_name} (id={search_term_id})")

    # Step 2: Delete from all match tables
    cur.execute("""
        DELETE FROM search_term_match_tweet
        WHERE search_term_id = %s
    """, (search_term_id,))
    cur.execute("""
        DELETE FROM search_term_match_reddit_comment
        WHERE search_term_id = %s
    """, (search_term_id,))
    cur.execute("""
        DELETE FROM search_term_match_reddit_submission
        WHERE search_term_id = %s
    """, (search_term_id,))
    cur.execute("""
        DELETE FROM search_term_match_podcast_segment
        WHERE search_term_id = %s
    """, (search_term_id,))

    # Step 3: Delete from search_term table
    cur.execute("""
        DELETE FROM search_term
        WHERE id = %s
    """, (search_term_id,))

    print("Deletion complete.")


def check_coverage_on_cut_terms():
    with open("low_pos_terms.txt") as f:
        low_pos_terms = f.read().split("\n")
    all_submissions = load_submissions()
    keep_id_list = []
    for search_term, submission_list in all_submissions.items():
        if search_term in low_pos_terms:
            search_term_ids = [i['id'] for i in submission_list]
            keep_id_list.extend(search_term_ids)
    keep_id_list = list(set(keep_id_list))
    df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    df = df[df['id'].isin(keep_id_list)]
    df = df[df['cgpt_response'].isin([1, 2])]
    cut_ids = df['id'].tolist()
    other_id_list = []
    for search_term, submission_list in all_submissions.items():
        if search_term not in low_pos_terms:
            search_term_ids = [i['id'] for i in submission_list]
            other_id_list.extend(search_term_ids)
    cut_unique = [x for x in cut_ids if x not in other_id_list]
    cut_dupes = [x for x in cut_ids if x in other_id_list]
    print(len(cut_unique), "will truly be cut")
    print(len(cut_dupes), "will be covered by other terms")
    # RESULT -> 20 will be covered, 15 will be truly cut


def remove_low_pos_terms(df):
    """remove rows associated with low pos terms in submissions json file"""
    with open("low_pos_terms.txt") as f:
        low_pos_terms = f.read().split("\n")
    all_submissions = load_submissions()
    keep_id_list = []
    for search_term, submission_list in all_submissions.items():
        if search_term not in low_pos_terms:
            search_term_ids = [i['id'] for i in submission_list]
            keep_id_list.extend(search_term_ids)
    keep_id_list = list(set(keep_id_list))
    df = df[df['id'].isin(keep_id_list)]
    return df


def identify_low_pos_terms():
    """finds search terms that produced <1% positive rate according to cgpt classification"""
    df = pd.read_csv(CGPT_RESPONSE_FILE, dtype={"id": str})
    df = filter_df_for_analysis(df)
    all_submissions = load_submissions()
    low_pos_terms = []
    for search_term, submission_list in all_submissions.items():
        pos_sub_list = [
            i for i in submission_list if i['id'] in df['id'].values]
        if len(submission_list) > 0:
            pos_rate = len(pos_sub_list)/len(submission_list)
            if pos_rate < 0.01:
                low_pos_terms.append(search_term)
    with open("low_pos_terms.txt", "w") as f:
        f.write("\n".join(low_pos_terms))
