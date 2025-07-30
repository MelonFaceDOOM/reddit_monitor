from vsm import getcursor, init_connection, cleanup
from cgpt import single_prompt_response
from scrape import get_submissions_until_duplicate, make_reddit_api_interface


def test_db():
    """will connect directly to db if SSH_TUNNEL=0 in .env
       will connect to ssh_tunnel is SSH_TUNNEL=1"""
    try:
        init_connection() # establish db/ssh connections
        with getcursor() as cur:
            cur.execute("""SELECT DISTINCT name FROM search_term""")
            r = cur.fetchall()
        print(f"db connection successful: {len(r)} search_terms found in search_term table")
    except Exception as e:
        print(f"db connection FAILED: {e}")

        
def test_ssh_tunnel():
    """will connect to db through ssh_tunnel"""
    try:
        with getcursor() as cur:
            cur.execute("""SELECT DISTINCT name FROM search_term""")
            r = cur.fetchall()
        print(f"ssh_tunnel->db successful: {len(r)} search_terms found in search_term table")
    except Exception as e:
        print(f"ssh_tunnel->db FAILED: {e}")

def test_openAI():
    try:
        response = single_prompt_response("return a cute and original sentence.")
        print(f"openAI query successful: {response}")
    except Exception as e:
        print(f"openAI query failed: {e}")
        
def test_reddit():
    try:
        reddit = make_reddit_api_interface()
        submissions = list(get_submissions_until_duplicate(reddit, "horrible news"))
        print(f"reddit API query successful: {len(submissions)} submissions found.")
    except Exception as e:
        print(f"reddit API query failed: {e}")
        
        
if __name__ == "__main__":
    init_connection()  # establish db/ssh connections
    test_db()
    cleanup()  # cleanup since the next test might change tunnel params
    init_connection()  # re-establish db/ssh connections
    test_ssh_tunnel()
    # don't cleaup again because that will be handled by @atexit
    test_openAI()
    test_reddit()