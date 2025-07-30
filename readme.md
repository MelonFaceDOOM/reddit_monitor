# Redditor Monitor
* monitor.py will run an infinite loop scraping search terms from vsm db
* digest.py calls some analysis stuff
* update_submissions.py will update comment/vote count for ALL submissions, but this typically isn't called
  * instead a func from it can be called to update a list of submission_ids relevant to a given analysis project
* requires .env with:
  * REDDIT_ID
  * REDDIT_SECRET
  * REDDIT_UA
  * OPENAI_API_KEY
  * PGHOST
  * PGUSER
  * PGPASSWORD
  * PGPORT
  * PGDATABASE
