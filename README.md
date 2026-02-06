# reddit-metadata-collector

## Purpose:
Scrape posts and comments from given subreddit(s)


## Setup:
python3 -m venv venv
source venv/bin/activate
python -m pip install -U pip
python -m pip install requests beautifulsoup4 lxml

## Details:
- Pulls posts from one or more subreddits via public listing JSON: https://www.reddit.com/r/{sub}/{sort}.json
- For each post, pulls the full nested comment tree via: https://www.reddit.com/comments/{post_id}.json
- Flattens ALL nested replies into a comments table with parent/child relationships
- It does NOT expand "more comments" placeholders (kind="more") that require additional calls.

## Run Examples:
### 50 newest posts from r/travel + all nested comments (as returned in the initial comments JSON)
  python reddit_scrape.py \
    --subs travel \
    --sort new \
    --post_limit 50 \
    --min_sleep 4 --max_sleep 7 \
    --posts_out ../out/posts/travel_posts.csv \
    --comments_out ../out/comments/travel_50_new_with_comments.csv

### Multiple subs, fewer posts, cap comments per post to avoid huge outputs
  python reddit_scrape.py \
    --subs travel solotravel \
    --sort hot \
    --post_limit 10 \
    --max_comments_per_post 500 \
    --min_sleep 3 --max_sleep 8 \
    --posts_out ../out/posts/posts.csv \
    --comments_out ../out/comments/comments.csv