#!/usr/bin/env python3
"""
Purpose:
Scrape posts and comments from given subreddit(s)

Setup:
python3 -m venv venv
source venv/bin/activate
python -m pip install -U pip
python -m pip install requests beautifulsoup4 lxml

Details:
- Pulls posts from one or more subreddits via public listing JSON: https://www.reddit.com/r/{sub}/{sort}.json
- For each post, pulls the full nested comment tree via: https://www.reddit.com/comments/{post_id}.json
- Flattens ALL nested replies into a comments table with parent/child relationships
- It does NOT expand "more comments" placeholders (kind="more") that require additional calls.

Run Examples:
  # 50 newest posts from r/travel + all nested comments (as returned in the initial comments JSON)
  python reddit_scrape.py \
    --subs travel \
    --sort new \
    --post_limit 50 \
    --min_sleep 4 --max_sleep 7 \
    --posts_out ../out/posts/travel_posts.csv \
    --comments_out ../out/comments/travel_50_new_with_comments.csv

  # Multiple subs, fewer posts, cap comments per post to avoid huge outputs
  python reddit_scrape.py \
    --subs Lufthansa Europetravel \
    --sort hot \
    --post_limit 20 \
    --max_comments_per_post 10 \
    --min_sleep 3 --max_sleep 7 \
    --posts_out ../out/posts/posts.csv \
    --comments_out ../out/comments/comments.csv

"""


from __future__ import annotations

import argparse
import csv
import json
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

WWW_BASE = "https://www.reddit.com"

UA_DEFAULT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0 Safari/537.36"
)

@dataclass
class RedditPost:
    subreddit: str
    post_id: str
    fullname: str
    title: str
    author: Optional[str]
    created_utc: Optional[str] 
    score: Optional[int]
    num_comments: Optional[int]
    upvote_ratio: Optional[float]
    over_18: Optional[bool]
    is_self: Optional[bool]
    link_flair_text: Optional[str]
    permalink: str
    post_url: Optional[str]
    selftext: Optional[str]


@dataclass
class RedditComment:
    subreddit: str
    post_id: str
    comment_id: str
    fullname: str
    parent_fullname: Optional[str]
    depth: int
    author: Optional[str]
    created_utc: Optional[str]   
    score: Optional[int]
    body: Optional[str]
    permalink: str
    is_submitter: Optional[bool]
    distinguished: Optional[str]
    stickied: Optional[bool]
    removed: Optional[bool]


def iso_utc_from_epoch(epoch: Any) -> Optional[str]:
    try:
        if epoch is None:
            return None
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def jitter_sleep(min_s: float, max_s: float) -> None:
    delay = random.uniform(min_s, max_s)
    print(f"  sleeping {delay:.2f}s")
    time.sleep(delay)


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "DNT": "1",
        }
    )
    return s


def safe_int(x: Any) -> Optional[int]:
    return int(x) if isinstance(x, int) else None


def safe_float(x: Any) -> Optional[float]:
    return float(x) if isinstance(x, (int, float)) else None


### POST SCRAPING ###

def listing_url(sub: str, sort: str, t: str, after: Optional[str], count: int) -> str:
    base = f"{WWW_BASE}/r/{sub}/{sort}.json"
    params = {"raw_json": 1, "limit": 100, "t": t}
    if after:
        params["after"] = after
        params["count"] = count
    return base + "?" + requests.compat.urlencode(params)


def scrape_posts_json(
    session: requests.Session,
    sub: str,
    sort: str,
    t: str,
    post_limit: int,
    min_sleep: float,
    max_sleep: float,
    timeout: int,
) -> List[RedditPost]:
    rows: List[RedditPost] = []
    seen_ids: set[str] = set()

    after: Optional[str] = None
    count = 0
    page = 0

    while len(rows) < post_limit:
        page += 1
        url = listing_url(sub, sort, t, after, count)
        print(f"  posts page {page}: after={after or 'None'}")

        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code in (403, 429):
            print(f"  !! blocked/rate-limited fetching posts (HTTP {resp.status_code})")
            break

        try:
            payload = resp.json()
        except Exception:
            print("  !! posts JSON parse failed (likely interstitial HTML)")
            break

        data = (payload or {}).get("data") or {}
        children = data.get("children") or []
        if not children:
            break

        for ch in children:
            d = (ch or {}).get("data") or {}
            post_id = d.get("id")
            if not post_id or post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            fullname = d.get("name")  # t3_xxx
            permalink = d.get("permalink") or ""
            if permalink.startswith("/"):
                permalink = WWW_BASE + permalink

            rows.append(
                RedditPost(
                    subreddit=sub,
                    post_id=post_id,
                    fullname=fullname or f"t3_{post_id}",
                    title=d.get("title") or "",
                    author=d.get("author"),
                    created_utc=iso_utc_from_epoch(d.get("created_utc")),
                    score=safe_int(d.get("score")),
                    num_comments=safe_int(d.get("num_comments")),
                    upvote_ratio=safe_float(d.get("upvote_ratio")),
                    over_18=d.get("over_18") if isinstance(d.get("over_18"), bool) else None,
                    is_self=d.get("is_self") if isinstance(d.get("is_self"), bool) else None,
                    link_flair_text=d.get("link_flair_text"),
                    permalink=permalink,
                    post_url=d.get("url"),
                    selftext=(d.get("selftext") or None),
                )
            )

            if len(rows) >= post_limit:
                break

        after = data.get("after")
        count += len(children)

        if not after:
            break

        if len(rows) < post_limit:
            jitter_sleep(min_sleep, max_sleep)

    return rows


### COMMENT SCRAPING ###

def comments_url(post_id: str, sort: str) -> str:
    # sort can be: confidence, top, new, controversial, old, qa
    return f"{WWW_BASE}/comments/{post_id}.json?raw_json=1&sort={sort}&limit=500"


def flatten_comment_tree(
    subreddit: str,
    post_id: str,
    post_fullname: str,
    comment_children: List[Dict[str, Any]],
    max_comments: int,
    max_depth: int,
) -> Tuple[List[RedditComment], int]:
    """
    Flattens comments recursively from the JSON structure:
      listing -> children (kind t1 for comments, kind more for placeholders)
    We ignore kind="more" placeholders (counted as skipped).
    """
    out: List[RedditComment] = []
    skipped_more = 0

    # Stack for DFS: (node, depth, parent_fullname)
    stack: List[Tuple[Dict[str, Any], int, Optional[str]]] = []
    for child in reversed(comment_children):
        stack.append((child, 0, post_fullname))

    while stack and len(out) < max_comments:
        node, depth, parent_fullname = stack.pop()

        kind = (node or {}).get("kind")
        data = (node or {}).get("data") or {}

        if kind == "more":
            # Placeholder for additional comments not included in this payload.
            skipped_more += 1
            continue

        if kind != "t1":
            continue

        # Depth cap
        if depth > max_depth:
            continue

        comment_id = data.get("id")
        fullname = data.get("name") or (f"t1_{comment_id}" if comment_id else None)
        if not comment_id or not fullname:
            continue

        body = data.get("body")
        removed = False
        # Sometimes removed/deleted comments have body like "[removed]" / "[deleted]" or empty
        if body in ("[removed]", "[deleted]") or body is None:
            removed = True

        permalink = data.get("permalink") or ""
        if permalink.startswith("/"):
            permalink = WWW_BASE + permalink

        out.append(
            RedditComment(
                subreddit=subreddit,
                post_id=post_id,
                comment_id=comment_id,
                fullname=fullname,
                parent_fullname=parent_fullname,
                depth=int(data.get("depth", depth)),
                author=data.get("author"),
                created_utc=iso_utc_from_epoch(data.get("created_utc")),
                score=safe_int(data.get("score")),
                body=body,
                permalink=permalink,
                is_submitter=data.get("is_submitter") if isinstance(data.get("is_submitter"), bool) else None,
                distinguished=data.get("distinguished"),
                stickied=data.get("stickied") if isinstance(data.get("stickied"), bool) else None,
                removed=removed,
            )
        )

        # Push replies (if any) onto the stack
        replies = data.get("replies")
        if replies and isinstance(replies, dict):
            rep_data = (replies.get("data") or {})
            rep_children = rep_data.get("children") or []
            # Depth increases by 1 for replies
            next_depth = depth + 1
            if next_depth <= max_depth:
                for rep in reversed(rep_children):
                    stack.append((rep, next_depth, fullname))

    return out, skipped_more


def scrape_comments_json(
    session: requests.Session,
    post: RedditPost,
    comment_sort: str,
    max_comments_per_post: int,
    max_depth: int,
    timeout: int,
) -> Tuple[List[RedditComment], int]:
    url = comments_url(post.post_id, comment_sort)
    resp = session.get(url, timeout=timeout, allow_redirects=True)

    if resp.status_code in (403, 429):
        print(f"  !! blocked/rate-limited fetching comments for {post.post_id} (HTTP {resp.status_code})")
        return [], 0

    try:
        payload = resp.json()
    except Exception:
        print(f"  !! comments JSON parse failed for {post.post_id} (likely interstitial HTML)")
        return [], 0

    if not isinstance(payload, list) or len(payload) < 2:
        return [], 0

    # payload[1] is the comment listing
    listing = payload[1] or {}
    data = (listing.get("data") or {})
    children = data.get("children") or []

    comments, skipped_more = flatten_comment_tree(
        subreddit=post.subreddit,
        post_id=post.post_id,
        post_fullname=post.fullname,
        comment_children=children,
        max_comments=max_comments_per_post,
        max_depth=max_depth,
    )
    return comments, skipped_more


### OUTPUT ###

def write_csv(path: str, rows: List[Any], fieldnames: List[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def write_jsonl(path: str, rows: Iterable[Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")


### MAIN ###

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", required=True, help="Subreddits (no r/), e.g. travel solotravel")
    ap.add_argument("--sort", default="new", choices=["new", "hot", "top"], help="Post listing sort")
    ap.add_argument("--t", default="week", choices=["hour", "day", "week", "month", "year", "all"], help="Time window for top")
    ap.add_argument("--post_limit", type=int, default=25, help="Posts per subreddit")
    ap.add_argument("--comment_sort", default="top",
                    choices=["confidence", "top", "new", "controversial", "old", "qa"],
                    help="Comment sort used by /comments/{id}.json")
    ap.add_argument("--max_comments_per_post", type=int, default=2000,
                    help="Hard cap on flattened comments per post (safety)")
    ap.add_argument("--max_depth", type=int, default=50,
                    help="Max reply depth to traverse (safety)")
    ap.add_argument("--min_sleep", type=float, default=3.0, help="Min seconds between requests")
    ap.add_argument("--max_sleep", type=float, default=8.0, help="Max seconds between requests")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    ap.add_argument("--posts_out", default="reddit_posts.csv", help="Posts output (.csv or .jsonl)")
    ap.add_argument("--comments_out", default="reddit_comments.csv", help="Comments output (.csv or .jsonl)")
    ap.add_argument("--user_agent", default=UA_DEFAULT, help="User-Agent")
    args = ap.parse_args()

    session = make_session(args.user_agent)

    all_posts: List[RedditPost] = []
    all_comments: List[RedditComment] = []

    # scrape posts per sub
    for sub in args.subs:
        print(f"\n== r/{sub} ==")
        posts = scrape_posts_json(
            session=session,
            sub=sub,
            sort=args.sort,
            t=args.t,
            post_limit=args.post_limit,
            min_sleep=args.min_sleep,
            max_sleep=args.max_sleep,
            timeout=args.timeout,
        )
        print(f"  collected {len(posts)} posts from r/{sub}")
        all_posts.extend(posts)

        # scrape comments for each post
        skipped_more_total = 0
        for i, post in enumerate(posts, start=1):
            print(f"  comments {i}/{len(posts)} for post {post.post_id}")
            comments, skipped_more = scrape_comments_json(
                session=session,
                post=post,
                comment_sort=args.comment_sort,
                max_comments_per_post=args.max_comments_per_post,
                max_depth=args.max_depth,
                timeout=args.timeout,
            )
            skipped_more_total += skipped_more
            all_comments.extend(comments)
            jitter_sleep(args.min_sleep, args.max_sleep)

        if skipped_more_total:
            print(f"  NOTE: skipped {skipped_more_total} 'more comments' placeholders in r/{sub} threads.")

    # global de-dup
    post_dedup: Dict[Tuple[str, str], RedditPost] = {}
    for p in all_posts:
        post_dedup[(p.subreddit, p.post_id)] = p
    all_posts = list(post_dedup.values())

    comment_dedup: Dict[str, RedditComment] = {}
    for c in all_comments:
        # fullname is unique across reddit
        comment_dedup[c.fullname] = c
    all_comments = list(comment_dedup.values())

    # write outputs
    if args.posts_out.lower().endswith(".jsonl"):
        write_jsonl(args.posts_out, all_posts)
    else:
        post_fields = list(asdict(RedditPost(
            subreddit="", post_id="", fullname="", title="", author=None, created_utc=None,
            score=None, num_comments=None, upvote_ratio=None, over_18=None, is_self=None,
            link_flair_text=None, permalink="", post_url=None, selftext=None
        )).keys())
        write_csv(args.posts_out, all_posts, post_fields)

    if args.comments_out.lower().endswith(".jsonl"):
        write_jsonl(args.comments_out, all_comments)
    else:
        comment_fields = list(asdict(RedditComment(
            subreddit="", post_id="", comment_id="", fullname="", parent_fullname=None,
            depth=0, author=None, created_utc=None, score=None, body=None, permalink="",
            is_submitter=None, distinguished=None, stickied=None, removed=None
        )).keys())
        write_csv(args.comments_out, all_comments, comment_fields)

    print(f"\nSaved {len(all_posts)} unique posts to {args.posts_out}")
    print(f"Saved {len(all_comments)} unique comments to {args.comments_out}")


if __name__ == "__main__":
    main()