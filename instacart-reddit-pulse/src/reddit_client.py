from __future__ import annotations

import hashlib
import json
import random
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


class RedditClient:
    """Small Reddit OAuth client with caching and rate-limit-aware requests."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str,
        cache_dir: str = "cache",
        cache_ttl_seconds: int = 6 * 60 * 60,
        db_path: str = "data/reddit_pulse.db",
        timeout: int = 30,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_seconds = cache_ttl_seconds
        self.db_path = Path(db_path)
        self.timeout = timeout
        self.base_url = "https://oauth.reddit.com"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.token_expires_at = 0.0
        self.access_token: Optional[str] = None

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posts(
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT,
                    created_utc INTEGER,
                    created_dt TEXT,
                    title TEXT,
                    body TEXT,
                    permalink TEXT,
                    score INTEGER,
                    num_comments INTEGER,
                    query_tag TEXT,
                    retrieved_utc INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS comments(
                    comment_id TEXT PRIMARY KEY,
                    post_id TEXT,
                    subreddit TEXT,
                    created_utc INTEGER,
                    created_dt TEXT,
                    body TEXT,
                    permalink TEXT,
                    score INTEGER,
                    retrieved_utc INTEGER
                )
                """
            )
            conn.commit()

    def _cache_key(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        payload = {"url": url, "params": params or {}}
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json"

    def _load_cache(self, key: str) -> Optional[Any]:
        path = self._cache_path(key)
        if not path.exists():
            return None
        if time.time() - path.stat().st_mtime > self.cache_ttl_seconds:
            return None
        return json.loads(path.read_text())

    def _save_cache(self, key: str, payload: Any) -> None:
        self._cache_path(key).write_text(json.dumps(payload))

    def _ensure_token(self) -> None:
        if self.access_token and time.time() < self.token_expires_at - 60:
            return

        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        token_data = resp.json()
        self.access_token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 3600))
        self.token_expires_at = time.time() + expires_in
        self.session.headers.update({"Authorization": f"bearer {self.access_token}"})

    def _respect_rate_limit(self, headers: Dict[str, str]) -> None:
        remaining = headers.get("X-Ratelimit-Remaining")
        reset = headers.get("X-Ratelimit-Reset")
        if remaining is None or reset is None:
            return

        try:
            remaining_f = float(remaining)
            reset_s = float(reset)
        except ValueError:
            return

        if remaining_f < 5:
            sleep_s = max(1, int(reset_s)) + random.uniform(0.2, 1.0)
            time.sleep(sleep_s)

    def safe_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, use_cache: bool = True) -> Any:
        self._ensure_token()
        url = f"{self.base_url}{endpoint}"
        key = self._cache_key(url, params)

        if use_cache:
            cached = self._load_cache(key)
            if cached is not None:
                return cached

        last_response = None
        for attempt in range(3):
            resp = self.session.get(url, params=params, timeout=self.timeout)
            last_response = resp
            self._respect_rate_limit(resp.headers)

            if resp.status_code == 429:
                reset = float(resp.headers.get("X-Ratelimit-Reset", 5))
                time.sleep(reset + random.uniform(0.3, 1.2))
                continue

            if 500 <= resp.status_code <= 599:
                time.sleep((2 ** attempt) + random.uniform(0.2, 0.8))
                continue

            resp.raise_for_status()
            payload = resp.json()
            if use_cache:
                self._save_cache(key, payload)
            return payload

        if last_response is None:
            raise RuntimeError("Request failed before receiving a response")
        last_response.raise_for_status()
        raise RuntimeError("Request failed after retries")

    def fetch_submissions(self, subreddit: str, query: str, limit: int = 50, time_filter: str = "week") -> List[Dict[str, Any]]:
        data = self.safe_get(
            f"/r/{subreddit}/search",
            params={
                "q": query,
                "restrict_sr": 1,
                "sort": "new",
                "t": time_filter,
                "limit": min(limit, 50),
            },
        )
        children = data.get("data", {}).get("children", [])
        rows: List[Dict[str, Any]] = []
        now_utc = int(time.time())
        for child in children:
            d = child.get("data", {})
            row = {
                "post_id": d.get("id"),
                "subreddit": d.get("subreddit"),
                "created_utc": int(d.get("created_utc", 0)),
                "created_dt": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(d.get("created_utc", 0))),
                "title": d.get("title", "") or "",
                "body": d.get("selftext", "") or "",
                "permalink": f"https://reddit.com{d.get('permalink', '')}",
                "score": int(d.get("score", 0)),
                "num_comments": int(d.get("num_comments", 0)),
                "query_tag": query,
                "retrieved_utc": now_utc,
            }
            if row["post_id"]:
                rows.append(row)
        return rows

    def fetch_comments(self, subreddit: str, post_id: str, limit: int = 20, depth: int = 2) -> List[Dict[str, Any]]:
        listing = self.safe_get(
            f"/r/{subreddit}/comments/{post_id}.json",
            params={"limit": min(limit, 20), "depth": min(depth, 2), "sort": "top"},
        )
        if not isinstance(listing, list) or len(listing) < 2:
            return []
        comment_nodes = listing[1].get("data", {}).get("children", [])
        now_utc = int(time.time())

        comments: List[Dict[str, Any]] = []
        for node in comment_nodes:
            data = node.get("data", {})
            if node.get("kind") != "t1":
                continue
            c_id = data.get("id")
            if not c_id:
                continue
            comments.append(
                {
                    "comment_id": c_id,
                    "post_id": post_id,
                    "subreddit": subreddit,
                    "created_utc": int(data.get("created_utc", 0)),
                    "created_dt": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(data.get("created_utc", 0))),
                    "body": data.get("body", "") or "",
                    "permalink": f"https://reddit.com{data.get('permalink', '')}",
                    "score": int(data.get("score", 0)),
                    "retrieved_utc": now_utc,
                }
            )
        return comments

    def upsert_posts(self, posts: List[Dict[str, Any]]) -> None:
        if not posts:
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO posts(post_id, subreddit, created_utc, created_dt, title, body, permalink, score, num_comments, query_tag, retrieved_utc)
                VALUES(:post_id, :subreddit, :created_utc, :created_dt, :title, :body, :permalink, :score, :num_comments, :query_tag, :retrieved_utc)
                ON CONFLICT(post_id) DO UPDATE SET
                    subreddit=excluded.subreddit,
                    created_utc=excluded.created_utc,
                    created_dt=excluded.created_dt,
                    title=excluded.title,
                    body=excluded.body,
                    permalink=excluded.permalink,
                    score=excluded.score,
                    num_comments=excluded.num_comments,
                    query_tag=excluded.query_tag,
                    retrieved_utc=excluded.retrieved_utc
                """,
                posts,
            )
            conn.commit()

    def upsert_comments(self, comments: List[Dict[str, Any]]) -> None:
        if not comments:
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO comments(comment_id, post_id, subreddit, created_utc, created_dt, body, permalink, score, retrieved_utc)
                VALUES(:comment_id, :post_id, :subreddit, :created_utc, :created_dt, :body, :permalink, :score, :retrieved_utc)
                ON CONFLICT(comment_id) DO UPDATE SET
                    post_id=excluded.post_id,
                    subreddit=excluded.subreddit,
                    created_utc=excluded.created_utc,
                    created_dt=excluded.created_dt,
                    body=excluded.body,
                    permalink=excluded.permalink,
                    score=excluded.score,
                    retrieved_utc=excluded.retrieved_utc
                """,
                comments,
            )
            conn.commit()
