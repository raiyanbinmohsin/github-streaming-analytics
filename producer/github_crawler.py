import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Generator

import requests
import pulsar

try:
    from producer.rate_limiter import GitHubRateLimiter
except ImportError:
    from rate_limiter import GitHubRateLimiter

BASE_URL = "https://api.github.com/search/repositories"
RAW_REPOSITORIES_TOPIC = "persistent://public/default/raw-repositories"
MAX_PAGES_PER_DAY = 10
LOGGER = logging.getLogger("producer.github_crawler")

def day_range(start: str, end: str) -> Generator[str, None, None]:
    """Yield each date string between start and end, inclusive."""
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    if start_date > end_date:
        raise ValueError("SEARCH_START_DATE must be less than or equal to SEARCH_END_DATE")
    cursor = start_date
    while cursor <= end_date:
        yield cursor.isoformat()
        cursor += timedelta(days=1)

def headers() -> Dict[str, str]:
    """Build GitHub API headers using token from environment when available."""
    token = os.getenv("GITHUB_TOKEN", "")
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def fetch_day(day: str, page: int = 1, per_page: int = 100) -> requests.Response:
    """Fetch one page of repositories updated on a specific day."""
    query = f"pushed:{day}"
    params = {
        "q": query,
        "sort": "updated",
        "order": "desc",
        "per_page": per_page,
        "page": page,
    }
    return requests.get(BASE_URL, headers=headers(), params=params, timeout=30)


def build_filtered_repo(repo: Dict[str, object]) -> Dict[str, object]:
    """Keep only the fields needed by downstream analytics consumers."""
    return {
        "full_name": repo.get("full_name"),
        "language": repo.get("language"),
        "commits_url": str(repo.get("commits_url", "")).replace("{/sha}", ""),
        "contents_url": str(repo.get("contents_url", "")).replace("{+path}", ""),
        "default_branch": repo.get("default_branch"),
    }

def main() -> None:
    """Crawl GitHub repositories and publish filtered payloads to Pulsar."""
    start = os.getenv("SEARCH_START_DATE", "2023-01-01")
    end = os.getenv("SEARCH_END_DATE", "2023-01-01")
    pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://pulsar-broker:6650")
    
    limiter = GitHubRateLimiter()

    # Initialize Pulsar resources once for the full crawl.
    LOGGER.info("Connecting to Apache Pulsar at %s...", pulsar_url)
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(RAW_REPOSITORIES_TOPIC)
    LOGGER.info("Successfully connected to Pulsar.")

    LOGGER.info("Crawling repositories from %s to %s", start, end)

    try:
        for day in day_range(start, end):
            for page in range(1, MAX_PAGES_PER_DAY + 1):
                try:
                    response = fetch_day(day=day, page=page)
                except requests.RequestException as exc:
                    LOGGER.warning("Request exception day=%s page=%s error=%s", day, page, exc)
                    continue

                limiter.sleep_if_needed(response.headers)

                if response.status_code >= 400:
                    LOGGER.warning(
                        "Request failed day=%s page=%s status=%s",
                        day,
                        page,
                        response.status_code,
                    )
                    # GitHub may send Retry-After for secondary limits.
                    retry_after = limiter.retry_after(response.headers)
                    if retry_after:
                        LOGGER.warning("Secondary rate limit hit. Sleeping for %s seconds.", retry_after)
                        time.sleep(retry_after)
                    continue

                try:
                    payload = response.json()
                except ValueError:
                    LOGGER.warning("Invalid JSON day=%s page=%s", day, page)
                    continue

                items = payload.get("items", [])
                if not items:
                    break

                for repo in items:
                    filtered_repo = build_filtered_repo(repo)
                    producer.send(json.dumps(filtered_repo).encode("utf-8"))

                LOGGER.info("Published day=%s page=%s count=%s", day, page, len(items))

    except ValueError as exc:
        LOGGER.error("Configuration error: %s", exc)
    finally:
        producer.flush()
        client.close()
        LOGGER.info("Crawl complete and Pulsar connection closed.")

if __name__ == "__main__":
    main()
