import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Mapping, Optional

class GitHubRateLimiter:
    """Simple helper for GitHub REST API rate-limit handling."""

    REMAINING_HEADER = "X-RateLimit-Remaining"
    RESET_HEADER = "X-RateLimit-Reset"
    RETRY_AFTER_HEADER = "Retry-After"

    @staticmethod
    def _parse_int(value: Optional[str]) -> Optional[int]:
        """Parse an integer header value safely."""
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def sleep_if_needed(self, headers: Mapping[str, str]) -> None:
        """Sleep until reset time when the primary rate limit is exhausted."""
        remaining_count = self._parse_int(headers.get(self.REMAINING_HEADER))
        reset_time = self._parse_int(headers.get(self.RESET_HEADER))

        if remaining_count is None or reset_time is None:
            return

        if remaining_count > 0:
            return

        wait_for = max(0, reset_time - int(time.time())) + 1
        if wait_for > 0:
            time.sleep(wait_for)

    @staticmethod
    def retry_after(headers: Mapping[str, str]) -> Optional[int]:
        """Return Retry-After seconds from numeric or HTTP-date header values."""
        value = headers.get(GitHubRateLimiter.RETRY_AFTER_HEADER)
        if not value:
            return None

        value = value.strip()

        # Common form: seconds as an integer.
        seconds = GitHubRateLimiter._parse_int(value)
        if seconds is not None:
            return max(0, seconds)

        # RFC-compatible fallback: absolute HTTP date.
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)

            now_utc = datetime.now(timezone.utc)
            return max(0, int((retry_at - now_utc).total_seconds()))
        except (TypeError, ValueError, OverflowError):
            return None
