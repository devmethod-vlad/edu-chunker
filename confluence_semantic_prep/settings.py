from __future__ import annotations

"""Application settings loaded from environment variables.

This module defines a small helper API to load configuration from the environment
and provides a `Settings` dataclass encapsulating all configuration options.
The settings are primarily consumed by the Confluence semantic preparation
pipeline. If you add new configuration options be sure to update both the
dataclass and the factory method accordingly.
"""

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str | None:
    """Return the trimmed value of an environment variable or the given default.

    Empty strings are treated as missing.
    """
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default


def _env_bool(name: str, default: bool) -> bool:
    """Parse an environment variable into a boolean.

    Accepts typical truthy strings such as ``"1"``, ``"true"``, ``"yes"`` or
    ``"on"``. Anything else (including absence) falls back to the provided
    default.
    """
    v = _env(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    """Parse an environment variable into an integer with a default.

    If parsing fails the default is returned.
    """
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _split_csv(v: str | None) -> list[str]:
    """Split a comma separated environment variable into a list.

    Leading and trailing whitespace on items is stripped and empty items are
    omitted. Returns an empty list if the input is falsy.
    """
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


@dataclass(frozen=True, slots=True)
class Settings:
    """Strongly typed configuration for the Confluence semantic prep pipeline."""

    # Confluence server connection details
    confluence_base_url: str
    confluence_rest_api_prefix: str
    confluence_use_anonymous: bool
    confluence_username: str | None
    confluence_api_token: str | None
    confluence_page_ids: list[str]
    confluence_page_list_limit: int
    confluence_concurrency: int
    confluence_timeout_seconds: int

    # Chunking/parsing parameters
    chunk_size_tokens: int
    chunk_min_tokens: int
    heading_levels_for_text: int
    add_page_prefix: bool
    add_section_prefix: bool
    ignore_tags: list[str]

    # Token counting
    token_count_strategy: str
    tokenizer_local_path: str | None

    # Output configuration
    output_dir: str
    output_prefix: str

    # Miscellaneous flags
    enable_progress_bar: bool

    @staticmethod
    def from_env() -> "Settings":
        """Construct a Settings object by reading from the environment.

        The python-dotenv package is used to load a ``.env`` file if present in
        the working directory. Each configuration option has a sensible default
        where appropriate. Required options raise a ``RuntimeError`` if not
        provided.
        """
        from dotenv import load_dotenv  # type: ignore

        # Load variables from .env file if present. This call is idempotent and
        # safe to call multiple times.
        load_dotenv()

        # Base URL for Confluence is mandatory because without it no HTTP
        # requests can be made.
        base_url = _env("CONFLUENCE_BASE_URL")
        if not base_url:
            raise RuntimeError("CONFLUENCE_BASE_URL is required")

        return Settings(
            confluence_base_url=base_url.rstrip("/"),
            confluence_rest_api_prefix=_env("CONFLUENCE_REST_API_PREFIX", "/rest/api") or "/rest/api",
            confluence_use_anonymous=_env_bool("CONFLUENCE_USE_ANONYMOUS", True),
            confluence_username=_env("CONFLUENCE_USERNAME"),
            confluence_api_token=_env("CONFLUENCE_API_TOKEN"),
            confluence_page_ids=_split_csv(_env("CONFLUENCE_PAGE_ID")),
            confluence_page_list_limit=_env_int("CONFLUENCE_PAGE_LIST_LIMIT", 100),
            confluence_concurrency=_env_int("CONFLUENCE_CONCURRENCY", 10),
            confluence_timeout_seconds=_env_int("CONFLUENCE_TIMEOUT_SECONDS", 30),
            chunk_size_tokens=_env_int("CHUNK_SIZE_TOKENS", 512),
            chunk_min_tokens=_env_int("CHUNK_MIN_TOKENS", 120),
            heading_levels_for_text=_env_int("HEADING_LEVELS_FOR_TEXT", 2),
            add_page_prefix=_env_bool("ADD_PAGE_PREFIX", True),
            add_section_prefix=_env_bool("ADD_SECTION_PREFIX", True),
            ignore_tags=[t.lower() for t in _split_csv(_env("IGNORE_TAGS", "code,script,style,hr"))],
            token_count_strategy=(_env("TOKEN_COUNT_STRATEGY", "simple") or "simple").lower(),
            tokenizer_local_path=_env("TOKENIZER_LOCAL_PATH"),
            output_dir=_env("OUTPUT_DIR", "./out") or "./out",
            output_prefix=_env("OUTPUT_PREFIX", "confluence_structured") or "confluence_structured",
            enable_progress_bar=_env_bool("ENABLE_PROGRESS_BAR", False),
        )