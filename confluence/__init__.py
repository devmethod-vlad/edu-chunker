"""Модуль для работы с Confluence API."""
from .models import ConfluencePage, ContentBlock, HeadingInfo, Chunk
from .api_client import ConfluenceAPIClient, fetch_confluence_pages

__all__ = [
    'ConfluencePage', 'ContentBlock', 'HeadingInfo', 'Chunk',
    'ConfluenceAPIClient', 'fetch_confluence_pages',
]
