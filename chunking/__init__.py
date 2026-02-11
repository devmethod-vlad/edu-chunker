"""Модуль для разбиения контента на чанки."""
from .strategies import ChunkingStrategy, TokenizerStrategy, SimpleStrategy, get_chunking_strategy
from .chunk_builder import ChunkBuilder, create_chunks_from_page

__all__ = [
    'ChunkingStrategy', 'TokenizerStrategy', 'SimpleStrategy', 'get_chunking_strategy',
    'ChunkBuilder', 'create_chunks_from_page',
]
