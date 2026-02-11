# Примеры использования

## Базовые примеры

### 1. Обработка всех страниц с анонимным доступом

```bash
# .env
CONFLUENCE_BASE_URL=https://confluence.example.com
CONFLUENCE_AUTH_TOKEN=
CONFLUENCE_PAGE_IDS=
```

```bash
python main.py
```

### 2. Обработка конкретных страниц с авторизацией

```bash
# .env
CONFLUENCE_BASE_URL=https://confluence.example.com
CONFLUENCE_AUTH_TOKEN=Bearer YOUR_PERSONAL_ACCESS_TOKEN
CONFLUENCE_PAGE_IDS=123456,789012,345678
```

```bash
python main.py
```

### 3. Настройка размера и перекрытия чанков

```bash
# .env
CHUNK_SIZE=1024
CHUNK_OVERLAP=100
CHUNKING_STRATEGY=simple
```

### 4. Использование точной стратегии токенизации

```bash
# Установите дополнительные зависимости
pip install transformers torch

# .env
CHUNKING_STRATEGY=tokenizer
CHUNK_SIZE=512
CHUNK_OVERLAP=50
```

### 5. Включение прогресс-бара и метрик

```bash
# .env
SHOW_PROGRESS_BAR=true
SHOW_PERFORMANCE_METRICS=true
```

## Продвинутые примеры

### Кастомизация блочных тегов

```bash
# .env
# Добавляем дополнительные теги для извлечения контента
BLOCK_TAGS=p,div,blockquote,pre,ul,ol,table,h1,h2,h3,h4,h5,h6,section,article,figure,figcaption,aside,main

# Исключаем больше тегов
EXCLUDED_TAGS=code,script,style,hr,nav,header,footer,iframe,noscript
```

### Настройка иерархии заголовков

```bash
# .env
# Сохранять до 3 уровней заголовков в тексте чанка
MAX_HEADING_LEVELS=3

# Включить/выключить теги в чанке
INCLUDE_PAGE_TAG=true
INCLUDE_SECTION_TAG=true
```

### Оптимизация производительности

```bash
# .env
# Увеличиваем количество одновременных запросов
MAX_CONCURRENT_REQUESTS=20

# Увеличиваем таймаут для медленных страниц
REQUEST_TIMEOUT=60
```

## Использование в коде

### Программный доступ к настройкам

```python
from config.settings import settings

# Проверка конфигурации
settings.validate()

# Получение значений
print(f"Chunk size: {settings.CHUNK_SIZE}")
print(f"Strategy: {settings.CHUNKING_STRATEGY}")
```

### Использование парсера отдельно

```python
from parser import parse_page_content

html_content = "<html><body><p>Test content</p></body></html>"
page_id = "12345"

blocks, headings = parse_page_content(html_content, page_id)

for block in blocks:
    print(f"Block {block.index}: {block.text[:50]}...")
```

### Использование построителя чанков

```python
from chunking import create_chunks_from_page
from parser import parse_page_content

# Парсим HTML
blocks, headings = parse_page_content(html_content, page_id)

# Создаем чанки
chunks = create_chunks_from_page(
    blocks=blocks,
    headings=headings,
    page_id="12345",
    page_title="Test Page",
    space_key="TEST",
    page_version=1,
    last_modified="2024-02-08T10:00:00",
    page_url="https://confluence.example.com/pages/12345"
)

for chunk in chunks:
    print(f"Chunk {chunk.chunk_id}:")
    print(f"  Text: {chunk.normalized_text[:100]}...")
    print(f"  Hierarchy: {' > '.join(chunk.text_heading_hierarchy)}")
```

### Использование таймера

```python
from utils.timer import timer

# Контекстный менеджер
with timer.measure("my_operation"):
    # Ваш код
    result = expensive_operation()

# Явные метки
timer.start("step1")
do_step1()
timer.end("step1")

timer.start("step2")
do_step2()
elapsed = timer.get_elapsed("step2")
print(f"Step 2 took: {elapsed:.4f}ms")
timer.end("step2")
```

### Прямое использование API клиента

```python
import asyncio
from confluence import ConfluenceAPIClient

async def fetch_specific_page():
    client = ConfluenceAPIClient(
        base_url="https://confluence.example.com",
        auth_token="Bearer TOKEN",
        max_concurrent=10,
        timeout=30
    )
    
    # Получить одну страницу
    page = await client.get_page_content("123456")
    print(f"Page title: {page.title}")
    print(f"Space: {page.space_key}")
    
    # Получить несколько страниц
    pages = await client.get_pages_batch(["123", "456", "789"])
    for page in pages:
        print(f"- {page.title}")

asyncio.run(fetch_specific_page())
```

### Пользовательская стратегия чанкования

```python
from chunking.strategies import ChunkingStrategy
from typing import List

class MyCustomStrategy(ChunkingStrategy):
    """Своя стратегия подсчета токенов."""
    
    def count_tokens(self, text: str) -> int:
        # Например, считаем символы / 4
        return len(text) // 4
    
    def split_text(self, text: str, max_tokens: int) -> List[str]:
        # Простое разбиение по длине
        max_chars = max_tokens * 4
        parts = []
        for i in range(0, len(text), max_chars):
            parts.append(text[i:i + max_chars])
        return parts

# Использование
from chunking import ChunkBuilder

strategy = MyCustomStrategy()
builder = ChunkBuilder(
    chunk_size=512,
    chunk_overlap=50,
    strategy=strategy
)
```

## Сценарии использования

### Сценарий 1: Первичная индексация портала

```bash
# 1. Настройка для обработки всех страниц
CONFLUENCE_BASE_URL=https://confluence.company.com
CONFLUENCE_AUTH_TOKEN=
CONFLUENCE_PAGE_IDS=
SHOW_PROGRESS_BAR=true

# 2. Запуск
python main.py

# 3. Результат в output/confluence_chunks_*.json
```

### Сценарий 2: Обновление конкретного раздела

```bash
# 1. Получить ID страниц раздела (например, через Confluence UI)
CONFLUENCE_PAGE_IDS=11111,22222,33333,44444

# 2. Запуск с метриками
SHOW_PERFORMANCE_METRICS=true
python main.py

# 3. Загрузить только новые чанки в векторную БД
```

### Сценарий 3: Тестирование настроек чанкования

```bash
# 1. Взять несколько тестовых страниц
CONFLUENCE_PAGE_IDS=12345,67890

# 2. Попробовать разные размеры
# Вариант A:
CHUNK_SIZE=256
CHUNK_OVERLAP=25

# Вариант B:
CHUNK_SIZE=512
CHUNK_OVERLAP=50

# Вариант C:
CHUNK_SIZE=1024
CHUNK_OVERLAP=100

# 3. Сравнить результаты
```

### Сценарий 4: Обработка с авторизацией для приватных страниц

```bash
# 1. Создать Personal Access Token в Confluence
# Settings → Personal Access Tokens → Create token

# 2. Настроить .env
CONFLUENCE_AUTH_TOKEN=Bearer YOUR_TOKEN_HERE
CONFLUENCE_PAGE_IDS=  # все доступные страницы

# 3. Запуск
python main.py
```

## Отладка

### Проверка парсинга конкретной страницы

```python
import asyncio
from confluence import ConfluenceAPIClient
from parser import parse_page_content

async def debug_page(page_id: str):
    client = ConfluenceAPIClient(
        base_url="https://confluence.example.com",
        auth_token=None
    )
    
    page = await client.get_page_content(page_id)
    print(f"Page: {page.title}")
    print(f"HTML length: {len(page.body_html)}")
    
    blocks, headings = parse_page_content(page.body_html, page.id)
    print(f"Blocks extracted: {len(blocks)}")
    print(f"Headings found: {len(headings)}")
    
    for i, block in enumerate(blocks[:5]):
        print(f"\nBlock {i}:")
        print(f"  Type: {block.block_type}")
        print(f"  Text: {block.text[:100]}...")

asyncio.run(debug_page("123456"))
```

### Проверка размера чанков

```python
from chunking import get_chunking_strategy

strategy = get_chunking_strategy('simple')

test_text = "Your test content here..."
token_count = strategy.count_tokens(test_text)
print(f"Text has {token_count} tokens")

parts = strategy.split_text(test_text, max_tokens=100)
print(f"Split into {len(parts)} parts")
for i, part in enumerate(parts):
    print(f"Part {i}: {strategy.count_tokens(part)} tokens")
```

## Интеграция с векторной БД (примеры концепций)

### Подготовка для OpenSearch

```python
import orjson
from pathlib import Path

# Загрузить результаты
with open("output/confluence_chunks_20240208_103000.json", "rb") as f:
    data = orjson.loads(f.read())

# Подготовить для индексации
for chunk in data['chunks']:
    doc = {
        'id': chunk['chunk_id'],
        'text': chunk['embedding_text'],  # Для эмбеддинга
        'metadata': {
            'page_id': chunk['page_id'],
            'page_title': chunk['page_title'],
            'space_key': chunk['space_key'],
            'url': chunk['navigation']['url']
        }
    }
    # Отправить в OpenSearch...
```

### Подготовка для Qdrant

```python
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

# Создать коллекцию
client = QdrantClient(url="http://localhost:6333")

client.create_collection(
    collection_name="confluence_chunks",
    vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
)

# Добавить чанки (концептуально)
for chunk in data['chunks']:
    # Сначала получить эмбеддинг для chunk['embedding_text']
    # embedding = model.encode(chunk['embedding_text'])
    
    # Затем добавить в Qdrant
    # client.upsert(...)
    pass
```
