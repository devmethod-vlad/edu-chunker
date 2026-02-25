# Confluence Parser → Blocks → Chunks (RAG)

Этот проект скачивает страницы Confluence, превращает HTML в логические **блоки**,
упаковывает их в **чанки** (с учётом заголовков и перекрытий) и сохраняет результат
для последующей индексации (векторная БД / поисковый индекс).

Пайплайн сделан потоковым (streaming), чтобы не упираться в память при парсинге больших порталов:
fetch → parse → chunk → write.

---

## Быстрый старт

1) Установи зависимости:

```bash
pip install -r requirements.txt
```

2) Создай `.env` рядом с `main.py` (можно скопировать `.env.example`):

```bash
cp .env.example .env
```

3) Запусти:

```bash
python main.py
```

---

## Основные переменные окружения

Полный список с комментариями — в `.env.example`.
Ниже — самое важное.

### Confluence

- `CONFLUENCE_BASE_URL` — базовый URL Confluence (без trailing slash)
- `CONFLUENCE_AUTH_TOKEN` — токен (опционально)
- `CONFLUENCE_PAGE_ID` — список ID страниц через запятую (опционально). Если пусто — скрипт обходит все доступные страницы.

### Chunking

- `CHUNK_SIZE` — целевой размер чанка (в токенах)
- `CHUNK_OVERLAP` — перекрытие (в токенах)
- `CHUNKING_STRATEGY` — `simple` или `tokenizer`
- `SENTENCE_SPLITTER` — `regex` / `razdel` / `spacy`

### Output

- `OUTPUT_DIR` — папка, куда складывается результат (JSON и/или SQLite)
- `INCLUDE_BLOCKS_IN_OUTPUT` — добавлять ли массив `blocks` в JSON (по умолчанию `false`)

---

## Форматы сохранения результата

Теперь доступно два канала сохранения: **JSON** и **SQLite**. Можно включать их независимо.

### Переключатели форматов

- `OUTPUT_WRITE_JSON=true|false` (по умолчанию `true`)
- `OUTPUT_WRITE_SQLITE=true|false` (по умолчанию `false`)

Можно включить оба — тогда запись идёт параллельно в оба формата.

Если выключить оба (`false/false`) — скрипт выведет предупреждение и завершится, потому что сохранять нечего.

---

## JSON (потоковый)

По умолчанию создаётся файл:

- `<OUTPUT_DIR>/confluence_chunks_<timestamp>.json`

Формат:

- `chunks` — массив чанков
- `pages` — массив метаданных страниц (пишется всегда)
- `blocks` — опционально (если `INCLUDE_BLOCKS_IN_OUTPUT=true`)
- `metadata` — агрегированные метаданные прогонки

JSON пишется **потоково**, поэтому подходит для больших порталов.

---

## SQLite

SQLite — это **один файл базы данных**, который лежит в `OUTPUT_DIR`.
Никаких контейнеров поднимать не нужно: это обычный файл.

### Параметры SQLite

- `SQLITE_DB_FILENAME` — имя файла БД внутри `OUTPUT_DIR` (по умолчанию `confluence_chunks.sqlite3`)
- `SQLITE_TABLE_NAME` — имя таблицы (по умолчанию `chunks`)
- `SQLITE_PAYLOAD_FIELD` — имя поля, где хранится JSON payload (по умолчанию `payload`)

Ограничение для `SQLITE_TABLE_NAME` и `SQLITE_PAYLOAD_FIELD`:
только `A-Za-z0-9_`, первый символ не цифра (это защита от ошибок и SQL-инъекций через env).

### Схема таблицы (упрощённая по требованиям)

- `chunk_id` — `TEXT PRIMARY KEY`
- `<payload_field>` — `TEXT NOT NULL` (JSON-строка со всеми полями чанка **кроме** `chunk_id`)

Запись выполняется `INSERT OR REPLACE`, чтобы повторный прогон был идемпотентным.

### Пример чтения

```sql
-- Найти конкретный чанк
SELECT chunk_id, payload
FROM chunks
WHERE chunk_id = 'EDU:12345:0-3';

-- Посчитать общее число чанков
SELECT COUNT(*) FROM chunks;
```

---

## Примечания по производительности

- Запись в SQLite делается транзакцией “одна страница → один commit”, это заметно быстрее,
  чем commit на каждый чанк.
- Если пишешь **только в SQLite**, JSON-файл не создаётся (и наоборот).

