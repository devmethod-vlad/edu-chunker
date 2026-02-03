from __future__ import annotations

import asyncio
from pathlib import Path

from confluence_semantic_prep.logging_config import setup_logging
from confluence_semantic_prep.settings import Settings
from confluence_semantic_prep.pipeline import run_pipeline


async def main() -> None:
    settings = Settings.from_env()
    setup_logging()

    Path(settings.output_dir).mkdir(parents=True, exist_ok=True)
    await run_pipeline(settings)


if __name__ == "__main__":
    asyncio.run(main())
