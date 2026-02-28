from pathlib import Path

from pydantic_settings import BaseSettings

_backend_dir = Path(__file__).resolve().parent.parent
_project_root = _backend_dir.parent


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://bjj:bjj_dev@localhost:5432/bjj_search"
    qdrant_url: str = "http://localhost:6333"
    openai_api_key: str = ""
    gemini_api_key: str = ""
    whisper_model: str = "large-v3"
    data_dir: str = "data"

    model_config = {
        "env_file": [
            str(_project_root / ".env"),
            str(_backend_dir / ".env"),
            ".env",
        ],
        "extra": "ignore",
    }


settings = Settings()
