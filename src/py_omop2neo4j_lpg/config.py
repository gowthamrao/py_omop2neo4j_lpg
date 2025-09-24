import logging
import sys
import tempfile
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


# --- Settings ---
class Settings(BaseSettings):
    """
    Manages application settings using Pydantic.
    Reads from environment variables or a .env file.
    """

    # PostgreSQL Connection Settings
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str  # No default value for secrets
    POSTGRES_DB: str = "ohdsi"
    OMOP_SCHEMA: str

    # Neo4j Connection Settings
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str  # No default value for secrets

    # ETL Configuration
    EXPORT_DIR: str = "export"
    LOG_FILE: str = "py-omop2neo4j-lpg.log"
    LOAD_CSV_BATCH_SIZE: int = 10000
    TRANSFORMATION_CHUNK_SIZE: int = 100000

    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


_settings = None


def get_settings() -> Settings:
    """Returns a cached settings object."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


settings = get_settings()

# --- Logging ---
# Create export directory if it doesn't exist to store logs
if "pytest" in sys.modules:
    log_dir = tempfile.gettempdir()
else:  # pragma: no cover
    log_dir = settings.EXPORT_DIR
    os.makedirs(log_dir, exist_ok=True)

log_file_path = os.path.join(log_dir, settings.LOG_FILE)


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
    if not logger.handlers:
        # Create handlers
        stream_handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.FileHandler(log_file_path)

        # Create formatters and add it to handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    return logger


# A default logger for general use
logger = get_logger("py_omop2neo4j_lpg")
logger.info("Configuration loaded and logger initialized.")
