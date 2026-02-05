# uvceed_alerts/config.py
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

def load_env() -> Path:
    """Load environment variables from the project's .env file.

    Explicit path avoids python-dotenv's find_dotenv edge cases (e.g., stdin/heredoc).
    """
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=env_path, override=False)
    return env_path

# Load on import (simple v1)
ENV_PATH = load_env()

OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
HUD_ZIP_API_KEY = os.getenv("HUD_ZIP_API_KEY", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Used by CDC Socrata datasets (wastewater, NSSP if applicable)
CDC_APP_TOKEN = os.getenv("CDC_APP_TOKEN", "") or SOCRATA_APP_TOKEN

# Backwards-compatible export used by ingestion scripts.
# NOTE: db.py reads DATABASE_URL directly from the environment as well.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
