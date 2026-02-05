from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

def load_env() -> Path:
    """Load env vars from repo-root .env if present."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    return env_path

ENV_PATH = load_env()

# Auth
UVCEED_API_KEY = os.getenv("UVCEED_API_KEY", "").strip()

# Signals exposed by /signals/latest
SIGNAL_TYPES = ["wastewater", "nssp_ed_visits"]

# Refresh behavior
REFRESH_TIMEOUT_SECONDS = int(os.getenv("UVCEED_REFRESH_TIMEOUT_SECONDS", "55"))

# per-signal cache TTL (staleness threshold)
TTL_HOURS_WASTEWATER = float(os.getenv("UVCEED_TTL_HOURS_WASTEWATER", "12"))
TTL_HOURS_NSSP_ED_VISITS = float(os.getenv("UVCEED_TTL_HOURS_NSSP_ED_VISITS", "12"))

# NSSP config
NSSP_PATHOGEN = (os.getenv("UVCEED_NSSP_PATHOGEN", "combined") or "combined").strip()
NSSP_WEEKS = int(os.getenv("UVCEED_NSSP_WEEKS", "16"))
