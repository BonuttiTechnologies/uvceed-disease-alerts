import os

def env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

UVCEED_API_KEY = env("UVCEED_API_KEY")  # required for auth in production

# Read-through cache TTLs (hours)
TTL_HOURS_WASTEWATER = float(env("UVCEED_TTL_HOURS_WASTEWATER", "12"))
TTL_HOURS_NSSP_ED_VISITS = float(env("UVCEED_TTL_HOURS_NSSP_ED_VISITS", "12"))

# NSSP configuration
NSSP_WEEKS = int(env("UVCEED_NSSP_WEEKS", "16"))
NSSP_PATHOGEN = env("UVCEED_NSSP_PATHOGEN", "combined") or "combined"

# API behavior
REFRESH_TIMEOUT_SECONDS = int(env("UVCEED_REFRESH_TIMEOUT_SECONDS", "55"))

# Which signal types this API serves (option 2: multiple signals per ZIP)
SIGNAL_TYPES = ["wastewater", "nssp_ed_visits"]
