from fastapi import Header, HTTPException
from .config import UVCEED_API_KEY

def require_api_key(authorization: str | None = Header(default=None)) -> None:
    if not UVCEED_API_KEY:
        # In development you might run without a key; keep endpoint open if unset.
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <API_KEY>")
    token = authorization.split(" ", 1)[1].strip()
    if token != UVCEED_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
