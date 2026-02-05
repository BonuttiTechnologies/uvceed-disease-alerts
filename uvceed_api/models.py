from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field

Risk = Literal["low", "moderate", "high", "unknown"]
Trend = Literal["rising", "falling", "flat", "unknown"]
Confidence = Literal["low", "moderate", "high"]

class SignalOut(BaseModel):
    signal_type: str
    risk: Risk = "unknown"
    trend: Trend = "unknown"
    confidence: Confidence = "low"
    generated_at: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None

class LatestSignalsOut(BaseModel):
    zip_code: str
    generated_at: Optional[str] = None
    signals: Dict[str, SignalOut]
    refreshed: bool = False
    errors: Optional[Dict[str, str]] = None

class RefreshIn(BaseModel):
    zip: str = Field(..., description="5-digit ZIP code")

class HealthOut(BaseModel):
    status: str = "ok"
