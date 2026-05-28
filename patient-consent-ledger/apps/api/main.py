"""Phase 0 placeholder. Real preference-serving API arrives in Phase 7."""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(
    title="Patient Consent Ledger API",
    version="0.0.0-phase0",
    description="Placeholder. Real endpoints arrive in Phase 7.",
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "phase": "0"}


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Patient Consent Ledger API. See /docs."}
