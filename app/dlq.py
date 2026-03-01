import json
from typing import Dict, Any
from .models import SessionLocal, DLQEvent


def dlq_store(payload: Dict[str, Any], reason: str = "error") -> None:
    db = SessionLocal()
    try:
        e = DLQEvent(reason=reason, payload=json.dumps(payload))
        db.add(e)
        db.commit()
    finally:
        db.close()

