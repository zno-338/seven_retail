import asyncio
import httpx
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from .config import settings
from .models import SessionLocal, Watermark
from .ingestion import process_event


def get_watermark_key() -> str:
    return "vendor_updated_since"


def get_watermark(db: Session) -> Optional[str]:
    w = db.query(Watermark).get(get_watermark_key())
    return w.value if w else None


def set_watermark(db: Session, value: str) -> None:
    w = db.query(Watermark).get(get_watermark_key())
    if not w:
        w = Watermark(id=get_watermark_key(), value=value)
        db.add(w)
    else:
        w.value = value
    db.commit()


async def poll_vendor():
    if not settings.vendor_api_base:
        return
    while True:
        try:
            db = SessionLocal()
            try:
                since = get_watermark(db)
            finally:
                db.close()
            headers = {}
            if settings.vendor_api_token:
                headers["Authorization"] = f"Bearer {settings.vendor_api_token}"
            params = {}
            if since:
                params["updated_since"] = since
            url = f"{settings.vendor_api_base.rstrip('/')}/rooms"
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(url, headers=headers, params=params)
                if r.status_code == 200:
                    rooms = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
                    max_ts = since
                    for room in rooms:
                        rid = str(room.get("id") or "")
                        mu = room.get("updated_at") or room.get("modified_at") or None
                        if rid:
                            mu_iso = None
                            if isinstance(mu, (int, float)):
                                mu_iso = datetime.fromtimestamp(mu, tz=timezone.utc).isoformat()
                            elif isinstance(mu, str):
                                mu_iso = mu
                            url_m = f"{settings.vendor_api_base.rstrip('/')}/rooms/{rid}/messages"
                            pr = await client.get(url_m, headers=headers, params={"since": since} if since else None)
                            if pr.status_code == 200:
                                msgs = pr.json() if isinstance(pr.json(), list) else pr.json().get("data", [])
                                for m in msgs:
                                    payload = {"event": "message_created", "room": room, "message": m}
                                    process_event(payload)
                            if mu_iso:
                                if not max_ts or mu_iso > max_ts:
                                    max_ts = mu_iso
                    if max_ts:
                        db2 = SessionLocal()
                        try:
                            set_watermark(db2, max_ts)
                        finally:
                            db2.close()
        except Exception:
            pass
        await asyncio.sleep(max(5, settings.vendor_poll_seconds))

