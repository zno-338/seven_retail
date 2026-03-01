import asyncio
import hmac
import hashlib
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from .models import init_db
from .ingestion import process_event
from .keyword_cache import keyword_reloader
from .scheduler import poll_vendor
from .config import settings
from .kafka_pub import get_publisher
from .dlq import dlq_store


app = FastAPI()


@app.on_event("startup")
async def on_startup():
    init_db()
    asyncio.create_task(keyword_reloader())
    asyncio.create_task(poll_vendor())
    app.state.publisher = get_publisher()
    try:
        app.state.publisher.start()
    except Exception:
        pass


@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def root():
    return {
        "service": "sparks-ingestion",
        "endpoints": {
            "health": "/health",
            "webhook_messages": "POST /webhooks/vendor/messages",
            "docs": "/docs",
            "openapi": "/openapi.json"
        }
    }


def verify_signature(secret: str, body: bytes, signature: str) -> bool:
    try:
        if signature.startswith("sha256="):
            signature = signature.split("=", 1)[1]
        mac = hmac.new(secret.encode(), msg=body, digestmod=hashlib.sha256).hexdigest()
        return hmac.compare_digest(mac, signature)
    except Exception:
        return False


@app.post("/webhooks/vendor/messages")
async def webhook_messages(request: Request):
    body = await request.body()
    if settings.webhook_secret:
        sig = request.headers.get("X-Signature") or ""
        if not verify_signature(settings.webhook_secret, body, sig):
            raise HTTPException(status_code=401)
    payload = await request.json()
    try:
        if hasattr(app.state, "publisher"):
            app.state.publisher.send_raw({"payload": payload})
    except Exception:
        try:
            if hasattr(app.state, "publisher"):
                app.state.publisher.send_dlq({"payload": payload})
        except Exception:
            dlq_store({"payload": payload}, "publish_failed")
    process_event(payload)
    return JSONResponse({"status": "ok"})
