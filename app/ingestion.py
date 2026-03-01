from datetime import datetime, timezone
from typing import Any, Dict, Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from .models import SessionLocal, Room, Message, Lead, Transaction
from .keyword_cache import cache


def parse_ts(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(tz=timezone.utc)


def extract(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    vendor = str(payload.get("vendor") or payload.get("app_id") or payload.get("source") or "vendor")
    evt = str(payload.get("event") or "")
    room = payload.get("room") or payload.get("conversation") or {}
    msg = payload.get("message") or {}
    sender = payload.get("sender") or {}
    room_id = str(room.get("id") or room.get("room_id") or room.get("unique_id") or payload.get("room_id") or "")
    channel = str(room.get("channel") or payload.get("channel") or "")
    customer = room.get("customer") or {}
    participants = room.get("participants") or []
    phone = None
    if isinstance(customer, dict):
        phone = customer.get("phone") or customer.get("phone_number")
    if not phone and isinstance(participants, list):
        for p in participants:
            if str(p.get("type")) == "customer":
                phone = p.get("phone") or p.get("phone_number")
                if phone:
                    break
    message_id = str(msg.get("id") or msg.get("message_id") or "")
    text = msg.get("text") or msg.get("body") or ""
    created_raw = msg.get("created_at") or msg.get("timestamp") or payload.get("created_at")
    created_at = parse_ts(created_raw)
    sender_type = str(sender.get("type") or payload.get("sender_type") or "")
    return {
        "vendor": vendor,
        "event": evt,
        "room_id": room_id,
        "channel": channel,
        "phone": phone,
        "message_id": message_id,
        "text": text,
        "sender_type": sender_type,
        "created_at": created_at.isoformat(),
    }


def detect_booking(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    return "book" in t or "trial" in t


def detect_transaction(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    t = text.lower()
    if "pay" in t or "lunas" in t or "bayar" in t:
        digits = []
        for ch in t:
            if ch.isdigit():
                digits.append(ch)
            elif digits:
                break
        try:
            if digits:
                return float("".join(digits))
        except Exception:
            return None
        return 0.0
    return None


def process_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = extract(payload)
    db: Session = SessionLocal()
    try:
        room = db.query(Room).get(data["room_id"]) if data["room_id"] else None
        if not room and data["room_id"]:
            room = Room(id=data["room_id"], channel=data["channel"], customer_phone=data["phone"])
            db.add(room)
            db.flush()
        if room and not room.customer_phone and data["phone"]:
            room.customer_phone = data["phone"]
        if room and data["channel"] and not room.channel:
            room.channel = data["channel"]
        if data["message_id"]:
            msg = Message(id=data["message_id"], room_id=data["room_id"], sender_type=data["sender_type"], text=str(data["text"] or ""), created_at=parse_ts(data["created_at"]), vendor=data["vendor"])
            try:
                db.add(msg)
                db.flush()
            except IntegrityError:
                db.rollback()
        lead = None
        if room:
            lead = db.query(Lead).filter(Lead.room_id == room.id).first()
        matched_kw = cache.match(str(data["text"] or ""))
        if not lead and matched_kw and room:
            leads_date = parse_ts(data["created_at"])
            ch = cache.resolve_channel(matched_kw, room.channel)
            lead = Lead(room_id=room.id, leads_date=leads_date, channel=ch or "", phone_number=room.customer_phone)
            db.add(lead)
        if lead and not lead.booking_date and detect_booking(str(data["text"] or "")):
            lead.booking_date = parse_ts(data["created_at"])
        trx_val = detect_transaction(str(data["text"] or ""))
        if trx_val is not None and room:
            trx = db.query(Transaction).filter(Transaction.room_id == room.id).first()
            if not trx:
                trx = Transaction(room_id=room.id, transaction_date=parse_ts(data["created_at"]), transaction_value=trx_val)
                db.add(trx)
            else:
                if not trx.transaction_date:
                    trx.transaction_date = parse_ts(data["created_at"])
                if not trx.transaction_value and trx_val:
                    trx.transaction_value = trx_val
        db.commit()
        return {"status": "ok"}
    finally:
        db.close()

