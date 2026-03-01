from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, DateTime, Float, ForeignKey, UniqueConstraint, create_engine, Text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from .config import settings


Base = declarative_base()


class Room(Base):
    __tablename__ = "rooms"
    id = Column(String, primary_key=True)
    channel = Column(String, index=True)
    customer_phone = Column(String, index=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    messages = relationship("Message", back_populates="room")
    lead = relationship("Lead", back_populates="room", uselist=False)
    transaction = relationship("Transaction", back_populates="room", uselist=False)


class Message(Base):
    __tablename__ = "messages"
    id = Column(String, primary_key=True)
    room_id = Column(String, ForeignKey("rooms.id"), index=True)
    sender_type = Column(String)
    text = Column(Text)
    created_at = Column(DateTime, index=True)
    vendor = Column(String)
    room = relationship("Room", back_populates="messages")
    __table_args__ = (UniqueConstraint("id", name="uq_messages_id"),)


class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(String, ForeignKey("rooms.id"), unique=True, index=True)
    leads_date = Column(DateTime, index=True)
    booking_date = Column(DateTime, index=True, nullable=True)
    channel = Column(String, index=True)
    phone_number = Column(String, index=True, nullable=True)
    room = relationship("Room", back_populates="lead")


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(String, ForeignKey("rooms.id"), unique=True, index=True)
    transaction_date = Column(DateTime, index=True, nullable=True)
    transaction_value = Column(Float, nullable=True)
    room = relationship("Room", back_populates="transaction")


class Watermark(Base):
    __tablename__ = "watermarks"
    id = Column(String, primary_key=True)
    value = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DLQEvent(Base):
    __tablename__ = "dlq_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason = Column(String)
    payload = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


engine = create_engine(settings.database_url, connect_args={"check_same_thread": False} if settings.database_url.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db():
    Base.metadata.create_all(bind=engine)
