import json
from typing import Optional, Dict, Any
from .config import settings


class NoopPublisher:
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def send_raw(self, payload: Dict[str, Any]) -> None:
        pass

    def send_dlq(self, payload: Dict[str, Any]) -> None:
        pass


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic_raw: str, topic_dlq: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic_raw = topic_raw
        self.topic_dlq = topic_dlq
        self._producer = None

    def start(self) -> None:
        try:
            from kafka import KafkaProducer
        except Exception:
            self._producer = None
            return
        self._producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers.split(","), value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    def stop(self) -> None:
        if self._producer:
            try:
                self._producer.flush(5)
                self._producer.close()
            except Exception:
                pass

    def _send(self, topic: str, payload: Dict[str, Any]) -> None:
        if not self._producer:
            return
        try:
            self._producer.send(topic, payload)
        except Exception:
            pass

    def send_raw(self, payload: Dict[str, Any]) -> None:
        self._send(self.topic_raw, payload)

    def send_dlq(self, payload: Dict[str, Any]) -> None:
        self._send(self.topic_dlq, payload)


def get_publisher():
    if not settings.kafka_bootstrap_servers:
        return NoopPublisher()
    return KafkaPublisher(settings.kafka_bootstrap_servers, settings.kafka_topic_raw, settings.kafka_topic_dlq)

