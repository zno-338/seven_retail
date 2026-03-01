import asyncio
import csv
import os
from typing import Dict, Set, Optional
from .config import settings


class KeywordCache:
    def __init__(self) -> None:
        self.keywords: Set[str] = set()
        self.channel_map: Dict[str, str] = {}

    def load_from_csv(self, path: Optional[str]) -> None:
        self.keywords.clear()
        self.channel_map.clear()
        if not path or not os.path.exists(path):
            return
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                kw = str(row.get("keyword", "")).strip()
                ch = str(row.get("channel", "")).strip()
                if kw:
                    self.keywords.add(kw.lower())
                    if ch:
                        self.channel_map[kw.lower()] = ch

    def match(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        t = text.lower()
        for kw in self.keywords:
            if kw in t:
                return kw
        return None

    def resolve_channel(self, keyword: Optional[str], fallback: Optional[str]) -> Optional[str]:
        if keyword and keyword.lower() in self.channel_map:
            return self.channel_map[keyword.lower()]
        return fallback


cache = KeywordCache()


async def keyword_reloader():
    while True:
        cache.load_from_csv(settings.opening_keywords_csv)
        await asyncio.sleep(max(5, settings.keyword_reload_seconds))

