# -*- coding: utf-8 -*-
"""
collect_players.py
第2段階：選手の自動収集と評価（最小実装）

使い方例:
  python scripts/collect_players.py --year 2025 \
    --best8_csv data/best8_autumn_2025.csv \
    --players_links data/players_links.csv \
    --out_dir data

設計ポイント:
- 画像(photo_url)は扱わない（列も出力しない）
- URL取得元は players_links.csv（推奨） or 将来の自動探索
- プロバイダ(サイト)ごとに抽出ロジックをアダプタ化
"""

import argparse
import csv
import dataclasses
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

JST = timezone(timedelta(hours=9))

# --------------------------
# ログ設定
# --------------------------
def setup_logger(log_path: str):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

# --------------------------
# データモデル
# --------------------------
@dataclasses.dataclass
class Player:
    year: int
    school_name: str
    prefecture: Optional[str]
    player_name: str
    grade: Optional[int]          # 1 or 2 を期待
    position: Optional[str]
    max_velocity: Optional[float] # km/h
    total_hr: Optional[int]
    ops: Optional[float]
    avg: Optional[float]
    hr: Optional[int]
    rbi: Optional[int]
    era: Optional[float]
    k9: Optional[float]
    scout_comment: Optional[str]
    youtube_url: Optional[str]
    source_url: str
    updated_at: str               # ISO8601

    @property
    def player_id(self) -> str:
        # year + hash(school+player) で安定IDを生成
        key = f"{self.year}:{self.school_name}:{self.player_name}"
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]
        # 学校略キー
        school_key = re.sub(r"\s+", "", self.school_name)[:8]
        return f"{self.year}-{school_key}-{h}"

# --------------------------
# ユーティリティ
# --------------------------
def now_iso_jst() -> str:
    return datetime.now(JST).isoformat()

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

def to_int(x: Optional[str]) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        # 数値が "144km/h" 等のケースに対応
        m = re.search(r"(\d+)", str(x))
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        return None

def normalize_position(pos: Optional[str]) -> Optional[str]:
    if not pos:
        return None
    pos = pos.strip()
    # よくある省略・ゆらぎ
    mapping = {
        "P": "投手", "投": "投手", "投手": "投手",
        "C": "捕手", "捕": "捕手", "捕手": "捕手",
        "1B": "一塁手", "一塁": "一塁手",
        "2B": "二塁手", "二塁": "二塁手",
        "3B": "三塁手", "三塁": "三塁手",
        "SS": "遊撃手", "遊": "遊撃手", "遊撃": "遊撃手",
        "LF": "左翼手", "左": "左翼手",
        "CF": "中堅手", "中": "中堅手",
        "RF": "右翼手", "右": "右翼手",
    }
    return mapping.get(pos, pos)

def normalize_grade(g: Optional[str]) -> Optional[int]:
    if g is None:
        return None
    s = str(g)
    # "2年", "高2", "2" 等を 2 に
    m = re.search(r"([123])", s)
    if m:
        return int(m.group(1))
    return None

def safe_div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or d is None or d == 0:
        return None
    return n / d

# --------------------------
# プロバイダ（サイト）アダプタ
# --------------------------
class ProviderBase:
    NAME = "base"

    def can_handle(self, url: str) -> bool:
        return False

    def fetch_and_parse(self, url: str) -> Dict:
        """URLから原文→テキスト抽出→簡易パースし、辞書を返す
        返却フォーマット（キーは任意、下で吸収）:
          {
            "player_name": "...",
            "grade": "2年",
            "position": "投手",
            "max_velocity": "144km/h",
            "total_hr": "12本",
            "avg": ".385",
            "ops": "0.921",
            "hr": "2",
            "rbi": "6",
            "era": "1.82",
            "k9": "10.5",
            "scout_comment": "....",
            "youtube_url": "https://...",
          }
        """
        raise NotImplementedError

    # 共通のHTTP取得（タイムアウト・UA・リトライ軽装備）
    def http_get(self, url: str, retry: int = 2, timeout: int = 12) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; koko-yakyu-collector/1.0)",
            "Accept-Language": "ja,en;q=0.8",
        }
        for i in range(retry + 1):
            try:
                r = requests.get(url, headers=headers, timeout=timeout)
                if r.status_code == 200 and r.text:
                    return r.text
                logging.warning(f"[{self.NAME}] status={r.status_code} url={url}")
            except Exception as e:
                logging.warning(f"[{self.NAME}] GET error ({i}/{retry}) {url}: {e}")
            time.sleep(1.2 * (i + 1))
        return None

class SportsBullProvider(ProviderBase):
    NAME = "sportsbull"

    def can_handle(self, url: str) -> bool:
        host = urlparse(url).netloc
        return "sportsbull.jp" in host or "vk.sportsbull.jp" in host

    def fetch_and_parse(self, url: str) -> Dict:
        html = self.http_get(url)
        if not html:
            return {}
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)

        # 超簡易抽出（正規表現ベースの最小版）
        out = {}
        # 名前（title等から）
        m = re.search(r"(?:(?:選手|プロフィール)[:：]\s*)?([^\s＜＜<>|｜\-\—]{2,12})\s*(?:選手|くん|さん)?\s*(?:\||｜| - )", text)
        if m:
            out["player_name"] = m.group(1)

        # 学年
        m = re.search(r"(?:学年|学年：|学年:)\s*([一二三12３]{1})", text)
        if not m:
            m = re.search(r"([12３])年(?:生)?", text)
        if m:
            out["grade"] = m.group(1)

        # ポジション
        m = re.search(r"(投手|捕手|一塁手|二塁手|三塁手|遊撃手|左翼手|中堅手|右翼手|P|C|SS|LF|CF|RF)", text)
        if m:
            out["position"] = m.group(1)

        # 数値類
        def pick_float(pattern):
            m = re.search(pattern, text)
            return m.group(1) if m else None

        out["max_velocity"] = pick_float(r"MAX(?:球速)?\s*([12]?\d{2})\s*km/h")
        out["total_hr"]    = pick_float(r"(?:高校通算|通算)\s*?(\d{1,3})\s*本")
        out["avg"]         = pick_float(r"(?:打率|AVG)[:：]?\s*0?\.(\d{3})")
        if out.get("avg"):
            out["av]()
