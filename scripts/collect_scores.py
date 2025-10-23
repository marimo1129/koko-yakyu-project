# -*- coding: utf-8 -*-
"""
vk.sportsbull.jp の大会ページを巡回し、試合スコアを収集して CSV 出力するスクリプト
出力:
  - data/matches.csv            … 1試合=1行（対戦履歴）
  - data/tournament_summary.csv … 大会ごとの優勝/準優勝/ベスト4
実行:
  $ python scripts/collect_scores.py
"""
import csv
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ================================
# 設定
# ================================
YEAR = datetime.now().year  # 例: 2025
BASE = "https://vk.sportsbull.jp"
OUT_MATCHES = "data/matches.csv"
OUT_SUMMARY = "data/tournament_summary.csv"
UA = {"User-Agent": f"koko-yakyu-project-bot/{YEAR}"}
SLEEP_SEC = 0.7  # アクセス間隔（優しめ）

# ----------------
# 収集対象セット
#   秋季/春季（各都道府県）
#   神宮/センバツ/夏（全国）
#   ※ IDレンジは既存コードの方針を踏襲。必要に応じて調整してください。
# ----------------
TARGETS = [
    ("秋季都道府県大会",  range(610, 656)),  # 例
    ("春季都道府県大会",  range(710, 756)),  # 例
    ("明治神宮大会",      [560]),            # 全国
    ("選抜(センバツ)",     [701]),            # 全国
    ("夏の甲子園",        [1001]),           # 全国
]

# 既存の PREF_MAP を流用（例）
PREF_MAP: Dict[int, str] = {
    610:"北海道",611:"青森",612:"岩手",613:"宮城",614:"秋田",615:"山形",616:"福島",
    617:"茨城",618:"栃木",619:"群馬",620:"埼玉",621:"千葉",622:"東京",623:"神奈川",
    624:"新潟",625:"富山",626:"石川",627:"福井",628:"山梨",629:"長野",
    630:"岐阜",631:"静岡",632:"愛知",633:"三重",
    634:"滋賀",635:"京都",636:"大阪",637:"兵庫",638:"奈良",639:"和歌山",
    640:"鳥取",641:"島根",642:"岡山",643:"広島",644:"山口",
    645:"徳島",646:"香川",647:"愛媛",648:"高知",
    649:"福岡",650:"佐賀",651:"長崎",652:"熊本",653:"大分",654:"宮崎",655:"鹿児島",
    656:"沖縄"
}

# ================================
# 型
# ================================
@dataclass
class MatchRow:
    year: int
    tournament: str         # 例: 秋季都道府県大会/明治神宮大会…
    tournament_id: int      # 収集元ID
    stage: str              # 例: 準々決勝/決勝 など（不明時は空）
    round_no: Optional[int] # 数字で取れたら
    date: str               # YYYY-MM-DD（取れなければ空）
    region: str             # 全国/地方
    prefecture: str         # 都道府県名（全国大会は空）
    team_a: str
    team_b: str
    score_a: Optional[int]
    score_b: Optional[int]
    winner: str
    venue: str              # 取れたら
    source_url: str

@dataclass
class SummaryRow:
    year: int
    tournament: str
    tournament_id: int
    prefecture: str       # 全国大会は空
    champion: str
    runner_up: str
    best4_a: str
    best4_b: str

# ================================
# ユーティリティ
# ================================
SCORE_RE = re.compile(r"(\d+)\s*[-－–]\s*(\d+)")

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def int_or_none(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None

# ================================
# 解析（サイトのマークアップは変わる可能性あり）
# セレクタは後で微調整しやすいよう一箇所に寄せています。
# ================================
def parse_listing_page(tournament_id: int) -> List[str]:
    """
    大会トップから各試合詳細ページURLの一覧を返す
    例: https://vk.sportsbull.jp/koshien/game/{YEAR}/{tournament_id}/
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    soup = get_soup(url)

    # ★重要: 下記セレクタは構造に合わせて調整してください
    links = []
    for a in soup.select("a[href*='/koshien/game/'][href*='/detail/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = BASE + href
        if f"/koshien/game/{YEAR}/" in href:
            links.append(href)
    # 代替（カード内リンクから集める）
    if not links:
        for a in soup.select("a[href*='/koshien/game/']"):
            href = a.get("href", "")
            if "/detail/" in href:
                if href.startswith("/"):
                    href = BASE + href
                links.append(href)
    return sorted(set(links))

def parse_listing_page(tournament_id: int) -> List[str]:
    """
    大会トップから各試合ページURLの一覧を返す
    例: https://vk.sportsbull.jp/koshien/game/{YEAR}/{tournament_id}/
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    soup = get_soup(url)
    links = set()

    # 現行構造: /match/ を拾う
    for a in soup.select("a[href*='/koshien/game/'][href*='/match/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = BASE + href
        links.add(href)

    # 一部ページ: /result/ を拾う
    for a in soup.select("a[href*='/koshien/game/'][href*='/result/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = BASE + href
        links.add(href)

    # 旧構造: /detail/ をフォールバックで拾う
    for a in soup.select("a[href*='/koshien/game/']"):
        href = a.get("href", "")
        if any(k in href for k in ["/detail/", "/match/", "/result/"]):
            if href.startswith("/"):
                href = BASE + href
            links.add(href)

    print(f"[DEBUG] listing_page {url} -> {len(links)} links")
    return sorted(links)
