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
import asyncio
from playwright.async_api import async_playwright

# ====== ヘッダ（User-Agent）設定（既存のUA定義をこれに置き換える） ======
# ====== 先頭付近に追加 ======
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

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
    """
    requestsでページを取得し、BeautifulSoupを返す。
    ブラウザ風ヘッダを付与してBot対策を回避。
    """
    try:
        r = requests.get(url, headers=UA, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] failed to GET {url}: {e}")
        return BeautifulSoup("", "html.parser")

    r.encoding = r.apparent_encoding
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
import json

# 先頭付近：reは既にimport済み。追加は不要。
# import json は不要です（文字列からURLを正規表現で抽出します）

def parse_listing_page(tournament_id: int) -> List[str]:
    """
    大会トップの HTML 内に埋め込まれた Next.js の __NEXT_DATA__ から
    試合ページURL（/match/{id}/）を抜き出す
    例: https://vk.sportsbull.jp/koshien/game/{YEAR}/{tournament_id}/
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    soup = get_soup(url)

    # __NEXT_DATA__ スクリプトを取得
    script = soup.select_one("script#__NEXT_DATA__")
    if not script or not script.string:
        print(f"[WARN] No __NEXT_DATA__ on page: {url}")
        return []

    # JSONを深く辿るより、文字列中の match URL を正規表現で直抜きする
    text = script.string
    pat = re.compile(rf"/koshien/game/{YEAR}/{tournament_id}/match/\d+/")
    links = sorted({(BASE + m.group(0)) for m in pat.finditer(text)})

    print(f"[DEBUG] listing_page (NEXT_DATA) {url} -> {len(links)} links")
    return links

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
def main():
    print("[INFO] Collecting test tournament 628 ...")
    listing = parse_listing_page(628)
    print(f"[DEBUG] Found {len(listing)} match URLs:")
    for url in listing[:5]:
        print("  ", url)
    print("[DONE] Test run complete")


if __name__ == "__main__":
    main()
