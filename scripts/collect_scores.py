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

async def _fetch_next_data_with_browser(url: str) -> str:
    """
    Playwright でページ読込中の XHR(JSON) を傍受して試合IDを収集。
    見つかったら /koshien/game/{year}/{tid}/match/{id}/ の行テキストを返す。
    最後の保険として __NEXT_DATA__ や DOM の a[href*="/match/"] も試す。
    返り値は常に str（改行区切り or 空文字）。
    """
    import re
    from urllib.parse import urljoin
    from playwright.async_api import async_playwright

    m = re.search(r"/koshien/game/(\d{4})/(\d+)/", url)
    year = m.group(1) if m else ""
    tid  = m.group(2) if m else ""

    def build_match_url(mid: int | str) -> str:
        return f"{BASE}/koshien/game/{year}/{tid}/match/{mid}/"

    match_urls: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(
            user_agent=UA.get("User-Agent"),
            locale="ja-JP",
            extra_http_headers=UA,
        )

        page = await ctx.new_page()

        # --- XHR(JSON) を傍受 ---
        async def on_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                url_l = resp.url.lower()
                if "application/json" in ctype and f"/{year}/{tid}" in url_l:
                    data = await resp.json()
                    # よくある形: {"games":[{"id":1234, ...}, ...]}
                    if isinstance(data, dict):
                        # 1) games[] に id がある
                        if "games" in data and isinstance(data["games"], list):
                            for g in data["games"]:
                                mid = g.get("id")
                                if mid is not None:
                                    match_urls.add(build_match_url(mid))
                        # 2) deeply nested な場合も総当りで id を拾う
                        def walk(x):
                            if isinstance(x, dict):
                                if "id" in x and isinstance(x["id"], (int, str)):
                                    match_urls.add(build_match_url(x["id"]))
                                for v in x.values(): walk(v)
                            elif isinstance(x, list):
                                for v in x: walk(v)
                        walk(data)
            except Exception:
                pass

        page.on("response", on_response)

        # --- 読み込み ---
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        # 追っかけで発火するXhrを待つ
        await page.wait_for_timeout(6000)

        # XHRで拾えたらそれで終了
        if match_urls:
            await browser.close()
            return "\n".join(sorted(match_urls))

        # --- 保険1: __NEXT_DATA__ を HTML から抜く ---
        html = await page.content()
        m_next = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
        if m_next and m_next.group(1).strip():
            await browser.close()
            return m_next.group(1).strip()

        # --- 保険2: DOM の a[href*="/match/"] を集める ---
        try:
            await page.wait_for_selector("a[href*='/match/']", timeout=4000)
        except Exception:
            pass
        els = await page.query_selector_all("a[href*='/match/']")
        for el in els:
            href = await el.get_attribute("href")
            if not href: 
                continue
            if href.startswith("/"):
                href = urljoin(url, href)
            if re.search(rf"/koshien/game/{year}/{tid}/match/\d+/?$", href):
                match_urls.add(href)

        await browser.close()
        return "\n".join(sorted(match_urls))

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
    __NEXT_DATA__から試合ページURL一覧を抽出する
    1) requestsで取得して試す
    2) 取れなければPlaywrightで再取得して確実に取る
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    print(f"[INFO] Collecting tournament {tournament_id} ...")
    soup = get_soup(url)

    # ---- まず requests で試す
    script = soup.select_one("script#__NEXT_DATA__")
    text = script.string if script and script.string else ""

    # ---- ダメならブラウザで再取得
    if not text:
        print(f"[INFO] fallback to browser: {url}")
        try:
            text = asyncio.run(_fetch_next_data_with_browser(url))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            text = loop.run_until_complete(_fetch_next_data_with_browser(url))
            loop.close()

    if not text:
        print(f"[WARN] __NEXT_DATA__ not found even with browser: {url}")
        return []

    # ---- 正規表現で /match/ID/ を抽出
    import re
    pat = re.compile(rf"/koshien/game/{YEAR}/{tournament_id}/match/\d+/")
    links = sorted({BASE + m.group(0) for m in pat.finditer(text)})

    print(f"[DEBUG] listing_page (NEXT_DATA) {url} -> {len(links)} links")
    return links

def main():
    print("[INFO] Collecting test tournament 628 ...")
    listing = parse_listing_page(628)
    print(f"[DEBUG] Found {len(listing)} match URLs:")
    for url in listing[:5]:
        print("  ", url)
    print("[DONE] Test run complete")


if __name__ == "__main__":
    main()
  
def parse_listing_page(tournament_id: int) -> List[str]:
    """
    まず requests+BS4 で __NEXT_DATA__ を探し、無ければ Playwright で
    描画後DOMから /match/ リンクを直接収集（確実版）
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    print(f"[INFO] Collecting tournament {tournament_id} ...")

    # 1) まずは requests で __NEXT_DATA__ を試す（取れれば最速）
    soup = get_soup(url)
    script = soup.select_one("script#__NEXT_DATA__")
    if script and script.string:
        import re
        pat = re.compile(rf"/koshien/game/{YEAR}/{tournament_id}/match/\d+/")
        links = sorted({BASE + m.group(0) for m in pat.finditer(script.string)})
        print(f"[DEBUG] listing_page (NEXT_DATA) {url} -> {len(links)} links")
        if links:
            return links
        # 取れなければブラウザへフォールバック

    # 2) ブラウザで描画後DOMから a[href*="/match/"] を拾う
    print(f"[INFO] fallback to browser: {url}")
    try:
        links = asyncio.run(_fetch_match_links_with_browser(tournament_id))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        links = loop.run_until_complete(_fetch_match_links_with_browser(tournament_id))
        loop.close()

    if not links:
        print(f"[WARN] match links not found with browser: {url}")
        return []

    print(f"[DEBUG] listing_page (DOM) {url} -> {len(links)} links")
    return links
