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
    Playwright(Chromium)でページを開き、試合リンク(/match/)を確実に収集して改行区切りで返す。
    流れ:
      0) 可能なら __NEXT_DATA__ をHTMLから抽出（最速）
      1) トップページのDOMから /match/ を回収
      2) 見つからなければ /result/ ページ群へ遷移して /match/ を回収
      3) それでも無ければ /detail/ ページ群へ遷移して /match/ を回収
    返り値は str（改行区切りのURL群 or ""）。既存の parse_listing_page の正規表現で拾える。
    """
    import re
    from urllib.parse import urljoin
    from playwright.async_api import async_playwright

    # tid と year を URL から推定
    m_tid = re.search(r"/koshien/game/(\d{4})/(\d+)/", url)
    year = m_tid.group(1) if m_tid else ""
    tid  = m_tid.group(2) if m_tid else ""

    def _normalize(href: str) -> str:
        if not href:
            return ""
        return href if href.startswith("http") else urljoin(url, href)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(
            user_agent=UA.get("User-Agent"),
            locale="ja-JP",
            extra_http_headers=UA,
        )
        page = await ctx.new_page()

        async def _collect_match_links_on_current_page() -> list[str]:
            els = await page.query_selector_all("a[href*='/match/']")
            tmp = []
            for el in els:
                href = await el.get_attribute("href")
                href = _normalize(href)
                if href and re.search(rf"/koshien/game/{year}/{tid}/match/\d+/?$", href):
                    tmp.append(href)
            return tmp

        async def _collect_links(selector_sub: str) -> list[str]:
            els = await page.query_selector_all(f"a[href*='/{selector_sub}/']")
            return list({_normalize(await el.get_attribute("href")) for el in els})

        # ── 0) ページ取得
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)

        # 0-1) __NEXT_DATA__（あれば最速）
        try:
            html = await page.content()
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                          html, re.DOTALL | re.IGNORECASE)
            if m and m.group(1).strip():
                # 既存の parse_listing_page は /match/ を正規表現で拾うので、
                # この JSON 文字列を返すだけでOK
                return m.group(1).strip()
        except Exception:
            pass

        # 1) トップのDOMから /match/ を収集
        try:
            await page.wait_for_selector("a[href*='/match/']", timeout=5000)
        except Exception:
            pass
        found = await _collect_match_links_on_current_page()
        if found:
            await browser.close()
            return "\n".join(sorted(set(found)))

        # 2) /result/ ページ群を辿って /match/ を収集
        try:
            result_links = await _collect_links("result")
            for rurl in result_links[:50]:  # 念のため上限
                await page.goto(rurl, wait_until="domcontentloaded", timeout=90000)
                try:
                    await page.wait_for_selector("a[href*='/match/']", timeout=5000)
                except Exception:
                    pass
                found.extend(await _collect_match_links_on_current_page())
            if found:
                await browser.close()
                return "\n".join(sorted(set(found)))
        except Exception:
            pass

        # 3) /detail/ ページ群を辿って /match/ を収集（最後の手段）
        try:
            detail_links = await _collect_links("detail")
            for durl in detail_links[:50]:
                await page.goto(durl, wait_until="domcontentloaded", timeout=90000)
                try:
                    await page.wait_for_selector("a[href*='/match/']", timeout=5000)
                except Exception:
                    pass
                found.extend(await _collect_match_links_on_current_page())
            if found:
                await browser.close()
                return "\n".join(sorted(set(found)))
        except Exception:
            pass

        await browser.close()
        return ""


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
