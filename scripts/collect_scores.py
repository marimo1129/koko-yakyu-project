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
    1) __NEXT_DATA__ があればそのJSON文字列を返す
    2) なければ「結果」タブをクリックして無限スクロール→DOMから /match/ を収集し、
       改行区切りテキストとして返す（既存の正規表現抽出がそのまま使える）
    """
    import re, asyncio
    from urllib.parse import urljoin
    from playwright.async_api import async_playwright

    m = re.search(r"/koshien/game/(\d{4})/(\d+)/", url)
    year = m.group(1) if m else ""
    tid  = m.group(2) if m else ""

    def normalize(href: str) -> str:
        if not href:
            return ""
        return href if href.startswith("http") else urljoin(url, href)

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

        # 早めの完了扱い
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)

        # まず __NEXT_DATA__ をHTMLから抜けるなら最速で返す
        html = await page.content()
        mnext = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
        if mnext and mnext.group(1).strip():
            await browser.close()
            return mnext.group(1).strip()

        # ===== ここからタブ操作＋無限スクロール =====
        # 1) 結果タブ（なければ詳細→結果）
        try:
            # すぐ「結果」があればクリック
            btn = await page.get_by_text("結果", exact=False)
            await btn.click(timeout=5000)
        except Exception:
            # 詳細→結果 で到達するパターン
            try:
                await page.get_by_text("詳細", exact=False).click(timeout=4000)
                await page.wait_for_timeout(500)
                await page.get_by_text("結果", exact=False).click(timeout=5000)
            except Exception:
                pass

        # 2) 無限スクロールで読み切る
        last_len = -1
        stable = 0
        for _ in range(60):  # 最大60回 * 300ms ≒ 18秒
            await page.mouse.wheel(0, 2500)
            await page.wait_for_timeout(300)
            els = await page.query_selector_all("a[href*='/match/']")
            cur_len = len(els)
            if cur_len == last_len:
                stable += 1
            else:
                stable = 0
            last_len = cur_len
            if stable >= 5:  # 5回連続で増えなければ読み切りとみなす
                break

        # 3) DOMから /match/ を収集
        els = await page.query_selector_all("a[href*='/match/']")
        found: set[str] = set()
        for el in els:
            href = normalize(await el.get_attribute("href"))
            if href and re.search(rf"/koshien/game/{year}/{tid}/match/\d+/?$", href):
                found.add(href)

        await browser.close()
        return "\n".join(sorted(found))

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

# --- ここまで既存の関数や定義がいろいろ ---
def main():
    # 例: 令和7(2025)年度 秋季東京都大会（hb_tid=1063）
    hb_tid = int(os.getenv("HB_TID", "1063"))
    year   = int(os.getenv("KOKO_YEAR", YEAR))
    print(f"[INFO] hb-nippon 大会 {hb_tid} ({year}) を収集します")
    rows = collect_from_hb_tournament(hb_tid, year)
    print(f"[DEBUG] {len(rows)} rows from hb-nippon")
    write_hb_rows_to_csv(rows)
    print("[DONE] hb-nippon -> data/matches.csv")


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
# ===== hb-nippon.com（高校野球ドットコム）1大会スクレイパ =====
def collect_from_hb_tournament(hb_tid: int, year: int) -> List[Tuple]:
    """
    例: hb_tid=1063 -> 令和7(2025)年度 秋季東京都大会
    ページ: https://www.hb-nippon.com/tournaments/{hb_tid}
    戻り値: [(date_str, round_label, team_left, score, team_right, src_url), ...]
    """
    import re
    url = f"https://www.hb-nippon.com/tournaments/{hb_tid}"
    soup = get_soup(url)

    # 1) 「試合結果」セクションの先頭見出しを探す
    header = None
    for tag in soup.find_all(["h2", "h3"]):
        if "試合結果" in tag.get_text(strip=True):
            header = tag
            break
    if not header:
        print(f"[WARN] 試合結果セクションが見つかりません: {url}")
        return []

    # 2) 次の見出しが来るまでを「試合結果」領域とみなして走査
    results_block = []
    for sib in header.next_siblings:
        name = getattr(sib, "name", None)
        if name in ("h2", "h3"):  # 次のセクションに到達
            break
        if not name:
            continue
        results_block.append(sib)

    rows: List[Tuple] = []
    # 日付・回戦を含むテキストから抽出し、aタグ3連（左チーム / スコア / 右チーム）で拾う
    for block in results_block:
        for line in block.find_all(["li", "p", "tr", "div"]):
            a_list = line.find_all("a")
            if len(a_list) < 3:
                continue
            left = a_list[0].get_text(strip=True)
            mid  = a_list[1].get_text(strip=True)
            right= a_list[2].get_text(strip=True)
            if not re.match(r"^\d+\s*-\s*\d+$", mid):
                continue

            # テキスト全体から日付と回戦を拾う（例: "2025 10月19日 2回戦 ...")
            raw = line.get_text(" ", strip=True)
            m_date = re.search(r"(\d{1,2})月(\d{1,2})日", raw)
            date_str = f"{year}-01-01"
            if m_date:
                mm, dd = m_date.groups()
                date_str = f"{year}-{int(mm):02d}-{int(dd):02d}"

            m_round = re.search(r"(決勝|準決勝|準々決勝|\d+回戦)", raw)
            round_label = m_round.group(1) if m_round else ""

            rows.append((date_str, round_label, left, mid, right, url))

    return rows


def write_hb_rows_to_csv(rows: List[Tuple], out_csv: str = "data/matches.csv") -> None:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    header = ["date", "round", "team_left", "score", "team_right", "source"]
    exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        for r in rows:
            w.writerow(r)
