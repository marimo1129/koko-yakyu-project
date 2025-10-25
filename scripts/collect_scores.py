# -*- coding: utf-8 -*-
"""
高校野球ドットコム（hb-nippon.com）の「大会データ」ページから
試合結果（対戦カードとスコア）を収集して CSV 出力する最小スクリプト。

出力:
  - data/matches.csv  … 1試合=1行
実行:
  $ python scripts/collect_scores.py
  # または GitHub Actions から HB_TID / KOKO_YEAR を指定して実行
"""

import os
import csv
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Tuple

# ================================
# 設定
# ================================
YEAR = int(os.getenv("KOKO_YEAR", datetime.now().year))  # 例: 2025
OUT_MATCHES = "data/matches.csv"

# ブラウザ風ヘッダ（Bot判定回避のため）
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

SCORE_RE = re.compile(r"(\d+)\s*[-－–]\s*(\d+)")

# ================================
# 基本ユーティリティ
# ================================
def get_soup(url: str) -> BeautifulSoup:
    """requestsでGETしてBeautifulSoupを返す（失敗時は空のSoup）。"""
    try:
        r = requests.get(url, headers=UA, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] GET {url}: {e}")
        return BeautifulSoup("", "html.parser")
    r.encoding = r.apparent_encoding
    return BeautifulSoup(r.text, "html.parser")


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


# ================================
# hb-nippon 1大会スクレイパ
# ================================
def collect_from_hb_tournament(hb_tid: int, year: int) -> List[Tuple]:
    """
    例: hb_tid=1063 -> 令和7(2025)年度 秋季東京都大会
    ページ: https://www.hb-nippon.com/tournaments/{hb_tid}

    戻り値: [(date_str, round_label, team_left, score, team_right, src_url), ...]
    """
    url = f"https://www.hb-nippon.com/tournaments/{hb_tid}"
    print(f"[INFO] hb-nippon から大会 {hb_tid} ({year}) を取得: {url}")
    soup = get_soup(url)
    if soup.text.strip() == "":
        print(f"[WARN] ページ取得に失敗: {url}")
        return []

    # --- 1) 「試合結果」セクションの開始見出しを探す ---
    # ページにより h2/h3 のどちらか。テキストに「試合結果」を含む要素を探す。
    header = None
    for tag in soup.find_all(["h2", "h3"]):
        if "試合結果" in tag.get_text(strip=True):
            header = tag
            break
    if not header:
        print(f"[WARN] 試合結果セクションが見つかりません: {url}")
        return []

    # --- 2) 次の見出しが来るまでを「試合結果」領域として取得 ---
    # （hb-nippon は見出し間にリストやテーブルが続く構造が多い）
    results_block = []
    for sib in header.next_siblings:
        name = getattr(sib, "name", None)
        if name in ("h2", "h3"):  # 次のセクションに到達
            break
        if name:
            results_block.append(sib)

    rows: List[Tuple] = []

    # --- 3) ライン走査：aタグ3連（左/スコア/右）を拾う ---
    # li/p/div/tr などを横断的に見る。構造差に耐えるため少しゆるく。
    for block in results_block:
        for line in block.find_all(["li", "p", "tr", "div"]):
            a_list = line.find_all("a")
            if len(a_list) < 3:
                continue

            left = norm(a_list[0].get_text())
            mid  = norm(a_list[1].get_text())
            right= norm(a_list[2].get_text())
            if not left or not right:
                continue
            if not SCORE_RE.match(mid):
                continue  # 真ん中が「スコア」ではない

            # テキスト全体から日付と回戦を拾う（例: "10月19日 2回戦 ..."）
            raw = norm(line.get_text(" "))
            m_date = re.search(r"(\d{1,2})月(\d{1,2})日", raw)
            date_str = f"{year}-01-01"
            if m_date:
                mm, dd = m_date.groups()
                date_str = f"{year}-{int(mm):02d}-{int(dd):02d}"

            m_round = re.search(r"(決勝|準決勝|準々決勝|\d+回戦)", raw)
            round_label = m_round.group(1) if m_round else ""

            rows.append((date_str, round_label, left, mid, right, url))

    print(f"[DEBUG] 取得行数: {len(rows)}")
    return rows


def write_hb_rows_to_csv(rows: List[Tuple], out_csv: str = OUT_MATCHES) -> None:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    header = ["date", "round", "team_left", "score", "team_right", "source"]
    exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


# ================================
# エントリポイント
# ================================
def main():
    # 例: 令和7(2025)年度 秋季東京都大会（hb_tid=1063）
    hb_tid = int(os.getenv("HB_TID", "1063"))
    year   = YEAR
    print(f"[INFO] hb-nippon 大会 {hb_tid} ({year}) を収集します")
    rows = collect_from_hb_tournament(hb_tid, year)
    print(f"[INFO] 書き込み: data/matches.csv  行数={len(rows)}")
    write_hb_rows_to_csv(rows)
    print("[DONE] hb-nippon -> data/matches.csv")


if __name__ == "__main__":
    main()
# ===== main() をこのコードに置き換え =====
import yaml, re, os

YAML_PATH = "data/hb_tournaments.yml"

def _extract_tid(url: str) -> int | None:
    m = re.search(r"/tournaments/(\d+)", url)
    return int(m.group(1)) if m else None

def _load_tournaments_from_yaml() -> tuple[int, list[tuple[int, str, str]]]:
    if not os.path.exists(YAML_PATH):
        print(f"[ERROR] YAML not found: {YAML_PATH}")
        return YEAR, []

    with open(YAML_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    year_cfg = int(cfg.get("year", YEAR))
    year = int(os.getenv("KOKO_YEAR", year_cfg))

    keys = ["autumn_pref", "autumn_regions", "jingu", "senbatsu", "spring_pref"]
    todos: list[tuple[int, str, str]] = []
    for key in keys:
        for item in cfg.get(key, []) or []:
            url = (item.get("url") or "").strip()
            name = str(item.get("name", key)).strip()
            if not url:
                continue
            tid = _extract_tid(url)
            if not tid:
                print(f"[WARN] not a tournaments page? {url}")
                continue
            todos.append((tid, name, url))

    print(f"[INFO] Found {len(todos)} tournaments in YAML (year={year}).")
    return year, todos

def main():
    year, todos = _load_tournaments_from_yaml()
    if not todos:
        print("[ERROR] No tournaments loaded from YAML. (URLが空か形式違いの可能性)")
        return
    total = 0
    for tid, name, url in todos:
        print(f"[INFO] ▶ {name} (id={tid})  {url}")
        rows = collect_from_hb_tournament(tid, year)
        print(f"[INFO]   {len(rows)} rows")
        write_hb_rows_to_csv(rows)
        total += len(rows)
    print(f"[DONE] total appended rows: {total}")

if __name__ == "__main__":
    main()
# ===== ここまで =====
