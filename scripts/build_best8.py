# -*- coding: utf-8 -*-
"""
hb_tournaments.yml（autumn_pref）に記載の各都道府県の大会URLを開き、
大会ページを直接解析して「ベスト8（準々決勝出場校）」を抽出。
CSV: data/best8_autumn_YYYY.csv を出力する。

抽出ロジック:
- 「準々決勝」「ベスト8」「4回戦」などの見出し直下を優先的に解析
- それが見つからない場合、ページ内のスコア行（"A 4-3 B" のような行）から
  チーム名を拾って重複なく8校そろえる
"""

import os
import re
import csv
import yaml
import time
import random
import traceback
from datetime import datetime
from pathlib import Path
from collections import OrderedDict
from typing import List, Tuple, Dict, Optional, Iterable

import requests
from bs4 import BeautifulSoup

CFG_PATH = Path("data/hb_tournaments.yml")
OUT_DIR  = Path("data")
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


# ---------------------------
# 設定のロード
# ---------------------------
def load_config() -> Tuple[int, List[Dict[str, str]]]:
    if not CFG_PATH.exists():
        raise FileNotFoundError(f"not found: {CFG_PATH}")

    data = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8")) or {}
    year = int(os.getenv("koko_year", data.get("year", datetime.now().year)))

    # YAML内のautumn_pref（URL空は除外）
    prefs = [p for p in (data.get("autumn_pref") or []) if p and p.get("url")]

    return year, prefs


def to_pref_name(yaml_name: str) -> str:
    """
    "大阪府 秋季大会" -> "大阪府" のように都道府県名を切り出す
    """
    yaml_name = (yaml_name or "").strip()
    m = re.match(r"(.+?)\s*秋季大会", yaml_name)
    return m.group(1) if m else yaml_name


# ---------------------------
# HTML取得
# ---------------------------
def fetch_html(url: str, timeout: int = 25) -> BeautifulSoup:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] GET failed: {url} -> {e}")
        return BeautifulSoup("", "html.parser")

# ========= 修正版：ここから置き換え =========
SCORE_RE = re.compile(r"[０-９0-9]+\s*[-\-－–]\s*[０-９0-9]+")

def norm(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()

def _ban(name: str) -> bool:
    name = name.lower()
    ban = {"高校野球ドットコム", "tiktok", "facebook", "instagram",
           "youtube", "新着記事", "選手名鑑", "チーム一覧", "大会ページ",
           "代表", "対戦", "ブロック"}
    return (name in ban) or len(name) > 25


def collect_pairs_by_score(root: BeautifulSoup) -> list[tuple[str, str]]:
    """
    スコア(3-1等)を含むテキストノードを起点に、その “行”（tr/li/div）内や
    前後の兄弟の中から <a> のチーム名を2つ集める。
    """
    pairs: list[tuple[str, str]] = []

    # スコア表記を含むテキストノードを列挙
    for txt in root.find_all(string=SCORE_RE):
        node = txt.parent

        # そのテキストの ‘行っぽい’ コンテナを見つける
        row = node
        while row and (getattr(row, "name", None) not in ("tr", "li", "div")):
            row = row.parent
        container = row or node

        def names_in(elem):
            res = []
            if not elem:
                return res
            for a in elem.find_all("a"):
                t = norm(a.get_text())
                if t and not _ban(t):
                    res.append(t)
            return res

        # まず同じコンテナ内の a から抽出
        cand = names_in(container)

        # 2つ未満なら前後の “行” からも補完
        if len(cand) < 2:
            prev = container.find_previous(["tr", "li", "div"])
            nxt  = container.find_next(["tr", "li", "div"])
            cand = (names_in(prev) + cand + names_in(nxt))

        # それでも足りなければ、コンテナのテキストから ‘／・ / ’ 等で分割して拾う軽い保険
        if len(cand) < 2:
            raw = norm(container.get_text())
            # スコアの左右にありそうな部分を切り分け
            m = SCORE_RE.search(raw)
            if m:
                left = raw[:m.start()].strip("　 \t:|（）()[]<>/・")
                right = raw[m.end():].strip("　 \t:|（）()[]<>/・")
                if left and right and not _ban(left) and not _ban(right):
                    cand = [left, right]

        if len(cand) >= 2:
            pairs.append((cand[0], cand[1]))

    return pairs


def extract_best8_from_soup(soup: BeautifulSoup) -> list[str]:
    """
    1) 「準々決勝/ベスト8/4回戦」セクションがあればそこでスコア起点に抽出
    2) それ以外はページ全体からスコア起点に抽出
    """
    HEAD_PAT = re.compile(r"(準々決勝|ベスト8|ベスト８|4回戦|４回戦)")
    picked: "OrderedDict[str, bool]" = OrderedDict()

    def uniq_push(name: str):
        name = norm(name)
        if name and not _ban(name):
            picked[name] = True

    # 1) 見出しセクションを優先
    for h in soup.find_all(re.compile(r"^h[1-6]$")):
        if HEAD_PAT.search(norm(h.get_text())):
            seg = []
            for sib in h.next_siblings:
                if getattr(sib, "name", None) and re.match(r"^h[1-6]$", sib.name):
                    break
                seg.append(sib)
            container = BeautifulSoup("", "html.parser").new_tag("div")
            for s in seg:
                container.append(s)
            for a, b in collect_pairs_by_score(container):
                uniq_push(a); uniq_push(b)
                if len(picked) >= 8:
                    return list(picked.keys())[:8]

    # 2) フォールバック：ページ全体
    for a, b in collect_pairs_by_score(soup):
        uniq_push(a); uniq_push(b)
        if len(picked) >= 8:
            return list(picked.keys())[:8]

    return list(picked.keys())[:8]
# ========= 置き換えここまで =========



# ---------------------------
# メイン
# ---------------------------
def build():
    year, prefs = load_config()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUT_DIR / f"best8_autumn_{year}.csv"

    results = []
    for i, item in enumerate(prefs, 1):
        url = (item.get("url") or "").strip()
        pref_full = item.get("name") or ""
        pref = to_pref_name(pref_full)

        if not url:
            print(f"[SKIP] empty url: {pref_full}")
            continue

        print(f"[{i:02d}/{len(prefs)}] {pref} -> {url}")
        soup = fetch_html(url)
        best8 = extract_best8_from_soup(soup)

        # デバッグしやすいようログ
        if len(best8) < 8:
            print(f"  [WARN] {pref}: extracted {len(best8)} teams -> {best8}")

        # 8校まで埋める
        while len(best8) < 8:
            best8.append("")

        results.append({
            "year": year,
            "prefecture": pref,
            "url": url,
            **{f"qf{i}": best8[i-1] for i in range(1, 9)}
        })

        # 負荷をかけすぎないよう少し待つ（0.5～1.2秒）
        time.sleep(0.5 + random.random() * 0.7)

    # CSV出力
    header = ["year", "prefecture", "url"] + [f"qf{i}" for i in range(1, 9)]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in sorted(results, key=lambda x: x["prefecture"]):
            w.writerow(row)

    print(f"[DONE] {len(results)} prefectures -> {out_csv}")


if __name__ == "__main__":
    try:
        build()
    except Exception:
        traceback.print_exc()
        raise
