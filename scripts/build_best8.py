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


# ---------------------------
# 解析ヘルパ
# ---------------------------
SCORE_RE = re.compile(r"\b(\d+)\s*[-－–]\s*(\d+)\b")

def norm(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()

def uniq_push(container: "OrderedDict[str, bool]", name: str):
    name = norm(name)
    if not name:
        return
    # よくある不要語排除（必要に応じて調整）
    ban = ["試合詳細", "大会", "ブロック", "対戦", "代表"]
    if any(x in name for x in ban):
        return
    container[name] = True

def collect_names_in_block(block) -> List[str]:
    """
    ブロック領域（セクション）から
    aタグなどの見出しに含まれるチーム名っぽい文字列を抽出
    """
    ret = OrderedDict()
    # aタグ優先
    for a in block.find_all("a"):
        text = norm(a.get_text())
        if SCORE_RE.search(text):
            # "A 4-3 B" の a に score が混ざることがあるので飛ばす
            continue
        if len(text) >= 2 and len(text) <= 20:
            uniq_push(ret, text)

    # li / p / td 等のテキストも拾う（scoreが含まれる行はペア抽出を別でやる）
    for tag in block.find_all(["li", "p", "td", "div", "span"]):
        text = norm(tag.get_text())
        if not text:
            continue
        if SCORE_RE.search(text):
            continue
        # 箇条書きに学校名だけ並んでいるケースを想定
        for piece in re.split(r"[、,\s]", text):
            piece = norm(piece)
            if 1 < len(piece) <= 20 and not SCORE_RE.search(piece):
                uniq_push(ret, piece)

    return list(ret.keys())


def collect_pairs_by_score(block) -> list[tuple[str, str]]:
    """
    ブロック内から「A 4-3 B」のようなスコア行を見つけ、左右のチーム名を返す。
    ナビやリンク類を拾わないため “スコア行だけ” をソースにする。
    """
    pairs: list[tuple[str, str]] = []
    for tag in block.find_all(["li", "p", "td", "div", "span", "a"]):
        text = norm(tag.get_text())
        m = SCORE_RE.search(text)
        if not m:
            continue
        # スコアの左右をそのまま使う（余計な語を落とす程度の軽い整形）
        left = text[:m.start()].strip("　 \t:|（）()[]<>")
        right = text[m.end():].strip("　 \t:|（）()[]<>")

        # よくある区切りで余分を削る
        left = re.split(r"[、,\s/・]", left)[-1] if left else ""
        right = re.split(r"[、,\s/・]", right)[0] if right else ""

        # 片方でも空なら捨てる
        if left and right:
            pairs.append((left, right))
    return pairs

def extract_best8_from_soup(soup: BeautifulSoup) -> list[str]:
    """
    準々決勝/ベスト8/4回戦の見出しブロックがあれば、まずそこから
    スコア行だけでチーム名を集める。なければページ全体からスコア行だけで集める。
    """
    HEAD_PAT = re.compile(r"(準々決勝|ベスト8|ベスト８|4回戦|４回戦)")
    picked: "OrderedDict[str, bool]" = OrderedDict()

    def uniq_push(name: str):
        name = norm(name)
        if not name:
            return
        # ナビ等の誤検知をさらに防ぐための簡易フィルタ
        ban = {"高校野球ドットコム","tiktok","TikTok","Facebook","Instagram","Youtube","YouTube",
               "新着記事","選手名鑑","チーム一覧","大会ページ","大会","ブロック","対戦","代表"}
        if name in ban or len(name) > 20:
            return
        picked[name] = True

    # 1) 準々決勝/ベスト8/4回戦のセクションがあれば優先
    for h in soup.find_all(re.compile(r"^h[1-6]$")):
        if HEAD_PAT.search(norm(h.get_text())):
            # 見出しの次セクションを1ブロックとして抽出
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

    # 2) フォールバック：ページ全体からスコアで拾う
    for a, b in collect_pairs_by_score(soup):
        uniq_push(a); uniq_push(b)
        if len(picked) >= 8:
            return list(picked.keys())[:8]

    return list(picked.keys())[:8]

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
