# -*- coding: utf-8 -*-
"""
hb_tournaments.yml（autumn_pref）に列挙した都道府県秋季大会ページを基準に、
data/matches.csv を集計し、都道府県ごとの「ベスト8（準々決勝出場校）」を
data/best8_autumn_YYYY.csv へ出力する。

前提:
- data/matches.csv は build済み（列: date,round,team_left,score,team_right,source）
- data/hb_tournaments.yml に "autumn_pref" があり、各要素が {url, name} を持つ
- ワークフロー入力 koko_year があればその年、なければ hb_tournaments.yml の year、さらに無ければ現在年
"""

from __future__ import annotations
import os
import re
import csv
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import yaml  # 要: pyyaml

# 入出力パス
CONF_YAML = Path("data/hb_tournaments.yml")
IN_MATCH = Path("data/matches.csv")
OUT_CSV_TPL = "data/best8_autumn_{year}.csv"


# -------------------------
# ユーティリティ
# -------------------------
def normalize_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    # 不要な末尾スラッシュを統一
    while u.endswith("/"):
        u = u[:-1]
    return u


def parse_pref_from_name(name: str) -> str:
    """
    例: '東京都 秋季大会' -> '東京都'
        '北海道 秋季大会' -> '北海道'
        名前がそのまま県名のみの場合はそのまま返す
    """
    name = (name or "").strip()
    m = re.match(r"(.+?)\s*秋季大会", name)
    return m.group(1) if m else name


def prefer_year() -> int:
    """
    優先順位:
      1) 環境変数 koko_year（Actions の input passing）
      2) hb_tournaments.yml の year
      3) 現在年
    """
    if os.getenv("koko_year"):
        return int(os.getenv("koko_year"))
    if CONF_YAML.exists():
        try:
            d = yaml.safe_load(CONF_YAML.read_text(encoding="utf-8")) or {}
            if "year" in d:
                return int(d["year"])
        except Exception:
            pass
    return datetime.now().year


# -------------------------
# 設定の読込
# -------------------------
def load_autumn_config() -> Tuple[int, List[Dict[str, str]]]:
    """
    hb_tournaments.yml を読み、(year, autumn_prefリスト) を返す。
    キー名の揺れ (autumn_pref / autumn_prefs) も吸収。
    """
    year = prefer_year()
    if not CONF_YAML.exists():
        print(f"[WARN] config not found: {CONF_YAML}")
        return year, []

    data = yaml.safe_load(CONF_YAML.read_text(encoding="utf-8")) or {}
    prefs = data.get("autumn_pref")
    if prefs is None:
        prefs = data.get("autumn_prefs")  # 旧名でも拾う

    # URL空やNoneは除外
    cleaned = []
    for p in prefs or []:
        url = normalize_url((p or {}).get("url", ""))
        name = (p or {}).get("name", "")
        if url:
            cleaned.append({"url": url, "name": name})

    print(f"[DEBUG] year={year} / autumn_pref count={len(cleaned)}")
    return year, cleaned


def build_url_to_pref_map(prefs: List[Dict[str, str]]) -> Dict[str, str]:
    """
    {大会URL -> 都道府県名} の辞書を作る。
    """
    m = {}
    for p in prefs:
        url = normalize_url(p["url"])
        pref = parse_pref_from_name(p.get("name", ""))
        if url:
            m[url] = pref
    return m


# -------------------------
# 集計ロジック
# -------------------------
def round_rank_value(round_label: str) -> int:
    """
    回戦を整数順位に変換（比較用）
    高いほど深いステージ。決勝を99、準決勝98、準々決勝97。
    '4回戦' などは該当数字。判別できない場合は0。
    """
    s = (round_label or "")
    if "決勝" in s and "準" not in s:
        return 99
    if "準決勝" in s:
        return 98
    if "準々決勝" in s or "ベスト8" in s:
        return 97
    m = re.search(r"(\d+)回戦", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return 0


def pick_best8_from_rows(rows: List[Dict[str, str]]) -> List[str]:
    """
    大会1つ分（その県）の試合 rows からベスト8（準々決勝出場校）を抽出。
    - まず準々決勝に登場した学校（team_left / team_right）を集める
    - なければ '4回戦' 以上を準々決勝相当として採用
    - それでも足りない場合は 準決勝 / 決勝 登場校で補完
    """
    # 準々決勝マッチ
    qf_rows = [x for x in rows if round_rank_value(x.get("round", "")) >= 97]
    teams = OrderedDict()  # 重複排除＆順序保持

    def push(lhs: str, rhs: str):
        if lhs:
            teams[lhs] = True
        if rhs:
            teams[rhs] = True

    if qf_rows:
        for x in qf_rows:
            push(x.get("team_left", ""), x.get("team_right", ""))
    else:
        # 4回戦以上を拾う
        r4_plus = [x for x in rows if round_rank_value(x.get("round", "")) >= 4]
        for x in r4_plus:
            push(x.get("team_left", ""), x.get("team_right", ""))

    if len(teams) < 8:
        # 準決勝・決勝で補完
        semi = [x for x in rows if "準決勝" in (x.get("round", "") or "")]
        fin = [x for x in rows if ("決勝" in (x.get("round", "") or "")) and ("準" not in (x.get("round", "") or ""))]
        for x in semi + fin:
            push(x.get("team_left", ""), x.get("team_right", ""))

    best8 = list(teams.keys())[:8]
    # 8校に満たない場合は空欄で揃える（UI側で扱いやすい）
    while len(best8) < 8:
        best8.append("")
    return best8


# -------------------------
# メイン
# -------------------------
def main():
    year, prefs = load_autumn_config()
    out_path = Path(OUT_CSV_TPL.format(year=year))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not IN_MATCH.exists():
        print(f"[ERROR] not found: {IN_MATCH}")
        # 空のヘッダだけ出しておく（UI 404回避）
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["year", "prefecture", "url"] + [f"qf{i}" for i in range(1, 9)])
        return

    url2pref = build_url_to_pref_map(prefs)
    if not url2pref:
        print("[WARN] autumn_pref not found in hb_tournaments.yml")
        # 空のヘッダだけ出力
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["year", "prefecture", "url"] + [f"qf{i}" for i in range(1, 9)])
        return

    # 読み込み
    # 行 -> 各県（大会URL）にグルーピング。source が トーナメントURLで始まれば同一大会とみなす。
    rows_by_url = defaultdict(list)
    urls = list(url2pref.keys())

    with IN_MATCH.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = normalize_url(row.get("source", ""))
            if not src:
                continue
            # どの県URLの配下か判定
            for u in urls:
                if src.startswith(u):
                    rows_by_url[u].append(row)
                    break

    results = []
    for u in urls:
        pref = url2pref[u]
        rows = rows_by_url.get(u, [])
        if not rows:
            print(f"[WARN] no rows for {pref} ({u})")
            best8 = [""] * 8
        else:
            best8 = pick_best8_from_rows(rows)
            if best8.count("") > 0:
                print(f"[INFO] {pref}: collected {8 - best8.count('')} of 8")

        results.append(
            {
                "year": year,
                "prefecture": pref,
                "url": u,
                **{f"qf{i}": best8[i - 1] for i in range(1, 9)},
            }
        )

    # 出力
    header = ["year", "prefecture", "url"] + [f"qf{i}" for i in range(1, 9)]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in sorted(results, key=lambda x: x["prefecture"]):
            w.writerow(r)

    print(f"[DONE] {len(results)} prefectures -> {out_path}")


if __name__ == "__main__":
    main()
