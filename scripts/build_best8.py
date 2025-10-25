# -*- coding: utf-8 -*-
"""
2025秋季の大会ページ（hb_tournaments.yml）に対応する data/matches.csv を集計し、
各都道府県大会の「ベスト8（準々決勝出場校）」を data/best8_autumn_YYYY.csv に出力する。

前提:
- data/matches.csv は build済み（列: date,round,team_left,score,team_right,source）
- data/hb_tournaments.yml に "autumn_pref" があり、url と name を持つ
"""

import csv
import os
import re
import yaml
from collections import defaultdict, OrderedDict
from datetime import datetime

IN_MATCH = "data/matches.csv"
CONF_YAML = "data/hb_tournaments.yml"
OUT_CSV  = "data/best8_autumn_{year}.csv"

def load_autumn_pref_map():
    with open(CONF_YAML, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    url2pref = {}
    for item in cfg.get("autumn_pref", []):
        url = (item.get("url") or "").strip().rstrip("/")
        name = (item.get("name") or "").strip()
        # 例: "東京都 秋季大会" -> "東京都"
        m = re.match(r"(.+?)\s*秋季大会", name)
        pref = m.group(1) if m else name
        if url:
            url2pref[url] = pref
    return url2pref

def parse_score_winner(score, left, right):
    m = re.match(r"\s*(\d+)\s*-\s*(\d+)\s*", (score or ""))
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    return left if a > b else right

def main():
    year = int(os.getenv("KOKO_YEAR", datetime.now().year))
    out_path = OUT_CSV.format(year=year)

    if not os.path.exists(IN_MATCH):
        print(f"[ERROR] not found: {IN_MATCH}")
        return

    url2pref = load_autumn_pref_map()
    targets = set(url2pref.keys())
    if not targets:
        print("[WARN] autumn_pref not found in hb_tournaments.yml")
        return

    # source(URL) -> rows
    rows_by_url = defaultdict(list)
    with open(IN_MATCH, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = (row.get("source") or "").strip().rstrip("/")
            if src in targets:
                rows_by_url[src].append(row)

    # URLごとにベスト8候補を抽出
    results = []
    for url, rows in rows_by_url.items():
        pref = url2pref.get(url, "")
        # 準々決勝を優先（"準々決勝" or "ベスト8" の文字を含む）
        qf = [x for x in rows if re.search(r"(準々決勝|ベスト8)", x.get("round",""))]

        teams = OrderedDict()  # 重複排除＋順序保持
        def push(left, right):
            if left:  teams[left]  = True
            if right: teams[right] = True

        if qf:
            for x in qf:
                push(x.get("team_left"), x.get("team_right"))
        else:
            # フォールバック: "4回戦" を準々決勝相当として扱う（ない大会もある）
            r4 = [x for x in rows if re.search(r"(\d+)回戦", x.get("round","")) and int(re.search(r"(\d+)回戦", x["round"]).group(1)) >= 4]
            for x in r4:
                push(x.get("team_left"), x.get("team_right"))

        # 最後の保険: まだ8校未満なら準決勝/決勝の出場校も加えて補完
        if len(teams) < 8:
            semi_final = [x for x in rows if "準決勝" in (x.get("round",""))]
            final = [x for x in rows if "決勝" in (x.get("round",""))]
            for x in semi_final + final:
                push(x.get("team_left"), x.get("team_right"))

        best8 = list(teams.keys())[:8]
        if len(best8) < 8:
            print(f"[WARN] {pref} ベスト8が8校に満たない（{len(best8)}校）。大会ページの表記が特殊かも: {url}")

        # プレースホルダを8校まで埋める（UI側で見やすくするため）
        while len(best8) < 8:
            best8.append("")

        results.append({
            "year": year,
            "prefecture": pref,
            "url": url,
            **{f"qf{i+1}": best8[i] for i in range(8)}
        })

    # 出力
    os.makedirs("data", exist_ok=True)
    header = ["year","prefecture","url"] + [f"qf{i}" for i in range(1,9)]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in sorted(results, key=lambda x: x["prefecture"]):
            w.writerow(r)

    print(f"[DONE] {len(results)} prefectures -> {out_path}")

if __name__ == "__main__":
    main()
