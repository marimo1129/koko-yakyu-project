# -*- coding: utf-8 -*-
"""
prefectural_best8.csv と（任意の）追加入力 data/watchlist_extra.yml を合体して
data/watch_teams.csv を更新する。
出力列: year, prefecture, team, source_url, tag
  - tag は 'best8' または 'manual' を想定
"""

import csv
import os
from datetime import datetime

import yaml

BEST8_CSV = "data/prefectural_best8.csv"
EXTRA_YAML = "data/watchlist_extra.yml"  # 任意。なければ読み飛ばし
OUT_CSV = "data/watch_teams.csv"

def read_best8(year):
    rows = []
    if not os.path.exists(BEST8_CSV):
        return rows
    with open(BEST8_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                y = int(row.get("year", "0"))
            except ValueError:
                continue
            if y != year:
                continue
            rows.append({
                "year": y,
                "prefecture": row.get("prefecture", "").strip(),
                "team": row.get("team", "").strip(),
                "source_url": row.get("source_url", "").strip(),
                "tag": "best8",
            })
    return rows

def read_extra(year):
    if not os.path.exists(EXTRA_YAML):
        return []
    with open(EXTRA_YAML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    out = []
    for item in data.get("teams", []):
        y = int(item.get("year", year))
        if y != year:
            continue
        out.append({
            "year": y,
            "prefecture": (item.get("prefecture") or "").strip(),
            "team": (item.get("team") or "").strip(),
            "source_url": (item.get("source_url") or "").strip(),
            "tag": "manual",
        })
    return out

def write_watchlist(rows):
    # 重複除去（year, pref, team）
    seen = set()
    dedup = []
    for r in rows:
        key = (r["year"], r["prefecture"], r["team"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    header = ["year", "prefecture", "team", "source_url", "tag"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in dedup:
            w.writerow(r)

def main():
    year = int(os.getenv("KOKO_YEAR", datetime.now().year))
    best8 = read_best8(year)
    extra = read_extra(year)
    rows = best8 + extra
    write_watchlist(rows)
    print(f"[DONE] watch_teams rows: {len(rows)} -> {OUT_CSV}")

if __name__ == "__main__":
    main()
