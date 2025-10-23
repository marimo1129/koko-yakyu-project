# scripts/bootstrap_best8.py
# -*- coding: utf-8 -*-
"""
全国版の下地（ベスト8の“枠”）を一気に作るスクリプト。
- data/teams.csv : 都道府県ごとにベスト8（優勝, 準優勝, ベスト4×2, ベスト8×4）の空行を生成
- data/area_results.csv : 地区大会（各地区のベスト8枠）の空行を生成
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

# 地域マップ（都道府県→地域）
REGION_MAP = {
    "北海道":"北海道","青森":"東北","岩手":"東北","宮城":"東北","秋田":"東北","山形":"東北","福島":"東北",
    "茨城":"関東","栃木":"関東","群馬":"関東","埼玉":"関東","千葉":"関東","東京":"関東","神奈川":"関東",
    "新潟":"北信越","富山":"北信越","石川":"北信越","福井":"北信越","長野":"北信越",
    "岐阜":"東海","静岡":"東海","愛知":"東海","三重":"東海",
    "滋賀":"近畿","京都":"近畿","大阪":"近畿","兵庫":"近畿","奈良":"近畿","和歌山":"近畿",
    "鳥取":"中国","島根":"中国","岡山":"中国","広島":"中国","山口":"中国",
    "徳島":"四国","香川":"四国","愛媛":"四国","高知":"四国",
    "福岡":"九州","佐賀":"九州","長崎":"九州","熊本":"九州","大分":"九州","宮崎":"九州","鹿児島":"九州","沖縄":"九州",
}

PREFS = list(REGION_MAP.keys())
AREAS = ["北海道","東北","関東","北信越","東海","近畿","中国","四国","九州"]

# ---------------------------
# teams.csv を生成
# ---------------------------
teams_csv = DATA / "teams.csv"
teams_headers = [
    "prefecture","team_name","result","prefectural_rank","seed","region","note"
]
RESULT_SLOTS = [
    ("優勝", 1), ("準優勝", 2),
    ("ベスト4", 3), ("ベスト4", 4),
    ("ベスト8", 5), ("ベスト8", 6), ("ベスト8", 7), ("ベスト8", 8),
]

with teams_csv.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=teams_headers)
    w.writeheader()
    for pref, region in REGION_MAP.items():
        for result_label, rank in RESULT_SLOTS:
            w.writerow({
                "prefecture": pref,
                "team_name": "",  # ←あとで実在高校名を入力
                "result": result_label,
                "prefectural_rank": rank,
                "seed": "",
                "region": region,
                "note": "",
            })

# ---------------------------
# area_results.csv を生成
# ---------------------------
area_csv = DATA / "area_results.csv"
area_headers = ["area","team_name","prefecture","result","round_exit","note"]

with area_csv.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=area_headers)
    w.writeheader()
    for area in AREAS:
        for i in range(1, 9):
            w.writerow({
                "area": area,
                "team_name": "",
                "prefecture": "",
                "result": "",
                "round_exit": "",
                "note": "",
            })

print("✅ Generated: data/teams.csv, data/area_results.csv")
