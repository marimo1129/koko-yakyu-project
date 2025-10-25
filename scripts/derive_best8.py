# -*- coding: utf-8 -*-
"""
matches.csv から都道府県別の秋季ベスト8を抽出して data/prefectural_best8.csv を更新する。
前提:
  - data/matches.csv     … date, round, team_left, score, team_right, source
  - data/hb_tournaments.yml  … autumn_pref に {url, name} が並ぶ（「東京都 秋季大会」など）
出力:
  - data/prefectural_best8.csv … year, prefecture, team, source_url
"""

import csv
import os
import re
import sys
from datetime import datetime

import yaml

MATCHES_CSV = "data/matches.csv"
YAML_PATH = "data/hb_tournaments.yml"
OUT_CSV = "data/prefectural_best8.csv"

def load_autumn_pref_map():
    """
    hb_tournaments.yml の autumn_pref から
    { base_url: prefecture_name } を作る。
    prefecture_name は name の「〇〇県 秋季大会」などから「〇〇県/〇〇」部分を抜く。
    """
    if not os.path.exists(YAML_PATH):
        print(f"[ERROR] YAML not found: {YAML_PATH}")
        return {}

    with open(YAML_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    pref_map = {}
    for item in (cfg.get("autumn_pref") or []):
        url = (item.get("url") or "").strip()
        name = (item.get("name") or "").strip()
        if not url:
            continue
        # URL正規化（末尾スラッシュを除去）
        base = re.sub(r"/+$", "", url)
        # name から「秋季大会」「秋季」などを除いて都道府県名だけに寄せる
        # 例: 「東京都 秋季大会」→「東京都」
        pref = re.sub(r"(秋季大会|秋季)\s*$", "", name).strip()
        # もし「（」以降の注釈があれば削る
        pref = re.split(r"[（(]", pref)[0].strip()
        pref_map[base] = pref

    return pref_map

def derive_best8(year: int):
    if not os.path.exists(MATCHES_CSV):
        print(f"[ERROR] matches not found: {MATCHES_CSV}")
        return []

    pref_map = load_autumn_pref_map()
    if not pref_map:
        print("[WARN] autumn_pref from YAML is empty. Nothing to do.")
        return []

    # 準々決勝を見つけ、出場校をベスト8候補にする
    best8_rows = []  # (year, pref, team, src)

    with open(MATCHES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            round_label = (r.get("round") or "").strip()
            src = (r.get("source") or "").strip()
            if "準々決勝" not in round_label:
                continue

            # src と YAML の URL を突き合わせて都道府県を特定
            # tournaments/1234 のような基底URLまでで比較できるよう正規化
            base_src = re.sub(r"/+$", "", src)
            # src がクエリ等を持つ可能性は低いが余分を落としておく
            base_src = re.split(r"[?#]", base_src)[0]

            # YAML の URL と startswith/完全一致 いずれでも拾えるようにする
            prefecture = None
            for y_url, pref in pref_map.items():
                if base_src.startswith(y_url):
                    prefecture = pref
                    break
            if not prefecture:
                continue  # 地区大会/神宮/センバツなどはスキップ

            team_left = r.get("team_left", "").strip()
            team_right = r.get("team_right", "").strip()
            if team_left:
                best8_rows.append((year, prefecture, team_left, src))
            if team_right:
                best8_rows.append((year, prefecture, team_right, src))

    # 重複除去（pref + team で一意）
    seen = set()
    deduped = []
    for y, p, t, s in best8_rows:
        key = (y, p, t)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((y, p, t, s))

    return deduped

def write_best8(rows):
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    header = ["year", "prefecture", "team", "source_url"]
    exists = os.path.exists(OUT_CSV)

    # 年+都道府県が同じ古い行は落として最新で上書きしたい場合は、
    # ここで既存行を読み込んでフィルタリングしてから追記する方式にしてもOK。
    # まずはシンプルに追記で回します（同一行は処理側で重複除去できるようにしておきます）。

    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

def main():
    year = int(os.getenv("KOKO_YEAR", datetime.now().year))
    print(f"[INFO] derive_best8 for year={year}")
    rows = derive_best8(year)
    print(f"[INFO] best8 rows = {len(rows)}")
    write_best8(rows)
    print(f"[DONE] write -> {OUT_CSV}")

if __name__ == "__main__":
    main()
