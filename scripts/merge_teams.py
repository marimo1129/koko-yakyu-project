#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
prefectural_best8.csv と area_results.csv を統合して teams.csv を生成します。
使い方:
  python scripts/merge_teams.py
"""
import csv, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

def slug(s):
    s = (s or "").strip().replace(' ', '').replace('　','')
    s = re.sub(r'[^\w\-一-龠ぁ-んァ-ヴー]', '-', s)
    return s

def load_csv(path):
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_csv(path, header, rows):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def build_pref_map():
    m = {}
    for row in load_csv(DATA / "areas.csv"):
        m[row['prefecture']] = {'region': row['region'], 'area': row['area_name']}
    return m

def main():
    pref_rows = load_csv(DATA / "prefectural_best8.csv")
    area_rows = load_csv(DATA / "area_results.csv")
    prefmap = build_pref_map()

    area_index = { (a['prefecture'], a['team_name']) : a for a in area_rows }

    out, seen = [], set()
    for p in pref_rows:
        pref = p['prefecture']; team = p['team_name']
        region = prefmap.get(pref, {}).get('region', '')
        area = prefmap.get(pref, {}).get('area', '')
        a = area_index.get((pref, team), {})
        team_id = f"t-{slug(pref)}-{slug(team)}".lower()
        out.append({
            'team_id': team_id,
            'team_name': team,
            'prefecture': pref,
            'region': region,
            'prefectural_result': p.get('result',''),
            'area': area,
            'area_result': a.get('result',''),
            'area_round': a.get('round_exit',''),
            'note': (p.get('note','') or a.get('note','')),
            'notable_players': ''
        })
        seen.add((pref, team))

    for a in area_rows:
        key = (a['prefecture'], a['team_name'])
        if key in seen: continue
        pref = a['prefecture']; team = a['team_name']
        region = prefmap.get(pref, {}).get('region', '')
        area = prefmap.get(pref, {}).get('area', '')
        team_id = f"t-{slug(pref)}-{slug(team)}".lower()
        out.append({
            'team_id': team_id,
            'team_name': team,
            'prefecture': pref,
            'region': region,
            'prefectural_result': '',
            'area': area,
            'area_result': a.get('result',''),
            'area_round': a.get('round_exit',''),
            'note': a.get('note',''),
            'notable_players': ''
        })

    header = ['team_id','team_name','prefecture','region','prefectural_result','area','area_result','area_round','note','notable_players']
    write_csv(DATA / "teams.csv", header, out)
    print(f"[OK] generated {DATA/'teams.csv'} ({len(out)} rows)")

if __name__ == "__main__":
    main()
