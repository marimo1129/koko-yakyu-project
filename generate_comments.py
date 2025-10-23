#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, argparse

def synthesize_comment(row: dict) -> str:
    name = row.get('player_name') or '選手'
    pos  = row.get('position') or '選手'
    team = row.get('team_name') or ''
    pref = row.get('prefecture') or ''
    region = row.get('region') or ''
    grad = (row.get('graduation_year') or '').strip()
    dest_t = row.get('dest_type') or ''
    dest_n = row.get('dest_name') or ''

    grad_phrase = f"{grad}年卒" if grad else (f"{row['grade']}在籍" if (row.get('grade') or '').strip() else '')
    if dest_t and dest_n: dest_phrase = f"{dest_t}の{dest_n}へ進む見込み"
    elif dest_t: dest_phrase = f"{dest_t}進路"
    elif dest_n: dest_phrase = f"{dest_n}進路"
    else: dest_phrase = ''

    team_area = '・'.join([v for v in [team, pref or region] if v])
    belong = f"{team_area}所属" if team_area else ''

    parts = [f"{name}は{pos}。"]
    if belong: parts.append(f"{belong}で、")
    parts.append("安定感のあるプレーを見せる。")
    if grad_phrase: parts.append(f"{grad_phrase}で")
    if dest_phrase: parts.append(f"{dest_phrase}。")
    else: parts.append("今後の成長にも期待がかかる。")
    return ''.join(parts).replace('。。','。').replace('、、','、').replace('、。','。')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='infile', required=True)
    ap.add_argument('--out', dest='outfile', required=True)
    ap.add_argument('--overwrite', action='store_true')
    args = ap.parse_args()

    with open(args.infile, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = list(reader)

    if 'comment' not in header:
        header.append('comment')

    for row in rows:
        if (row.get('comment') or '').strip() and not args.overwrite:
            continue
        row['comment'] = synthesize_comment(row)

    with open(args.outfile, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == '__main__':
    main()
