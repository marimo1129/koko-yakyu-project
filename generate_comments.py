# -*- coding: utf-8 -*-
"""
players.csv の comment_ai を自動生成（API不要）
- data/players.csv を読み込み
- comment_ai が空の選手に短評を自動生成
- is_alumni を true/false に正規化
- CSV を上書き保存（UTF-8 LF）
"""

from __future__ import annotations
import csv, re, pathlib

import os
ROOT = pathlib.Path(os.getcwd())
CSV_PATH = ROOT / "data" / "players.csv"

HEADER = [
    "player_id","player_name","grade","position","team_id","team_name","prefecture","region",
    "is_alumni","graduation_year","dest_type","dest_name","youtube_url","comment","comment_ai"
]

def norm_bool(v: str) -> str:
    s = (v or "").strip().lower()
    if s in ("true","t","1","yes","y","卒","alumni"):
        return "true"
    return "false"

def grade_label(g: str) -> str:
    s = (g or "").strip()
    if s in ("卒","3","３"): return "3年（卒）" if s == "卒" else "3年"
    if s in ("2","２"): return "2年"
    if s in ("1","１"): return "1年"
    return s or "—"

def pos_label(p: str) -> str:
    return (p or "").strip() or "—"

def make_ai_comment(row: dict) -> str:
    name = (row.get("player_name") or "").strip()
    grade = grade_label(row.get("grade"))
    pos = pos_label(row.get("position"))
    team = (row.get("team_name") or "").strip()
    pref = (row.get("prefecture") or "").strip()
    dest_type = (row.get("dest_type") or "").strip()
    dest_name = (row.get("dest_name") or "").strip()

    lines = []
    head = f"{name}は{pos}。{team}（{pref}）所属。"
    if "卒" in grade or row.get("is_alumni","").lower() == "true":
        head = f"{name}は{pos}。{team}（{pref}）出身。"
    lines.append(head)

    if "投" in pos:
        lines.append("直球の質と制球が持ち味。緩急とコースで勝負するタイプ。")
    elif "捕" in pos:
        lines.append("強肩とリード面で安定感。守備でチームを引き締める。")
    elif "内" in pos:
        lines.append("守備範囲が広くハンドリングが柔らかい。状況判断に長ける。")
    elif "外" in pos:
        lines.append("走攻守のバランスが良く、長打と機動力の両面で貢献。")
    else:
        lines.append("総合力が高く、チームに安定感をもたらすタイプ。")

    if norm_bool(row.get("is_alumni")) == "true":
        if dest_type or dest_name:
            lines.append(f"進路は{dest_type or '—'}{('・'+dest_name) if dest_name else ''}。")
        else:
            lines.append("卒業後の進路にも注目。")
    else:
        if re.search(r"[12]年", grade) or grade in ("1年","2年","1","2"):
            lines.append("安定感のあるプレー。今後の伸びしろにも期待。")
        else:
            lines.append("安定したパフォーマンスでチームを牽引。")

    return " ".join(lines)

def read_rows():
    with CSV_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows, reader.fieldnames

def write_rows(rows):
    with CSV_PATH.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        for r in rows:
            for h in HEADER: r.setdefault(h, "")
            writer.writerow({k: r.get(k, "") for k in HEADER})

def main():
    print(f"Load: {CSV_PATH}")
    rows, _ = read_rows()
    updated = 0
    for r in rows:
        r["is_alumni"] = norm_bool(r.get("is_alumni",""))
        if not (r.get("comment_ai") or "").strip():
            r["comment_ai"] = make_ai_comment(r)
            updated += 1
    write_rows(rows)
    print(f"Done. updated={updated}, total={len(rows)}")

if __name__ == "__main__":
    main()
