# -*- coding: utf-8 -*-
"""
collect_player_links.py
- 入力: data/best8_autumn_YYYY.csv（列: year,prefecture,url,qf1..qf8）
- 処理: 大会ページ(url)を開いて、各ベスト8校(qf1..qf8)の『学校ページ(/school/xxxx)』を特定。
        学校ページ内の『選手ページ(/player/xxxx)』リンクを拾い、上位N件(既定2件)を書き出す。
        選手ページが見つからない場合は、学校ページのみログ出力してスキップ。
- 出力: data/players_links.csv（列: year,school_name,player_name,url,grade,position）

前提:
  pip install requests beautifulsoup4 lxml
"""

import csv
import logging
import os
import re
import sys
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; koko-yakyu-collector/1.0)",
    "Accept-Language": "ja,en;q=0.8",
}
SLEEP = 1.2  # アクセス間隔（秒）


def fetch_html(url: str, retry: int = 2, timeout: int = 12) -> Optional[str]:
    for i in range(retry + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            logging.warning(f"status={r.status_code} url={url}")
        except Exception as e:
            logging.warning(f"GET error ({i}/{retry}) {url}: {e}")
        time.sleep(SLEEP * (i + 1))
    return None


def norm_text(s: str) -> str:
    return re.sub(r"\s+", "", s)


def find_school_links_from_tournament(tournament_url: str, school_names: List[str]) -> Dict[str, str]:
    """大会ページからベスト8校の学校ページ(/school/xxxx)リンクを探す"""
    html = fetch_html(tournament_url)
    result: Dict[str, str] = {}
    if not html:
        return result

    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    for school in school_names:
        ns = norm_text(school)
        # 選定ルール:
        #  - アンカーテキストか周辺テキストに学校名が含まれる
        #  - hrefに '/school/' を含む
        best = None
        for a in anchors:
            t = a.get_text(strip=True)
            href = a["href"]
            if "/school/" in href and ns in norm_text(t):
                best = urljoin(tournament_url, href)
                break
        if not best:
            # テキストに学校名が無くても、近傍（親要素）で拾う簡易策
            for a in anchors:
                href = a["href"]
                if "/school/" in href:
                    parent = a.find_parent()
                    pt = parent.get_text(" ", strip=True) if parent else ""
                    if ns in norm_text(pt):
                        best = urljoin(tournament_url, href)
                        break
        if best:
            result[school] = best
    return result


def pick_player_links_from_school(school_url: str, top_n: int = 2) -> List[Dict]:
    """学校ページから選手ページ(/player/xxxx)リンクを上位N件拾う"""
    html = fetch_html(school_url)
    out: List[Dict] = []
    if not html:
        return out

    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)

    # 1) /player/ を含む個別ページへのリンクを候補に
    cand = []
    for a in anchors:
        href = a["href"]
        if "/player/" in href:
            name = a.get_text(strip=True)
            # 学年・ポジションは後でcollect_players.py側で補完するのでここでは空でもOK
            cand.append({
                "player_name": name[:20] if name else "",
                "url": urljoin(school_url, href),
            })

    # 2) 重複排除・シンプルスコア（テキスト長が2〜12程度を優先）
    seen = set()
    ranked = []
    for c in cand:
        key = c["url"]
        if key in seen:
            continue
        seen.add(key)
        name = c["player_name"]
        score = 0
        if 2 <= len(name) <= 12:
            score += 2
        # URL末尾が数字なら個別IDっぽいので加点
        if re.search(r"/player/\d+", c["url"]):
            score += 3
        ranked.append((score, c))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in ranked[:top_n]]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--best8_csv", type=str, required=True)
    parser.add_argument("--out_csv", type=str, default="data/players_links.csv")
    parser.add_argument("--per_school", type=int, default=2, help="各校の上位何名ぶん拾うか")
    parser.add_argument("--log_path", type=str, default="data/logs/collect_player_links.log")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    os.makedirs(os.path.dirname(args.log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(args.log_path, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )

    # best8ファイルを読む
    rows = []
    with open(args.best8_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if int(r.get("year", 0)) != args.year:
                continue
            rows.append(r)
    logging.info(f"best8 rows: {len(rows)}")

    # 出力準備
    out_rows = []
    miss_school = []  # 学校ページ見つからず
    miss_player = []  # 選手ページ見つからず

    for r in rows:
        t_url = (r.get("url") or "").strip()
        if not t_url or "hb-nippon.com" not in t_url:
            logging.info(f"skip: tournament url not hb-nippon: {t_url}")
            continue

        schools = [r.get(f"qf{i}", "").strip() for i in range(1, 9)]
        schools = [s for s in schools if s]
        if not schools:
            continue

        logging.info(f"tournament: {t_url} schools={len(schools)}")
        school_links = find_school_links_from_tournament(t_url, schools)
        time.sleep(SLEEP)

        for s in schools:
            s_url = school_links.get(s)
            if not s_url:
                miss_school.append((s, t_url))
                logging.warning(f"[MISS school] {s} @ {t_url}")
                continue

            players = pick_player_links_from_school(s_url, top_n=args.per_school)
            time.sleep(SLEEP)
            if not players:
                miss_player.append((s, s_url))
                logging.warning(f"[MISS player] {s_url}")
                continue

            for p in players:
                out_rows.append({
                    "year": args.year,
                    "school_name": s,
                    "player_name": p.get("player_name", ""),
                    "url": p["url"],
                    "grade": "",      # 学年・ポジションはcollect_players.py側で抽出
                    "position": "",
                })

    # 出力
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["year","school_name","player_name","url","grade","position"])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    logging.info(f"players_links written: {args.out_csv}")
    logging.info(f"schools without page: {len(miss_school)}")
    logging.info(f"schools without players: {len(miss_player)}")


if __name__ == "__main__":
    main()
