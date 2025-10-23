# -*- coding: utf-8 -*-
"""
vk.sportsbull.jp の大会ページを巡回し、試合スコアを収集して CSV 出力するスクリプト
出力:
  - data/matches.csv            … 1試合=1行（対戦履歴）
  - data/tournament_summary.csv … 大会ごとの優勝/準優勝/ベスト4
実行:
  $ python scripts/collect_scores.py
"""
import csv
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ================================
# 設定
# ================================
YEAR = datetime.now().year  # 例: 2025
BASE = "https://vk.sportsbull.jp"
OUT_MATCHES = "data/matches.csv"
OUT_SUMMARY = "data/tournament_summary.csv"
UA = {"User-Agent": f"koko-yakyu-project-bot/{YEAR}"}
SLEEP_SEC = 0.7  # アクセス間隔（優しめ）

# ----------------
# 収集対象セット
#   秋季/春季（各都道府県）
#   神宮/センバツ/夏（全国）
#   ※ IDレンジは既存コードの方針を踏襲。必要に応じて調整してください。
# ----------------
TARGETS = [
    ("秋季都道府県大会",  range(610, 656)),  # 例
    ("春季都道府県大会",  range(710, 756)),  # 例
    ("明治神宮大会",      [560]),            # 全国
    ("選抜(センバツ)",     [701]),            # 全国
    ("夏の甲子園",        [1001]),           # 全国
]

# 既存の PREF_MAP を流用（例）
PREF_MAP: Dict[int, str] = {
    610:"北海道",611:"青森",612:"岩手",613:"宮城",614:"秋田",615:"山形",616:"福島",
    617:"茨城",618:"栃木",619:"群馬",620:"埼玉",621:"千葉",622:"東京",623:"神奈川",
    624:"新潟",625:"富山",626:"石川",627:"福井",628:"山梨",629:"長野",
    630:"岐阜",631:"静岡",632:"愛知",633:"三重",
    634:"滋賀",635:"京都",636:"大阪",637:"兵庫",638:"奈良",639:"和歌山",
    640:"鳥取",641:"島根",642:"岡山",643:"広島",644:"山口",
    645:"徳島",646:"香川",647:"愛媛",648:"高知",
    649:"福岡",650:"佐賀",651:"長崎",652:"熊本",653:"大分",654:"宮崎",655:"鹿児島",
    656:"沖縄"
}

# ================================
# 型
# ================================
@dataclass
class MatchRow:
    year: int
    tournament: str         # 例: 秋季都道府県大会/明治神宮大会…
    tournament_id: int      # 収集元ID
    stage: str              # 例: 準々決勝/決勝 など（不明時は空）
    round_no: Optional[int] # 数字で取れたら
    date: str               # YYYY-MM-DD（取れなければ空）
    region: str             # 全国/地方
    prefecture: str         # 都道府県名（全国大会は空）
    team_a: str
    team_b: str
    score_a: Optional[int]
    score_b: Optional[int]
    winner: str
    venue: str              # 取れたら
    source_url: str

@dataclass
class SummaryRow:
    year: int
    tournament: str
    tournament_id: int
    prefecture: str       # 全国大会は空
    champion: str
    runner_up: str
    best4_a: str
    best4_b: str

# ================================
# ユーティリティ
# ================================
SCORE_RE = re.compile(r"(\d+)\s*[-－–]\s*(\d+)")

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def int_or_none(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None

# ================================
# 解析（サイトのマークアップは変わる可能性あり）
# セレクタは後で微調整しやすいよう一箇所に寄せています。
# ================================
def parse_listing_page(tournament_id: int) -> List[str]:
    """
    大会トップから各試合詳細ページURLの一覧を返す
    例: https://vk.sportsbull.jp/koshien/game/{YEAR}/{tournament_id}/
    """
    url = f"{BASE}/koshien/game/{YEAR}/{tournament_id}/"
    soup = get_soup(url)

    # ★重要: 下記セレクタは構造に合わせて調整してください
    links = []
    for a in soup.select("a[href*='/koshien/game/'][href*='/detail/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = BASE + href
        if f"/koshien/game/{YEAR}/" in href:
            links.append(href)
    # 代替（カード内リンクから集める）
    if not links:
        for a in soup.select("a[href*='/koshien/game/']"):
            href = a.get("href", "")
            if "/detail/" in href:
                if href.startswith("/"):
                    href = BASE + href
                links.append(href)
    return sorted(set(links))

def parse_detail_page(url: str, tournament_name: str, tournament_id: int) -> Tuple[MatchRow, Optional[str]]:
    """
    試合詳細ページから1試合分のレコードを構築。
    ついでに「この大会のステージ情報（決勝/準決勝など）」を拾い、要約判定に使う。
    """
    soup = get_soup(url)

    # チーム名
    team_nodes = soup.select(".team-name, .c-teamName, .team__name")
    teams = [norm(t.get_text()) for t in team_nodes if norm(t.get_text())]
    team_a = teams[0] if len(teams) > 0 else ""
    team_b = teams[1] if len(teams) > 1 else ""

    # スコア
    score_text = ""
    for node in soup.select(".score, .c-score, .match__score, .game-score"):
        t = norm(node.get_text())
        if SCORE_RE.search(t):
            score_text = t
            break
    m = SCORE_RE.search(score_text)
    s_a, s_b = (int(m.group(1)), int(m.group(2))) if m else (None, None)

    # 勝者
    winner = team_a
    if s_a is not None and s_b is not None:
        if s_b > s_a:
            winner = team_b

    # 試合日
    date_txt = ""
    date_node = soup.select_one(".match-date, .c-date, time, .game-date")
    if date_node:
        date_txt = norm(date_node.get_text())
        # YYYY/MM/DD or YYYY年M月D日 → YYYY-MM-DD に寄せる（ざっくり）
        date_txt = date_txt.replace("年", "-").replace("月", "-").replace("日", "")
        date_txt = re.sub(r"[./]", "-", date_txt)
        date_txt = re.sub(r"[^0-9\-]", "", date_txt)
        if len(date_txt) >= 8 and date_txt.count("-") >= 2:
            ymd = date_txt.split("-")[:3]
            ymd = [p.zfill(2) if i>0 else p for i,p in enumerate(ymd)]
            date_txt = "-".join(ymd)
        else:
            date_txt = ""

    # ステージ（決勝/準決勝など）
    stage = ""
    stage_node = soup.select_one(".round, .stage, .game-round, .c-round")
    if stage_node:
        stage = norm(stage_node.get_text())
    round_no = int_or_none(re.search(r"(\d+)", stage).group(1)) if re.search(r"(\d+)", stage) else None

    # 会場
    venue = ""
    v = soup.select_one(".venue, .c-venue, .game-venue")
    if v:
        venue = norm(v.get_text())

    # 都道府県名（都道府県大会はURLやパンくずから推測）
    prefecture = ""
    # パンくずのどこかに都道府県名が入ることが多い
    bc = soup.select(".breadcrumb li, .pankuzu li, .bread-crumb li, nav[aria-label='breadcrumb'] li")
    if bc:
        crumbs = " ".join(norm(li.get_text()) for li in bc)
        for _id, name in PREF_MAP.items():
            if name in crumbs:
                prefecture = name
                break

    # 全国/地方
    region = "全国" if prefecture == "" else "地方"

    row = MatchRow(
        year=YEAR,
        tournament=tournament_name,
        tournament_id=tournament_id,
        stage=stage,
        round_no=round_no,
        date=date_txt,
        region=region,
        prefecture=prefecture,
        team_a=team_a,
        team_b=team_b,
        score_a=s_a,
        score_b=s_b,
        winner=winner,
        venue=venue,
        source_url=url,
    )
    return row, stage or None

# ================================
# サマリー算出（優勝/準優勝/ベスト4）
# ================================
def summarize_tournament(tournament_name: str, tournament_id: int, matches: List[MatchRow]) -> List[SummaryRow]:
    """
    簡易ロジック:
      - 「決勝」ステージの試合 → 勝者=優勝、敗者=準優勝
      - 「準決勝」ステージの試合 → 勝者2校で決勝へ、敗者2校=ベスト4
      ※ ステージ名の取り方はサイトの表記に依存。取り切れない場合は空欄になる。
    """
    pref = ""  # 都道府県大会なら later で一括決定（混在対策のため）
    if all(m.prefecture == matches[0].prefecture for m in matches if m.prefecture):
        pref = matches[0].prefecture

    final = [m for m in matches if "決勝" in m.stage]
    semi  = [m for m in matches if "準決勝" in m.stage]

    champion = runner_up = best4_a = best4_b = ""

    if final:
        f = final[-1]  # 最終の決勝
        champion = f.winner
        # 敗者名
        if f.team_a and f.team_b:
            loser = f.team_b if f.winner == f.team_a else f.team_a
            runner_up = loser

    if semi:
        losers = []
        for s in semi:
            if s.team_a and s.team_b:
                loser = s.team_b if s.winner == s.team_a else s.team_a
                losers.append(loser)
        if losers:
            best4_a = losers[0]
        if len(losers) >= 2:
            best4_b = losers[1]

    return [SummaryRow(
        year=YEAR,
        tournament=tournament_name,
        tournament_id=tournament_id,
        prefecture=pref,
        champion=champion,
        runner_up=runner_up,
        best4_a=best4_a,
        best4_b=best4_b,
    )]

# ================================
# メイン
# ================================
def main():
    all_matches: List[MatchRow] = []
    all_summaries: List[SummaryRow] = []

    for name, ids in TARGETS:
        id_list = list(ids)
        for tid in id_list:
            try:
                listing = parse_listing_page(tid)
            except Exception as e:
                print(f"[WARN] listing failed tid={tid}: {e}")
                time.sleep(SLEEP_SEC)
                continue

            print(f"[INFO] {name} tid={tid} -> {len(listing)} games")
            tour_matches: List[MatchRow] = []

            for u in listing:
                try:
                    row, _ = parse_detail_page(u, name, tid)
                    tour_matches.append(row)
                except Exception as e:
                    print(f"[WARN] detail failed url={u}: {e}")
                time.sleep(SLEEP_SEC)

            if tour_matches:
                all_matches.extend(tour_matches)
                try:
                    all_summaries.extend(summarize_tournament(name, tid, tour_matches))
                except Exception as e:
                    print(f"[WARN] summarize failed tid={tid}: {e}")

    # CSV出力
    with open(OUT_MATCHES, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(all_matches[0]).keys()) if all_matches else [
            "year","tournament","tournament_id","stage","round_no","date","region","prefecture",
            "team_a","team_b","score_a","score_b","winner","venue","source_url"
        ])
        w.writeheader()
        for r in all_matches:
            w.writerow(asdict(r))

    with open(OUT_SUMMARY, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(SummaryRow(YEAR,"",0,"","","","","")).keys()))
        w.writeheader()
        for r in all_summaries:
            w.writerow(asdict(r))

    print(f"[DONE] matches={len(all_matches)} -> {OUT_MATCHES}")
    print(f"[DONE] summaries={len(all_summaries)} -> {OUT_SUMMARY}")

if __name__ == "__main__":
    main()
