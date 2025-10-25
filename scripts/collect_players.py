# -*- coding: utf-8 -*-
"""
collect_players.py
第2段階：選手の自動収集と評価（最小実装）

使い方例:
  python scripts/collect_players.py --year 2025 \
    --best8_csv data/best8_autumn_2025.csv \
    --players_links data/players_links.csv \
    --out_dir data

設計ポイント:
- 画像(photo_url)は扱わない（列も出力しない）
- URL取得元は players_links.csv（推奨） or 将来の自動探索
- プロバイダ(サイト)ごとに抽出ロジックをアダプタ化
"""

import argparse
import csv
import dataclasses
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

JST = timezone(timedelta(hours=9))

# --------------------------
# ログ設定
# --------------------------
def setup_logger(log_path: str):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

# --------------------------
# データモデル
# --------------------------
@dataclasses.dataclass
class Player:
    year: int
    school_name: str
    prefecture: Optional[str]
    player_name: str
    grade: Optional[int]          # 1 or 2 を期待
    position: Optional[str]
    max_velocity: Optional[float] # km/h
    total_hr: Optional[int]
    ops: Optional[float]
    avg: Optional[float]
    hr: Optional[int]
    rbi: Optional[int]
    era: Optional[float]
    k9: Optional[float]
    scout_comment: Optional[str]
    youtube_url: Optional[str]
    source_url: str
    updated_at: str               # ISO8601

    @property
    def player_id(self) -> str:
        # year + hash(school+player) で安定IDを生成
        key = f"{self.year}:{self.school_name}:{self.player_name}"
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]
        # 学校略キー
        school_key = re.sub(r"\s+", "", self.school_name)[:8]
        return f"{self.year}-{school_key}-{h}"

# --------------------------
# ユーティリティ
# --------------------------
def now_iso_jst() -> str:
    return datetime.now(JST).isoformat()

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

def to_int(x: Optional[str]) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        # 数値が "144km/h" 等のケースに対応
        m = re.search(r"(\d+)", str(x))
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        return None

def normalize_position(pos: Optional[str]) -> Optional[str]:
    if not pos:
        return None
    pos = pos.strip()
    # よくある省略・ゆらぎ
    mapping = {
        "P": "投手", "投": "投手", "投手": "投手",
        "C": "捕手", "捕": "捕手", "捕手": "捕手",
        "1B": "一塁手", "一塁": "一塁手",
        "2B": "二塁手", "二塁": "二塁手",
        "3B": "三塁手", "三塁": "三塁手",
        "SS": "遊撃手", "遊": "遊撃手", "遊撃": "遊撃手",
        "LF": "左翼手", "左": "左翼手",
        "CF": "中堅手", "中": "中堅手",
        "RF": "右翼手", "右": "右翼手",
    }
    return mapping.get(pos, pos)

def normalize_grade(g: Optional[str]) -> Optional[int]:
    if g is None:
        return None
    s = str(g)
    # "2年", "高2", "2" 等を 2 に
    m = re.search(r"([123])", s)
    if m:
        return int(m.group(1))
    return None

def safe_div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or d is None or d == 0:
        return None
    return n / d

# --------------------------
# プロバイダ（サイト）アダプタ
# --------------------------
class ProviderBase:
    NAME = "base"

    def can_handle(self, url: str) -> bool:
        return False

    def fetch_and_parse(self, url: str) -> Dict:
        """URLから原文→テキスト抽出→簡易パースし、辞書を返す
        返却フォーマット（キーは任意、下で吸収）:
          {
            "player_name": "...",
            "grade": "2年",
            "position": "投手",
            "max_velocity": "144km/h",
            "total_hr": "12本",
            "avg": ".385",
            "ops": "0.921",
            "hr": "2",
            "rbi": "6",
            "era": "1.82",
            "k9": "10.5",
            "scout_comment": "....",
            "youtube_url": "https://...",
          }
        """
        raise NotImplementedError

    # 共通のHTTP取得（タイムアウト・UA・リトライ軽装備）
    def http_get(self, url: str, retry: int = 2, timeout: int = 12) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; koko-yakyu-collector/1.0)",
            "Accept-Language": "ja,en;q=0.8",
        }
        for i in range(retry + 1):
            try:
                r = requests.get(url, headers=headers, timeout=timeout)
                if r.status_code == 200 and r.text:
                    return r.text
                logging.warning(f"[{self.NAME}] status={r.status_code} url={url}")
            except Exception as e:
                logging.warning(f"[{self.NAME}] GET error ({i}/{retry}) {url}: {e}")
            time.sleep(1.2 * (i + 1))
        return None

class SportsBullProvider(ProviderBase):
    NAME = "sportsbull"

    def can_handle(self, url: str) -> bool:
        host = urlparse(url).netloc
        return "sportsbull.jp" in host or "vk.sportsbull.jp" in host

    def fetch_and_parse(self, url: str) -> Dict:
        html = self.http_get(url)
        if not html:
            return {}
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)

        # 超簡易抽出（正規表現ベースの最小版）
        out = {}
        # 名前（title等から）
        m = re.search(r"(?:(?:選手|プロフィール)[:：]\s*)?([^\s＜＜<>|｜\-\—]{2,12})\s*(?:選手|くん|さん)?\s*(?:\||｜| - )", text)
        if m:
            out["player_name"] = m.group(1)

        # 学年
        m = re.search(r"(?:学年|学年：|学年:)\s*([一二三12３]{1})", text)
        if not m:
            m = re.search(r"([12３])年(?:生)?", text)
        if m:
            out["grade"] = m.group(1)

        # ポジション
        m = re.search(r"(投手|捕手|一塁手|二塁手|三塁手|遊撃手|左翼手|中堅手|右翼手|P|C|SS|LF|CF|RF)", text)
        if m:
            out["position"] = m.group(1)

        # 数値類
        def pick_float(pattern):
            m = re.search(pattern, text)
            return m.group(1) if m else None

        out["max_velocity"] = pick_float(r"MAX(?:球速)?\s*([12]?\d{2})\s*km/h")
        out["total_hr"]    = pick_float(r"(?:高校通算|通算)\s*?(\d{1,3})\s*本")
        out["avg"]         = pick_float(r"(?:打率|AVG)[:：]?\s*0?\.(\d{3})")
        if out.get("avg"):
            out["avg"] = f"0.{out['avg']}"
        out["ops"]         = pick_float(r"(?:OPS)[:：]?\s*(\d\.\d{3})")
        out["hr"]          = pick_float(r"(?:本塁打|HR)[:：]?\s*(\d{1,3})")
        out["rbi"]         = pick_float(r"(?:打点|RBI)[:：]?\s*(\d{1,3})")
        out["era"]         = pick_float(r"(?:防御率|ERA)[:：]?\s*(\d\.\d{2})")
        out["k9"]          = pick_float(r"(?:奪三振率|K\/9)[:：]?\s*(\d{1,2}\.?\d{0,2})")

        # 短評（簡易）
        m = re.search(r"(?:評価|寸評|コメント)[:：]\s*([^|｜]{10,80})", text)
        if m:
            out["scout_comment"] = m.group(1).strip()

        # YouTubeリンク（埋め込み含む）
        m = re.search(r"(https?://(?:www\.)?youtube\.com/[^\s\"'<]+)", text)
        if not m:
            m = re.search(r"(https?://youtu\.be/[^\s\"'<]+)", text)
        if m:
            out["youtube_url"] = m.group(1)

        return out

class HighSchoolBaseballComProvider(ProviderBase):
    NAME = "hbcom"

    def can_handle(self, url: str) -> bool:
        host = urlparse(url).netloc
        return "hb-nippon.com" in host or "www.hb-nippon.com" in host

    def fetch_and_parse(self, url: str) -> Dict:
        html = self.http_get(url)
        if not html:
            return {}
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)

        out = {}
        # 名前（パンくず/タイトル帯から拾う簡易）
        m = re.search(r"(?:選手名|氏名|名前)[:：]\s*([^\s＜＜<>|｜\-\—]{2,12})", text)
        if not m:
            m = re.search(r"([^\s]{2,12})\s*(?:選手|くん|さん)\s*(?:\||｜| - )", text)
        if m:
            out["player_name"] = m.group(1)

        # 学年/ポジションの簡易抽出
        m = re.search(r"(?:学年)[:：]?\s*([12３])", text)
        if m:
            out["grade"] = m.group(1)

        m = re.search(r"(投手|捕手|一塁手|二塁手|三塁手|遊撃手|左翼手|中堅手|右翼手|P|C|SS|LF|CF|RF)", text)
        if m:
            out["position"] = m.group(1)

        # 数値類
        def pick_float(pattern):
            m = re.search(pattern, text)
            return m.group(1) if m else None

        out["max_velocity"] = pick_float(r"(?:MAX|最速)[:：]?\s*([12]?\d{2})\s*km/h")
        out["total_hr"]    = pick_float(r"(?:高校通算|通算)\s*?(\d{1,3})\s*本")
        out["avg"]         = pick_float(r"(?:打率|AVG)[:：]?\s*0?\.(\d{3})")
        if out.get("avg"):
            out["avg"] = f"0.{out['avg']}"
        out["ops"]         = pick_float(r"(?:OPS)[:：]?\s*(\d\.\d{3})")
        out["hr"]          = pick_float(r"(?:本塁打|HR)[:：]?\s*(\d{1,3})")
        out["rbi"]         = pick_float(r"(?:打点|RBI)[:：]?\s*(\d{1,3})")
        out["era"]         = pick_float(r"(?:防御率|ERA)[:：]?\s*(\d\.\d{2})")
        out["k9"]          = pick_float(r"(?:奪三振率|K\/9)[:：]?\s*(\d{1,2}\.?\d{0,2})")

        # 短評
        m = re.search(r"(?:寸評|スカウト評|評価|コメント)[:：]\s*([^|｜]{10,80})", text)
        if m:
            out["scout_comment"] = m.group(1).strip()

        # YouTube
        m = re.search(r"(https?://(?:www\.)?youtube\.com/[^\s\"'<]+)", text)
        if not m:
            m = re.search(r"(https?://youtu\.be/[^\s\"'<]+)", text)
        if m:
            out["youtube_url"] = m.group(1)

        return out

PROVIDERS: List[ProviderBase] = [
    SportsBullProvider(),
    HighSchoolBaseballComProvider(),
]

def dispatch_provider(url: str) -> Optional[ProviderBase]:
    for p in PROVIDERS:
        if p.can_handle(url):
            return p
    return None

# --------------------------
# 入力読取
# --------------------------
def read_best8(best8_csv: str) -> Dict[str, Dict]:
    """
    return: school_name -> {"prefecture": "..."}
    """
    out = {}
    with open(best8_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            school = (row.get("school_name") or row.get("team") or "").strip()
            if not school:
                continue
            pref = (row.get("prefecture") or row.get("pref") or "").strip() or None
            out[school] = {"prefecture": pref}
    return out

def read_players_links(path: str) -> List[Dict]:
    """
    期待する列（最低限）:
      year, school_name, player_name, url
    任意で:
      grade, position など手入力があれば上書きに使用
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r.get("url"):
                continue
            rows.append(r)
    return rows

# --------------------------
# スコア計算
# --------------------------
def compute_ai_score(p: Player) -> Tuple[Optional[float], str]:
    """
    打者:
      AI = (AVG*300 + OPS*200 + HR*10 + RBI*3)/10
    投手:
      AI = max(0, 100 - ERA*15) + (K9*4) + (MAXV/3 if exists)
    値が無い項目は0扱い。basis で根拠を返す。
    """
    pos = p.position or ""
    is_pitcher = ("投手" in pos) or (pos.upper() == "P")

    if is_pitcher:
        era = p.era or 0.0
        k9 = p.k9 or 0.0
        mv = p.max_velocity or 0.0
        ai = max(0.0, 100.0 - era * 15.0) + (k9 * 4.0) + (mv / 3.0 if mv else 0.0)
        return round(ai, 1), "ERA/K9/MAXV"
    else:
        avg = p.avg or 0.0
        ops = p.ops or 0.0
        hr = p.hr or 0
        rbi = p.rbi or 0
        ai = (avg * 300.0 + ops * 200.0 + hr * 10.0 + rbi * 3.0) / 10.0
        return round(ai, 1), "AVG/OPS/HR/RBI"

# --------------------------
# メイン処理
# --------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--best8_csv", type=str, required=True)
    parser.add_argument("--players_links", type=str, default=None,
                        help="学校・選手のURL一覧CSV（推奨）")
    parser.add_argument("--out_dir", type=str, default="data")
    parser.add_argument("--log_path", type=str, default="data/logs/collect_players.log")
    parser.add_argument("--sleep", type=float, default=1.2, help="アクセス間隔(秒)")
    args = parser.parse_args()

    setup_logger(args.log_path)
    logging.info("=== collect_players start ===")

    school_index = read_best8(args.best8_csv)  # school_name -> prefecture
    logging.info(f"best8 schools loaded: {len(school_index)}")

    input_rows: List[Dict] = []
    if args.players_links and os.path.exists(args.players_links):
        input_rows = read_players_links(args.players_links)
        logging.info(f"players_links rows: {len(input_rows)}")
    else:
        logging.warning("players_links.csv が指定されていないため、自動探索は未実装（今後対応）")
        input_rows = []

    players: List[Player] = []
    failed: List[Tuple[str, str]] = []  # (school, url)

    for r in input_rows:
        if str(r.get("year", "")).strip() and int(r["year"]) != args.year:
            continue  # 年度フィルタ

        school_name = (r.get("school_name") or "").strip()
        player_name_hint = (r.get("player_name") or "").strip()
        url = r.get("url", "").strip()
        if not (school_name and url):
            continue

        pref = school_index.get(school_name, {}).get("prefecture")
        provider = dispatch_provider(url)
        raw = {}
        if provider:
            raw = provider.fetch_and_parse(url)
            time.sleep(args.sleep)
        else:
            logging.info(f"no provider for url: {url}")

        # プレーンな行で上書き（手入力優先）
        # 例：grade, position などを links CSV に暫定入力しておけば使える
        for k in ["player_name", "grade", "position", "max_velocity", "total_hr",
                  "ops", "avg", "hr", "rbi", "era", "k9", "scout_comment", "youtube_url"]:
            if r.get(k):
                raw[k] = r[k]

        # 正規化
        player_name = (raw.get("player_name") or player_name_hint or "").strip()
        if not player_name:
            failed.append((school_name, url))
            logging.info(f"skip: no player_name extracted: {url}")
            continue

        grade = normalize_grade(raw.get("grade"))
        # 学年フィルタ（1〜2年のみ）
        if grade and grade not in (1, 2):
            logging.info(f"skip: grade {grade} not in (1,2): {player_name} {url}")
            continue

        position = normalize_position(raw.get("position"))
        mv = to_int(raw.get("max_velocity"))
        thr = to_int(raw.get("total_hr"))
        ops = to_float(raw.get("ops"))
        avg = to_float(raw.get("avg"))
        hr = to_int(raw.get("hr"))
        rbi = to_int(raw.get("rbi"))
        era = to_float(raw.get("era"))
        k9 = to_float(raw.get("k9"))
        scout_comment = (raw.get("scout_comment") or None)
        youtube_url = (raw.get("youtube_url") or None)

        p = Player(
            year=args.year,
            school_name=school_name,
            prefecture=pref,
            player_name=player_name,
            grade=grade,
            position=position,
            max_velocity=float(mv) if mv is not None else None,
            total_hr=thr,
            ops=ops,
            avg=avg,
            hr=hr,
            rbi=rbi,
            era=era,
            k9=k9,
            scout_comment=scout_comment,
            youtube_url=youtube_url,
            source_url=url,
            updated_at=now_iso_jst(),
        )
        players.append(p)

    # 出力
    os.makedirs(args.out_dir, exist_ok=True)
    players_csv = os.path.join(args.out_dir, f"players_{args.year}.csv")
    scores_csv = os.path.join(args.out_dir, f"player_scores_{args.year}.csv")

    with open(players_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "player_id","year","school_name","prefecture","player_name","grade","position",
            "max_velocity","total_hr","ops","avg","hr","rbi","era","k9",
            "scout_comment","youtube_url","source_url","updated_at"
        ])
        for p in players:
            writer.writerow([
                p.player_id, p.year, p.school_name, p.prefecture, p.player_name, p.grade, p.position,
                p.max_velocity, p.total_hr, p.ops, p.avg, p.hr, p.rbi, p.era, p.k9,
                p.scout_comment, p.youtube_url, p.source_url, p.updated_at
            ])

    with open(scores_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year","school_name","player_name","position","AI_score","basis"])
        for p in players:
            score, basis = compute_ai_score(p)
            writer.writerow([p.year, p.school_name, p.player_name, p.position, score, basis])

    # 失敗ログ
    if failed:
        logging.warning(f"extract failed: {len(failed)}")
        for school, url in failed:
            logging.warning(f"FAILED\t{school}\t{url}")

    logging.info(f"players written: {players_csv}")
    logging.info(f"scores  written: {scores_csv}")
    logging.info("=== collect_players done ===")

if __name__ == "__main__":
    main()
