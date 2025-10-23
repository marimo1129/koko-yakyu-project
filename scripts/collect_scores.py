import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime

# =========================
# 設定
# =========================
YEAR = datetime.now().year
OUTPUT = "data/all_pref_results.csv"

# 大会セット（秋・春・神宮・センバツ・夏）
TARGETS = [
    ("秋季大会", range(610, 656)),  # 各都道府県秋季
    ("春季大会", range(710, 756)),  # 各都道府県春季
    ("神宮大会", [560]),           # 全国
    ("センバツ", [101]),           # 全国
    ("夏の甲子園", [100])          # 全国
]

# 各IDに対応する都道府県（秋季ベース）
PREF_MAP = {
    610: "北海道", 611: "青森", 612: "岩手", 613: "宮城", 614: "秋田",
    615: "山形", 616: "福島", 617: "茨城", 618: "栃木", 619: "群馬",
    620: "埼玉", 621: "千葉", 622: "東京", 623: "神奈川", 624: "新潟",
    625: "富山", 626: "石川", 627: "福井", 628: "兵庫", 629: "大阪",
    630: "奈良", 631: "和歌山", 632: "京都", 633: "滋賀", 634: "鳥取",
    635: "島根", 636: "岡山", 637: "広島", 638: "山口", 639: "徳島",
    640: "香川", 641: "愛媛", 642: "高知", 643: "福岡", 644: "佐賀",
    645: "長崎", 646: "熊本", 647: "大分", 648: "宮崎", 649: "鹿児島",
    650: "沖縄"
}

# =========================
# 関数群
# =========================

def get_games(game_id, tournament_name):
    """大会ページから試合データを抽出"""
    url = f"https://vk.sportsbull.jp/koshien/game/{YEAR}/{game_id}/"
    print(f"→ {tournament_name} ({url})")
    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if res.status_code != 200:
        print(f"  × 取得失敗 ({res.status_code})")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    games = []
    for game in soup.select(".score-table"):
        try:
            round_name = game.find_previous("h3").get_text(strip=True)
            teams = game.select("tbody tr td.team")
            scores = game.select("tbody tr td.score")
            if len(teams) >= 2 and len(scores) >= 2:
                t1 = teams[0].get_text(strip=True)
                t2 = teams[1].get_text(strip=True)
                s1 = scores[0].get_text(strip=True)
                s2 = scores[1].get_text(strip=True)
                score = f"{s1}-{s2}"
                result = "勝利" if s1.isdigit() and s2.isdigit() and int(s1) > int(s2) else "敗戦"
                pref_name = PREF_MAP.get(game_id, "全国")
                games.append({
                    "year": YEAR,
                    "tournament": tournament_name,
                    "prefecture": pref_name,
                    "round": round_name,
                    "team_name": t1,
                    "opponent": t2,
                    "score": score,
                    "result": result
                })
        except Exception as e:
            print(f"⚠️ 解析エラー: {e}")
    return games

def main():
    all_games = []
    for tname, id_list in TARGETS:
        for gid in id_list:
            all_games.extend(get_games(gid, tname))
            time.sleep(2)
    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "year", "tournament", "prefecture", "round", "team_name", "opponent", "score", "result"
        ])
        writer.writeheader()
        writer.writerows(all_games)
    print(f"\n✅ {len(all_games)} 試合を保存しました → {OUTPUT}")

if __name__ == "__main__":
    main()

