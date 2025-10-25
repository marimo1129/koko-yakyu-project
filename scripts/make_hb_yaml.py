# scripts/make_hb_yaml.py
import os, re, sys, textwrap, yaml, requests
from bs4 import BeautifulSoup

OUT = "data/hb_tournaments.yml"

CATEGORIES = [
    ("autumn_pref",   "秋季 都道府県", [
        "北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県",
        "茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県",
        "新潟県","富山県","石川県","福井県","山梨県","長野県",
        "岐阜県","静岡県","愛知県","三重県",
        "滋賀県","京都府","大阪府","兵庫県","奈良県","和歌山県",
        "鳥取県","島根県","岡山県","広島県","山口県",
        "徳島県","香川県","愛媛県","高知県",
        "福岡県","佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県",
    ]),
    ("autumn_regions","秋季 地区（東北/関東/北信越/東海/近畿/中国/四国/九州）", [
        "東北","関東","北信越","東海","近畿","中国","四国","九州",
    ]),
    ("jingu",   "明治神宮大会", ["明治神宮大会"]),
    ("senbatsu","春の選抜（センバツ）", ["選抜(センバツ)"]),
    ("spring_pref","春季 都道府県", [
        "北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県",
        "茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県",
        "新潟県","富山県","石川県","福井県","山梨県","長野県",
        "岐阜県","静岡県","愛知県","三重県",
        "滋賀県","京都府","大阪府","兵庫県","奈良県","和歌山県",
        "鳥取県","島根県","岡山県","広島県","山口県",
        "徳島県","香川県","愛媛県","高知県",
        "福岡県","佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県",
    ]),
]

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ja,en;q=0.8",
}

def is_tournaments_page(url: str) -> bool:
    """hb-nipponの大会データか簡易チェック"""
    if not re.search(r"https?://www\.hb-nippon\.com/tournaments/\d+", url):
        return False
    try:
        r = requests.get(url, headers=UA, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # ページ内に「大会データ」や大会情報っぽい見出しがあるか確認
        text = soup.get_text(" ", strip=True)
        return "大会データ" in text or "試合結果" in text
    except Exception:
        return False

def prompt_url(label: str, year_hint: str) -> str:
    print(f"\n▶ {label} の『大会データ』ページURLを貼り付け（例: https://www.hb-nippon.com/tournaments/1063）")
    print(f"   * 年度は {year_hint} を目安に。見つからなければ Enter でスキップ可能です。")
    print("   * 右クリック→リンクをコピーでOK。Google検索: site:hb-nippon.com/tournaments 大会データ 県名 でも可。")
    url = input("URL: ").strip()
    if not url:
        return ""
    if not is_tournaments_page(url):
        print("  × URLの形式 or ページ確認に失敗しました。もう一度確認してください。スキップするなら空Enter。")
        url2 = input("URL(再入力/空=スキップ): ").strip()
        if not url2:
            return ""
        if not is_tournaments_page(url2):
            print("  × それでも確認できませんでした。スキップします。")
            return ""
        return url2
    return url

def main():
    os.makedirs("data", exist_ok=True)
    print(textwrap.dedent("""
        =========================================================
        hb-nippon 大会URL収集 → data/hb_tournaments.yml を自動生成
        ---------------------------------------------------------
        - 表示された項目ごとに『大会データ』ページの URL を貼り付けます
        - 貼ったURLが正しいかを自動チェックして YAML を作ります
        - 空Enterでスキップできます。後で追加可能です
        =========================================================
    """).strip())

    year = input("収集する年（例: 2025。空なら今年）: ").strip()
    if not year:
        year = str(datetime.now().year)
    cfg = {"year": int(year)}

    for key, title, items in CATEGORIES:
        print(f"\n==== {title} ====")
        bucket = []
        for name in items:
            label = f"{name} {title.split()[0]}"
            url = prompt_url(label, year)
            if url:
                bucket.append({"url": url, "name": f"{name} {title.split()[0]}"})
        cfg[key] = bucket

    with open(OUT, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
    print(f"\n[SAVED] {OUT}")
    print("  → これで Actions の collect-scores を実行すると、YAMLの全部を巡回します。")

if __name__ == "__main__":
    from datetime import datetime
    main()
