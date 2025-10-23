import csv
import openai
import os

# -------------------------------
# OpenAI API設定
# -------------------------------
openai.api_key = os.getenv("OPENAI_API_KEY")

# 入出力ファイル
INPUT_FILE = "data/players.csv"
OUTPUT_FILE = "data/players.csv"

# -------------------------------
# AIコメント生成関数
# -------------------------------
def generate_comment(player):
    name = player.get("名前", "")
    year = player.get("学年", "")
    position = player.get("ポジション", "")
    school = player.get("所属高校", "")
    stats = player.get("成績", "")
    summary = player.get("選手評", "")

    prompt = f"""
あなたは高校野球専門のスカウト兼ライターです。
次の選手情報をもとに、自然で読みやすく、ポジティブな短評を100文字前後で作成してください。

名前: {name}
学年: {year}
ポジション: {position}
所属高校: {school}
成績・特徴: {stats or summary}

例: 「○○高校の△△は打撃センスが光る右打者。ミート力に優れ、将来の中軸候補として期待される。」

出力は日本語の短文のみで返してください。
"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "あなたは野球専門のライターです。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"Error for {name}: {e}")
        return "コメント生成エラー"

# -------------------------------
# CSV読み込み＆AIコメント生成
# -------------------------------
players = []
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["comment_ai"] = generate_comment(row)
        players.append(row)

# -------------------------------
# CSV上書き保存
# -------------------------------
with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=players[0].keys())
    writer.writeheader()
    writer.writerows(players)

print("✅ AIコメント生成が完了しました！")
