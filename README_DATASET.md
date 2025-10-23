# 全国データ準備テンプレ（秋季県大会ベスト8＋エリア大会）
使うファイル:
- data/prefectural_best8.csv … 県大会ベスト8（優勝/準優勝/ベスト4/ベスト8）
- data/area_results.csv … 近畿/関東などエリア大会の結果
- data/areas.csv … 県→地域/エリア対応表
- data/teams.csv … 出力（merge_teams.pyで生成）
- data/players.csv … 選手データ（写真なし・YouTube埋め込み・comment列）

手順:
1) prefectural_best8.csv に県ごとのベスト8を入力
2) area_results.csv にエリア大会の結果を入力
3) 実行: `python scripts/merge_teams.py` → teams.csv が更新

アプリへの反映:
- teams.csv は将来チームページ/一覧で使用
- players.csv は既存の alumni/選手一覧で表示可能（写真なし、youtube埋め込み）
