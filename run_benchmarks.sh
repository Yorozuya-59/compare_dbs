#!/bin/bash

# =========================================================
# DBベンチマーク自動実行スクリプト
# 実行方法: bash run_benchmarks.sh
# =========================================================

# テストするレコード数の配列 (10^3, 10^4, 10^5, 10^6, 10^7)
# ※ 1億件(100000000)以上はマシンスペックに応じて追加してください
RECORDS=(1000 10000 100000 1000000 10000000)

# 結果を保存するディレクトリを作成
mkdir -p results

# 現在の日時をファイル名に含める
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="results/benchmark_${TIMESTAMP}.log"

echo "=== ベンチマークを自動実行します ===" | tee -a "$LOG_FILE"
echo "テスト対象件数: ${RECORDS[*]}" | tee -a "$LOG_FILE"

# 1. 通常データ（文字列あり）での検証
for rec in "${RECORDS[@]}"; do
    echo "---------------------------------------------------" | tee -a "$LOG_FILE"
    echo "🚀 [Insert] 文字列あり: $rec 件のデータ投入を開始..." | tee -a "$LOG_FILE"
    # insertスクリプトは実行時に各DBの中身をTruncate(全削除)するため、事前削除は不要です
    docker compose exec benchmarker python src/bench_insert.py --db all --records "$rec" | tee -a "$LOG_FILE"

    echo "🔍 [Query] 文字列あり: $rec 件のデータに対する検索・集計を開始..." | tee -a "$LOG_FILE"
    docker compose exec benchmarker python src/bench_query.py --db all | tee -a "$LOG_FILE"
done

# 2. 文字列なし（純粋な数値のみ）での検証
for rec in "${RECORDS[@]}"; do
    echo "---------------------------------------------------" | tee -a "$LOG_FILE"
    echo "🚀 [Insert] 文字列なし(exclude-strings): $rec 件のデータ投入を開始..." | tee -a "$LOG_FILE"
    docker compose exec benchmarker python src/bench_insert.py --db all --records "$rec" --exclude-strings | tee -a "$LOG_FILE"

    echo "🔍 [Query] 文字列なし: $rec 件のデータに対する検索・集計を開始..." | tee -a "$LOG_FILE"
    # ※ 文字列なしデータを挿入した状態でのQuery検証
    docker compose exec benchmarker python src/bench_query.py --db all | tee -a "$LOG_FILE"
done

echo "=== 全ての検証が完了しました！ 結果は $LOG_FILE に保存されています ===" | tee -a "$LOG_FILE"