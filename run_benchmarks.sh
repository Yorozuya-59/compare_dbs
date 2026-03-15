#!/bin/bash

# =========================================================
# DBベンチマーク自動実行スクリプト（CSV出力 + スマート待機・省メモリ版）
# =========================================================

# DBS=("mysql" "postgres" "mongodb" "redis" "surrealdb" "clickhouse" "timescaledb" "influxdb" "scylladb" "duckdb" "questdb" "starrocks")
DBS=("duckdb" "questdb" "starrocks")
RECORDS=(1000 10000 100000 1000000 10000000)

mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="results/benchmark_optimized_${TIMESTAMP}.log"
CSV_FILE="results/benchmark_optimized_${TIMESTAMP}.csv" # ホスト側のCSVパス
CONTAINER_CSV_PATH="/app/${CSV_FILE}"                   # コンテナ内から見たCSVのパス

echo "=== CSV対応 スマート待機・省メモリ版 ベンチマーク自動実行 ===" | tee -a "$LOG_FILE"
echo "結果は $LOG_FILE と $CSV_FILE に出力されます" | tee -a "$LOG_FILE"

docker compose down -v > /dev/null 2>&1

for db in "${DBS[@]}"; do
    echo "===================================================" | tee -a "$LOG_FILE"
    echo "🚀 [Start] データベース: $db のテストを開始します" | tee -a "$LOG_FILE"
    
    # --- 修正箇所：DuckDBは専用コンテナを持たないための特別扱い ---
    if [ "$db" == "duckdb" ]; then
        docker compose up -d benchmarker
    else
        docker compose up -d $db benchmarker
    fi
    # -------------------------------------------------------------
    
    echo -n "⏳ $db の起動完了を待機しています..." | tee -a "$LOG_FILE"
    MAX_RETRIES=60
    RETRY_COUNT=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if docker compose exec benchmarker python src/check_ready.py --db "$db" > /dev/null 2>&1; then
            echo -e "\n✅ $db の起動を確認しました！" | tee -a "$LOG_FILE"
            break
        fi
        echo -n "."
        sleep 2
        RETRY_COUNT=$((RETRY_COUNT+1))
    done

    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "\n❌ $db の起動確認がタイムアウトしました。スキップします。" | tee -a "$LOG_FILE"
        docker compose down -v > /dev/null 2>&1
        continue
    fi
    
    for rec in "${RECORDS[@]}"; do
        echo "---------------------------------------------------" | tee -a "$LOG_FILE"
        
        # 1. 文字列あり
        echo "📝 [$db] Insert (文字列あり): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$rec" --csv "$CONTAINER_CSV_PATH" | tee -a "$LOG_FILE"

        echo "🔍 [$db] Query (文字列あり): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_query.py --db "$db" --records "$rec" --csv "$CONTAINER_CSV_PATH" | tee -a "$LOG_FILE"
        
        # 2. 文字列なし
        echo "📝 [$db] Insert (文字列なし): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$rec" --exclude-strings --csv "$CONTAINER_CSV_PATH" | tee -a "$LOG_FILE"

        echo "🔍 [$db] Query (文字列なし): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_query.py --db "$db" --records "$rec" --exclude-strings --csv "$CONTAINER_CSV_PATH" | tee -a "$LOG_FILE"
    done
    
    echo "🧹 [$db] のすべてのテストが完了しました。環境を破棄します。" | tee -a "$LOG_FILE"
    docker compose down -v > /dev/null 2>&1
done

echo "===================================================" | tee -a "$LOG_FILE"
echo "🎉 全てのDBのベンチマークが完了しました！ CSV結果は $CSV_FILE をご確認ください" | tee -a "$LOG_FILE"