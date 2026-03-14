#!/bin/bash

# =========================================================
# DBベンチマーク自動実行スクリプト（スマート待機・省メモリ版）
# =========================================================

# テスト対象のデータベースのリスト
DBS=("mysql" "postgres" "mongodb" "redis" "surrealdb")
# DBS=("mysql" "postgres" "mongodb" "redis" "surrealdb" "clickhouse" "timescaledb" "influxdb" "scylladb")

# テストするレコード数の配列
RECORDS=(1000 10000 100000 1000000 10000000)

# 結果を保存するディレクトリを作成
mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="results/benchmark_optimized_${TIMESTAMP}.log"

echo "=== スマート待機・省メモリ版 ベンチマーク自動実行 ===" | tee -a "$LOG_FILE"
echo "テスト対象DB: ${DBS[*]}" | tee -a "$LOG_FILE"
echo "テスト対象件数: ${RECORDS[*]}" | tee -a "$LOG_FILE"

# 一旦、すべてのコンテナとボリュームを綺麗に削除する
docker compose down -v > /dev/null 2>&1

for db in "${DBS[@]}"; do
    echo "===================================================" | tee -a "$LOG_FILE"
    echo "🚀 [Start] データベース: $db のテストを開始します" | tee -a "$LOG_FILE"
    
    # 対象のDBとbenchmarkerコンテナのみを起動
    docker compose up -d $db benchmarker
    
    # --- 起動待機のロジック ---
    echo -n "⏳ $db の起動完了を待機しています..." | tee -a "$LOG_FILE"
    MAX_RETRIES=60 # 最大試行回数 (2秒 × 60回 = 最大2分)
    RETRY_COUNT=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # check_ready.py を実行し、終了コードが 0 (成功) ならループを抜ける
        if docker compose exec benchmarker python src/check_ready.py --db "$db" > /dev/null 2>&1; then
            echo -e "\n✅ $db の起動を確認しました！ (${RETRY_COUNT}回目で成功)" | tee -a "$LOG_FILE"
            break
        fi
        echo -n "."
        sleep 2
        RETRY_COUNT=$((RETRY_COUNT+1))
    done

    # タイムアウトした場合の処理
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "\n❌ $db の起動確認がタイムアウトしました。次のDBへスキップします。" | tee -a "$LOG_FILE"
        docker compose down -v > /dev/null 2>&1
        continue
    fi
    # --------------------------
    
    # ベンチマーク本体の実行
    for rec in "${RECORDS[@]}"; do
        echo "---------------------------------------------------" | tee -a "$LOG_FILE"
        
        echo "📝 [$db] Insert (文字列あり): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$rec" | tee -a "$LOG_FILE"

        echo "🔍 [$db] Query (文字列あり): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_query.py --db "$db" | tee -a "$LOG_FILE"
        
        echo "📝 [$db] Insert (文字列なし): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$rec" --exclude-strings | tee -a "$LOG_FILE"

        echo "🔍 [$db] Query (文字列なし): $rec 件" | tee -a "$LOG_FILE"
        docker compose exec benchmarker python src/bench_query.py --db "$db" | tee -a "$LOG_FILE"
    done
    
    echo "🧹 [$db] のすべてのテストが完了しました。環境を破棄します。" | tee -a "$LOG_FILE"
    docker compose down -v > /dev/null 2>&1
done

echo "===================================================" | tee -a "$LOG_FILE"
echo "🎉 全てのDBのベンチマークが完了しました！ 結果は $LOG_FILE に保存されています" | tee -a "$LOG_FILE"