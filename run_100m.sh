#!/bin/bash

# =========================================================
# 1億件 (100M) ベンチマーク & ストレージ容量測定スクリプト
# 対象: MongoDB, ClickHouse, QuestDB
# 実行: bash run_100m.sh
# =========================================================

DBS=("mongodb" "clickhouse" "questdb")

# 1回の挿入件数とループ回数
RECORDS_PER_BATCH=10000000
BATCHES=10
TOTAL_RECORDS=$((RECORDS_PER_BATCH * BATCHES))

mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="results/benchmark_100m_${TIMESTAMP}.log"
CSV_FILE="results/benchmark_100m_${TIMESTAMP}.csv"
CONTAINER_CSV_PATH="/app/${CSV_FILE}"

echo "=== 1億件ベンチマーク自動実行 ===" | tee -a "$LOG_FILE"
echo "対象: ${DBS[*]}" | tee -a "$LOG_FILE"
echo "結果は $LOG_FILE と $CSV_FILE に出力されます" | tee -a "$LOG_FILE"

docker compose down -v > /dev/null 2>&1

for db in "${DBS[@]}"; do
    echo "===================================================" | tee -a "$LOG_FILE"
    echo "🚀 [Start] データベース: $db の1億件テストを開始します" | tee -a "$LOG_FILE"
    
    docker compose up -d $db benchmarker
    
    # 起動待機
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
    
    echo "📝 [$db] Insert (文字列なし): ${RECORDS_PER_BATCH}件 x ${BATCHES}回 = 計1億件" | tee -a "$LOG_FILE"
    for i in $(seq 1 $BATCHES); do
        echo -n "  -> Batch $i/$BATCHES 実行中... " | tee -a "$LOG_FILE"
        
        if [ $i -eq 1 ]; then
            # 1回目はテーブル初期化（--append なし）
            docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$RECORDS_PER_BATCH" --exclude-strings --csv "$CONTAINER_CSV_PATH" >> "$LOG_FILE" 2>&1
        else
            # 2回目以降は既存のテーブルに追記（--append あり）
            docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$RECORDS_PER_BATCH" --exclude-strings --append --csv "$CONTAINER_CSV_PATH" >> "$LOG_FILE" 2>&1
        fi
        echo "Done" | tee -a "$LOG_FILE"
    done
    echo "✅ [$db] 1億件のInsertが完了しました" | tee -a "$LOG_FILE"

    # =========================================================
    # ▼▼▼ 追加箇所：ストレージ計測前の強制フラッシュと待機 ▼▼▼
    # =========================================================
    echo "⏳ [$db] ストレージ計測の前に、ディスクへの永続化とデータ圧縮を強制実行します..." | tee -a "$LOG_FILE"
    
    # 1. DB固有のフラッシュ/マージコマンドを実行
    docker compose exec benchmarker python src/force_flush.py --db "$db" --records "$TOTAL_RECORDS" >> "$LOG_FILE" 2>&1
    
    # 2. OSレベルのページキャッシュを物理ディスクにフラッシュ (duコマンドの精度を100%にするため)
    docker compose exec "$db" sync
    
    # 3. 念のためファイルシステムが落ち着くまで数秒待機
    sleep 5
    # =========================================================
    # ▲▲▲ 追加箇所ここまで ▲▲▲
    # =========================================================

    # --- ストレージ容量の測定 ---
    echo "💾 [$db] 1億件時のストレージ使用量を測定" | tee -a "$LOG_FILE"
    if [ "$db" == "mongodb" ]; then
        SIZE_MB=$(docker compose exec mongodb du -sm /data/db | awk '{print $1}')
    elif [ "$db" == "clickhouse" ]; then
        SIZE_MB=$(docker compose exec clickhouse du -sm /var/lib/clickhouse | awk '{print $1}')
    elif [ "$db" == "questdb" ]; then
        SIZE_MB=$(docker compose exec questdb du -sm /root/.questdb | awk '{print $1}')
    fi
    
    # MBをGBに変換して小数点第2位まで表示
    SIZE_GB=$(awk "BEGIN {printf \"%.2f\", $SIZE_MB/1024}")
    echo "  -> 📊 ストレージ使用量: $SIZE_GB GB ($SIZE_MB MB)" | tee -a "$LOG_FILE"
    
    # CSVへストレージ容量を記録 (Operation列に "storage_gb" として記録)
    echo "$TIMESTAMP,$db,storage_gb,$TOTAL_RECORDS,True,$SIZE_GB" >> "$CSV_FILE"

    # --- 1億件のクエリ速度検証 ---
    echo "🔍 [$db] Query (1億件に対する検索・集計):" | tee -a "$LOG_FILE"
    docker compose exec benchmarker python src/bench_query.py --db "$db" --records "$TOTAL_RECORDS" --exclude-strings --csv "$CONTAINER_CSV_PATH" | tee -a "$LOG_FILE"
    
    echo "🧹 [$db] のすべてのテストが完了しました。環境を破棄します。" | tee -a "$LOG_FILE"
    docker compose down -v > /dev/null 2>&1
done

echo "===================================================" | tee -a "$LOG_FILE"
echo "🎉 全てのDBの1億件ベンチマークが完了しました！" | tee -a "$LOG_FILE"