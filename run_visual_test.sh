#!/bin/bash

# =========================================================
# 可視化ツール確認用セットアップスクリプト
# 対象DB: MongoDB, ClickHouse, QuestDB
# 可視化: Mongo Express, Grafana, Metabase
# =========================================================

DBS=("mongodb" "clickhouse" "questdb")
RECORDS=100000

echo "=== 可視化環境のセットアップを開始します ==="

# 環境のリセット（クリーンな状態から10万件を入れるため）
echo "🧹 既存のコンテナとボリュームをクリーンアップしています..."
docker compose down -v > /dev/null 2>&1

# 今回の検証に必要なコンテナ群だけをピンポイントで起動
echo "🚀 DBおよび可視化ツールを起動しています..."
docker compose up -d mongodb clickhouse questdb mongo-express grafana metabase benchmarker

# DBの起動待機
for db in "${DBS[@]}"; do
    echo -n "⏳ $db の起動完了を待機しています..."
    MAX_RETRIES=60
    RETRY_COUNT=0
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if docker compose exec benchmarker python src/check_ready.py --db "$db" > /dev/null 2>&1; then
            echo -e "\n✅ $db の起動を確認しました！"
            break
        fi
        echo -n "."
        sleep 2
        RETRY_COUNT=$((RETRY_COUNT+1))
    done
done

# データのインサート（GUIで見栄えが良いように MACアドレス文字列を含める）
for db in "${DBS[@]}"; do
    echo "📝 [$db] ${RECORDS}件のデータをインサート中..."
    docker compose exec benchmarker python src/bench_insert.py --db "$db" --records "$RECORDS" > /dev/null 2>&1
    echo "✅ [$db] インサート完了！"
done

# データのフラッシュ（ClickHouseのマージなどを強制実行し、即座に検索できるようにする）
echo "💾 データをディスクにフラッシュ・最適化しています..."
for db in "${DBS[@]}"; do
    docker compose exec benchmarker python src/force_flush.py --db "$db" --records "$RECORDS" > /dev/null 2>&1
done

echo "==================================================="
echo "🎉 セットアップが完了しました！"
echo "以下のURLから各ツールにアクセスしてデータを確認できます。"
echo "==================================================="
echo "🟢 Mongo Express (MongoDB用データ確認)"
echo "   👉 URL: http://localhost:8081"
echo "   👉 認証: admin / pass"
echo "   ※ mydatabase -> sensor_data でBSONデータの中身を直接見られます。"
echo ""
echo "🟢 Grafana (QuestDB / ClickHouse 時系列グラフ用)"
echo "   👉 URL: http://localhost:3000"
echo "   👉 認証: admin / admin"
echo "   ※ Data sourcesからPostgreSQL(QuestDB用: 8812ポート)やClickHouseを追加してください。"
echo ""
echo "🟢 Metabase (ダッシュボード・集計用)"
echo "   👉 URL: http://localhost:3001"
echo "   👉 認証: (初回アクセス時にアカウント作成ウィザードが開きます)"
echo "   ※ ウィザード内でそのままMongoDBやClickHouseを連携できます。"
echo "==================================================="