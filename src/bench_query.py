import time
import argparse
import asyncio

import mysql.connector
import psycopg2
from pymongo import MongoClient
import redis
from surrealdb import Surreal

# テストで検索するターゲットのデバイスID
TARGET_DEVICE_ID = 500

# --- 1. MySQL ---
def query_mysql():
    print("[MySQL] 検索・集計ベンチマーク...")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    
    # ① Point Query: 特定デバイスの最新データを10件取得
    start_time = time.time()
    cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    cursor.fetchall()
    q1_time = time.time() - start_time
    print(f"  ├ 特定デバイス検索: {q1_time:.4f} 秒")
    
    # ② Aggregation: デバイスごとのレコード数とRSSIの平均値を計算
    start_time = time.time()
    cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id")
    cursor.fetchall()
    q2_time = time.time() - start_time
    print(f"  └ デバイス別集計  : {q2_time:.4f} 秒\n")
    
    cursor.close()
    conn.close()

# --- 2. PostgreSQL ---
def query_postgres():
    print("[PostgreSQL] 検索・集計ベンチマーク...")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    start_time = time.time()
    cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    cursor.fetchall()
    q1_time = time.time() - start_time
    print(f"  ├ 特定デバイス検索: {q1_time:.4f} 秒")
    
    start_time = time.time()
    cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id")
    cursor.fetchall()
    q2_time = time.time() - start_time
    print(f"  └ デバイス別集計  : {q2_time:.4f} 秒\n")
    
    cursor.close()
    conn.close()

# --- 3. MongoDB ---
def query_mongodb():
    print("[MongoDB] 検索・集計ベンチマーク...")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    collection = client["mydatabase"]["sensor_data"]
    
    # ① Point Query
    start_time = time.time()
    list(collection.find({"device_id": TARGET_DEVICE_ID}).sort("recorded_at", -1).limit(10))
    q1_time = time.time() - start_time
    print(f"  ├ 特定デバイス検索: {q1_time:.4f} 秒")
    
    # ② Aggregation (MongoDBの集計パイプライン)
    start_time = time.time()
    pipeline = [
        {"$group": {
            "_id": "$device_id",
            "count": {"$sum": 1},
            "avg_rssi": {"$avg": "$rssi"}
        }}
    ]
    list(collection.aggregate(pipeline))
    q2_time = time.time() - start_time
    print(f"  └ デバイス別集計  : {q2_time:.4f} 秒\n")
    
    client.close()

# --- 4. Redis ---
def query_redis():
    print("[Redis] 検索・集計ベンチマーク...")
    # 記事向けの重要な考察ポイント：
    # RedisのList型にJSONを突っ込む設計では、RDBのような「特定のdevice_idによるフィルタリング」や
    # 「グループ集計」は不可能です。これを行うには、全てのJSONを取得してPython側でループ処理するか、
    # Redisのデータ構造自体（HashやSorted Setなどを駆使する）を複雑に設計し直す必要があります。
    print("  ├ 特定デバイス検索: Not Supported (List構造のためフルスキャンが必要)")
    print("  └ デバイス別集計  : Not Supported (RDB的なGroup By機能なし)\n")

# --- 5. SurrealDB ---
async def query_surrealdb():
    print("[SurrealDB] 検索・集計ベンチマーク...")
    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        
        # ① Point Query
        start_time = time.time()
        await db.query("SELECT * FROM sensor_data WHERE device_id = $id ORDER BY recorded_at DESC LIMIT 10", {"id": TARGET_DEVICE_ID})
        q1_time = time.time() - start_time
        print(f"  ├ 特定デバイス検索: {q1_time:.4f} 秒")
        
        # ② Aggregation
        start_time = time.time()
        await db.query("SELECT device_id, count(), math::mean(rssi) AS avg_rssi FROM sensor_data GROUP BY device_id")
        q2_time = time.time() - start_time
        print(f"  └ デバイス別集計  : {q2_time:.4f} 秒\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor Data DB Query Benchmark")
    parser.add_argument("--db", type=str, required=True, 
                        choices=["mysql", "postgres", "mongodb", "redis", "surrealdb", "all"])
    args = parser.parse_args()
    
    print(f"=== 検索・集計テスト: 対象DB={args.db} ===")
    
    if args.db in ["mysql", "all"]: query_mysql()
    if args.db in ["postgres", "all"]: query_postgres()
    if args.db in ["mongodb", "all"]: query_mongodb()
    if args.db in ["redis", "all"]: query_redis()
    if args.db in ["surrealdb", "all"]: asyncio.run(query_surrealdb())