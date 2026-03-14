import time
import argparse
import asyncio
from datetime import datetime, timedelta

import mysql.connector
import psycopg2
from pymongo import MongoClient
import redis
from surrealdb import Surreal
import clickhouse_connect
from influxdb_client import InfluxDBClient
from cassandra.cluster import Cluster

TARGET_DEVICE_ID = 500

# 時間範囲検索のための時刻を生成 (直近12時間前〜11時間前の1時間分を抽出)
# データ生成スクリプトは現在時刻から過去24時間のランダム生成のため、必ずデータがヒットします
BASE_TIME = datetime.now()
RANGE_START = BASE_TIME - timedelta(hours=12)
RANGE_END = BASE_TIME - timedelta(hours=11)

DB_CHOICES = ["mysql", "postgres", "mongodb", "redis", "surrealdb", "clickhouse", "timescaledb", "influxdb", "scylladb", "all"]

def print_result(name, elapsed):
    print(f"  ├ {name}: {elapsed:.4f} 秒")

# --- 1. MySQL ---
def query_mysql():
    print("[MySQL]")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time()
    cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    cursor.fetchall()
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id")
    cursor.fetchall()
    print_result("全デバイス別集計", time.time() - t0)
    
    t0 = time.time()
    cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END))
    cursor.fetchall()
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")
    conn.close()

# --- 2. PostgreSQL ---
def query_postgres():
    print("[PostgreSQL]")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time()
    cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    cursor.fetchall()
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id")
    cursor.fetchall()
    print_result("全デバイス別集計", time.time() - t0)

    t0 = time.time()
    cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END))
    cursor.fetchall()
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")
    conn.close()

# --- 3. MongoDB ---
def query_mongodb():
    print("[MongoDB]")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    collection = client["mydatabase"]["sensor_data"]
    
    t0 = time.time()
    list(collection.find({"device_id": TARGET_DEVICE_ID}).sort("recorded_at", -1).limit(10))
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    list(collection.aggregate([{"$group": {"_id": "$device_id", "count": {"$sum": 1}, "avg_rssi": {"$avg": "$rssi"}}}]))
    print_result("全デバイス別集計", time.time() - t0)
    
    t0 = time.time()
    list(collection.count_documents({"recorded_at": {"$gte": RANGE_START, "$lte": RANGE_END}}))
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")
    client.close()

# --- 4. Redis ---
def query_redis():
    print("[Redis]")
    print_result("特定デバイス検索", 0)
    print("      └ Not Supported (List構造のためフルスキャンが必要)")
    print_result("全デバイス別集計", 0)
    print("      └ Not Supported (RDB的なGroup By機能なし)")
    print_result("時間範囲検索(1h)", 0)
    print("      └ Not Supported\n")

# --- 5. SurrealDB ---
async def query_surrealdb():
    print("[SurrealDB]")
    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        
        t0 = time.time()
        await db.query("SELECT * FROM sensor_data WHERE device_id = $id ORDER BY recorded_at DESC LIMIT 10", {"id": TARGET_DEVICE_ID})
        print_result("特定デバイス検索", time.time() - t0)
        
        t0 = time.time()
        await db.query("SELECT device_id, count(), math::mean(rssi) AS avg_rssi FROM sensor_data GROUP BY device_id")
        print_result("全デバイス別集計", time.time() - t0)

        t0 = time.time()
        await db.query("SELECT count() FROM sensor_data WHERE recorded_at >= $start AND recorded_at <= $end", {"start": RANGE_START.isoformat(), "end": RANGE_END.isoformat()})
        print_result("時間範囲検索(1h)", time.time() - t0)
        print("")

# --- 6. ClickHouse ---
def query_clickhouse():
    print("[ClickHouse]")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    
    t0 = time.time()
    client.query(f"SELECT * FROM sensor_data WHERE device_id = {TARGET_DEVICE_ID} ORDER BY recorded_at DESC LIMIT 10")
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    client.query("SELECT device_id, count(*), avg(rssi) FROM sensor_data GROUP BY device_id")
    print_result("全デバイス別集計", time.time() - t0)
    
    t0 = time.time()
    client.query(f"SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= '{RANGE_START.strftime('%Y-%m-%d %H:%M:%S')}' AND recorded_at <= '{RANGE_END.strftime('%Y-%m-%d %H:%M:%S')}'")
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")

# --- 7. TimescaleDB ---
def query_timescaledb():
    print("[TimescaleDB]")
    conn = psycopg2.connect(host="timescaledb", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time()
    cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    cursor.fetchall()
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id")
    cursor.fetchall()
    print_result("全デバイス別集計", time.time() - t0)

    t0 = time.time()
    cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END))
    cursor.fetchall()
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")
    conn.close()

# --- 8. InfluxDB ---
def query_influxdb():
    print("[InfluxDB] (Flux クエリを使用)")
    client = InfluxDBClient(url="http://influxdb:8086", token="benchmark_token_12345", org="benchmark_org", timeout=60000)
    query_api = client.query_api()
    
    t0 = time.time()
    q1 = f'from(bucket:"benchmark_bucket") |> range(start: 0) |> filter(fn: (r) => r.device_id == "{TARGET_DEVICE_ID}") |> sort(columns: ["_time"], desc: true) |> limit(n: 10)'
    query_api.query(q1)
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    q2 = 'from(bucket:"benchmark_bucket") |> range(start: 0) |> filter(fn: (r) => r._field == "rssi") |> group(columns: ["device_id"]) |> mean(column: "_value")'
    query_api.query(q2)
    print_result("全デバイス別集計", time.time() - t0)
    
    t0 = time.time()
    # InfluxDBはRFC3339の時刻形式を要求
    start_str = RANGE_START.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = RANGE_END.strftime('%Y-%m-%dT%H:%M:%SZ')
    q3 = f'from(bucket:"benchmark_bucket") |> range(start: {start_str}, stop: {end_str}) |> filter(fn: (r) => r._field == "rssi") |> count()'
    query_api.query(q3)
    print_result("時間範囲検索(1h)", time.time() - t0)
    print("")
    client.close()

# --- 9. ScyllaDB ---
def query_scylladb():
    print("[ScyllaDB]")
    cluster = Cluster(['scylladb'])
    session = cluster.connect('benchmark')
    session.default_timeout = 60.0 # タイムアウトを長めに設定
    
    t0 = time.time()
    # ScyllaDBはパーティションキー(device_id)を指定すれば爆速
    session.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,))
    print_result("特定デバイス検索", time.time() - t0)
    
    t0 = time.time()
    print_result("全デバイス別集計", 0)
    print("      └ Not Supported (Cassandraモデルは全パーティションをまたぐGROUP BYをサポートしません)")

    t0 = time.time()
    try:
        # パーティションキーを指定せずに時間だけで検索するにはALLOW FILTERINGが必要（非常に遅い）
        session.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s ALLOW FILTERING", (RANGE_START, RANGE_END))
        print_result("時間範囲検索(1h)", time.time() - t0)
        print("      └ 警告: ALLOW FILTERING はアンチパターンであり、データ量が多いとタイムアウトします")
    except Exception as e:
        print_result("時間範囲検索(1h)", time.time() - t0)
        print("      └ Error: タイムアウト、または全スキャン不可")
    
    print("")
    cluster.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True, choices=DB_CHOICES)
    args = parser.parse_args()
    
    print(f"=== 検索・集計テスト: 対象DB={args.db} ===\n")
    
    if args.db in ["mysql", "all"]: query_mysql()
    if args.db in ["postgres", "all"]: query_postgres()
    if args.db in ["mongodb", "all"]: query_mongodb()
    if args.db in ["redis", "all"]: query_redis()
    if args.db in ["surrealdb", "all"]: asyncio.run(query_surrealdb())
    if args.db in ["clickhouse", "all"]: query_clickhouse()
    if args.db in ["timescaledb", "all"]: query_timescaledb()
    if args.db in ["influxdb", "all"]: query_influxdb()
    if args.db in ["scylladb", "all"]: query_scylladb()