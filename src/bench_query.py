import os
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
BASE_TIME = datetime.now()
RANGE_START = BASE_TIME - timedelta(hours=12)
RANGE_END = BASE_TIME - timedelta(hours=11)

DB_CHOICES = ["mysql", "postgres", "mongodb", "redis", "surrealdb", "clickhouse", "timescaledb", "influxdb", "scylladb", "all"]

def print_result(name, elapsed):
    if elapsed is not None:
        print(f"  ├ {name}: {elapsed:.4f} 秒")

# --- CSV出力用の共通関数 ---
def append_to_csv(csv_path, db_name, operation, records, exclude_strings, elapsed_time):
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write("Timestamp,Database,Operation,Records,Exclude_Strings,Elapsed_Time_sec\n")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed_str = f"{elapsed_time:.4f}" if elapsed_time is not None else "N/A"
        f.write(f"{timestamp},{db_name},{operation},{records},{exclude_strings},{elapsed_str}\n")

# --- 1. MySQL ---
def query_mysql():
    print("[MySQL]")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time(); cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,)); cursor.fetchall(); q1 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id"); cursor.fetchall(); q2 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END)); cursor.fetchall(); q3 = time.time() - t0
    
    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    conn.close(); return q1, q2, q3

# --- 2. PostgreSQL ---
def query_postgres():
    print("[PostgreSQL]")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time(); cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,)); cursor.fetchall(); q1 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id"); cursor.fetchall(); q2 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END)); cursor.fetchall(); q3 = time.time() - t0

    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    conn.close(); return q1, q2, q3

# --- 3. MongoDB ---
def query_mongodb():
    print("[MongoDB]")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    collection = client["mydatabase"]["sensor_data"]
    
    t0 = time.time(); list(collection.find({"device_id": TARGET_DEVICE_ID}).sort("recorded_at", -1).limit(10)); q1 = time.time() - t0
    t0 = time.time(); list(collection.aggregate([{"$group": {"_id": "$device_id", "count": {"$sum": 1}, "avg_rssi": {"$avg": "$rssi"}}}])); q2 = time.time() - t0
    t0 = time.time(); collection.count_documents({"recorded_at": {"$gte": RANGE_START, "$lte": RANGE_END}}); q3 = time.time() - t0
    
    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    client.close(); return q1, q2, q3

# --- 4. Redis ---
def query_redis():
    print("[Redis]")
    print("  ├ 特定デバイス検索: Not Supported")
    print("  ├ 全デバイス別集計: Not Supported")
    print("  └ 時間範囲検索(1h): Not Supported\n")
    return None, None, None

# --- 5. SurrealDB ---
async def query_surrealdb():
    print("[SurrealDB]")
    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        
        t0 = time.time(); await db.query("SELECT * FROM sensor_data WHERE device_id = $id ORDER BY recorded_at DESC LIMIT 10", {"id": TARGET_DEVICE_ID}); q1 = time.time() - t0
        t0 = time.time(); await db.query("SELECT device_id, count(), math::mean(rssi) AS avg_rssi FROM sensor_data GROUP BY device_id"); q2 = time.time() - t0
        t0 = time.time(); await db.query("SELECT count() FROM sensor_data WHERE recorded_at >= $start AND recorded_at <= $end GROUP ALL", {"start": RANGE_START.isoformat(), "end": RANGE_END.isoformat()}); q3 = time.time() - t0
        
        print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
        return q1, q2, q3

# --- 6. ClickHouse ---
def query_clickhouse():
    print("[ClickHouse]")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='rootpassword')
    
    t0 = time.time(); client.query(f"SELECT * FROM sensor_data WHERE device_id = {TARGET_DEVICE_ID} ORDER BY recorded_at DESC LIMIT 10"); q1 = time.time() - t0
    t0 = time.time(); client.query("SELECT device_id, count(*), avg(rssi) FROM sensor_data GROUP BY device_id"); q2 = time.time() - t0
    t0 = time.time(); client.query(f"SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= '{RANGE_START.strftime('%Y-%m-%d %H:%M:%S')}' AND recorded_at <= '{RANGE_END.strftime('%Y-%m-%d %H:%M:%S')}'"); q3 = time.time() - t0
    
    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    return q1, q2, q3

# --- 7. TimescaleDB ---
def query_timescaledb():
    print("[TimescaleDB]")
    conn = psycopg2.connect(host="timescaledb", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    t0 = time.time(); cursor.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,)); cursor.fetchall(); q1 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT device_id, COUNT(*), AVG(rssi) FROM sensor_data GROUP BY device_id"); cursor.fetchall(); q2 = time.time() - t0
    t0 = time.time(); cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s", (RANGE_START, RANGE_END)); cursor.fetchall(); q3 = time.time() - t0

    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    conn.close(); return q1, q2, q3

# --- 8. InfluxDB ---
def query_influxdb():
    print("[InfluxDB] (Flux クエリを使用)")
    client = InfluxDBClient(url="http://influxdb:8086", token="benchmark_token_12345", org="benchmark_org", timeout=60000)
    query_api = client.query_api()
    
    t0 = time.time(); query_api.query(f'from(bucket:"benchmark_bucket") |> range(start: 0) |> filter(fn: (r) => r.device_id == "{TARGET_DEVICE_ID}") |> sort(columns: ["_time"], desc: true) |> limit(n: 10)'); q1 = time.time() - t0
    t0 = time.time(); query_api.query('from(bucket:"benchmark_bucket") |> range(start: 0) |> filter(fn: (r) => r._field == "rssi") |> group(columns: ["device_id"]) |> mean(column: "_value")'); q2 = time.time() - t0
    
    start_str = RANGE_START.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = RANGE_END.strftime('%Y-%m-%dT%H:%M:%SZ')
    t0 = time.time(); query_api.query(f'from(bucket:"benchmark_bucket") |> range(start: {start_str}, stop: {end_str}) |> filter(fn: (r) => r._field == "rssi") |> count()'); q3 = time.time() - t0
    
    print_result("特定デバイス検索", q1); print_result("全デバイス別集計", q2); print_result("時間範囲検索(1h)", q3); print("")
    client.close(); return q1, q2, q3

# --- 9. ScyllaDB ---
def query_scylladb():
    print("[ScyllaDB]")
    cluster = Cluster(['scylladb'])
    session = cluster.connect('benchmark')
    session.default_timeout = 60.0
    
    t0 = time.time(); session.execute("SELECT * FROM sensor_data WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 10", (TARGET_DEVICE_ID,)); q1 = time.time() - t0
    
    q2 = None
    print("  ├ 全デバイス別集計: Not Supported")

    t0 = time.time()
    try:
        session.execute("SELECT COUNT(*) FROM sensor_data WHERE recorded_at >= %s AND recorded_at <= %s ALLOW FILTERING", (RANGE_START, RANGE_END))
        q3 = time.time() - t0
        print_result("時間範囲検索(1h)", q3)
    except Exception as e:
        q3 = None
        print("  └ 時間範囲検索(1h): Error (タイムアウト/全スキャン不可)")
    
    print("")
    cluster.shutdown(); return q1, q2, q3

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True, choices=DB_CHOICES)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--exclude-strings", action="store_true")
    parser.add_argument("--csv", type=str, default="/app/results/benchmark_results.csv")
    args = parser.parse_args()
    
    print(f"=== 検索・集計テスト: 対象DB={args.db} ===\n")
    
    def process_and_save(db_func, db_name):
        q1, q2, q3 = db_func
        append_to_csv(args.csv, db_name, "query_point", args.records, args.exclude_strings, q1)
        append_to_csv(args.csv, db_name, "query_agg", args.records, args.exclude_strings, q2)
        append_to_csv(args.csv, db_name, "query_range", args.records, args.exclude_strings, q3)

    if args.db in ["mysql", "all"]: process_and_save(query_mysql(), "mysql")
    if args.db in ["postgres", "all"]: process_and_save(query_postgres(), "postgres")
    if args.db in ["mongodb", "all"]: process_and_save(query_mongodb(), "mongodb")
    if args.db in ["redis", "all"]: process_and_save(query_redis(), "redis")
    if args.db in ["surrealdb", "all"]: process_and_save(asyncio.run(query_surrealdb()), "surrealdb")
    if args.db in ["clickhouse", "all"]: process_and_save(query_clickhouse(), "clickhouse")
    if args.db in ["timescaledb", "all"]: process_and_save(query_timescaledb(), "timescaledb")
    if args.db in ["influxdb", "all"]: process_and_save(query_influxdb(), "influxdb")
    if args.db in ["scylladb", "all"]: process_and_save(query_scylladb(), "scylladb")