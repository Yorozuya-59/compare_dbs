import os
import time
import json
import random
import argparse
import asyncio
from datetime import datetime
import numpy as np
import polars as pl

# DB Drivers
import mysql.connector
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
import redis
from surrealdb import Surreal
import clickhouse_connect
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import duckdb
from questdb.ingress import Sender

# DB_CHOICES = ["mysql", "postgres", "mongodb", "redis", "surrealdb", "clickhouse", "timescaledb", "influxdb", "scylladb", "all"]
DB_CHOICES = ["mysql", "postgres", "mongodb", "redis", "surrealdb", "clickhouse", "timescaledb", "influxdb", "scylladb", "duckdb", "questdb", "starrocks", "all"]

# --- CSV出力用の共通関数 ---
def append_to_csv(csv_path, db_name, operation, records, exclude_strings, elapsed_time):
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write("Timestamp,Database,Operation,Records,Exclude_Strings,Elapsed_Time_sec\n")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed_str = f"{elapsed_time:.4f}" if elapsed_time is not None else "N/A"
        f.write(f"{timestamp},{db_name},{operation},{records},{exclude_strings},{elapsed_str}\n")

def generate_sensor_data(num_records, exclude_strings=False):
    print(f"{num_records:,}件のセンサデータを生成中... (文字列除外: {exclude_strings})")
    start_gen = time.time()
    base_time = datetime.now()
    device_ints = np.random.randint(1, 1001, size=num_records)
    rssi_vals = np.random.uniform(-100.0, -30.0, size=num_records).astype(np.float32)
    time_offsets = np.random.randint(0, 86400, size=num_records)
    
    df = pl.DataFrame({"device_id": device_ints, "rssi": rssi_vals, "offset_sec": time_offsets})
    df = df.with_columns(recorded_at=base_time - pl.duration(seconds=pl.col("offset_sec")))
    columns = ["device_id", "rssi", "recorded_at"]
    
    if not exclude_strings:
        mac_pool = [f"{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}" for _ in range(1000)]
        devices_df = pl.DataFrame({"device_id": np.arange(1, 1001), "mac_address": mac_pool})
        df = df.join(devices_df, on="device_id", how="left")
        columns = ["device_id", "mac_address", "rssi", "recorded_at"]
    
    records = df.select(columns).rows()
    print(f"データ生成完了: {time.time() - start_gen:.4f} 秒\n")
    return records, columns

def insert_mysql(data, columns):
    print("[MySQL] インサート準備中...")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "FLOAT", "recorded_at": "DATETIME"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS sensor_data (id INT AUTO_INCREMENT PRIMARY KEY, {col_defs})")
    cursor.execute("TRUNCATE TABLE sensor_data")
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    
    print("[MySQL] 計測開始...")
    start_time = time.time()
    for i in range(0, len(data), 50000):
        cursor.executemany(query, data[i:i + 50000])
    conn.commit()
    elapsed = time.time() - start_time
    print(f"✅ [MySQL] Insert完了: {elapsed:.4f} 秒\n")
    cursor.close(); conn.close()
    return elapsed

def insert_postgres(data, columns):
    print("[PostgreSQL] インサート準備中...")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "REAL", "recorded_at": "TIMESTAMP"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS sensor_data (id SERIAL PRIMARY KEY, {col_defs})")
    cursor.execute("TRUNCATE TABLE sensor_data")
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    
    print("[PostgreSQL] 計測開始...")
    start_time = time.time()
    psycopg2.extras.execute_batch(cursor, query, data, page_size=10000)
    conn.commit()
    elapsed = time.time() - start_time
    print(f"✅ [PostgreSQL] Insert完了: {elapsed:.4f} 秒\n")
    cursor.close(); conn.close()
    return elapsed

def insert_mongodb(data, columns):
    print("[MongoDB] インサート準備中...")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    collection = client["mydatabase"]["sensor_data"]
    collection.delete_many({})
    dict_data = [dict(zip(columns, row)) for row in data]
    
    print("[MongoDB] 計測開始...")
    start_time = time.time()
    collection.insert_many(dict_data)
    elapsed = time.time() - start_time
    print(f"✅ [MongoDB] Insert完了: {elapsed:.4f} 秒\n")
    client.close()
    return elapsed

def insert_redis(data, columns):
    print("[Redis] インサート準備中...")
    r = redis.Redis(host='redis', port=6379, password='rootpassword', decode_responses=True)
    r.delete('sensor_data')
    pipe = r.pipeline()
    
    print("[Redis] 計測開始...")
    start_time = time.time()
    for i, row in enumerate(data):
        row_dict = dict(zip(columns, row))
        row_dict['recorded_at'] = row_dict['recorded_at'].isoformat()
        pipe.rpush('sensor_data', json.dumps(row_dict))
        if (i + 1) % 10000 == 0: pipe.execute()
    pipe.execute()
    elapsed = time.time() - start_time
    print(f"✅ [Redis] Insert完了: {elapsed:.4f} 秒\n")
    r.close()
    return elapsed

async def insert_surrealdb(data, columns):
    print("[SurrealDB] インサート準備中...")
    dict_data = [dict(zip(columns, row)) for row in data]
    for d in dict_data: d['recorded_at'] = d['recorded_at'].isoformat()
    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        await db.query("REMOVE TABLE sensor_data")
        
        print("[SurrealDB] 計測開始...")
        start_time = time.time()
        for i in range(0, len(dict_data), 5000):
            await db.query("INSERT INTO sensor_data $data", {"data": dict_data[i:i+5000]})
        elapsed = time.time() - start_time
        print(f"✅ [SurrealDB] Insert完了: {elapsed:.4f} 秒\n")
        return elapsed

def insert_clickhouse(data, columns):
    print("[ClickHouse] インサート準備中...")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='rootpassword')
    type_map = {"device_id": "Int32", "mac_address": "String", "rssi": "Float32", "recorded_at": "DateTime64(3)"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    client.command("DROP TABLE IF EXISTS sensor_data")
    client.command(f"CREATE TABLE sensor_data ({col_defs}) ENGINE = MergeTree() ORDER BY (device_id, recorded_at)")
    
    print("[ClickHouse] 計測開始...")
    start_time = time.time()
    client.insert('sensor_data', data, column_names=columns)
    elapsed = time.time() - start_time
    print(f"✅ [ClickHouse] Insert完了: {elapsed:.4f} 秒\n")
    return elapsed

def insert_timescaledb(data, columns):
    print("[TimescaleDB] インサート準備中...")
    conn = psycopg2.connect(host="timescaledb", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "REAL", "recorded_at": "TIMESTAMP"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cursor.execute("DROP TABLE IF EXISTS sensor_data CASCADE;")
    cursor.execute(f"CREATE TABLE sensor_data ({col_defs});")
    cursor.execute("SELECT create_hypertable('sensor_data', 'recorded_at');")
    
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    
    print("[TimescaleDB] 計測開始...")
    start_time = time.time()
    psycopg2.extras.execute_batch(cursor, query, data, page_size=10000)
    conn.commit()
    elapsed = time.time() - start_time
    print(f"✅ [TimescaleDB] Insert完了: {elapsed:.4f} 秒\n")
    cursor.close(); conn.close()
    return elapsed

def insert_influxdb(data, columns):
    print("[InfluxDB] インサート準備中...")
    client = InfluxDBClient(url="http://influxdb:8086", token="benchmark_token_12345", org="benchmark_org")
    write_data = []
    for row in data:
        row_dict = dict(zip(columns, row))
        point = {"measurement": "sensor_data", "tags": {"device_id": str(row_dict["device_id"])},
                 "fields": {"rssi": float(row_dict["rssi"])}, "time": row_dict["recorded_at"]}
        if "mac_address" in row_dict:
            point["tags"]["mac_address"] = row_dict["mac_address"]
        write_data.append(point)
        
    print("[InfluxDB] 計測開始...")
    start_time = time.time()
    with client.write_api(write_options=WriteOptions(batch_size=50000, flush_interval=10000)) as write_api:
        write_api.write(bucket="benchmark_bucket", record=write_data)
    elapsed = time.time() - start_time
    print(f"✅ [InfluxDB] Insert完了: {elapsed:.4f} 秒\n")
    client.close()
    return elapsed

def insert_scylladb(data, columns):
    print("[ScyllaDB] インサート準備中...")
    cluster = Cluster(['scylladb'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS benchmark WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.set_keyspace('benchmark')
    
    type_map = {"device_id": "int", "mac_address": "text", "rssi": "float", "recorded_at": "timestamp"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    session.execute("DROP TABLE IF EXISTS sensor_data")
    session.execute(f"CREATE TABLE sensor_data ({col_defs}, PRIMARY KEY (device_id, recorded_at))")
    
    placeholders = ", ".join(["?"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    prepared = session.prepare(query)
    
    print("[ScyllaDB] 計測開始...")
    start_time = time.time()
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        execute_concurrent_with_args(session, prepared, data[i:i + chunk_size], concurrency=100)
    elapsed = time.time() - start_time
    print(f"✅ [ScyllaDB] Insert完了: {elapsed:.4f} 秒\n")
    cluster.shutdown()
    return elapsed

def insert_duckdb(data, columns):
    print("[DuckDB] インサート準備中...")
    conn = duckdb.connect('/app/results/benchmark.duckdb')
    type_map = {"device_id": "INTEGER", "mac_address": "VARCHAR", "rssi": "FLOAT", "recorded_at": "TIMESTAMP"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    conn.execute("DROP TABLE IF EXISTS sensor_data")
    conn.execute(f"CREATE TABLE sensor_data ({col_defs})")
    
    print("[DuckDB] 計測開始...")
    start_time = time.time()
    
    # --- 修正箇所：appenderの代わりに、より安全な executemany を使用 ---
    placeholders = ", ".join(["?"] * len(columns))
    query = f"INSERT INTO sensor_data VALUES ({placeholders})"
    conn.executemany(query, data)
    # -------------------------------------------------------------
    
    elapsed = time.time() - start_time
    print(f"✅ [DuckDB] Insert完了: {elapsed:.4f} 秒\n")
    conn.close()
    return elapsed

def insert_questdb(data, columns):
    print("[QuestDB] インサート準備中...")
    print("[QuestDB] 計測開始...")
    start_time = time.time()
    
    # --- 修正箇所：QuestDB 4.x 向けの from_conf 記法に変更 ---
    with Sender.from_conf('tcp::addr=questdb:9009;') as sender:
        for row in data:
            row_dict = dict(zip(columns, row))
            sender.row(
                'sensor_data',
                symbols={'device_id': str(row_dict['device_id']), 
                         **({'mac_address': row_dict['mac_address']} if 'mac_address' in row_dict else {})},
                columns={'rssi': float(row_dict['rssi'])},
                at=row_dict['recorded_at']
            )
        sender.flush()
    # ---------------------------------------------------------
        
    elapsed = time.time() - start_time
    print(f"✅ [QuestDB] Insert完了: {elapsed:.4f} 秒\n")
    return elapsed

def insert_starrocks(data, columns):
    print("[StarRocks] インサート準備中...")
    conn = mysql.connector.connect(host="starrocks", port=9030, user="root", password="")
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS benchmark")
    cursor.execute("USE benchmark")
    
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "FLOAT", "recorded_at": "DATETIME"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    cursor.execute("DROP TABLE IF EXISTS sensor_data")
    # StarRocksは分散処理のための分散キー(DISTRIBUTED BY)が必須
    cursor.execute(f"""
        CREATE TABLE sensor_data ({col_defs}) 
        DUPLICATE KEY(device_id, recorded_at) 
        DISTRIBUTED BY HASH(device_id) BUCKETS 4 
        PROPERTIES("replication_num" = "1")
    """)
    
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    
    print("[StarRocks] 計測開始...")
    start_time = time.time()
    for i in range(0, len(data), 50000):
        cursor.executemany(query, data[i:i + 50000])
    conn.commit()
    elapsed = time.time() - start_time
    print(f"✅ [StarRocks] Insert完了: {elapsed:.4f} 秒\n")
    cursor.close(); conn.close()
    return elapsed

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True, choices=DB_CHOICES)
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--exclude-strings", action="store_true")
    # CSVのパスを受け取るオプションを追加
    parser.add_argument("--csv", type=str, default="/app/results/benchmark_results.csv")
    args = parser.parse_args()

    print(f"=== ベンチマーク開始: 対象DB={args.db}, 件数={args.records:,}, 文字列除外={args.exclude_strings} ===")
    dummy_data, data_columns = generate_sensor_data(args.records, args.exclude_strings)
    
    # 実行とCSV追記
    if args.db in ["mysql", "all"]: 
        append_to_csv(args.csv, "mysql", "insert", args.records, args.exclude_strings, insert_mysql(dummy_data, data_columns))
    if args.db in ["postgres", "all"]: 
        append_to_csv(args.csv, "postgres", "insert", args.records, args.exclude_strings, insert_postgres(dummy_data, data_columns))
    if args.db in ["mongodb", "all"]: 
        append_to_csv(args.csv, "mongodb", "insert", args.records, args.exclude_strings, insert_mongodb(dummy_data, data_columns))
    if args.db in ["redis", "all"]: 
        append_to_csv(args.csv, "redis", "insert", args.records, args.exclude_strings, insert_redis(dummy_data, data_columns))
    if args.db in ["surrealdb", "all"]: 
        append_to_csv(args.csv, "surrealdb", "insert", args.records, args.exclude_strings, asyncio.run(insert_surrealdb(dummy_data, data_columns)))
    if args.db in ["clickhouse", "all"]: 
        append_to_csv(args.csv, "clickhouse", "insert", args.records, args.exclude_strings, insert_clickhouse(dummy_data, data_columns))
    if args.db in ["timescaledb", "all"]: 
        append_to_csv(args.csv, "timescaledb", "insert", args.records, args.exclude_strings, insert_timescaledb(dummy_data, data_columns))
    if args.db in ["influxdb", "all"]: 
        append_to_csv(args.csv, "influxdb", "insert", args.records, args.exclude_strings, insert_influxdb(dummy_data, data_columns))
    if args.db in ["scylladb", "all"]: 
        append_to_csv(args.csv, "scylladb", "insert", args.records, args.exclude_strings, insert_scylladb(dummy_data, data_columns))
    if args.db in ["duckdb", "all"]: 
        append_to_csv(args.csv, "duckdb", "insert", args.records, args.exclude_strings, insert_duckdb(dummy_data, data_columns))
    if args.db in ["questdb", "all"]: 
        append_to_csv(args.csv, "questdb", "insert", args.records, args.exclude_strings, insert_questdb(dummy_data, data_columns))
    if args.db in ["starrocks", "all"]: 
        append_to_csv(args.csv, "starrocks", "insert", args.records, args.exclude_strings, insert_starrocks(dummy_data, data_columns))