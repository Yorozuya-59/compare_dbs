import time
import json
import random
import argparse
import asyncio
from datetime import datetime
import numpy as np
import polars as pl

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

DB_CHOICES = ["mysql", "postgres", "mongodb", "redis", "surrealdb", "clickhouse", "timescaledb", "influxdb", "scylladb", "all"]

def generate_sensor_data(num_records, exclude_strings=False):
    print(f"{num_records:,}件のセンサデータを生成中... (文字列除外: {exclude_strings})")
    start_gen = time.time()
    
    base_time = datetime.now()
    
    # 1. 基本となる数値データの生成 (Device ID: 1〜1000, RSSI: -100.0〜-30.0)
    device_ints = np.random.randint(1, 1001, size=num_records)
    rssi_vals = np.random.uniform(-100.0, -30.0, size=num_records).astype(np.float32)
    time_offsets = np.random.randint(0, 86400, size=num_records)
    
    df = pl.DataFrame({
        "device_id": device_ints,
        "rssi": rssi_vals,
        "offset_sec": time_offsets
    })
    
    # 2. タイムスタンプの計算
    df = df.with_columns(
        recorded_at=base_time - pl.duration(seconds=pl.col("offset_sec"))
    )
    
    columns = ["device_id", "rssi", "recorded_at"]
    
    # 3. 文字列(MACアドレス)の動的追加
    if not exclude_strings:
        # 1000台分の固定MACアドレスマスタを作成
        mac_pool = [
            f"{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}:"
            f"{random.randint(0,255):02X}:{random.randint(0,255):02X}:{random.randint(0,255):02X}"
            for _ in range(1000)
        ]
        devices_df = pl.DataFrame({
            "device_id": np.arange(1, 1001),
            "mac_address": mac_pool
        })
        # デバイスIDをキーにして高速結合
        df = df.join(devices_df, on="device_id", how="left")
        
        # カラム順序の再定義（DBのスキーマ順序に合わせる）
        columns = ["device_id", "mac_address", "rssi", "recorded_at"]
    
    # 必要なカラムだけを抽出し、タプルのリストに変換
    records = df.select(columns).rows()
    
    end_gen = time.time()
    print(f"データ生成完了: {end_gen - start_gen:.4f} 秒\n")
    
    return records, columns

# --- 1. MySQL ---
def insert_mysql(data, columns):
    print("[MySQL] インサート準備中...")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    
    # 動的スキーマ生成
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "FLOAT", "recorded_at": "DATETIME"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS sensor_data (id INT AUTO_INCREMENT PRIMARY KEY, {col_defs})")
    cursor.execute("TRUNCATE TABLE sensor_data")
    
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    
    print("[MySQL] 計測開始...")
    start_time = time.time()
    
    # --- ここから修正：データを分割（チャンク）してインサート ---
    chunk_size = 50000  # 5万件ずつ送信
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        cursor.executemany(query, chunk)
    
    conn.commit()
    # --- 修正ここまで ---
    
    end_time = time.time()
    
    print(f"✅ [MySQL] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    cursor.close()
    conn.close()

# --- 2. PostgreSQL ---
def insert_postgres(data, columns):
    print("[PostgreSQL] インサート準備中...")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    # 動的スキーマ生成
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "REAL", "recorded_at": "TIMESTAMP"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS sensor_data (id SERIAL PRIMARY KEY, {col_defs})")
    cursor.execute("TRUNCATE TABLE sensor_data")
    
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    
    print("[PostgreSQL] 計測開始...")
    start_time = time.time()
    psycopg2.extras.execute_batch(cursor, query, data)
    conn.commit()
    end_time = time.time()
    
    print(f"✅ [PostgreSQL] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    cursor.close()
    conn.close()

# --- 3. MongoDB ---
def insert_mongodb(data, columns):
    print("[MongoDB] インサート準備中...")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    db = client["mydatabase"]
    collection = db["sensor_data"]
    collection.delete_many({})
    
    # 動的に辞書型へ変換
    dict_data = [dict(zip(columns, row)) for row in data]
    
    print("[MongoDB] 計測開始...")
    start_time = time.time()
    collection.insert_many(dict_data)
    end_time = time.time()
    
    print(f"✅ [MongoDB] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    client.close()

# --- 4. Redis ---
def insert_redis(data, columns):
    print("[Redis] インサート準備中...")
    r = redis.Redis(host='redis', port=6379, password='rootpassword', decode_responses=True)
    r.delete('sensor_data')
    pipe = r.pipeline()
    
    print("[Redis] 計測開始...")
    start_time = time.time()
    for i, row in enumerate(data):
        row_dict = dict(zip(columns, row))
        # 日時オブジェクトを文字列化
        row_dict['recorded_at'] = row_dict['recorded_at'].isoformat()
        
        pipe.rpush('sensor_data', json.dumps(row_dict))
        if (i + 1) % 10000 == 0:
            pipe.execute()
            
    pipe.execute()
    end_time = time.time()
    print(f"✅ [Redis] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    r.close()

# --- 5. SurrealDB ---
async def insert_surrealdb(data, columns):
    print("[SurrealDB] インサート準備中...")
    
    dict_data = []
    for row in data:
        row_dict = dict(zip(columns, row))
        row_dict['recorded_at'] = row_dict['recorded_at'].isoformat()
        dict_data.append(row_dict)

    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        await db.query("REMOVE TABLE sensor_data")
        
        print("[SurrealDB] 計測開始...")
        start_time = time.time()
        
        chunk_size = 5000
        for i in range(0, len(dict_data), chunk_size):
            chunk = dict_data[i:i+chunk_size]
            await db.query("INSERT INTO sensor_data $data", {"data": chunk})
            
        end_time = time.time()
        print(f"✅ [SurrealDB] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")

def insert_clickhouse(data, columns):
    print("[ClickHouse] インサート準備中...")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='rootpassword')    
    type_map = {"device_id": "Int32", "mac_address": "String", "rssi": "Float32", "recorded_at": "DateTime64(3)"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    client.command("DROP TABLE IF EXISTS sensor_data")
    # ClickHouseはMergeTreeエンジンとORDER BY(主キー)が必須
    client.command(f"CREATE TABLE sensor_data ({col_defs}) ENGINE = MergeTree() ORDER BY (device_id, recorded_at)")
    
    print("[ClickHouse] 計測開始...")
    start_time = time.time()
    # ネイティブのバルクインサート
    client.insert('sensor_data', data, column_names=columns)
    print(f"✅ [ClickHouse] Insert完了: {time.time() - start_time:.4f} 秒\n")

def insert_timescaledb(data, columns):
    print("[TimescaleDB] インサート準備中...")
    # ホスト名が timescaledb になるだけで、ドライバは PostgreSQL と同じ
    conn = psycopg2.connect(host="timescaledb", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    
    type_map = {"device_id": "INT", "mac_address": "VARCHAR(17)", "rssi": "REAL", "recorded_at": "TIMESTAMP"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cursor.execute("DROP TABLE IF EXISTS sensor_data CASCADE;")
    cursor.execute(f"CREATE TABLE sensor_data ({col_defs});")
    # ハイパーテーブル（時系列データ用パーティショニング）の作成
    cursor.execute("SELECT create_hypertable('sensor_data', 'recorded_at');")
    
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    
    print("[TimescaleDB] 計測開始...")
    start_time = time.time()
    psycopg2.extras.execute_batch(cursor, query, data, page_size=10000)
    conn.commit()
    print(f"✅ [TimescaleDB] Insert完了: {time.time() - start_time:.4f} 秒\n")
    cursor.close(); conn.close()

def insert_influxdb(data, columns):
    print("[InfluxDB] インサート準備中...")
    client = InfluxDBClient(url="http://influxdb:8086", token="benchmark_token_12345", org="benchmark_org")
    
    # InfluxDBのLine Protocol向けに辞書を作成（device_idとmac_addressはTag、rssiはFieldとする）
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
    # InfluxDBクライアントのバッチ処理機能を使用
    with client.write_api(write_options=WriteOptions(batch_size=50000, flush_interval=10000)) as write_api:
        write_api.write(bucket="benchmark_bucket", record=write_data)
    
    print(f"✅ [InfluxDB] Insert完了: {time.time() - start_time:.4f} 秒\n")
    client.close()

def insert_scylladb(data, columns):
    print("[ScyllaDB] インサート準備中... (起動に時間がかかる場合があります)")
    cluster = Cluster(['scylladb'])
    session = cluster.connect()
    
    session.execute("CREATE KEYSPACE IF NOT EXISTS benchmark WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.set_keyspace('benchmark')
    
    type_map = {"device_id": "int", "mac_address": "text", "rssi": "float", "recorded_at": "timestamp"}
    col_defs = ", ".join([f"{col} {type_map[col]}" for col in columns])
    
    session.execute("DROP TABLE IF EXISTS sensor_data")
    # Cassandra系はPRIMARY KEYの設計が命。device_idをパーティションキー、recorded_atをクラスタリングキーにする
    session.execute(f"CREATE TABLE sensor_data ({col_defs}, PRIMARY KEY (device_id, recorded_at))")
    
    placeholders = ", ".join(["?"] * len(columns))
    query = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({placeholders})"
    prepared = session.prepare(query)
    
    print("[ScyllaDB] 計測開始...")
    start_time = time.time()
    
    # 並行書き込み処理
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        execute_concurrent_with_args(session, prepared, chunk, concurrency=100)
        
    print(f"✅ [ScyllaDB] Insert完了: {time.time() - start_time:.4f} 秒\n")
    cluster.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor Data DB Benchmark Tool")
    parser.add_argument("--db", type=str, required=True, choices=DB_CHOICES, 
                        help="ベンチマーク対象のデータベース (例: mysql, postgres, mongodb, redis, surrealdb, clickhouse, timescaledb, influxdb, scylladb, all)")
    parser.add_argument("--records", type=int, default=100000, 
                        help="生成するセンサデータの件数 (デフォルト: 100,000)")
    # --- 文字列データを除外するフラグを追加 ---
    parser.add_argument("--exclude-strings", action="store_true", 
                        help="MACアドレスなどの文字列カラムをデータから除外して評価する")
    args = parser.parse_args()

    print(f"=== ベンチマーク開始: 対象DB={args.db}, 件数={args.records:,}, 文字列除外={args.exclude_strings} ===")
    
    # データの生成（タプルのリストと、カラム名のリストを受け取る）
    dummy_data, data_columns = generate_sensor_data(args.records, args.exclude_strings)
    
    if args.db in ["mysql", "all"]:
        insert_mysql(dummy_data, data_columns)
        
    if args.db in ["postgres", "all"]:
        insert_postgres(dummy_data, data_columns)
        
    if args.db in ["mongodb", "all"]:
        insert_mongodb(dummy_data, data_columns)
        
    if args.db in ["redis", "all"]:
        insert_redis(dummy_data, data_columns)
        
    if args.db in ["surrealdb", "all"]:
        asyncio.run(insert_surrealdb(dummy_data, data_columns))
    
    if args.db in ["clickhouse", "all"]:
        insert_clickhouse(dummy_data, data_columns)
        
    if args.db in ["timescaledb", "all"]:
        insert_timescaledb(dummy_data, data_columns)
        
    if args.db in ["influxdb", "all"]:
        insert_influxdb(dummy_data, data_columns)
        
    if args.db in ["scylladb", "all"]:
        insert_scylladb(dummy_data, data_columns)
        
    print("=== 全てのベンチマークが完了しました ===")