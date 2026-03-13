import time
import json
import argparse
import asyncio
from datetime import datetime
import numpy as np
import polars as pl

# データベースドライバー
import mysql.connector
import psycopg2
import psycopg2.extras  # PostgreSQLの高速バルクインサート用
from pymongo import MongoClient
import redis
from surrealdb import Surreal

def generate_dummy_data_fast(num_records):
    print(f"{num_records:,}件のダミーデータを numpy/polars で生成中...")
    start_gen = time.time()
    
    base_time = datetime.now()
    
    device_ints = np.random.randint(1, 1001, size=num_records)
    x_coords = np.random.uniform(0.0, 100.0, size=num_records).astype(np.float32)
    y_coords = np.random.uniform(0.0, 100.0, size=num_records).astype(np.float32)
    time_offsets = np.random.randint(0, 86400, size=num_records)
    
    df = pl.DataFrame({
        "device_int": device_ints,
        "x_coord": x_coords,
        "y_coord": y_coords,
        "offset_sec": time_offsets
    })
    
    df_final = df.with_columns(
        device_id=pl.format("device_{}", pl.col("device_int").cast(pl.String).str.zfill(4)),
        recorded_at=base_time - pl.duration(seconds=pl.col("offset_sec"))
    ).select(["device_id", "x_coord", "y_coord", "recorded_at"])
    
    # RDBMS向けにタプルのリストとして出力
    records = df_final.rows()
    
    end_gen = time.time()
    print(f"データ生成完了: {end_gen - start_gen:.4f} 秒\n")
    
    return records

# --- 1. MySQL ---
def insert_mysql(data):
    print("[MySQL] インサート準備中...")
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracking_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            device_id VARCHAR(50), x_coord FLOAT, y_coord FLOAT, recorded_at DATETIME
        )
    """)
    cursor.execute("TRUNCATE TABLE tracking_data")
    
    query = "INSERT INTO tracking_data (device_id, x_coord, y_coord, recorded_at) VALUES (%s, %s, %s, %s)"
    
    print("[MySQL] 計測開始...")
    start_time = time.time()
    cursor.executemany(query, data)
    conn.commit()
    end_time = time.time()
    
    print(f"✅ [MySQL] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    cursor.close()
    conn.close()

# --- 2. PostgreSQL ---
def insert_postgres(data):
    print("[PostgreSQL] インサート準備中...")
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracking_data (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(50), x_coord REAL, y_coord REAL, recorded_at TIMESTAMP
        )
    """)
    cursor.execute("TRUNCATE TABLE tracking_data")
    
    query = "INSERT INTO tracking_data (device_id, x_coord, y_coord, recorded_at) VALUES (%s, %s, %s, %s)"
    
    print("[PostgreSQL] 計測開始...")
    start_time = time.time()
    # psycopg2の標準executemanyは1件ずつの処理になり遅いため、execute_batchを使用
    psycopg2.extras.execute_batch(cursor, query, data)
    conn.commit()
    end_time = time.time()
    
    print(f"✅ [PostgreSQL] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    cursor.close()
    conn.close()

# --- 3. MongoDB ---
def insert_mongodb(data):
    print("[MongoDB] インサート準備中...")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    db = client["mydatabase"]
    collection = db["tracking_data"]
    collection.delete_many({}) # 初期化
    
    # Mongo用に辞書型に変換（計測時間には含めない）
    dict_data = [
        {"device_id": d[0], "x_coord": float(d[1]), "y_coord": float(d[2]), "recorded_at": d[3]} 
        for d in data
    ]
    
    print("[MongoDB] 計測開始...")
    start_time = time.time()
    collection.insert_many(dict_data)
    end_time = time.time()
    
    print(f"✅ [MongoDB] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    client.close()

# --- 4. Redis ---
def insert_redis(data):
    print("[Redis] インサート準備中...")
    # decode_responses=Trueで文字列として扱う
    r = redis.Redis(host='redis', port=6379, password='rootpassword', decode_responses=True)
    r.delete('tracking_data')
    
    # ネットワークオーバーヘッドを減らすためにパイプラインを使用
    pipe = r.pipeline()
    
    print("[Redis] 計測開始...")
    start_time = time.time()
    # List型にJSON文字列としてPushしていくアプローチ
    for i, d in enumerate(data):
        payload = json.dumps({"d": d[0], "x": float(d[1]), "y": float(d[2]), "t": d[3].isoformat()})
        pipe.rpush('tracking_data', payload)
        
        # 1万件ごとにネットワーク送信（メモリ枯渇防止）
        if (i + 1) % 10000 == 0:
            pipe.execute()
            
    # 残りのデータを送信
    pipe.execute()
    end_time = time.time()
    
    print(f"✅ [Redis] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")
    r.close()

# --- 5. SurrealDB (非同期処理) ---
async def insert_surrealdb(data):
    print("[SurrealDB] インサート準備中...")
    
    # SurrealDB用に辞書型に変換し、日時は文字列(ISO8601)にしておく
    dict_data = [
        {"device_id": d[0], "x_coord": float(d[1]), "y_coord": float(d[2]), "recorded_at": d[3].isoformat()} 
        for d in data
    ]

    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})
        await db.use("benchmark", "benchmark")
        await db.query("REMOVE TABLE tracking_data")
        
        print("[SurrealDB] 計測開始...")
        start_time = time.time()
        
        # SurrealDBは1リクエストのペイロード上限があるため、チャンクに分割してインサート
        chunk_size = 5000
        for i in range(0, len(dict_data), chunk_size):
            chunk = dict_data[i:i+chunk_size]
            await db.query("INSERT INTO tracking_data $data", {"data": chunk})
            
        end_time = time.time()
        print(f"✅ [SurrealDB] {len(data):,}件のInsert完了: {end_time - start_time:.4f} 秒\n")


if __name__ == "__main__":
    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description="Database Benchmark Tool")
    parser.add_argument("--db", type=str, required=True, 
                        choices=["mysql", "postgres", "mongodb", "redis", "surrealdb", "all"],
                        help="ベンチマークを実行するデータベースを指定します")
    parser.add_argument("--records", type=int, default=100000, 
                        help="生成するダミーデータの件数 (デフォルト: 100,000)")
    args = parser.parse_args()

    print(f"=== ベンチマーク開始: 対象DB={args.db}, データ件数={args.records:,} ===")
    
    # データ生成
    dummy_data = generate_dummy_data_fast(args.records)
    
    # 選択されたDBの関数を実行
    if args.db in ["mysql", "all"]:
        insert_mysql(dummy_data)
        
    if args.db in ["postgres", "all"]:
        insert_postgres(dummy_data)
        
    if args.db in ["mongodb", "all"]:
        insert_mongodb(dummy_data)
        
    if args.db in ["redis", "all"]:
        insert_redis(dummy_data)
        
    if args.db in ["surrealdb", "all"]:
        # SurrealDBは非同期関数のため asyncio で実行
        asyncio.run(insert_surrealdb(dummy_data))
        
    print("=== 全てのベンチマークが完了しました ===")