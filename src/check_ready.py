import argparse
import sys
import asyncio

# 各DBのドライバ
import mysql.connector
import psycopg2
from pymongo import MongoClient
import redis
from surrealdb import Surreal
import clickhouse_connect
from influxdb_client import InfluxDBClient
from cassandra.cluster import Cluster
import duckdb

def check_mysql():
    conn = mysql.connector.connect(host="mysql", user="root", password="rootpassword", database="mydatabase")
    conn.close()

def check_postgres():
    conn = psycopg2.connect(host="postgres", user="postgres", password="rootpassword", dbname="mydatabase")
    conn.close()

def check_mongodb():
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    client.admin.command('ping')
    client.close()

def check_redis():
    r = redis.Redis(host='redis', port=6379, password='rootpassword')
    r.ping()
    r.close()

async def check_surrealdb():
    async with Surreal("ws://surrealdb:8000/rpc") as db:
        await db.signin({"user": "root", "pass": "rootpassword"})

def check_clickhouse():
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='rootpassword')
    client.command('SELECT 1')

def check_timescaledb():
    conn = psycopg2.connect(host="timescaledb", user="postgres", password="rootpassword", dbname="mydatabase")
    conn.close()

def check_influxdb():
    client = InfluxDBClient(url="http://influxdb:8086", token="benchmark_token_12345", org="benchmark_org")
    if not client.ping():
        raise Exception("Ping failed")
    client.close()

def check_scylladb():
    cluster = Cluster(['scylladb'])
    session = cluster.connect()
    cluster.shutdown()

def check_duckdb():
    # DuckDBはローカルファイルなので常にReady
    pass

def check_questdb():
    conn = psycopg2.connect(host="questdb", port=8812, user="admin", password="quest", dbname="qdb")
    conn.close()

def check_starrocks():
    # StarRocksはFE(9030)が起動しても、BE(バックエンド)の準備ができるまで書き込めない
    conn = mysql.connector.connect(host="starrocks", port=9030, user="root", password="")
    cursor = conn.cursor()
    cursor.execute("SHOW BACKENDS")
    backends = cursor.fetchall()
    
    # バックエンドのリストの中に、Alive状態(True/true)のものがあるか確認
    is_alive = False
    for row in backends:
        # rowの中に 'true' または True が含まれていれば起動完了
        if 'true' in str(row).lower() or True in row or 1 in row:
            is_alive = True
            break
            
    conn.close()
    if not is_alive:
        raise Exception("StarRocks FE is up, but BE is not alive yet.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True)
    args = parser.parse_args()

    # 接続に成功すれば exit(0)、例外が起きれば exit(1) を返す
    try:
        if args.db == "mysql": check_mysql()
        elif args.db == "postgres": check_postgres()
        elif args.db == "mongodb": check_mongodb()
        elif args.db == "redis": check_redis()
        elif args.db == "surrealdb": asyncio.run(check_surrealdb())
        elif args.db == "clickhouse": check_clickhouse()
        elif args.db == "timescaledb": check_timescaledb()
        elif args.db == "influxdb": check_influxdb()
        elif args.db == "scylladb": check_scylladb()
        elif args.db == "duckdb": check_duckdb()
        elif args.db == "questdb": check_questdb()
        elif args.db == "starrocks": check_starrocks()
        else:
            sys.exit(1)
        sys.exit(0)
    except Exception as e:
        sys.exit(1)