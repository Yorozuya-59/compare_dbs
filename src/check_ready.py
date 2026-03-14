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
        else:
            sys.exit(1)
        sys.exit(0)
    except Exception as e:
        sys.exit(1)