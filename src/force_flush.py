import time
import argparse
import psycopg2
from pymongo import MongoClient
import clickhouse_connect

def flush_mongodb():
    print("  -> [MongoDB] fsyncを実行し、メモリ上のデータをディスクへ強制書き出し中...")
    client = MongoClient("mongodb://root:rootpassword@mongodb:27017/")
    # fsync: 1 で強制的にディスクへフラッシュ
    client.admin.command('fsync', 1)
    client.close()

def flush_clickhouse():
    print("  -> [ClickHouse] OPTIMIZE FINALを実行し、データパーツをマージ＆圧縮中...")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='rootpassword', send_receive_timeout=600)
    # 散らばったデータを1つの最適なブロックに圧縮・結合する
    client.command("OPTIMIZE TABLE sensor_data FINAL")

def flush_questdb(expected_records):
    print("  -> [QuestDB] 非同期WALが完全にテーブルに適用されるまで待機中...")
    conn = psycopg2.connect(host="questdb", port=8812, user="admin", password="quest", dbname="qdb")
    cursor = conn.cursor()
    while True:
        try:
            cursor.execute("SELECT COUNT(*) FROM sensor_data")
            count = cursor.fetchone()[0]
            if count >= expected_records:
                break
        except Exception:
            conn.rollback()
        time.sleep(2)
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, required=True)
    parser.add_argument("--records", type=int, required=True)
    args = parser.parse_args()

    if args.db == "mongodb":
        flush_mongodb()
    elif args.db == "clickhouse":
        flush_clickhouse()
    elif args.db == "questdb":
        flush_questdb(args.records)
        
    print(f"  -> [{args.db}] 永続化および整理処理が完了しました。")