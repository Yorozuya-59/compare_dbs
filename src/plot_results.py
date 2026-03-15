import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_data(data_dir="/app/results"):
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not all_files:
        print(f"[{data_dir}] にCSVファイルが見つかりません。")
        return None
    df_list = [pd.read_csv(f) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    return df

def plot_insert_scaling(df, out_dir):
    # インサート処理 ＆ 文字列なし（純粋なパフォーマンス比較用）のデータに絞る
    df_insert = df[(df['Operation'] == 'insert') & (df['Exclude_Strings'] == True)]
    
    plt.figure(figsize=(12, 7))
    sns.lineplot(data=df_insert, x='Records', y='Elapsed_Time_sec', hue='Database', marker='o', linewidth=2)
    
    # 件数と時間は対数スケール（Log）にしないと1000万件の差が見えない
    plt.xscale('log')
    plt.yscale('log')
    
    plt.title('Insert Time vs Records (Log-Log Scale)', fontsize=16)
    plt.xlabel('Number of Records', fontsize=12)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, "insert_scaling.png")
    plt.savefig(save_path, dpi=300)
    print(f"📈 グラフを保存しました: {save_path}")
    plt.close()

def plot_query_performance_10m(df, out_dir):
    # 1000万件 ＆ クエリ処理 ＆ 文字列なし のデータに絞る
    df_query = df[(df['Operation'].str.startswith('query_')) & 
                  (df['Records'] == 10000000) & 
                  (df['Exclude_Strings'] == True)].copy()
    
    if df_query.empty:
        print("1000万件のクエリデータが存在しないため、棒グラフはスキップします。")
        return
        
    plt.figure(figsize=(14, 7))
    # DBごとに各クエリ（Point, Aggregation, Range）の時間を並べる
    sns.barplot(data=df_query, x='Database', y='Elapsed_Time_sec', hue='Operation')
    
    plt.yscale('log')
    plt.title('Query Performance at 10M Records (Log Scale)', fontsize=16)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.xlabel('Database', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title='Query Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, "query_performance_10m.png")
    plt.savefig(save_path, dpi=300)
    print(f"📊 グラフを保存しました: {save_path}")
    plt.close()

if __name__ == "__main__":
    OUT_DIR = "/app/results"
    
    print("=== グラフ自動生成スクリプト ===")
    df = load_data(OUT_DIR)
    
    if df is not None:
        # データにNaN（Redisなどの未サポート処理）がある場合は0や除外してプロット
        df['Elapsed_Time_sec'] = pd.to_numeric(df['Elapsed_Time_sec'], errors='coerce')
        
        plot_insert_scaling(df, OUT_DIR)
        plot_query_performance_10m(df, OUT_DIR)
        print("=== 完了 ===")