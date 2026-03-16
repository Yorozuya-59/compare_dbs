import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_data(file_path):
    if not os.path.exists(file_path):
        print(f"[{file_path}] が見つかりません。パスを確認してください。")
        return None
        
    df = pd.read_csv(file_path)
    
    # 結合によって混入した不要なヘッダー行を除外
    df = df[df['Operation'] != 'Operation'].copy()
    
    # 列を正しいデータ型に強制変換
    df['Records'] = pd.to_numeric(df['Records'], errors='coerce')
    df['Exclude_Strings'] = df['Exclude_Strings'].astype(str).str.lower() == 'true'
    df['Elapsed_Time_sec'] = pd.to_numeric(df['Elapsed_Time_sec'], errors='coerce')
    
    # 欠損値（NaN）を含む行を削除
    df = df.dropna(subset=['Records', 'Elapsed_Time_sec'])
    
    return df

# 1. インサート処理のスケーリング（折れ線グラフ）
def plot_insert_scaling(df, out_dir, exclude_strings, suffix):
    df_plot = df[(df['Operation'] == 'insert') & (df['Exclude_Strings'] == exclude_strings)]
    if df_plot.empty: return
    
    plt.figure(figsize=(12, 7))
    
    # --- 修正箇所: style='Database', markers=True を追加 ---
    sns.lineplot(
        data=df_plot, 
        x='Records', 
        y='Elapsed_Time_sec', 
        hue='Database', 
        style='Database',   # DBごとに線のスタイル/マーカーを変える
        markers=True,       # マーカー（点）を表示する
        dashes=False,       # 線自体はすべて実線にする（点線が混ざると見にくいため）
        linewidth=2,
        markersize=9        # マーカーを少し大きめにして見やすくする
    )
    # ----------------------------------------------------
    
    plt.xscale('log')
    plt.yscale('log')
    title_str = "Numbers Only" if exclude_strings else "With Strings"
    plt.title(f'Insert Time vs Records ({title_str})', fontsize=16)
    plt.xlabel('Number of Records', fontsize=12)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, f"insert_scaling_{suffix}.png")
    plt.savefig(save_path, dpi=300)
    print(f"📈 グラフ生成: {save_path}")
    plt.close()

# 2. 各クエリ処理のスケーリング（折れ線グラフ）
def plot_query_scaling(df, out_dir, query_type):
    # 文字列なし（純粋なインデックス性能）をベースに比較
    df_plot = df[(df['Operation'] == query_type) & (df['Exclude_Strings'] == True)]
    if df_plot.empty: return
    
    plt.figure(figsize=(12, 7))
    
    # --- 修正箇所: style='Database', markers=True を追加 ---
    sns.lineplot(
        data=df_plot, 
        x='Records', 
        y='Elapsed_Time_sec', 
        hue='Database', 
        style='Database', 
        markers=True, 
        dashes=False, 
        linewidth=2,
        markersize=9
    )
    # ----------------------------------------------------
    
    plt.xscale('log')
    plt.yscale('log')
    plt.title(f'{query_type} Time vs Records (Log-Log Scale)', fontsize=16)
    plt.xlabel('Number of Records', fontsize=12)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, f"query_scaling_{query_type.replace('query_', '')}.png")
    plt.savefig(save_path, dpi=300)
    print(f"📈 グラフ生成: {save_path}")
    plt.close()

# 3. 指定件数における全クエリ性能の比較（棒グラフ）
def plot_query_performance(df, out_dir, records, exclude_strings, suffix):
    df_plot = df[(df['Operation'].str.startswith('query_')) & 
                 (df['Records'] == records) & 
                 (df['Exclude_Strings'] == exclude_strings)].copy()
    if df_plot.empty: return
        
    plt.figure(figsize=(14, 7))
    sns.barplot(data=df_plot, x='Database', y='Elapsed_Time_sec', hue='Operation')
    
    plt.yscale('log')
    title_str = "Numbers Only" if exclude_strings else "With Strings"
    plt.title(f'Query Performance at {records:,} Records ({title_str})', fontsize=16)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.xlabel('Database', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title='Query Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, f"query_performance_{suffix}.png")
    plt.savefig(save_path, dpi=300)
    print(f"📊 グラフ生成: {save_path}")
    plt.close()

# 4. 文字列の有無がインサート性能に与える影響（棒グラフ）
def plot_string_impact(df, out_dir, records):
    df_plot = df[(df['Operation'] == 'insert') & (df['Records'] == records)].copy()
    if df_plot.empty: return
    
    # 凡例をわかりやすく変換
    df_plot['Payload Type'] = df_plot['Exclude_Strings'].map({True: 'Numbers Only', False: 'With MAC Address'})
    
    plt.figure(figsize=(14, 7))
    sns.barplot(data=df_plot, x='Database', y='Elapsed_Time_sec', hue='Payload Type')
    
    plt.yscale('log')
    plt.title(f'Impact of Strings on Insert Time ({records:,} Records)', fontsize=16)
    plt.ylabel('Elapsed Time (seconds)', fontsize=12)
    plt.xlabel('Database', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title='Payload Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', ls="--", alpha=0.5)
    plt.tight_layout()
    
    save_path = os.path.join(out_dir, f"string_impact_insert_{records}.png")
    plt.savefig(save_path, dpi=300)
    print(f"📊 グラフ生成: {save_path}")
    plt.close()

if __name__ == "__main__":
    TARGET_CSV = "/app/results/agg_results.csv"
    OUT_DIR = "/app/results"
    
    print(f"=== グラフ一括生成スクリプト ({TARGET_CSV}) ===")
    df = load_data(TARGET_CSV)
    
    if df is not None:
        # 1. インサートのスケーリング
        plot_insert_scaling(df, OUT_DIR, exclude_strings=True, suffix="no_strings")
        plot_insert_scaling(df, OUT_DIR, exclude_strings=False, suffix="with_strings")
        
        # 2. クエリごとのスケーリング
        plot_query_scaling(df, OUT_DIR, "query_point")
        plot_query_scaling(df, OUT_DIR, "query_agg")
        plot_query_scaling(df, OUT_DIR, "query_range")
        
        # 3. 1000万件でのクエリ性能比較
        plot_query_performance(df, OUT_DIR, records=10000000, exclude_strings=True, suffix="10m_no_strings")
        plot_query_performance(df, OUT_DIR, records=10000000, exclude_strings=False, suffix="10m_with_strings")
        
        # 4. 文字列有無のインパクト（1000万件）
        plot_string_impact(df, OUT_DIR, records=10000000)
        
        print("=== 全てのグラフ出力が完了しました ===")