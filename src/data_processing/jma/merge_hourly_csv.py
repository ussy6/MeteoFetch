import os
import pandas as pd

def merge_hourly_csv_files(input_dir, output_dir):
    """
    指定されたディレクトリ内の毎時CSVファイルを統合し、単一のCSVファイルにまとめます。

    Parameters:
        input_dir (str): CSVファイルが格納されているディレクトリ
        output_dir (str): 統合後のCSVファイルの出力先ディレクトリ
    """

    # 入力ディレクトリ内のすべてのCSVファイルを取得
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

    if not csv_files:
        print("指定されたディレクトリにCSVファイルがありません。")
        return

    # データフレームのリストを作成
    data_frames = []
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        print(f"ファイルを読み込み中: {file_path}")
        df = pd.read_csv(file_path)
        data_frames.append(df)

    # データフレームを結合
    combined_df = pd.concat(data_frames, ignore_index=True)

    # データを時系列でソート（"日時1"列を基準）
    if "日時1" in combined_df.columns:
        combined_df = combined_df.sort_values(by="日時1")

    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)

    # 出力ファイル名を生成
    output_file = ""
    if csv_files:
        first_file = csv_files[0]
        parts = first_file.split('_')
        if len(parts) >= 2:
            prec_no = parts[0]
            block_no = parts[1]
            output_file = os.path.join(output_dir, f"{prec_no}_{block_no}_hourly.csv")

    # 統合データをCSVとして保存
    combined_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"統合されたデータを保存しました: {output_file}")

if __name__ == "__main__":
    # 入力ディレクトリと出力ディレクトリの指定
    input_dir = "data/raw/scraped/hourly"
    output_dir = "data/processed/merged/hourly"

    # CSVファイルを統合
    merge_hourly_csv_files(input_dir, output_dir)
