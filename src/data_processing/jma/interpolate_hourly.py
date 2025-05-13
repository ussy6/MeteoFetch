import os
import pandas as pd
import numpy as np
from tqdm import tqdm

def replace_long_invalid_sequences(hourly_data):
    """
    "×"および連続する"--"が30日以上続く場合、その範囲を積雪データに対してのみ0に置き換える。

    Parameters:
        hourly_data (DataFrame): 毎時データ

    Returns:
        DataFrame: 修正後のデータ
    """
    hourly_data['日時'] = pd.to_datetime(hourly_data['日時1'], format='%Y%m%d%H', errors='coerce')
    hourly_data.set_index('日時', inplace=True)

    column = '積雪_cm'  # 積雪データに限定
    if column in hourly_data.columns:
        if hourly_data[column].dtype == 'object':
            hourly_data[column] = hourly_data[column].replace({"×": np.nan, "--": np.nan})

        # "×" や "--" を NaN に置き換えた後、30日以上続く NaN を 0 に置き換え
        hourly_data['is_nan'] = hourly_data[column].isna()
        hourly_data['nan_group'] = (~hourly_data['is_nan']).cumsum()

        nan_counts = hourly_data.groupby('nan_group')['is_nan'].transform('sum')
        long_nan_mask = (nan_counts >= 30 * 24)  # 30日分の時間
        hourly_data.loc[long_nan_mask, column] = 0

        hourly_data.drop(columns=['is_nan', 'nan_group'], inplace=True)

    hourly_data.reset_index(inplace=True)
    return hourly_data

def linear_interpolation(data, columns):
    """
    時間方向に線形補完を行う。

    Parameters:
        data (DataFrame): データフレーム
        columns (list): 補完対象の列名

    Returns:
        DataFrame: 補完後のデータ
    """
    for column in tqdm(columns, desc="線形補完実行中"):
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors='coerce')  # 数値型に変換
            data[column] = data[column].interpolate(method='linear', limit_direction='both')
    return data

def fill_missing_days(hourly_data, daily_data, columns_mapping):
    """
    毎時データが1日に1つも存在しない場合、日データを使用して値を補完する。

    Parameters:
        hourly_data (DataFrame): 毎時データ
        daily_data (DataFrame): 日データ
        columns_mapping (dict): 毎時データと日データの列の対応

    Returns:
        DataFrame: 補完後のデータ
    """
    hourly_data['日時'] = pd.to_datetime(hourly_data['日時1'], format='%Y%m%d%H', errors='coerce')
    hourly_data.set_index('日時', inplace=True)
    daily_data['年月日'] = pd.to_datetime(daily_data['年月日1'], format='%Y%m%d', errors='coerce')

    for date in tqdm(daily_data['年月日'].dropna(), desc="日データを使用した補完実行中"):
        day_str = date.strftime('%Y-%m-%d')
        daily_value = daily_data.loc[daily_data['年月日'] == date, columns_mapping['積雪_cm']].values

        if len(daily_value) > 0:  # 日データに値がある場合
            # その日の積雪_cmデータが1つもない場合、全時間帯を日最深積雪_cmで補完
            mask = (hourly_data.index.strftime('%Y-%m-%d') == day_str)
            if hourly_data.loc[mask, '積雪_cm'].isna().all():
                hourly_data.loc[mask, '積雪_cm'] = daily_value[0]

    hourly_data.sort_index(inplace=True)
    return hourly_data.reset_index()

def replace_invalid_values(hourly_data):
    """
    データ内の不正な値（例: "×"）を前後の数値で線形補完する。

    Parameters:
        hourly_data (DataFrame): 毎時データ

    Returns:
        DataFrame: 修正後のデータ
    """
    hourly_data['日時'] = pd.to_datetime(hourly_data['日時1'], format='%Y%m%d%H', errors='coerce')
    hourly_data.set_index('日時', inplace=True)

    for column in hourly_data.columns:
        if hourly_data[column].dtype == 'object':
            # "×" や数値以外を NaN に変換
            hourly_data[column] = hourly_data[column].replace("×", np.nan)
        hourly_data[column] = pd.to_numeric(hourly_data[column], errors='coerce')  # 数値型に変換

    # 線形補完で数値以外の部分を補完
    hourly_data = hourly_data.interpolate(method='linear', limit_direction='both')
    hourly_data.reset_index(inplace=True)
    return hourly_data

def save_yearly_files(data, output_dir):
    """
    データを年ごとに切り出し、ファイルとして保存する。

    Parameters:
        data (DataFrame): データフレーム
        output_dir (str): 出力ディレクトリ
    """
    os.makedirs(output_dir, exist_ok=True)
    for year, group in tqdm(data.groupby(data['日時'].dt.year), desc="年ごとのデータを保存中"):
        file_name = f"Kushiro_{year}-01-01_to_{year}-12-31_hourly.csv"
        file_path = os.path.join(output_dir, file_name)
        group = group.round(2)  # 小数点以下2桁に丸める
        group.to_csv(file_path, index=False, encoding='utf-8-sig')  # BOM付きで保存
        print(f"保存しました: {file_path}")

def process_yearly_data(year, hourly_file, daily_file, output_dir, columns_to_interpolate, columns_mapping):
    """
    指定された年のデータを処理し、年単位のファイルを保存する。

    Parameters:
        year (int): 処理対象の年
        hourly_file (str): 毎時データの入力ファイル
        daily_file (str): 日データの入力ファイル
        output_dir (str): 出力ディレクトリ
        columns_to_interpolate (list): 補完対象の列名リスト
        columns_mapping (dict): 毎時データと日データの列の対応
    """
    # データ読み込み
    hourly_data = pd.read_csv(hourly_file)
    daily_data = pd.read_csv(daily_file)

    # データを指定された年でフィルタリング
    hourly_data['日時'] = pd.to_datetime(hourly_data['日時1'], format='%Y%m%d%H', errors='coerce')
    hourly_data = hourly_data[(hourly_data['日時'].dt.year == year)]
    daily_data['年月日'] = pd.to_datetime(daily_data['年月日1'], format='%Y%m%d', errors='coerce')
    daily_data = daily_data[(daily_data['年月日'].dt.year == year)]

    # 日データを使用した補完
    hourly_data = fill_missing_days(hourly_data, daily_data, columns_mapping)

    # "×"が5日以上続く部分を0に置き換え
    hourly_data = replace_long_invalid_sequences(hourly_data)

    # 不正値を前後の数値で補完
    hourly_data = replace_invalid_values(hourly_data)

    # 時間方向に線形補完
    hourly_data = linear_interpolation(hourly_data, columns_to_interpolate)

    # 年ごとの補完後のデータを保存
    save_yearly_files(hourly_data, output_dir)

if __name__ == "__main__":
    # 入力ファイルと出力ディレクトリ
    hourly_file = "data/processed/merged/hourly/19_47418_hourly.csv"
    daily_file = "data/processed/merged/dayly/19_47418_dayly.csv"
    yearly_output_dir = "data/processed/interpolated"

    # 必要なディレクトリを作成
    os.makedirs(yearly_output_dir, exist_ok=True)

    # 処理期間の指定
    # start_year = 2014
    start_year = 2023
    # end_year = 2014
    end_year = 2024

    # 補完対象の列を設定
    columns_to_interpolate = [
        "気圧_現地_hPa", "気圧_海面_hPa", "降水量_mm", "気温_dC", "露点温度_dC", "蒸気圧_hPa",
        "湿度_per", "風速_mpers", "風向", "日照時間_h", "全天日射量_MJperm2", "降雪_cm", "積雪_cm"
    ]
    columns_mapping = {
        "積雪_cm": "日最深積雪_cm"
    }

    # 年ごとに処理
    for year in range(start_year, end_year + 1):
        process_yearly_data(year, hourly_file, daily_file, yearly_output_dir, columns_to_interpolate, columns_mapping)
