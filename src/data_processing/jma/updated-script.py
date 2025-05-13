import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm

def add_metadata_header(file_path, metadata):
    """
    CSVファイルにメタデータヘッダーを追加する

    Parameters:
        file_path (str): CSVファイルのパス
        metadata (dict): メタデータの辞書
    """
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    metadata_lines = []
    for key, value in metadata.items():
        metadata_lines.append(f"# {key}: {value}")
    
    metadata_header = '\n'.join(metadata_lines) + '\n'
    
    with open(file_path, 'w', encoding='utf-8-sig') as f:
        f.write(metadata_header + content)

def format_date_columns(data):
    """
    データフレームにISO 8601形式の日時列を追加する

    Parameters:
        data (DataFrame): 日時列を含むデータフレーム

    Returns:
        DataFrame: ISO形式の日時列を含む処理済みデータフレーム
    """
    # 元の日時列を確認
    if '日時1' in data.columns:
        date_col = '日時1'
    elif '年月日1' in data.columns:
        date_col = '年月日1'
    else:
        raise ValueError("日時または年月日の列が見つかりません")

    # 日時に変換
    # 毎時データの場合（YYYYMMDDHHの形式）
    if date_col == '日時1':
        data['datetime'] = pd.to_datetime(data[date_col], format='%Y%m%d%H', errors='coerce')
    # 日データの場合（YYYYMMDDの形式）
    else:
        data['datetime'] = pd.to_datetime(data[date_col], format='%Y%m%d', errors='coerce')
    
    # ISO 8601形式の日時を追加
    data['date_iso_jst'] = data['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S+09:00')
    
    # UTC変換 (JST - 9時間)
    data['datetime_utc'] = data['datetime'] - timedelta(hours=9)
    data['date_iso_utc'] = data['datetime_utc'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # 不要な列を削除
    data.drop(['datetime', 'datetime_utc'], axis=1, inplace=True)
    
    # 元の日時関連列を削除
    columns_to_drop = [
        date_col,  # 日時1または年月日1
        '日時2' if '日時2' in data.columns else None,
        '年月日2' if '年月日2' in data.columns else None,
        'date_excel' if 'date_excel' in data.columns else None
    ]
    columns_to_drop = [col for col in columns_to_drop if col is not None]
    data.drop(columns_to_drop, axis=1, inplace=True)
    
    # 日時列がある場合はヘッダーを変更（削除はしない）
    if '日時' in data.columns:
        data.rename(columns={'日時': 'date'}, inplace=True)
    
    return data

def create_dual_header(data):
    """
    日本語と英語の両方を含む2段ヘッダーを作成する

    Parameters:
        data (DataFrame): 元のデータフレーム

    Returns:
        DataFrame: 2段ヘッダーを持つデータフレーム
    """
    # 日本語と英語のヘッダーマッピング
    header_mapping = {
        'date_iso_jst': 'datetime_iso_jst',
        'date_iso_utc': 'datetime_iso_utc',
        '時刻': 'hour',
        '気温_dC': 'air_temperature[degC]',
        '気圧_現地_hPa': 'pressure_station[hPa]',
        '気圧_海面_hPa': 'pressure_sea_level[hPa]',
        '降水量_mm': 'precipitation[mm]',
        '露点温度_dC': 'dew_point[degC]',
        '蒸気圧_hPa': 'vapor_pressure[hPa]',
        '湿度_per': 'relative_humidity[%]',
        '風速_mpers': 'wind_speed[m/s]',
        '風向': 'wind_direction',
        '日照時間_h': 'sunshine_duration[h]',
        '全天日射量_MJperm2': 'global_radiation[MJ/m2]',
        '降雪_cm': 'snowfall[cm]',
        '積雪_cm': 'snow_depth[cm]',
        '天気': 'weather',
        '雲量': 'cloud_cover',
        '視程_km': 'visibility[km]',
        '日降水量_mm': 'daily_precipitation[mm]',
        '日最大降水量_1h_mm': 'daily_maximum_precipitation_1h[mm]',
        '日最大降水量_10m_mm': 'daily_maximum_precipitation_10m[mm]',
        '日平均気温_dC': 'daily_mean_temperature[degC]',
        '日最高気温_dC': 'daily_maximum_temperature[degC]',
        '日最低気温_dC': 'daily_minimum_temperature[degC]',
        '日平均湿度_per': 'daily_mean_humidity[%]',
        '日最小湿度_per': 'daily_minimum_humidity[%]',
        '日平均風速_ms': 'daily_mean_wind_speed[m/s]',
        '日最大風速_ms': 'daily_maximum_wind_speed[m/s]',
        '日最大風速の風向': 'daily_maximum_wind_direction',
        '日最大瞬間風速_ms': 'daily_maximum_instantaneous_wind_speed[m/s]',
        '日最大瞬間風速の風向': 'daily_maximum_instantaneous_wind_direction',
        '日照時間_h': 'sunshine_duration[h]',
        '日降雪深_cm': 'daily_snowfall[cm]',
        '日最深積雪_cm': 'daily_maximum_snow_depth[cm]',
        '天気概況_昼_06-18': 'weather_daytime_06-18',
        '天気概況_夜_18-06': 'weather_nighttime_18-06',
        'date': 'date'  # 日時列の英語名を追加
    }
    
    # 日本語ヘッダーの単位付き表記への変換
    jp_header_with_units = {
        '気温_dC': '気温[℃]',
        '気圧_現地_hPa': '気圧[現地hPa]',
        '気圧_海面_hPa': '気圧[海面hPa]',
        '降水量_mm': '降水量[mm]',
        '露点温度_dC': '露点温度[℃]',
        '蒸気圧_hPa': '蒸気圧[hPa]',
        '湿度_per': '湿度[%]',
        '風速_mpers': '風速[m/s]',
        '日照時間_h': '日照時間[h]',
        '全天日射量_MJperm2': '全天日射量[MJ/m²]',
        '降雪_cm': '降雪[cm]',
        '積雪_cm': '積雪[cm]',
        '視程_km': '視程[km]',
        '日降水量_mm': '日降水量[mm]',
        '日最大降水量_1h_mm': '日最大降水量[1h/mm]',
        '日最大降水量_10m_mm': '日最大降水量[10m/mm]',
        '日平均気温_dC': '日平均気温[℃]',
        '日最高気温_dC': '日最高気温[℃]',
        '日最低気温_dC': '日最低気温[℃]',
        '日平均湿度_per': '日平均湿度[%]',
        '日最小湿度_per': '日最小湿度[%]',
        '日平均風速_ms': '日平均風速[m/s]',
        '日最大風速_ms': '日最大風速[m/s]',
        '日最大瞬間風速_ms': '日最大瞬間風速[m/s]',
        '日照時間_h': '日照時間[h]',
        '日降雪深_cm': '日降雪深[cm]',
        '日最深積雪_cm': '日最深積雪[cm]'
    }
    
    # 元のデータフレームのコピーを作成
    original_data = data.copy()
    
    # 新しいヘッダーを作成
    japanese_headers = []
    english_headers = []
    
    for col in original_data.columns:
        # 日本語ヘッダーを単位付きに変換
        if col in jp_header_with_units:
            japanese_headers.append(jp_header_with_units[col])
        else:
            if col == 'date_iso_jst':
                japanese_headers.append('日時[ISO8601 JST]')
            elif col == 'date_iso_utc':
                japanese_headers.append('日時[ISO8601 UTC]')
            elif col == 'date':
                japanese_headers.append('日時')  # 日時列の日本語名
            else:
                japanese_headers.append(col)  # 変換マップにない場合は元のまま
        
        # 英語ヘッダーマッピング
        if col in header_mapping:
            english_headers.append(header_mapping[col])
        else:
            english_headers.append(col)  # マッピングにない場合は元のまま
    
    # ヘッダーをマルチインデックスに変換
    multi_index = pd.MultiIndex.from_arrays([japanese_headers, english_headers])
    original_data.columns = multi_index
    
    return original_data

def save_with_dual_header(data, output_file):
    """
    2段ヘッダーでCSVファイルに保存する

    Parameters:
        data (DataFrame): 2段ヘッダーを持つデータフレーム
        output_file (str): 出力ファイルのパス
    """
    # 日本語ヘッダーと英語ヘッダーを取得
    japanese_headers = data.columns.get_level_values(0).tolist()
    english_headers = data.columns.get_level_values(1).tolist()
    
    # 一時的に列名を変更してデータをコピー
    temp_data = data.copy()
    temp_data.columns = english_headers
    
    # ファイルにヘッダー行を手動で書き込む
    with open(output_file, 'w', encoding='utf-8-sig') as f:
        f.write(','.join(japanese_headers) + '\n')
        f.write(','.join(english_headers) + '\n')
    
    # データをヘッダーなしで追加
    temp_data.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8-sig')

def process_and_save_data(input_file, output_file, weather_station, location, data_type='hourly'):
    """
    気象データを処理し、新しい形式で保存する

    Parameters:
        input_file (str): 入力ファイルのパス
        output_file (str): 出力ファイルのパス
        weather_station (str): 気象観測所名
        location (dict): 観測所の位置情報（緯度、経度、標高）
        data_type (str): データタイプ（'hourly'または'daily'）
    """
    # データを読み込む
    data = pd.read_csv(input_file, encoding='utf-8-sig')
    
    # 日時列を処理
    data = format_date_columns(data)
    
    # 2段ヘッダーに変換
    dual_header_data = create_dual_header(data)
    
    # 2段ヘッダーでファイルに保存
    save_with_dual_header(dual_header_data, output_file)
    
    # ファイル名から年を抽出
    year = os.path.basename(output_file).split('_')[1].split('-')[0]
    
    # メタデータを設定
    metadata = {
        '観測地点': weather_station,
        '緯度': location['latitude'],
        '経度': location['longitude'],
        '標高': f"{location['elevation']}m",
        'データ期間': f"{year}-01-01から{year}-12-31",
        'データタイプ': '毎時データ' if data_type == 'hourly' else '日別データ',
        '欠測値': 'NaN',
        '作成日': datetime.now().strftime('%Y-%m-%d'),
    }
    
    # メタデータをファイルに追加
    add_metadata_header(output_file, metadata)
    
    print(f"処理完了: {output_file}")

def process_year_data(year, input_dir, output_dir, weather_station, location):
    """
    指定年の気象データを処理し、毎時および日別データを新しい形式で保存する

    Parameters:
        year (int): 処理する年
        input_dir (str): 入力ディレクトリ
        output_dir (str): 出力ディレクトリ
        weather_station (str): 気象観測所名
        location (dict): 観測所の位置情報
    """
    # 出力ディレクトリを作成
    os.makedirs(output_dir, exist_ok=True)
    
    # 毎時データの処理
    hourly_input = os.path.join(input_dir, 'interpolated', f"Kushiro_{year}-01-01_to_{year}-12-31_hourly.csv")
    hourly_output = os.path.join(output_dir, f"Kushiro_{year}-01-01_to_{year}-12-31_hourly_new.csv")
    
    if os.path.exists(hourly_input):
        process_and_save_data(hourly_input, hourly_output, weather_station, location, 'hourly')
    else:
        print(f"毎時データが見つかりません: {hourly_input}")
    
    # 日別データの処理（必要に応じて）
    daily_input = os.path.join(input_dir, 'merged', 'dayly', f"19_47418_dayly.csv")
    daily_output = os.path.join(output_dir, f"Kushiro_{year}-01-01_to_{year}-12-31_daily_new.csv")
    
    if os.path.exists(daily_input):
        process_and_save_data(daily_input, daily_output, weather_station, location, 'daily')
    else:
        print(f"日別データが見つかりません: {daily_input}")

if __name__ == "__main__":
    # 処理する年の範囲を設定
    start_year = 2023
    end_year = 2024
    
    # ディレクトリ設定
    base_input_dir = "data/processed"
    output_dir = "data/processed/formatted"
    
    # 気象観測所の情報
    weather_station = "釧路気象観測所"
    location = {
        "latitude": "42.9752 N",
        "longitude": "144.3726 E",
        "elevation": 31
    }
    
    # 各年のデータを処理
    for year in range(start_year, end_year + 1):
        process_year_data(year, base_input_dir, output_dir, weather_station, location)
