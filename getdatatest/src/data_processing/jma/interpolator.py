import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta

class DataInterpolator:
    """データ補間処理クラス"""
    
    def __init__(self, config):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
        """
        self.config = config
        self.hourly_file = config['data_paths']['processed']['merged']['hourly'] + '/hourly_merged.csv'
        self.daily_file = config['data_paths']['processed']['merged']['daily'] + '/daily_merged.csv'
        self.output_dir = config['data_paths']['processed']['interpolated']
        self.columns_to_interpolate = config['columns']['interpolate_columns']
        self.columns_mapping = config['columns']['columns_mapping']
        
        # 補間設定（デフォルト値）
        self.long_invalid_seq_days = 30  # 30日以上続く無効値を0に置き換え
        
    def replace_long_invalid_sequences(self, hourly_data):
        """
        "×"および連続する"--"が指定日数以上続く場合、その範囲を積雪データに対してのみ0に置き換える。
        
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
            
            # "×" や "--" を NaN に置き換えた後、長期間続く NaN を 0 に置き換え
            hourly_data['is_nan'] = hourly_data[column].isna()
            hourly_data['nan_group'] = (~hourly_data['is_nan']).cumsum()
            
            nan_counts = hourly_data.groupby('nan_group')['is_nan'].transform('sum')
            long_nan_mask = (nan_counts >= self.long_invalid_seq_days * 24)  # 指定日数分の時間
            hourly_data.loc[long_nan_mask, column] = 0
            
            hourly_data.drop(columns=['is_nan', 'nan_group'], inplace=True)
        
        hourly_data.reset_index(inplace=True)
        return hourly_data
    
    def linear_interpolation(self, data, columns):
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
    
    def fill_missing_days(self, hourly_data, daily_data, columns_mapping):
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
            
            for hourly_col, daily_col in columns_mapping.items():
                # 日データから値を取得
                daily_value = daily_data.loc[daily_data['年月日'] == date, daily_col].values
                
                if len(daily_value) > 0 and hourly_col in hourly_data.columns:  # 日データに値がある場合
                    # その日の列データがすべてNaNの場合、日データの値で補完
                    mask = (hourly_data.index.strftime('%Y-%m-%d') == day_str)
                    if hourly_data.loc[mask, hourly_col].isna().all() and len(hourly_data.loc[mask]) > 0:
                        hourly_data.loc[mask, hourly_col] = daily_value[0]
        
        hourly_data.sort_index(inplace=True)
        return hourly_data.reset_index()
    
    def replace_invalid_values(self, hourly_data):
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
                hourly_data[column] = hourly_data[column].replace(["×", "--", "*", "/"], np.nan)
            
            # 数値型に変換（エラーがあればNaNに）
            hourly_data[column] = pd.to_numeric(hourly_data[column], errors='coerce')
        
        # 線形補完で数値以外の部分を補完
        hourly_data = hourly_data.interpolate(method='linear', limit_direction='both')
        hourly_data.reset_index(inplace=True)
        return hourly_data
    
    def save_yearly_files(self, data, station_id, output_dir):
        """
        データを年ごとに切り出し、ファイルとして保存する。
        
        Parameters:
            data (DataFrame): データフレーム
            station_id (str): 観測所ID
            output_dir (str): 出力ディレクトリ
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # 観測所名を取得（IDからマッピング）
        station_name = station_id.split('_')[0]  # デフォルトはID先頭部分
        
        # 日時列の確認と変換
        if not '日時' in data.columns and '日時1' in data.columns:
            data['日時'] = pd.to_datetime(data['日時1'], format='%Y%m%d%H', errors='coerce')
        
        if not '日時' in data.columns:
            print(f"警告: 日時列がありません。データを年ごとに分割できません。")
            # ファイル全体を保存
            output_file = os.path.join(output_dir, f"{station_name}_all_hourly.csv")
            data = data.round(2)  # 小数点以下2桁に丸める
            data.to_csv(output_file, index=False, encoding='utf-8-sig')  # BOM付きで保存
            print(f"保存しました: {output_file}")
            return
        
        # 年ごとにグループ化して保存
        for year, group in tqdm(data.groupby(data['日時'].dt.year), desc="年ごとのデータを保存中"):
            file_name = f"{station_name}_{year}-01-01_to_{year}-12-31_hourly.csv"
            file_path = os.path.join(output_dir, file_name)
            group = group.round(2)  # 小数点以下2桁に丸める
            group.to_csv(file_path, index=False, encoding='utf-8-sig')  # BOM付きで保存
            print(f"保存しました: {file_path}")
    
    def process_yearly_data(self, year, station_id):
        """
        指定された年のデータを処理し、年単位のファイルを保存する。
        
        Parameters:
            year (int): 処理対象の年
            station_id (str): 観測所ID
        """
        # 観測所IDを分割
        try:
            prec_no, block_no = station_id.split('_')
            station_dir = f"{prec_no}_{block_no}"
        except ValueError:
            print(f"無効な観測所ID: {station_id}、形式は 'prec_no_block_no' である必要があります")
            return
        
        # 毎時データと日データのファイルパス
        hourly_file = os.path.join(self.config['data_paths']['processed']['merged']['hourly'], f"{station_dir}_hourly.csv")
        daily_file = os.path.join(self.config['data_paths']['processed']['merged']['daily'], f"{station_dir}_daily.csv")
        
        # ファイルの存在確認
        if not os.path.exists(hourly_file):
            print(f"毎時データが見つかりません: {hourly_file}")
            return
        
        print(f"{station_id}の{year}年データを処理中...")
        
        try:
            # データ読み込み
            hourly_data = pd.read_csv(hourly_file, encoding='utf-8-sig')
            
            # 日時列の追加
            hourly_data['日時'] = pd.to_datetime(hourly_data['日時1'], format='%Y%m%d%H', errors='coerce')
            
            # 指定された年でフィルタリング
            hourly_data = hourly_data[hourly_data['日時'].dt.year == year]
            
            if hourly_data.empty:
                print(f"{year}年のデータがありません")
                return
            
            # 日データを使用した補完（存在する場合）
            if os.path.exists(daily_file):
                daily_data = pd.read_csv(daily_file, encoding='utf-8-sig')
                daily_data['年月日'] = pd.to_datetime(daily_data['年月日1'], format='%Y%m%d', errors='coerce')
                daily_data = daily_data[daily_data['年月日'].dt.year == year]
                
                if not daily_data.empty:
                    hourly_data = self.fill_missing_days(hourly_data, daily_data, self.columns_mapping)
            
            # "×"が長期間続く部分を0に置き換え
            hourly_data = self.replace_long_invalid_sequences(hourly_data)
            
            # 不正値を前後の数値で補完
            hourly_data = self.replace_invalid_values(hourly_data)
            
            # 時間方向に線形補完
            hourly_data = self.linear_interpolation(hourly_data, self.columns_to_interpolate)
            
            # 年ごとの補完後のデータを保存
            self.save_yearly_files(hourly_data, station_id, self.output_dir)
        
        except Exception as e:
            print(f"データ処理中にエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
    
    def interpolate_all_data(self):
        """すべての観測所・年のデータを補間する"""
        # 補間対象の年と観測所を決定
        interpolate_years = self.config.get('data_processing', {}).get('interpolate', {}).get('years', [])
        station_ids = self.config.get('data_processing', {}).get('interpolate', {}).get('station_ids', [])
        
        # 年の指定がない場合は、マージされたデータから自動的に検出
        if not interpolate_years:
            # 最初のファイルから年の範囲を検出（すべてのステーションで同じと仮定）
            if station_ids:
                first_station = station_ids[0]
                first_file = os.path.join(self.config['data_paths']['processed']['merged']['hourly'], 
                                         f"{first_station}_hourly.csv")
            else:
                # 観測所の指定もない場合は、マージディレクトリからCSVファイルを検索
                hourly_dir = self.config['data_paths']['processed']['merged']['hourly']
                csv_files = [f for f in os.listdir(hourly_dir) if f.endswith('_hourly.csv')]
                
                if not csv_files:
                    print("補間対象のデータが見つかりません")
                    return
                
                first_file = os.path.join(hourly_dir, csv_files[0])
            
            # ファイルから年の範囲を検出
            try:
                data = pd.read_csv(first_file, encoding='utf-8-sig')
                data['日時'] = pd.to_datetime(data['日時1'], format='%Y%m%d%H', errors='coerce')
                years = sorted(data['日時'].dt.year.unique())
                interpolate_years = [int(year) for year in years if not pd.isna(year)]
            except Exception as e:
                print(f"年の検出中にエラーが発生しました: {e}")
                # デフォルトとして現在の年を使用
                current_year = datetime.now().year
                interpolate_years = [current_year]
        
        # 観測所の指定がない場合は、マージディレクトリから検出
        if not station_ids:
            hourly_dir = self.config['data_paths']['processed']['merged']['hourly']
            csv_files = [f for f in os.listdir(hourly_dir) if f.endswith('_hourly.csv')]
            
            # ファイル名から観測所IDを抽出（例: 19_47418_hourly.csv → 19_47418）
            station_ids = []
            for file in csv_files:
                station_part = file.replace('_hourly.csv', '')
                if '_' in station_part:  # 正しい形式かチェック
                    station_ids.append(station_part)
        
        # 出力ディレクトリの作成
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 各観測所・年のデータを処理
        for station_id in station_ids:
            for year in interpolate_years:
                self.process_yearly_data(year, station_id)
