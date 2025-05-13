from datetime import datetime
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import sys
sys.path.append('.')
from data_acquisition.jma.scraper import BaseJMAScraper

class DailyJMAScraper(BaseJMAScraper):
    """日別気象データスクレイピングクラス"""
    
    def __init__(self, config, station_data):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
            station_data (dict): 観測所情報
        """
        super().__init__(config, station_data)
        self.base_url = config['scraping']['jma']['base_url']['daily']
        self.output_dir = config['data_paths']['raw']['daily']
        self.header = config['columns']['daily_header']
    
    def scrape_weather_data(self, year, month, station):
        """
        指定された年月の気象データをスクレイピングする
        
        Parameters:
            year (int): 年
            month (int): 月
            station (dict): 観測所情報
        """
        params = {
            "prec_no": station['prec_no'],
            "block_no": station['block_no'],
            "year": year,
            "month": month,
            "day": 1,
            "view": "p1"
        }
        
        # 出力ディレクトリの作成
        self._create_output_dir(self.output_dir)
        
        # ファイル名を生成
        output_file = os.path.join(
            self.output_dir, 
            f"{station['prec_no']}_{station['block_no']}_{year}{month:02d}.csv"
        )
        
        # データを取得
        soup = self._fetch_data(self.base_url, params)
        if not soup:
            logging.error(f"データが取得できませんでした: {year}年 {month}月")
            return
        
        # 表を抽出
        table = soup.find("table", class_="data2_s")
        if not table:
            logging.warning(f"データが見つかりませんでした: {year}年 {month}月")
            return
        
        # データを解析
        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = [col.text.strip().replace('"', '') for col in row.find_all(["th", "td"])]
            data.append(cols)
        
        # ヘッダとデータ分離
        data = data[4:]  # 先頭4行を削除
        
        # 日付の形式を変換
        data_with_date2 = []
        for row in data:
            if row:
                try:
                    date_str = row[0]
                    date_obj = datetime.strptime(date_str, "%d")
                    yyyymmdd = f"{year}{month:02d}{date_obj.strftime('%d')}"
                    yyyy_mm_dd = f"{year}/{month}/{int(date_obj.strftime('%d'))}"
                    row.insert(1, yyyy_mm_dd)  # 年月日2を年月日1の隣に挿入
                    row[0] = yyyymmdd  # 年月日1にyyyymmdd形式を設定
                    data_with_date2.append(row)
                except ValueError:
                    pass
        
        # データフレームに変換
        df = pd.DataFrame(data_with_date2, columns=self.header)
        
        # CSVファイルとして保存
        self._save_to_csv(df, output_file)
    
    def scrape_all_data(self, parallel=True):
        """
        すべての設定された範囲のデータを収集する
        
        Parameters:
            parallel (bool): 並列処理を行うかどうか
        """
        period = self.config['scraping']['jma']['target_period']
        start_year = period['start_year']
        end_year = period['end_year']
        start_month = period['start_month']
        end_month = period['end_month']
        
        tasks = []
        for station in self.station_data['stations']:
            for year in range(start_year, end_year + 1):
                for month in range(start_month, end_month + 1):
                    if parallel:
                        tasks.append((year, month, station))
                    else:
                        self.scrape_weather_data(year, month, station)
        
        if parallel and tasks:
            max_workers = self.config['scraping']['jma']['parallel_processing']['max_workers']
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda args: self.scrape_weather_data(*args), tasks)
