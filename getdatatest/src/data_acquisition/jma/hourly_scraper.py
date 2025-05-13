from datetime import datetime
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import sys
sys.path.append('.')
from data_acquisition.jma.scraper import BaseJMAScraper

class HourlyJMAScraper(BaseJMAScraper):
    """毎時気象データスクレイピングクラス"""
    
    def __init__(self, config, station_data):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
            station_data (dict): 観測所情報
        """
        super().__init__(config, station_data)
        self.base_url = config['scraping']['jma']['base_url']['hourly']
        self.output_dir = config['data_paths']['raw']['hourly']
        self.header = config['columns']['hourly_header']
    
    def scrape_hourly_weather(self, year, month, day, station):
        """
        指定された年月日の毎時気象データをスクレイピングする
        
        Parameters:
            year (int): 年
            month (int): 月
            day (int): 日
            station (dict): 観測所情報
        """
        params = {
            "prec_no": station['prec_no'],
            "block_no": station['block_no'],
            "year": year,
            "month": month,
            "day": day,
            "view": "p1"
        }
        
        # 出力ディレクトリの作成
        self._create_output_dir(self.output_dir)
        
        # ファイル名を生成
        output_file = os.path.join(
            self.output_dir, 
            f"{station['prec_no']}_{station['block_no']}_{year}{month:02d}{day:02d}.csv"
        )
        
        # データを取得
        soup = self._fetch_data(self.base_url, params)
        if not soup:
            logging.error(f"データが取得できませんでした: {year}年 {month}月 {day}日")
            return
        
        # 表を抽出
        table = soup.find("table", class_="data2_s")
        if not table:
            logging.warning(f"データが見つかりませんでした: {year}年 {month}月 {day}日")
            return
        
        # データを解析
        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = [col.text.strip().replace('"', '') for col in row.find_all(["th", "td"])]
            data.append(cols)
        
        # ヘッダとデータ分離
        data = data[1:]  # 先頭1行を削除
        
        # 日付と時刻の形式を変換
        data_with_datetime = []
        for row in data:
            if row and len(row) > 1:
                try:
                    hour = int(row[0])  # 時刻（0-23）
                    yyyymmddhh = f"{year}{month:02d}{day:02d}{hour:02d}"
                    yyyy_mm_dd_hh = f"{year}/{month:02d}/{day:02d} {hour:02d}:00"
                    row.insert(0, yyyy_mm_dd_hh)  # 日時2を追加
                    row.insert(0, yyyymmddhh)  # 日時1を追加
                    data_with_datetime.append(row)
                except ValueError:
                    pass
        
        # 最終行の24時（翌日の0時）のデータで上書き
        if data_with_datetime:
            last_row = data_with_datetime[-1][2:]
            next_day = datetime(year, month, day) + pd.Timedelta(days=1)
            next_day_0h = f"{next_day.year}/{next_day.month:02d}/{next_day.day:02d} 00:00"
            next_day_yyyymmddhh = f"{next_day.year}{next_day.month:02d}{next_day.day:02d}00"
            data_with_datetime[-1] = [next_day_yyyymmddhh, next_day_0h, "24"] + last_row[1:]
        
        # データフレームに変換
        df = pd.DataFrame(data_with_datetime, columns=self.header)
        
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
        start_day = period['start_day']
        end_day = period['end_day']
        
        tasks = []
        for station in self.station_data['stations']:
            for year in range(start_year, end_year + 1):
                for month in range(start_month, end_month + 1):
                    for day in range(start_day, end_day + 1):
                        # 月ごとの日数に基づいて有効なパラメータのみ処理
                        if self._is_valid_date(year, month, day):
                            if parallel:
                                tasks.append((year, month, day, station))
                            else:
                                self.scrape_hourly_weather(year, month, day, station)
        
        if parallel and tasks:
            max_workers = self.config['scraping']['jma']['parallel_processing']['max_workers']
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda args: self.scrape_hourly_weather(*args), tasks)
    
    def _is_valid_date(self, year, month, day):
        """
        指定された年月日が有効かどうかを判定する
        
        Parameters:
            year (int): 年
            month (int): 月
            day (int): 日
            
        Returns:
            bool: 有効な日付ならTrue
        """
        try:
            datetime(year, month, day)
            return True
        except ValueError:
            return False
