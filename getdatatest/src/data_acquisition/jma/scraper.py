import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging
from datetime import datetime

class BaseJMAScraper:
    """気象庁データスクレイピングの基底クラス"""
    
    def __init__(self, config, station_data):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
            station_data (dict): 観測所情報
        """
        self.config = config
        self.station_data = station_data
        self.output_base_dir = config['data_paths']['raw']
    
    def _create_output_dir(self, output_dir):
        """
        出力ディレクトリを作成する
        
        Parameters:
            output_dir (str): 出力ディレクトリパス
        """
        os.makedirs(output_dir, exist_ok=True)
    
    def _fetch_data(self, url, params):
        """
        データを取得する
        
        Parameters:
            url (str): URL
            params (dict): リクエストパラメータ
            
        Returns:
            BeautifulSoup: パース済みのHTMLデータ
        """
        try:
            response = requests.get(url, params=params)
            response.encoding = "utf-8"  # 日本語の文字コードに対応
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logging.error(f"データ取得中にエラーが発生しました: {e}")
            return None
    
    def _save_to_csv(self, df, output_file):
        """
        データをCSVファイルとして保存する
        
        Parameters:
            df (DataFrame): データフレーム
            output_file (str): 出力ファイルパス
        """
        try:
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            logging.info(f"データを保存しました: {output_file}")
        except Exception as e:
            logging.error(f"データ保存中にエラーが発生しました: {e}")
