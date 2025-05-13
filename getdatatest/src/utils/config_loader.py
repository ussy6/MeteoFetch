import json
import os
import logging

class ConfigLoader:
    """設定ファイルを読み込むためのクラス"""
    
    def __init__(self, config_file_path):
        """
        コンストラクタ
        
        Parameters:
            config_file_path (str): 設定ファイルのパス
        """
        self.config_file_path = config_file_path
        self.config = self._load_config()
    
    def _load_config(self):
        """
        設定ファイルを読み込む
        
        Returns:
            dict: 設定情報
        """
        try:
            with open(self.config_file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            logging.error(f"設定ファイルが見つかりません: {self.config_file_path}")
            raise
        except json.JSONDecodeError:
            logging.error(f"設定ファイルのフォーマットが不正です: {self.config_file_path}")
            raise
    
    def get_config(self, section=None):
        """
        指定されたセクションの設定を取得する
        
        Parameters:
            section (str): セクション名（Noneの場合は全設定を返す）
            
        Returns:
            dict: 設定情報
        """
        if section is None:
            return self.config
        
        if section in self.config:
            return self.config[section]
        else:
            logging.warning(f"設定セクションが見つかりません: {section}")
            return {}
    
    def get_station_data(self, station_file_path):
        """
        観測所情報を読み込む
        
        Parameters:
            station_file_path (str): 観測所情報ファイルのパス
            
        Returns:
            dict: 観測所情報
        """
        try:
            with open(station_file_path, 'r', encoding='utf-8') as f:
                station_data = json.load(f)
            return station_data
        except FileNotFoundError:
            logging.error(f"観測所情報ファイルが見つかりません: {station_file_path}")
            raise
        except json.JSONDecodeError:
            logging.error(f"観測所情報ファイルのフォーマットが不正です: {station_file_path}")
            raise
