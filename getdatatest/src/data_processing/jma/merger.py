import os
import pandas as pd
from tqdm import tqdm
import logging

class BaseMerger:
    """ベースとなるデータ結合クラス"""
    
    def __init__(self, config):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def _create_output_dir(self, output_dir):
        """
        出力ディレクトリを作成する
        
        Parameters:
            output_dir (str): 出力ディレクトリパス
        """
        os.makedirs(output_dir, exist_ok=True)
    
    def _save_to_csv(self, df, output_file):
        """
        データをCSVファイルとして保存する
        
        Parameters:
            df (DataFrame): データフレーム
            output_file (str): 出力ファイルパス
        """
        try:
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            self.logger.info(f"データを保存しました: {output_file}")
        except Exception as e:
            self.logger.error(f"データ保存中にエラーが発生しました: {e}")


class DailyDataMerger(BaseMerger):
    """日別データ結合クラス"""
    
    def __init__(self, config):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
        """
        super().__init__(config)
        self.input_dir = config['data_paths']['raw']['daily']
        self.output_dir = config['data_paths']['processed']['merged']['daily']
    
    def merge_files_for_station(self, station_id):
        """
        指定された観測所の日別データファイルを結合する
        
        Parameters:
            station_id (str): 観測所ID (例: "19_47418")
        
        Returns:
            bool: 成功した場合はTrue、失敗した場合はFalse
        """
        try:
            # ステーションIDを解析
            parts = station_id.split('_')
            if len(parts) != 2:
                self.logger.error(f"無効な観測所ID形式: {station_id}")
                return False
            
            prec_no, block_no = parts
            
            # 入力ディレクトリから該当するCSVファイルを検索
            csv_files = []
            for file in os.listdir(self.input_dir):
                if file.startswith(f"{prec_no}_{block_no}_") and file.endswith('.csv'):
                    csv_files.append(os.path.join(self.input_dir, file))
            
            if not csv_files:
                self.logger.warning(f"観測所 {station_id} のCSVファイルが見つかりません")
                return False
            
            # データフレームのリストを作成
            data_frames = []
            for csv_file in tqdm(csv_files, desc=f"観測所 {station_id} のファイルを読み込み中"):
                try:
                    df = pd.read_csv(csv_file, encoding="utf-8-sig")
                    data_frames.append(df)
                except Exception as e:
                    self.logger.error(f"ファイル読み込み中にエラーが発生しました: {csv_file}, エラー: {e}")
            
            if not data_frames:
                self.logger.warning(f"観測所 {station_id} の有効なデータフレームがありません")
                return False
            
            # データフレームを結合
            combined_df = pd.concat(data_frames, ignore_index=True)
            
            # データを時系列でソート（"年月日1"列を基準）
            if "年月日1" in combined_df.columns:
                combined_df = combined_df.sort_values(by="年月日1")
            
            # 重複行を削除
            if "年月日1" in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset="年月日1", keep="first")
            
            # 出力ディレクトリの作成
            self._create_output_dir(self.output_dir)
            
            # 結合データをCSVとして保存
            output_file = os.path.join(self.output_dir, f"{station_id}_daily.csv")
            self._save_to_csv(combined_df, output_file)
            
            return True
        
        except Exception as e:
            self.logger.error(f"観測所 {station_id} のデータ結合中にエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def merge_all_files(self):
        """
        すべての観測所の日別データファイルを結合する
        """
        # 結合対象の観測所IDを取得
        station_ids = self.config.get('data_processing', {}).get('merge', {}).get('station_ids', [])
        
        # 観測所IDが指定されていない場合は、入力ディレクトリからすべての観測所を検出
        if not station_ids:
            seen_station_ids = set()
            for file in os.listdir(self.input_dir):
                if file.endswith('.csv'):
                    parts = file.split('_')
                    if len(parts) >= 3:
                        station_id = f"{parts[0]}_{parts[1]}"
                        seen_station_ids.add(station_id)
            
            station_ids = list(seen_station_ids)
        
        if not station_ids:
            self.logger.warning("結合対象の観測所が見つかりません")
            return
        
        # 各観測所のデータを結合
        success_count = 0
        for station_id in tqdm(station_ids, desc="観測所データを結合中"):
            if self.merge_files_for_station(station_id):
                success_count += 1
        
        self.logger.info(f"日別データの結合が完了しました: {success_count}/{len(station_ids)} 観測所")


class HourlyDataMerger(BaseMerger):
    """毎時データ結合クラス"""
    
    def __init__(self, config):
        """
        コンストラクタ
        
        Parameters:
            config (dict): 設定情報
        """
        super().__init__(config)
        self.input_dir = config['data_paths']['raw']['hourly']
        self.output_dir = config['data_paths']['processed']['merged']['hourly']
    
    def merge_files_for_station(self, station_id):
        """
        指定された観測所の毎時データファイルを結合する
        
        Parameters:
            station_id (str): 観測所ID (例: "19_47418")
        
        Returns:
            bool: 成功した場合はTrue、失敗した場合はFalse
        """
        try:
            # ステーションIDを解析
            parts = station_id.split('_')
            if len(parts) != 2:
                self.logger.error(f"無効な観測所ID形式: {station_id}")
                return False
            
            prec_no, block_no = parts
            
            # 入力ディレクトリから該当するCSVファイルを検索
            csv_files = []
            for file in os.listdir(self.input_dir):
                if file.startswith(f"{prec_no}_{block_no}_") and file.endswith('.csv'):
                    csv_files.append(os.path.join(self.input_dir, file))
            
            if not csv_files:
                self.logger.warning(f"観測所 {station_id} のCSVファイルが見つかりません")
                return False
            
            # データフレームのリストを作成
            data_frames = []
            for csv_file in tqdm(csv_files, desc=f"観測所 {station_id} のファイルを読み込み中"):
                try:
                    df = pd.read_csv(csv_file, encoding="utf-8-sig")
                    data_frames.append(df)
                except Exception as e:
                    self.logger.error(f"ファイル読み込み中にエラーが発生しました: {csv_file}, エラー: {e}")
            
            if not data_frames:
                self.logger.warning(f"観測所 {station_id} の有効なデータフレームがありません")
                return False
            
            # データフレームを結合
            combined_df = pd.concat(data_frames, ignore_index=True)
            
            # データを時系列でソート（"日時1"列を基準）
            if "日時1" in combined_df.columns:
                combined_df = combined_df.sort_values(by="日時1")
            
            # 重複行を削除
            if "日時1" in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset="日時1", keep="first")
            
            # 出力ディレクトリの作成
            self._create_output_dir(self.output_dir)
            
            # 結合データをCSVとして保存
            output_file = os.path.join(self.output_dir, f"{station_id}_hourly.csv")
            self._save_to_csv(combined_df, output_file)
            
            return True
        
        except Exception as e:
            self.logger.error(f"観測所 {station_id} のデータ結合中にエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def merge_all_files(self):
        """
        すべての観測所の毎時データファイルを結合する
        """
        # 結合対象の観測所IDを取得
        station_ids = self.config.get('data_processing', {}).get('merge', {}).get('station_ids', [])
        
        # 観測所IDが指定されていない場合は、入力ディレクトリからすべての観測所を検出
        if not station_ids:
            seen_station_ids = set()
            for file in os.listdir(self.input_dir):
                if file.endswith('.csv'):
                    parts = file.split('_')
                    if len(parts) >= 3:
                        station_id = f"{parts[0]}_{parts[1]}"
                        seen_station_ids.add(station_id)
            
            station_ids = list(seen_station_ids)
        
        if not station_ids:
            self.logger.warning("結合対象の観測所が見つかりません")
            return
        
        # 各観測所のデータを結合
        success_count = 0
        for station_id in tqdm(station_ids, desc="観測所データを結合中"):
            if self.merge_files_for_station(station_id):
                success_count += 1
        
        self.logger.info(f"毎時データの結合が完了しました: {success_count}/{len(station_ids)} 観測所")
