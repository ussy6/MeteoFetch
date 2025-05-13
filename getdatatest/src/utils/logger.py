import logging
import os
import sys
from datetime import datetime

class Logger:
    """ロギングユーティリティクラス"""
    
    def __init__(self, name, log_dir='logs', log_level=logging.INFO, console_output=True):
        """
        コンストラクタ
        
        Parameters:
            name (str): ロガー名
            log_dir (str): ログファイルの保存ディレクトリ
            log_level (int): ログレベル
            console_output (bool): コンソール出力を有効にするかどうか
        """
        self.name = name
        self.log_dir = log_dir
        self.log_level = log_level
        self.console_output = console_output
        
        # ロガーの設定
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """
        ロガーの設定を行う
        
        Returns:
            Logger: 設定済みのロガーオブジェクト
        """
        # ログディレクトリの作成
        os.makedirs(self.log_dir, exist_ok=True)
        
        # ロガーの取得
        logger = logging.getLogger(self.name)
        logger.setLevel(self.log_level)
        
        # 既存のハンドラをクリア
        if logger.handlers:
            logger.handlers.clear()
        
        # 現在の日時に基づいたログファイル名を生成
        log_filename = os.path.join(
            self.log_dir, 
            f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        
        # ファイルハンドラの設定
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(self.log_level)
        
        # コンソールハンドラの設定（オプション）
        if self.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            
            # フォーマッタの設定
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        # ファイルハンドラのフォーマッタ
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # ハンドラをロガーに追加
        logger.addHandler(file_handler)
        
        return logger
    
    def get_logger(self):
        """
        設定済みのロガーを返す
        
        Returns:
            Logger: ロガーオブジェクト
        """
        return self.logger


def setup_root_logger(log_dir='logs', log_level=logging.INFO):
    """
    ルートロガーを設定する
    
    Parameters:
        log_dir (str): ログファイルの保存ディレクトリ
        log_level (int): ログレベル
        
    Returns:
        Logger: 設定済みのルートロガー
    """
    # ログディレクトリの作成
    os.makedirs(log_dir, exist_ok=True)
    
    # ルートロガーの取得
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 既存のハンドラをクリア
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    # 現在の日時に基づいたログファイル名を生成
    log_filename = os.path.join(
        log_dir, 
        f"weather_data_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    # ファイルハンドラの設定
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(log_level)
    
    # コンソールハンドラの設定
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # フォーマッタの設定
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # ハンドラをロガーに追加
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger


# ロギングレベルのマッピング
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def get_logger_from_config(config, name=None):
    """
    設定から適切なログレベルでロガーを取得する
    
    Parameters:
        config (dict): 設定情報
        name (str): ロガー名（Noneの場合は呼び出し元のモジュール名）
        
    Returns:
        Logger: 設定済みのロガー
    """
    # ロガー名が指定されていない場合は呼び出し元のモジュール名を使用
    if name is None:
        import inspect
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        name = module.__name__
    
    # 設定からログレベルを取得
    log_level_str = config.get('logging', {}).get('level', 'INFO')
    log_level = LOG_LEVELS.get(log_level_str, logging.INFO)
    
    # ログディレクトリを取得
    log_dir = config.get('logging', {}).get('directory', 'logs')
    
    # ロガーを設定して返す
    logger = Logger(name, log_dir=log_dir, log_level=log_level).get_logger()
    return logger
