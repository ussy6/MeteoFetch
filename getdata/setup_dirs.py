#!/usr/bin/env python3
"""
必要なディレクトリ構造を作成するスクリプト
"""

import os
import argparse
import json
import sys

def load_config(config_file):
    """設定ファイルを読み込む"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"設定ファイルが見つかりません: {config_file}")
        return None
    except json.JSONDecodeError:
        print(f"設定ファイルのフォーマットが不正です: {config_file}")
        return None

def create_directories(config, verbose=False):
    """設定ファイルから必要なディレクトリ構造を作成"""
    if not config:
        return False
    
    try:
        # データパス設定を取得
        data_paths = config.get('data_paths', {})
        
        # 生データディレクトリの作成
        raw_paths = data_paths.get('raw', {})
        for key, path in raw_paths.items():
            os.makedirs(path, exist_ok=True)
            if verbose:
                print(f"ディレクトリを作成しました: {path}")
        
        # 処理済みデータディレクトリの作成
        processed_paths = data_paths.get('processed', {})
        for key, value in processed_paths.items():
            if isinstance(value, dict):
                for sub_key, path in value.items():
                    os.makedirs(path, exist_ok=True)
                    if verbose:
                        print(f"ディレクトリを作成しました: {path}")
            else:
                os.makedirs(value, exist_ok=True)
                if verbose:
                    print(f"ディレクトリを作成しました: {value}")
        
        # ログディレクトリの作成
        os.makedirs('logs', exist_ok=True)
        if verbose:
            print("ディレクトリを作成しました: logs")
        
        # ソースディレクトリの作成（存在しない場合）
        src_dirs = [
            'src',
            'src/config',
            'src/data_acquisition',
            'src/data_acquisition/jma',
            'src/data_processing',
            'src/data_processing/jma',
            'src/utils'
        ]
        
        for src_dir in src_dirs:
            os.makedirs(src_dir, exist_ok=True)
            if verbose:
                print(f"ディレクトリを作成しました: {src_dir}")
        
        # __init__.py ファイルの作成
        init_dirs = [
            'src',
            'src/data_acquisition',
            'src/data_acquisition/jma',
            'src/data_processing',
            'src/data_processing/jma',
            'src/utils'
        ]
        
        for init_dir in init_dirs:
            init_file = os.path.join(init_dir, '__init__.py')
            if not os.path.exists(init_file):
                with open(init_file, 'w', encoding='utf-8') as f:
                    f.write(f"# {init_dir} package\n")
                if verbose:
                    print(f"ファイルを作成しました: {init_file}")
        
        return True
    
    except Exception as e:
        print(f"ディレクトリ作成中にエラーが発生しました: {e}")
        return False

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='必要なディレクトリ構造を作成するスクリプト')
    
    parser.add_argument(
        '--config', 
        default='src/config/config.json',
        help='設定ファイルのパス (デフォルト: src/config/config.json)'
    )
    
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='詳細な出力を表示'
    )
    
    args = parser.parse_args()
    
    try:
        # 設定ファイルが存在しない場合はデフォルト設定を使用
        if not os.path.exists(args.config):
            print(f"設定ファイルが存在しません: {args.config}")
            print("デフォルト設定を使用します")
            
            # デフォルト設定を作成（export_config.pyの内容を部分的に再利用）
            from export_config import create_default_config
            config = create_default_config()
        else:
            config = load_config(args.config)
        
        if create_directories(config, args.verbose):
            print("必要なディレクトリ構造の作成が完了しました")
        else:
            print("ディレクトリ構造の作成に失敗しました")
            sys.exit(1)
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
