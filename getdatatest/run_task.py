#!/usr/bin/env python3
"""
個別タスク実行用スクリプト
特定の処理タスクのみを実行する場合に使用します
"""

import os
import argparse
import logging
import json
import sys
from pathlib import Path

# ルートディレクトリを追加（相対インポートのため）
sys.path.append(str(Path(__file__).parent))

from utils.config_loader import ConfigLoader
from data_acquisition.jma.daily_scraper import DailyJMAScraper
from data_acquisition.jma.hourly_scraper import HourlyJMAScraper
from data_processing.jma.merger import DailyDataMerger, HourlyDataMerger
from data_processing.jma.interpolator import DataInterpolator
from data_processing.jma.formatter import DataFormatter

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/weather_data_task.log'),
        logging.StreamHandler()
    ]
)

def setup_argparser():
    """コマンドライン引数のパーサーを設定"""
    parser = argparse.ArgumentParser(description='気象データ個別タスク実行スクリプト')
    
    parser.add_argument(
        '--config', 
        default='src/config/config.json',
        help='設定ファイルのパス (デフォルト: src/config/config.json)'
    )
    
    parser.add_argument(
        '--station', 
        default='src/config/station_data.json',
        help='観測所情報ファイルのパス (デフォルト: src/config/station_data.json)'
    )
    
    parser.add_argument(
        '--task',
        required=True,
        choices=[
            'scrape-daily', 
            'scrape-hourly', 
            'merge-daily', 
            'merge-hourly', 
            'interpolate', 
            'format'
        ],
        help='実行するタスク'
    )
    
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='並列処理を有効にする'
    )
    
    parser.add_argument(
        '--station-id',
        help='特定の観測所IDを指定（例: 19_47418）'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        help='特定の年を指定'
    )
    
    parser.add_argument(
        '--month',
        type=int,
        help='特定の月を指定'
    )
    
    parser.add_argument(
        '--day',
        type=int,
        help='特定の日を指定（毎時データのみ有効）'
    )
    
    return parser

def filter_stations(station_data, station_id):
    """指定されたIDに一致する観測所のみをフィルタリング"""
    if not station_id:
        return station_data
    
    filtered_stations = []
    for station in station_data['stations']:
        current_id = f"{station['prec_no']}_{station['block_no']}"
        if current_id == station_id:
            filtered_stations.append(station)
            break
    
    if not filtered_stations:
        logging.warning(f"指定されたID '{station_id}' の観測所が見つかりません")
        return station_data
    
    return {'stations': filtered_stations}

def scrape_daily(config, station_data, args):
    """日別データスクレイピングタスク"""
    logging.info("日別データのスクレイピングを開始します...")
    
    # 観測所のフィルタリング
    filtered_stations = filter_stations(station_data, args.station_id)
    
    # 期間設定のオーバーライド
    if args.year:
        config['scraping']['jma']['target_period']['start_year'] = args.year
        config['scraping']['jma']['target_period']['end_year'] = args.year
    
    if args.month:
        config['scraping']['jma']['target_period']['start_month'] = args.month
        config['scraping']['jma']['target_period']['end_month'] = args.month
    
    # スクレイピング実行
    daily_scraper = DailyJMAScraper(config, filtered_stations)
    daily_scraper.scrape_all_data(parallel=args.parallel)

def scrape_hourly(config, station_data, args):
    """毎時データスクレイピングタスク"""
    logging.info("毎時データのスクレイピングを開始します...")
    
    # 観測所のフィルタリング
    filtered_stations = filter_stations(station_data, args.station_id)
    
    # 期間設定のオーバーライド
    if args.year:
        config['scraping']['jma']['target_period']['start_year'] = args.year
        config['scraping']['jma']['target_period']['end_year'] = args.year
    
    if args.month:
        config['scraping']['jma']['target_period']['start_month'] = args.month
        config['scraping']['jma']['target_period']['end_month'] = args.month
    
    if args.day:
        config['scraping']['jma']['target_period']['start_day'] = args.day
        config['scraping']['jma']['target_period']['end_day'] = args.day
    
    # スクレイピング実行
    hourly_scraper = HourlyJMAScraper(config, filtered_stations)
    hourly_scraper.scrape_all_data(parallel=args.parallel)

def merge_daily(config, args):
    """日別データ結合タスク"""
    logging.info("日別データの結合を開始します...")
    
    # 観測所の指定があれば、その観測所のみを処理
    if args.station_id:
        config['data_processing']['merge']['station_ids'] = [args.station_id]
    
    daily_merger = DailyDataMerger(config)
    daily_merger.merge_all_files()

def merge_hourly(config, args):
    """毎時データ結合タスク"""
    logging.info("毎時データの結合を開始します...")
    
    # 観測所の指定があれば、その観測所のみを処理
    if args.station_id:
        config['data_processing']['merge']['station_ids'] = [args.station_id]
    
    hourly_merger = HourlyDataMerger(config)
    hourly_merger.merge_all_files()

def interpolate_data(config, args):
    """データ補間タスク"""
    logging.info("データ補間処理を開始します...")
    
    # 年の指定があれば、その年のみを処理
    if args.year:
        config['data_processing']['interpolate']['years'] = [args.year]
    
    # 観測所の指定があれば、その観測所のみを処理
    if args.station_id:
        config['data_processing']['interpolate']['station_ids'] = [args.station_id]
    
    interpolator = DataInterpolator(config)
    interpolator.interpolate_all_data()

def format_data(config, station_data, args):
    """データフォーマットタスク"""
    logging.info("データフォーマット処理を開始します...")
    
    # 観測所のフィルタリング
    filtered_stations = filter_stations(station_data, args.station_id)
    
    # 年の指定があれば、その年のみを処理
    if args.year:
        config['data_processing']['format']['years'] = [args.year]
    
    formatter = DataFormatter(config, filtered_stations)
    formatter.format_all_data()

def main():
    """メイン関数"""
    # コマンドライン引数の解析
    parser = setup_argparser()
    args = parser.parse_args()
    
    # ログディレクトリを作成
    os.makedirs('logs', exist_ok=True)
    
    # 設定ファイルの読み込み
    try:
        config_loader = ConfigLoader(args.config)
        config = config_loader.get_config()
        station_data = config_loader.get_station_data(args.station)
    except Exception as e:
        logging.error(f"設定ファイル読み込み中にエラーが発生しました: {e}")
        return
    
    # タスクに基づいて処理を実行
    try:
        if args.task == 'scrape-daily':
            scrape_daily(config, station_data, args)
        elif args.task == 'scrape-hourly':
            scrape_hourly(config, station_data, args)
        elif args.task == 'merge-daily':
            merge_daily(config, args)
        elif args.task == 'merge-hourly':
            merge_hourly(config, args)
        elif args.task == 'interpolate':
            interpolate_data(config, args)
        elif args.task == 'format':
            format_data(config, station_data, args)
    except Exception as e:
        logging.error(f"処理中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return

if __name__ == "__main__":
    main()
