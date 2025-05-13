#!/usr/bin/env python3
"""
気象データ処理パイプライン実行スクリプト
データ収集から加工、出力までの一連の処理を実行します
"""

import os
import sys
import argparse
import logging
import json
import sys
from pathlib import Path
import time

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
        logging.FileHandler('logs/weather_data_pipeline.log'),
        logging.StreamHandler()
    ]
)

def setup_argparser():
    """コマンドライン引数のパーサーを設定"""
    parser = argparse.ArgumentParser(description='気象データ処理パイプライン実行スクリプト')
    
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
        '--parallel',
        action='store_true',
        help='並列処理を有効にする'
    )
    
    parser.add_argument(
        '--skip-acquisition',
        action='store_true',
        help='データ収集ステップをスキップする'
    )
    
    parser.add_argument(
        '--skip-merge',
        action='store_true',
        help='データ結合ステップをスキップする'
    )
    
    parser.add_argument(
        '--skip-interpolation',
        action='store_true',
        help='データ補間ステップをスキップする'
    )
    
    parser.add_argument(
        '--skip-formatting',
        action='store_true',
        help='データフォーマットステップをスキップする'
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
        '--output-dir',
        help='最終出力ディレクトリを指定'
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

def run_data_acquisition(config, station_data, args):
    """データ収集ステップを実行"""
    if args.skip_acquisition:
        logging.info("データ収集ステップをスキップします")
        return
    
    start_time = time.time()
    logging.info("データ収集ステップを開始します...")
    
    # 観測所のフィルタリング
    filtered_stations = filter_stations(station_data, args.station_id)
    
    # 期間設定のオーバーライド
    if args.year:
        config['scraping']['jma']['target_period']['start_year'] = args.year
        config['scraping']['jma']['target_period']['end_year'] = args.year
    
    # 日別データスクレイピング
    logging.info("日別データのスクレイピングを開始します...")
    daily_scraper = DailyJMAScraper(config, filtered_stations)
    daily_scraper.scrape_all_data(parallel=args.parallel)
    
    # 毎時データスクレイピング
    logging.info("毎時データのスクレイピングを開始します...")
    hourly_scraper = HourlyJMAScraper(config, filtered_stations)
    hourly_scraper.scrape_all_data(parallel=args.parallel)
    
    elapsed_time = time.time() - start_time
    logging.info(f"データ収集ステップが完了しました（所要時間: {elapsed_time:.2f}秒）")

def run_data_merge(config, args):
    """データ結合ステップを実行"""
    if args.skip_merge:
        logging.info("データ結合ステップをスキップします")
        return
    
    start_time = time.time()
    logging.info("データ結合ステップを開始します...")
    
    # 観測所の指定があれば、その観測所のみを処理
    if args.station_id:
        config['data_processing']['merge']['station_ids'] = [args.station_id]
    
    # 日別データ結合
    logging.info("日別データの結合を開始します...")
    daily_merger = DailyDataMerger(config)
    daily_merger.merge_all_files()
    
    # 毎時データ結合
    logging.info("毎時データの結合を開始します...")
    hourly_merger = HourlyDataMerger(config)
    hourly_merger.merge_all_files()
    
    elapsed_time = time.time() - start_time
    logging.info(f"データ結合ステップが完了しました（所要時間: {elapsed_time:.2f}秒）")

def run_data_interpolation(config, args):
    """データ補間ステップを実行"""
    if args.skip_interpolation:
        logging.info("データ補間ステップをスキップします")
        return
    
    start_time = time.time()
    logging.info("データ補間ステップを開始します...")
    
    # 年の指定があれば、その年のみを処理
    if args.year:
        config['data_processing']['interpolate']['years'] = [args.year]
    
    # 観測所の指定があれば、その観測所のみを処理
    if args.station_id:
        config['data_processing']['interpolate']['station_ids'] = [args.station_id]
    
    # 出力ディレクトリの指定があれば上書き
    if args.output_dir:
        config['data_paths']['processed']['interpolated'] = args.output_dir
    
    interpolator = DataInterpolator(config)
    interpolator.interpolate_all_data()
    
    elapsed_time = time.time() - start_time
    logging.info(f"データ補間ステップが完了しました（所要時間: {elapsed_time:.2f}秒）")

def run_data_formatting(config, station_data, args):
    """データフォーマットステップを実行"""
    if args.skip_formatting:
        logging.info("データフォーマットステップをスキップします")
        return
    
    start_time = time.time()
    logging.info("データフォーマットステップを開始します...")
    
    # 観測所のフィルタリング
    filtered_stations = filter_stations(station_data, args.station_id)
    
    # 年の指定があれば、その年のみを処理
    if args.year:
        config['data_processing']['format']['years'] = [args.year]
    
    # 出力ディレクトリの指定があれば上書き
    if args.output_dir:
        config['data_paths']['processed']['formatted'] = args.output_dir
    
    formatter = DataFormatter(config, filtered_stations)
    formatter.format_all_data()
    
    elapsed_time = time.time() - start_time
    logging.info(f"データフォーマットステップが完了しました（所要時間: {elapsed_time:.2f}秒）")

def main():
    """メイン関数"""
    # 処理開始時刻
    total_start_time = time.time()
    
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
    
    # 並列処理設定のオーバーライド
    if args.parallel:
        config['scraping']['jma']['parallel_processing']['enable'] = True
    
    # パイプラインの各ステップを実行
    try:
        # 1. データ収集
        run_data_acquisition(config, station_data, args)
        
        # 2. データ結合
        run_data_merge(config, args)
        
        # 3. データ補間
        run_data_interpolation(config, args)
        
        # 4. データフォーマット
        run_data_formatting(config, station_data, args)
        
        # 処理終了
        total_elapsed_time = time.time() - total_start_time
        logging.info(f"全ての処理が完了しました（総所要時間: {total_elapsed_time:.2f}秒）")
        
    except Exception as e:
        logging.error(f"処理中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return

if __name__ == "__main__":
    main()
