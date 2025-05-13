#!/usr/bin/env python3
"""
現在の設定を標準的なJSONファイルにエクスポートするスクリプト
"""

import json
import os
import argparse
from datetime import datetime
import sys

def create_default_config():
    """デフォルト設定を生成"""
    config = {
        "scraping": {
            "comments": "気象データスクレイピングの設定",
            "jma": {
                "base_url": {
                    "daily": "https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php",
                    "hourly": "https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php"
                },
                "target_period": {
                    "start_year": 2023,
                    "end_year": 2024,
                    "start_month": 1,
                    "end_month": 12,
                    "start_day": 1,
                    "end_day": 31
                },
                "parallel_processing": {
                    "enable": True,
                    "max_workers": 4
                }
            }
        },
        "data_paths": {
            "raw": {
                "daily": "data/raw/scraped/daily",
                "hourly": "data/raw/scraped/hourly"
            },
            "processed": {
                "merged": {
                    "daily": "data/processed/merged/daily",
                    "hourly": "data/processed/merged/hourly"
                },
                "interpolated": "data/processed/interpolated",
                "formatted": "data/processed/formatted"
            }
        },
        "data_processing": {
            "merge": {
                "station_ids": []  # 空の場合はすべての観測所を処理
            },
            "interpolate": {
                "years": [],  # 空の場合はすべての年を処理
                "station_ids": []  # 空の場合はすべての観測所を処理
            },
            "format": {
                "years": []  # 空の場合はすべての年を処理
            }
        },
        "columns": {
            "daily_header": [
                "年月日1", "年月日2", "気圧_現地_hPa", "気圧_海面_hPa", "日降水量_mm", "日最大降水量_1h_mm",
                "日最大降水量_10m_mm", "日平均気温_dC", "日最高気温_dC", "日最低気温_dC", "日平均湿度_per", 
                "日最小湿度_per", "日平均風速_ms", "日最大風速_ms", "日最大風速の風向", "日最大瞬間風速_ms", 
                "日最大瞬間風速の風向", "日照時間_h", "日降雪深_cm", "日最深積雪_cm", "天気概況_昼_06-18", 
                "天気概況_夜_18-06"
            ],
            "hourly_header": [
                "日時1", "日時2", "時刻", "気圧_現地_hPa", "気圧_海面_hPa", "降水量_mm", "気温_dC", 
                "露点温度_dC", "蒸気圧_hPa", "湿度_per", "風速_mpers", "風向", "日照時間_h", 
                "全天日射量_MJperm2", "降雪_cm", "積雪_cm", "天気", "雲量", "視程_km"
            ],
            "interpolate_columns": [
                "気圧_現地_hPa", "気圧_海面_hPa", "降水量_mm", "気温_dC", "露点温度_dC", "蒸気圧_hPa",
                "湿度_per", "風速_mpers", "風向", "日照時間_h", "全天日射量_MJperm2", "降雪_cm", "積雪_cm"
            ],
            "columns_mapping": {
                "積雪_cm": "日最深積雪_cm"
            }
        }
    }
    return config

def create_default_station_data():
    """デフォルトの観測所情報を生成"""
    station_data = {
        "stations": [
            {
                "prec_no": 19,
                "block_no": 47418,
                "name": "釧路気象観測所",
                "location": {
                    "latitude": "42.9752 N",
                    "longitude": "144.3726 E",
                    "elevation": 31
                }
            },
            {
                "prec_no": 13,
                "block_no": 47412,
                "name": "札幌気象観測所",
                "location": {
                    "latitude": "43.0642 N",
                    "longitude": "141.3469 E",
                    "elevation": 19
                }
            }
        ]
    }
    return station_data

def export_config(output_dir, add_comments=True):
    """
    設定ファイルをエクスポートする
    
    Parameters:
        output_dir (str): 出力ディレクトリ
        add_comments (bool): コメントを追加するかどうか
    """
    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)
    
    # デフォルト設定の取得
    config = create_default_config()
    station_data = create_default_station_data()
    
    # コメント付きの設定ファイルを作成
    if add_comments:
        config_with_comments = {
            "# 設定ファイル": "気象データ収集・処理システム",
            "# 作成日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "# 説明": "このファイルは気象庁のデータをスクレイピングして処理するための設定です",
            **config
        }
        
        station_data_with_comments = {
            "# 観測所情報ファイル": "気象データ収集・処理システム",
            "# 作成日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "# 説明": "このファイルは処理対象の気象観測所の情報です",
            **station_data
        }
    else:
        config_with_comments = config
        station_data_with_comments = station_data
    
    # JSONファイルとして保存
    config_file = os.path.join(output_dir, "config.json")
    station_file = os.path.join(output_dir, "station_data.json")
    
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config_with_comments, f, ensure_ascii=False, indent=2)
    
    with open(station_file, 'w', encoding='utf-8') as f:
        json.dump(station_data_with_comments, f, ensure_ascii=False, indent=2)
    
    print(f"設定ファイルをエクスポートしました: {config_file}")
    print(f"観測所情報ファイルをエクスポートしました: {station_file}")

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='設定ファイルをエクスポートするスクリプト')
    
    parser.add_argument(
        '--output', 
        default='src/config',
        help='出力ディレクトリ (デフォルト: src/config)'
    )
    
    parser.add_argument(
        '--no-comments',
        action='store_true',
        help='コメントを追加しない'
    )
    
    args = parser.parse_args()
    
    try:
        export_config(args.output, not args.no_comments)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
