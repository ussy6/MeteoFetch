#!/usr/bin/env python3
"""
気象観測所情報を管理するスクリプト
- 観測所情報の追加
- 観測所情報の検索
- 観測所情報の一覧表示
"""

import json
import os
import argparse
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd

def load_station_data(file_path):
    """観測所情報を読み込む"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            station_data = json.load(f)
        return station_data
    except FileNotFoundError:
        print(f"観測所ファイルが見つかりません: {file_path}")
        return {"stations": []}
    except json.JSONDecodeError:
        print(f"観測所ファイルのフォーマットが不正です: {file_path}")
        return {"stations": []}

def save_station_data(file_path, station_data):
    """観測所情報を保存する"""
    try:
        # 出力ディレクトリの作成
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(station_data, f, ensure_ascii=False, indent=2)
        print(f"観測所情報を保存しました: {file_path}")
        return True
    except Exception as e:
        print(f"観測所情報の保存中にエラーが発生しました: {e}")
        return False

def fetch_jma_station_list():
    """気象庁サイトから観測所リストを取得"""
    url = "https://www.data.jma.go.jp/obd/stats/etrn/select/prefecture.php"
    
    try:
        response = requests.get(url)
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 都道府県のリンクを取得
        prefecture_links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and 'prec_no' in href:
                prefecture_links.append(f"https://www.data.jma.go.jp/obd/stats/etrn/{href}")
        
        # 各都道府県ページから観測所情報を取得
        stations = []
        for prefecture_url in prefecture_links:
            response = requests.get(prefecture_url)
            response.encoding = "utf-8"
            prefecture_soup = BeautifulSoup(response.text, "html.parser")
            
            # 都道府県名の取得
            prefecture_name = prefecture_soup.find('h1').text.strip() if prefecture_soup.find('h1') else ""
            
            # 観測所のリンクを取得
            for link in prefecture_soup.find_all('a'):
                href = link.get('href')
                if href and 'prec_no' in href and 'block_no' in href:
                    station_name = link.text.strip()
                    
                    # URLからパラメータを抽出
                    params = {}
                    for param in href.split('?')[1].split('&'):
                        key, value = param.split('=')
                        params[key] = value
                    
                    # 観測所情報を追加
                    stations.append({
                        "name": station_name,
                        "prefecture": prefecture_name,
                        "prec_no": int(params.get("prec_no", 0)),
                        "block_no": int(params.get("block_no", 0))
                    })
        
        return pd.DataFrame(stations)
    
    except Exception as e:
        print(f"観測所リストの取得中にエラーが発生しました: {e}")
        return pd.DataFrame()

def search_stations(df, keyword=""):
    """観測所を検索"""
    if keyword:
        # 観測所名または都道府県名で検索
        result = df[df['name'].str.contains(keyword) | df['prefecture'].str.contains(keyword)]
    else:
        result = df
    
    return result

def list_stations(station_file):
    """観測所一覧を表示"""
    station_data = load_station_data(station_file)
    
    if not station_data["stations"]:
        print("観測所情報がありません")
        return
    
    print("\n登録済み観測所一覧:")
    print("-" * 60)
    print(f"{'観測所名':<15} {'地域番号':<10} {'地点番号':<10} {'緯度':<15} {'経度':<15} {'標高':<10}")
    print("-" * 60)
    
    for station in station_data["stations"]:
        location = station.get("location", {})
        print(f"{station['name']:<15} {station['prec_no']:<10} {station['block_no']:<10} "
              f"{location.get('latitude', 'N/A'):<15} {location.get('longitude', 'N/A'):<15} "
              f"{location.get('elevation', 'N/A'):<10}")
    
    print("-" * 60)
    print(f"合計: {len(station_data['stations'])}件")

def add_station(station_file, prec_no, block_no, name, latitude="", longitude="", elevation=""):
    """観測所情報を追加"""
    # 既存の観測所情報を読み込む
    station_data = load_station_data(station_file)
    
    # 既に登録されているか確認
    for station in station_data["stations"]:
        if station["prec_no"] == prec_no and station["block_no"] == block_no:
            print(f"この観測所は既に登録されています: {name} (prec_no: {prec_no}, block_no: {block_no})")
            return False
    
    # 新しい観測所情報を追加
    new_station = {
        "prec_no": prec_no,
        "block_no": block_no,
        "name": name,
        "location": {
            "latitude": latitude,
            "longitude": longitude,
            "elevation": elevation
        }
    }
    
    station_data["stations"].append(new_station)
    
    # 保存
    return save_station_data(station_file, station_data)

def run_interactive_mode(station_file):
    """インタラクティブモードで実行"""
    print("=" * 60)
    print("  気象観測所情報管理ツール（インタラクティブモード）")
    print("=" * 60)
    
    while True:
        print("\nメニュー:")
        print("1) 観測所一覧の表示")
        print("2) 観測所の検索")
        print("3) 観測所の追加")
        print("4) 終了")
        
        choice = input("\n選択 (1-4): ")
        
        if choice == "1":
            list_stations(station_file)
        
        elif choice == "2":
            print("\n観測所検索")
            keyword = input("検索キーワード（観測所名または都道府県名）: ")
            
            # 気象庁サイトから観測所リストを取得
            print("気象庁サイトから観測所情報を取得中...")
            df = fetch_jma_station_list()
            
            if df.empty:
                print("観測所情報を取得できませんでした")
                continue
            
            # 検索実行
            result = search_stations(df, keyword)
            
            if result.empty:
                print(f"'{keyword}'に一致する観測所が見つかりませんでした")
                continue
            
            # 検索結果の表示
            print(f"\n検索結果: {len(result)}件")
            print(result[['name', 'prefecture', 'prec_no', 'block_no']].to_string(index=False))
            
            # 選択して追加
            add_choice = input("\n観測所を追加しますか？ (y/n): ")
            if add_choice.lower() == 'y':
                try:
                    idx = int(input("追加する観測所の行番号を入力: "))
                    selected = result.iloc[idx]
                    
                    lat = input(f"緯度（例: {selected['name']}）: ")
                    lon = input(f"経度（例: 135.1234 E）: ")
                    elev = input(f"標高（例: 25）: ")
                    
                    if add_station(
                        station_file, 
                        int(selected['prec_no']), 
                        int(selected['block_no']), 
                        selected['name'],
                        lat, lon, elev
                    ):
                        print(f"観測所を追加しました: {selected['name']}")
                except (ValueError, IndexError) as e:
                    print(f"無効な入力です: {e}")
        
        elif choice == "3":
            print("\n観測所の手動追加")
            try:
                name = input("観測所名: ")
                prec_no = int(input("地域番号 (prec_no): "))
                block_no = int(input("地点番号 (block_no): "))
                lat = input("緯度（例: 35.6895 N）: ")
                lon = input("経度（例: 139.6917 E）: ")
                elev = input("標高（例: 25）: ")
                
                if add_station(station_file, prec_no, block_no, name, lat, lon, elev):
                    print(f"観測所を追加しました: {name}")
            except ValueError as e:
                print(f"無効な入力です: {e}")
        
        elif choice == "4":
            print("プログラムを終了します")
            break
        
        else:
            print("無効な選択です。1から4の数字を入力してください。")

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='気象観測所情報管理スクリプト')
    
    parser.add_argument(
        '--station', 
        default='src/config/station_data.json',
        help='観測所情報ファイルのパス (デフォルト: src/config/station_data.json)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='サブコマンド')
    
    # list サブコマンド
    list_parser = subparsers.add_parser('list', help='観測所一覧を表示')
    
    # search サブコマンド
    search_parser = subparsers.add_parser('search', help='観測所を検索')
    search_parser.add_argument('keyword', nargs='?', default="", help='検索キーワード')
    
    # add サブコマンド
    add_parser = subparsers.add_parser('add', help='観測所を追加')
    add_parser.add_argument('--prec-no', type=int, required=True, help='地域番号')
    add_parser.add_argument('--block-no', type=int, required=True, help='地点番号')
    add_parser.add_argument('--name', required=True, help='観測所名')
    add_parser.add_argument('--latitude', default="", help='緯度')
    add_parser.add_argument('--longitude', default="", help='経度')
    add_parser.add_argument('--elevation', default="", help='標高')
    
    # interactive サブコマンド
    interactive_parser = subparsers.add_parser('interactive', help='インタラクティブモードで実行')
    
    args = parser.parse_args()
    
    # サブコマンドに基づいて処理を実行
    try:
        if args.command == 'list':
            list_stations(args.station)
        
        elif args.command == 'search':
            df = fetch_jma_station_list()
            if not df.empty:
                result = search_stations(df, args.keyword)
                print(result[['name', 'prefecture', 'prec_no', 'block_no']].to_string(index=False))
        
        elif args.command == 'add':
            if add_station(
                args.station, 
                args.prec_no, 
                args.block_no, 
                args.name,
                args.latitude, 
                args.longitude, 
                args.elevation
            ):
                print(f"観測所を追加しました: {args.name}")
        
        elif args.command == 'interactive' or args.command is None:
            run_interactive_mode(args.station)
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
