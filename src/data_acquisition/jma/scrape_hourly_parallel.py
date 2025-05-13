import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def scrape_hourly_weather(year, month, day, prec_no, block_no, output_dir):
    """
    指定された年月日の毎時気象データをスクレイピングしてCSVファイルに保存します。

    Parameters:
        year (int): 年
        month (int): 月
        day (int): 日
        prec_no (int): 地域番号
        block_no (int): 地点番号
        output_dir (str): 保存先のディレクトリ
    """
    base_url = "https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php"
    params = {
        "prec_no": prec_no,
        "block_no": block_no,
        "year": year,
        "month": month,
        "day": day,
        "view": "p1"
    }

    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)

    # ファイル名を生成
    output_file = os.path.join(output_dir, f"{prec_no}_{block_no}_{year}{month:02d}{day:02d}.csv")

    # データを取得
    response = requests.get(base_url, params=params)
    response.encoding = "utf-8"  # 日本語の文字コードに対応
    soup = BeautifulSoup(response.text, "html.parser")

    # 表を抽出
    table = soup.find("table", class_="data2_s")  # 表を識別するクラス名
    if not table:
        print(f"データが見つかりませんでした: {year}年 {month}月 {day}日")
        return

    # データを解析
    rows = table.find_all("tr")
    data = []
    for row in rows:
        cols = [col.text.strip().replace('"', '') for col in row.find_all(["th", "td"])]
        data.append(cols)

    # ヘッダとデータ分離
    data = data[1:]  # 先頭4行を削除

    # ヘッダを手動で設定
    header = [
        "日時1", "日時2", "時刻", "気圧_現地_hPa", "気圧_海面_hPa", "降水量_mm", "気温_dC", "露点温度_dC", "蒸気圧_hPa",
        "湿度_per", "風速_mpers", "風向", "日照時間_h", "全天日射量_MJperm2", "降雪_cm", "積雪_cm",
        "天気", "雲量", "視程_km"
    ]

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
    df = pd.DataFrame(data_with_datetime, columns=header)

    # CSVファイルとして保存
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"データを保存しました: {output_file}")

def process_parallel(start_year, end_year, start_month, end_month, start_day, end_day, prec_no, block_no, output_dir):
    """
    指定された範囲のデータを並列処理でスクレイピングする。

    Parameters:
        start_year (int): 開始年
        end_year (int): 終了年
        start_month (int): 開始月
        end_month (int): 終了月
        start_day (int): 開始日
        end_day (int): 終了日
        prec_no (int): 地域番号
        block_no (int): 地点番号
        output_dir (str): 保存先のディレクトリ
    """
    tasks = []
    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            for day in range(start_day, end_day + 1):
                tasks.append((year, month, day, prec_no, block_no, output_dir))

    def task_wrapper(args):
        scrape_hourly_weather(*args)

    with ThreadPoolExecutor() as executor:
        executor.map(task_wrapper, tasks)

if __name__ == "__main__":
    # スクレイピング対象の範囲を指定
    start_year = 2023
    end_year = 2024
    start_month = 1
    end_month = 12
    start_day = 1
    end_day = 31
    prec_no = 19
    block_no = 47418
    output_dir = "data/raw/scraped/hourly"

    # 並列処理でスクレイピング実行
    process_parallel(start_year, end_year, start_month, end_month, start_day, end_day, prec_no, block_no, output_dir)
