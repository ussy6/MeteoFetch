import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

def scrape_weather_data(year, month, prec_no, block_no, output_dir):
    """
    指定された年と月の気象データをスクレイピングしてCSVファイルに保存します。

    Parameters:
        year (int): 年
        month (int): 月
        prec_no (int): 地域番号
        block_no (int): 地点番号
        output_dir (str): 保存先のディレクトリ
    """
    base_url = "https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php"
    params = {
        "prec_no": prec_no,
        "block_no": block_no,
        "year": year,
        "month": month,
        "day": 1,
        "view": "p1"
    }

    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)

    # ファイル名を生成
    output_file = os.path.join(output_dir, f"{prec_no}_{block_no}_{year}{month:02d}.csv")

    # データを取得
    response = requests.get(base_url, params=params)
    response.encoding = "utf-8"  # 日本語の文字コードに対応
    soup = BeautifulSoup(response.text, "html.parser")

    # 表を抽出
    table = soup.find("table", class_="data2_s")  # 表を識別するクラス名
    if not table:
        print(f"データが見つかりませんでした: {year}年 {month}月")
        return

    # データを解析
    rows = table.find_all("tr")
    data = []
    for row in rows:
        cols = [col.text.strip().replace('"', '') for col in row.find_all(["th", "td"])]
        data.append(cols)

    # ヘッダとデータ分離
    data = data[4:]  # 先頭4行を削除

    # ヘッダを手動で設定
    header = [
        "年月日1", "年月日2", "気圧_現地_hPa", "気圧_海面_hPa", "日降水量_mm", "日最大降水量_1h_mm", "日最大降水量_10m_mm",
        "日平均気温_dC", "日最高気温_dC", "日最低気温_dC", "日平均湿度_per", "日最小湿度_per",
        "日平均風速_ms", "日最大風速_ms", "日最大風速の風向", "日最大瞬間風速_ms", "日最大瞬間風速の風向",
        "日照時間_h", "日降雪深_cm", "日最深積雪_cm", "天気概況_昼_06-18", "天気概況_夜_18-06"
    ]

    # 日付の形式を変換
    data_with_date2 = []
    for row in data:
        if row:
            try:
                date_str = row[0]
                date_obj = datetime.strptime(date_str, "%d")
                yyyymmdd = f"{year}{month:02d}{date_obj.strftime('%d')}"
                yyyy_mm_dd = f"{year}/{month}/{int(date_obj.strftime('%d'))}"
                row.insert(1, yyyy_mm_dd)  # 年月日2を年月日1の隣に挿入
                row[0] = yyyymmdd  # 年月日1にyyyymmdd形式を設定
                data_with_date2.append(row)
            except ValueError:
                pass

    # データフレームに変換
    df = pd.DataFrame(data_with_date2, columns=header)

    # CSVファイルとして保存
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"データを保存しました: {output_file}")

if __name__ == "__main__":
    # スクレイピング対象の範囲を指定
    start_year = 2023
    end_year = 2024
    start_month = 1
    end_month = 12
    prec_no = 19
    block_no = 47418
    output_dir = "data/raw/scraped/dayly"

    # 指定範囲のデータを収集
    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            scrape_weather_data(year, month, prec_no, block_no, output_dir)
