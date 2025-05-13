# 気象データ処理システム

このリポジトリは、気象庁のウェブサイトから気象データをスクレイピングし、処理・加工するためのシステムです。日別データと毎時データの両方に対応しており、データの収集、結合、補間、フォーマット変換などの一連の処理を自動化します。

## 目次

1. [機能概要](#機能概要)
2. [システム要件](#システム要件)
3. [インストール方法](#インストール方法)
4. [使用方法](#使用方法)
5. [設定ファイル](#設定ファイル)
6. [ディレクトリ構造](#ディレクトリ構造)
7. [各モジュールの説明](#各モジュールの説明)
8. [処理フロー](#処理フロー)
9. [エラー対処方法](#エラー対処方法)
10. [貢献方法](#貢献方法)
11. [ライセンス](#ライセンス)

## 機能概要

- **データ収集**: 気象庁ウェブサイトから日別・毎時気象データをスクレイピング
- **データ結合**: 期間別に分割されたファイルを観測所ごとに結合
- **データ補間**: 欠測値やエラー値を線形補間等で補完
- **データフォーマット**: ISO 8601形式の日時や2段ヘッダーなどの標準化
- **並列処理**: 大量データの高速処理をサポート
- **柔軟な設定**: JSON形式による設定の一元管理
- **観測所管理**: 観測所情報の検索や追加機能

## システム要件

- Python 3.8以上
- 必要なPythonパッケージ:
  - requests
  - beautifulsoup4
  - pandas
  - numpy
  - tqdm

## インストール方法

1. リポジトリをクローンします:

```bash
git clone https://github.com/yourusername/weather-data-processor.git
cd weather-data-processor
```

2. 必要なパッケージをインストールします:

```bash
pip install -r requirements.txt
```

3. 初期セットアップを実行します:

```bash
make setup
```

これにより、必要なディレクトリ構造と設定ファイルが自動的に作成されます。

## 使用方法

### 基本的な使い方

全処理（データ収集、結合、補間、フォーマット）を一括で実行するには:

```bash
make run
```

並列処理で高速に実行するには:

```bash
make run-parallel
```

### 個別の処理を実行

特定の処理のみを実行することもできます:

```bash
# データ収集のみ実行
make scrape

# データ結合のみ実行
make merge

# データ補間のみ実行
make interpolate

# データフォーマットのみ実行
make format
```

### 特定の年や観測所を処理

```bash
# 2023年のデータのみ処理
make year YEAR=2023

# 釧路観測所(ID: 19_47418)のデータのみ処理
make station STATION_ID=19_47418
```

### 観測所情報の管理

観測所情報を管理するインタラクティブツールを実行するには:

```bash
make stations
```

または直接コマンドを使用:

```bash
# 観測所一覧の表示
python manage_stations.py list

# 観測所の検索
python manage_stations.py search 釧路

# 観測所の追加
python manage_stations.py add --prec-no 19 --block-no 47418 --name "釧路気象観測所" --latitude "42.9752 N" --longitude "144.3726 E" --elevation 31
```

### ヘルプの表示

```bash
make help
```

## 設定ファイル

設定ファイルは `src/config/` ディレクトリにあります:

### config.json

主要な設定パラメータを管理するファイルです:

```json
{
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
        "enable": true,
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
  "columns": {
    "daily_header": [...],
    "hourly_header": [...],
    "interpolate_columns": [...],
    "columns_mapping": {
      "積雪_cm": "日最深積雪_cm"
    }
  }
}
```

### station_data.json

観測所情報を管理するファイルです:

```json
{
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
```

## ディレクトリ構造

```
weather-data-processor/
├── data/                          # データディレクトリ
│   ├── raw/                       # 生データ
│   │   └── scraped/               # スクレイピングした生データ
│   │       ├── daily/             # 日別データ
│   │       └── hourly/            # 毎時データ
│   └── processed/                 # 処理済みデータ
│       ├── merged/                # 結合したデータ
│       │   ├── daily/             # 結合した日別データ
│       │   └── hourly/            # 結合した毎時データ
│       ├── interpolated/          # 補間したデータ
│       └── formatted/             # フォーマット済みデータ
├── logs/                          # ログファイル
├── src/                           # ソースコード
│   ├── config/                    # 設定ファイル
│   │   ├── config.json            # メイン設定ファイル
│   │   └── station_data.json      # 観測所情報ファイル
│   ├── data_acquisition/          # データ収集モジュール
│   │   └── jma/                   # 気象庁データ収集
│   │       ├── __init__.py
│   │       ├── daily_scraper.py   # 日別データスクレイピング
│   │       ├── hourly_scraper.py  # 毎時データスクレイピング
│   │       └── scraper.py         # 共通スクレイピング機能
│   ├── data_processing/           # データ処理モジュール
│   │   └── jma/                   # 気象庁データ処理
│   │       ├── __init__.py
│   │       ├── formatter.py       # データフォーマット処理
│   │       ├── interpolator.py    # データ補間処理
│   │       └── merger.py          # データ結合処理
│   └── utils/                     # ユーティリティ
│       ├── __init__.py
│       ├── config_loader.py       # 設定ファイル読み込み
│       └── logger.py              # ロギング機能
├── export_config.py               # 設定ファイルエクスポート
├── interactive_run.sh             # インタラクティブ実行スクリプト
├── Makefile                       # ビルド自動化
├── manage_stations.py             # 観測所情報管理
├── requirements.txt               # 依存パッケージ一覧
├── run.sh                         # 簡易実行スクリプト
├── run_pipeline.py                # パイプライン実行スクリプト
├── run_task.py                    # 個別タスク実行スクリプト
└── setup_dirs.py                  # ディレクトリ構造作成
```

## 各モジュールの説明

### データ収集モジュール

#### scraper.py

スクレイピングの基底クラスを提供します。基本的なHTTPリクエストやHTMLパース、ファイル保存などの共通機能を実装しています。

#### daily_scraper.py

日別気象データのスクレイピングを担当します。特定の年月の日別データを取得し、CSVファイルとして保存します。

#### hourly_scraper.py

毎時気象データのスクレイピングを担当します。特定の年月日の毎時データを取得し、CSVファイルとして保存します。並列処理にも対応しています。

### データ処理モジュール

#### merger.py

スクレイピングで取得した複数のCSVファイルを観測所ごとに結合します。日別データと毎時データの両方に対応しています。

#### interpolator.py

データ内の欠測値や不正な値（"×"や"--"など）を補間処理します。線形補間や日別データを使った補完などを実装しています。特に積雪データでは、長期間の欠測を0に置き換える特殊処理も行います。

#### formatter.py

データのフォーマット変換を行います。ISO 8601形式の日時表現への変換、日本語と英語の2段ヘッダーの作成、メタデータの追加などを行います。

### ユーティリティモジュール

#### config_loader.py

設定ファイルを読み込むためのユーティリティです。JSONファイルからの設定読み込みと構造化を行います。

#### logger.py

統一されたログ記録機能を提供します。ファイルとコンソールの両方にログを出力し、デバッグを支援します。

### 実行スクリプト

#### run_pipeline.py

データ収集から加工までの一連の処理を実行するメインスクリプトです。コマンドライン引数によりさまざまな処理オプションを指定できます。

#### run_task.py

個別のタスク（スクレイピング、結合、補間、フォーマット）のみを実行するためのスクリプトです。

#### manage_stations.py

観測所情報の管理（検索、追加、一覧表示）を行うスクリプトです。インタラクティブモードも提供します。

#### setup_dirs.py

必要なディレクトリ構造を作成するためのセットアップスクリプトです。

#### export_config.py

設定ファイルのテンプレートを生成します。

#### interactive_run.sh

対話形式でシステムを操作するためのシェルスクリプトです。

#### run.sh

よく使う処理をショートカットで実行するためのシェルスクリプトです。

#### Makefile

各種処理をコマンド一つで実行するためのビルド自動化ファイルです。

## 処理フロー

1. **データ収集**
   - 気象庁ウェブサイトから指定された期間の気象データをスクレイピング
   - 日別データと毎時データを別々に収集
   - データはCSVファイルとして保存（年月日ごとに分割）

2. **データ結合**
   - 同じ観測所の複数のCSVファイルを一つに結合
   - 時系列でソートし、重複を除去
   - 観測所ごとに1つの大きなCSVファイルを作成

3. **データ補間**
   - 欠測値や不正な値（"×"など）を検出
   - 積雪データに対する特殊処理（長期欠測を0に）
   - 線形補間による欠測値の補完
   - 日別データを使った毎時データの補完

4. **データフォーマット**
   - 日時をISO 8601形式（JST & UTC）に変換
   - 日本語と英語の2段ヘッダーを作成
   - 単位表記の統一
   - メタデータの付与
   - 最終的なCSVファイルの出力

## エラー対処方法

### モジュールが見つからないエラー

```
ModuleNotFoundError: No module named 'utils'
```

**解決策**: PYTHONPATHを設定してください。

```bash
# 一時的な設定
PYTHONPATH=. python run_pipeline.py

# または、Makefileを使用
make run  # Makefile内でPYTHONPATHを設定しています
```

### スクレイピング中のエラー

- **アクセス制限**: 短時間に多くのリクエストを送ると制限される場合があります。
  - 解決策: `config.json`の`parallel_processing.max_workers`の値を小さくするか、並列処理を無効にしてください。

- **データが見つからない**: 指定した期間のデータが存在しない場合があります。
  - 解決策: 気象庁のウェブサイトで該当するデータの有無を確認してください。

### データ処理中のエラー

- **メモリ不足**: 大量のデータを処理する場合、メモリ不足になることがあります。
  - 解決策: 年単位や観測所単位で処理を分割して実行してください。

## 貢献方法

1. リポジトリをフォークします
2. 機能ブランチを作成します (`git checkout -b my-new-feature`)
3. 変更をコミットします (`git commit -am 'Add some feature'`)
4. ブランチにプッシュします (`git push origin my-new-feature`)
5. プルリクエストを送信します

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は[LICENSE](LICENSE)ファイルを参照してください。

## 謝辞

このプロジェクトは、気象庁が提供する公開データを活用しています。データの提供に感謝いたします。
