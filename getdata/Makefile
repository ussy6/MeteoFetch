# 気象データ処理システム用Makefile

# 設定ファイルのパス
CONFIG_FILE = src/config/config.json
STATION_FILE = src/config/station_data.json

# デフォルトターゲット
.PHONY: all
all: setup run

# 環境セットアップ
.PHONY: setup
setup: dirs config
	@echo "セットアップ完了"

# 必要なディレクトリ構造の作成
.PHONY: dirs
dirs:
	@echo "ディレクトリ構造を作成中..."
	python setup_dirs.py --verbose

# 設定ファイルの作成
.PHONY: config
config:
	@echo "設定ファイルを作成中..."
	python export_config.py

# 全処理の実行
.PHONY: run
run:
	@echo "全処理を実行中..."
	python run_pipeline.py --config $(CONFIG_FILE) --station $(STATION_FILE)

# 並列処理で実行
.PHONY: run-parallel
run-parallel:
	@echo "並列処理で全処理を実行中..."
	python run_pipeline.py --config $(CONFIG_FILE) --station $(STATION_FILE) --parallel

# データ収集のみ実行
.PHONY: scrape
scrape:
	@echo "データ収集を実行中..."
	python run_task.py --config $(CONFIG_FILE) --station $(STATION_FILE) --task scrape-daily
	python run_task.py --config $(CONFIG_FILE) --station $(STATION_FILE) --task scrape-hourly

# データ結合のみ実行
.PHONY: merge
merge:
	@echo "データ結合を実行中..."
	python run_task.py --config $(CONFIG_FILE) --station $(STATION_FILE) --task merge-daily
	python run_task.py --config $(CONFIG_FILE) --station $(STATION_FILE) --task merge-hourly

# データ補間のみ実行
.PHONY: interpolate
interpolate:
	@echo "データ補間を実行中..."
	python run_task.py --config $(CONFIG_FILE) --station $(STATION_FILE) --task interpolate

# データフォーマットのみ実行
.PHONY: format
format:
	@echo "データフォーマットを実行中..."
	python run_task.py --config $(CONFIG_FILE) --station

# 観測所情報の管理
.PHONY: stations
stations:
	@echo "観測所情報管理ツールを実行中..."
	python manage_stations.py interactive

# 特定の年のデータのみ処理
.PHONY: year
year:
	@if [ -z "$(YEAR)" ]; then \
		echo "ERROR: YEAR変数が指定されていません。例: make year YEAR=2023"; \
		exit 1; \
	fi
	@echo "$(YEAR)年のデータを処理中..."
	python run_pipeline.py --config $(CONFIG_FILE) --station $(STATION_FILE) --year $(YEAR)

# 特定の観測所のデータのみ処理
.PHONY: station
station:
	@if [ -z "$(STATION_ID)" ]; then \
		echo "ERROR: STATION_ID変数が指定されていません。例: make station STATION_ID=19_47418"; \
		exit 1; \
	fi
	@echo "観測所$(STATION_ID)のデータを処理中..."
	python run_pipeline.py --config $(CONFIG_FILE) --station $(STATION_FILE) --station-id $(STATION_ID)

# クリーンアップ（生成されたデータを削除）
.PHONY: clean
clean:
	@echo "生成されたデータを削除中..."
	rm -rf data/processed/*
	@echo "データ削除完了"

# 完全クリーンアップ（すべてのデータとログを削除）
.PHONY: clean-all
clean-all: clean
	@echo "すべてのデータとログを削除中..."
	rm -rf data/raw/scraped/*
	rm -rf logs/*
	@echo "すべてのデータとログの削除完了"

# ヘルプを表示
.PHONY: help
help:
	@echo "気象データ処理システム用Makefile"
	@echo ""
	@echo "使用方法:"
	@echo "  make [ターゲット]"
	@echo ""
	@echo "ターゲット:"
	@echo "  setup         - 必要なディレクトリ構造と設定ファイルを作成"
	@echo "  run           - すべての処理を実行"
	@echo "  run-parallel  - 並列処理ですべての処理を実行"
	@echo "  scrape        - データ収集のみ実行"
	@echo "  merge         - データ結合のみ実行"
	@echo "  interpolate   - データ補間のみ実行"
	@echo "  format        - データフォーマットのみ実行"
	@echo "  stations      - 観測所情報管理ツールを実行"
	@echo "  year          - 特定の年のデータのみ処理 (例: make year YEAR=2023)"
	@echo "  station       - 特定の観測所のデータのみ処理 (例: make station STATION_ID=19_47418)"
	@echo "  clean         - 生成されたデータを削除"
	@echo "  clean-all     - すべてのデータとログを削除"
	@echo "  help          - このヘルプを表示"
	@echo ""
	@echo "設定:"
	@echo "  CONFIG_FILE   = $(CONFIG_FILE)"
	@echo "  STATION_FILE  = $(STATION_FILE)"
