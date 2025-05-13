#!/bin/bash
# 気象データ処理の簡易実行スクリプト

# デフォルト設定
CONFIG_FILE="src/config/config.json"
STATION_FILE="src/config/station_data.json"
PARALLEL=false
YEAR=""
STATION_ID=""

# 使用方法を表示する関数
function show_usage {
    echo "気象データ処理スクリプト"
    echo ""
    echo "使用方法:"
    echo "  $0 [オプション] <コマンド>"
    echo ""
    echo "コマンド:"
    echo "  all                 すべての処理を実行"
    echo "  scrape              データ収集のみ実行"
    echo "  merge               データ結合のみ実行"
    echo "  interpolate         データ補間のみ実行"
    echo "  format              データフォーマットのみ実行"
    echo ""
    echo "オプション:"
    echo "  -c, --config FILE   設定ファイルを指定 (デフォルト: $CONFIG_FILE)"
    echo "  -s, --station FILE  観測所ファイルを指定 (デフォルト: $STATION_FILE)"
    echo "  -p, --parallel      並列処理を有効にする"
    echo "  -y, --year YEAR     処理対象の年を指定"
    echo "  -i, --station-id ID 処理対象の観測所IDを指定 (例: 19_47418)"
    echo "  -h, --help          このヘルプを表示"
    echo ""
    echo "例:"
    echo "  $0 all                         すべての処理を実行"
    echo "  $0 -p scrape                   並列処理でデータ収集を実行"
    echo "  $0 -y 2023 -i 19_47418 format  2023年の釧路データのみフォーマット"
    echo ""
    exit 1
}

# 引数がなければ使用方法を表示
if [ $# -eq 0 ]; then
    show_usage
fi

# オプションの解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -s|--station)
            STATION_FILE="$2"
            shift 2
            ;;
        -p|--parallel)
            PARALLEL=true
            shift
            ;;
        -y|--year)
            YEAR="$2"
            shift 2
            ;;
        -i|--station-id)
            STATION_ID="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            ;;
        all|scrape|merge|interpolate|format)
            COMMAND="$1"
            shift
            ;;
        *)
            echo "不明なオプション: $1"
            show_usage
            ;;
    esac
done

# コマンドが指定されていない場合はエラー
if [ -z "$COMMAND" ]; then
    echo "エラー: コマンドが指定されていません"
    show_usage
fi

# 並列処理オプション
PARALLEL_OPT=""
if [ "$PARALLEL" = true ]; then
    PARALLEL_OPT="--parallel"
fi

# 年オプション
YEAR_OPT=""
if [ -n "$YEAR" ]; then
    YEAR_OPT="--year $YEAR"
fi

# 観測所IDオプション
STATION_ID_OPT=""
if [ -n "$STATION_ID" ]; then
    STATION_ID_OPT="--station-id $STATION_ID"
fi

# コマンドに応じて処理を実行
case $COMMAND in
    all)
        echo "すべての処理を実行します..."
        python3 run_pipeline.py --config "$CONFIG_FILE" --station "$STATION_FILE" $PARALLEL_OPT $YEAR_OPT $STATION_ID_OPT
        ;;
    scrape)
        echo "データ収集を実行します..."
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task scrape-daily $PARALLEL_OPT $YEAR_OPT $STATION_ID_OPT
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task scrape-hourly $PARALLEL_OPT $YEAR_OPT $STATION_ID_OPT
        ;;
    merge)
        echo "データ結合を実行します..."
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task merge-daily $YEAR_OPT $STATION_ID_OPT
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task merge-hourly $YEAR_OPT $STATION_ID_OPT
        ;;
    interpolate)
        echo "データ補間を実行します..."
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task interpolate $YEAR_OPT $STATION_ID_OPT
        ;;
    format)
        echo "データフォーマットを実行します..."
        python3 run_task.py --config "$CONFIG_FILE" --station "$STATION_FILE" --task format $YEAR_OPT $STATION_ID_OPT
        ;;
esac

echo "処理が完了しました"
