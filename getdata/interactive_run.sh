#!/bin/bash
# 気象データ処理のインタラクティブ実行スクリプト
# interactive_run.sh

echo "========================================="
echo "  気象データ処理インタラクティブ実行ツール"
echo "========================================="
echo ""

# 使用する設定ファイルを選択
echo "使用する設定ファイルを選択してください:"
echo "1) デフォルト (src/config/config.json)"
echo "2) カスタム設定ファイル"
read -p "選択 (1-2): " config_choice

if [ "$config_choice" = "2" ]; then
    read -p "設定ファイルのパスを入力: " config_file
else
    config_file="src/config/config.json"
fi

# 観測所ファイルを選択
echo ""
echo "使用する観測所ファイルを選択してください:"
echo "1) デフォルト (src/config/station_data.json)"
echo "2) カスタム観測所ファイル"
read -p "選択 (1-2): " station_choice

if [ "$station_choice" = "2" ]; then
    read -p "観測所ファイルのパスを入力: " station_file
else
    station_file="src/config/station_data.json"
fi

# 並列処理の有効化
echo ""
echo "並列処理を有効にしますか?"
echo "1) はい"
echo "2) いいえ"
read -p "選択 (1-2): " parallel_choice

if [ "$parallel_choice" = "1" ]; then
    parallel_opt="--parallel"
else
    parallel_opt=""
fi

# 特定の年を指定するか
echo ""
echo "特定の年を指定しますか?"
echo "1) はい"
echo "2) いいえ (すべての年)"
read -p "選択 (1-2): " year_choice

if [ "$year_choice" = "1" ]; then
    read -p "処理する年を入力: " year_value
    year_opt="--year $year_value"
else
    year_opt=""
fi

# 特定の観測所を指定するか
echo ""
echo "特定の観測所を指定しますか?"
echo "1) はい"
echo "2) いいえ (すべての観測所)"
read -p "選択 (1-2): " station_id_choice

if [ "$station_id_choice" = "1" ]; then
    read -p "観測所ID（例: 19_47418）を入力: " station_id_value
    station_id_opt="--station-id $station_id_value"
else
    station_id_opt=""
fi

# 実行するコマンドを選択
echo ""
echo "実行する処理を選択してください:"
echo "1) すべての処理を実行"
echo "2) データ収集のみ"
echo "3) データ結合のみ"
echo "4) データ補間のみ"
echo "5) データフォーマットのみ"
read -p "選択 (1-5): " command_choice

case $command_choice in
    1)
        command="./run.sh all"
        ;;
    2)
        command="./run.sh scrape"
        ;;
    3)
        command="./run.sh merge"
        ;;
    4)
        command="./run.sh interpolate"
        ;;
    5)
        command="./run.sh format"
        ;;
    *)
        echo "無効な選択です"
        exit 1
        ;;
esac

# 実行コマンドの構築
full_command="$command --config \"$config_file\" --station \"$station_file\" $parallel_opt $year_opt $station_id_opt"

# 実行前の確認
echo ""
echo "========================================="
echo "以下のコマンドを実行します:"
echo "$full_command"
echo "========================================="
echo ""
read -p "実行しますか? (y/n): " confirm

if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    echo "処理を開始します..."
    eval $full_command
else
    echo "処理をキャンセルしました"
fi
