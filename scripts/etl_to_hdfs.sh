#!/bin/bash
# ETL đơn giản: theo dõi thư mục realtime-data, đẩy các file mới lên HDFS /data/cdr
# rồi chuyển sang thư mục "archived" để tránh copy trùng lần sau.
#
# Chạy bên trong container namenode (hoặc máy đã cài hadoop client), ví dụ:
#   docker cp scripts/etl_to_hdfs.sh namenode:/etl_to_hdfs.sh
#   docker exec -it namenode bash /etl_to_hdfs.sh
#
# Có thể đặt vào crontab / watch loop để chạy định kỳ mỗi T.

set -e

SRC_DIR="/home/hduser/realtime-data"
ARCHIVE_DIR="/home/hduser/realtime-data/_archived"
HDFS_DIR="/data/cdr"

mkdir -p "$ARCHIVE_DIR"

# Tạo thư mục đích trên HDFS nếu chưa có
hdfs dfs -mkdir -p "$HDFS_DIR"

shopt -s nullglob
FILES=("$SRC_DIR"/Telecom-*.csv)

if [ ${#FILES[@]} -eq 0 ]; then
    echo "Không có file mới để ETL."
    exit 0
fi

echo "Đang đẩy ${#FILES[@]} file lên HDFS $HDFS_DIR ..."
hdfs dfs -put -f "${FILES[@]}" "$HDFS_DIR/"

echo "Di chuyển file đã xử lý vào $ARCHIVE_DIR ..."
mv "${FILES[@]}" "$ARCHIVE_DIR/"

echo "ETL xong. Tổng số file hiện có trên HDFS:"
hdfs dfs -count "$HDFS_DIR"
