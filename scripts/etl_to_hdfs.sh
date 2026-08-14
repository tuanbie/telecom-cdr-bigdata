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

COUNT=$(find "$SRC_DIR" -maxdepth 1 -name "Telecom-*.csv" | wc -l)

if [ "$COUNT" -eq 0 ]; then
    echo "Không có file mới để ETL."
    exit 0
fi

# Dùng find | xargs thay vì mảng bash để tránh lỗi "Argument list too long"
# khi backlog tích luỹ nhiều file (N shop x nhiều giờ dồn lại).
echo "Đang đẩy $COUNT file lên HDFS $HDFS_DIR ..."
find "$SRC_DIR" -maxdepth 1 -name "Telecom-*.csv" -print0 \
    | xargs -0 -n 1000 sh -c 'hdfs dfs -put -f -- "$@" "$0"' "$HDFS_DIR/"

echo "Di chuyển file đã xử lý vào $ARCHIVE_DIR ..."
find "$SRC_DIR" -maxdepth 1 -name "Telecom-*.csv" -print0 \
    | xargs -0 -n 1000 mv -t "$ARCHIVE_DIR/" --

echo "ETL xong. Tổng số file hiện có trên HDFS:"
hdfs dfs -count "$HDFS_DIR"
