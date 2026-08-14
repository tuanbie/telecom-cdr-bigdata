#!/bin/bash
# Gộp các file CDR nhỏ theo ngày trước khi nạp lên HDFS, để tránh small-file problem
# (388.800 file Telecom-<StationID>-<YYYYMMDD>-<hh>.csv -> ~360 file Telecom-ALL-<YYYYMMDD>.csv).
#
# Cách dùng:
#   bash merge_by_day.sh /home/hduser/data /home/hduser/data_merged
set -e
SRC="$1"
DST="$2"
mkdir -p "$DST"

ls "$SRC" | sed -E 's/^Telecom-[0-9]+-([0-9]{8})-[0-9]{2}\.csv$/\1/' | sort -u > /tmp/days.txt
echo "Số ngày: $(wc -l < /tmp/days.txt)"

while read -r day; do
  cat "$SRC"/Telecom-*-"$day"-*.csv > "$DST/Telecom-ALL-$day.csv"
done < /tmp/days.txt

echo "Đã gộp xong. Số file kết quả: $(ls "$DST" | wc -l)"
