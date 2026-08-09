"""
Đưa kết quả đầu ra (đã lưu dưới dạng CSV trên HDFS /output/...) vào MySQL Server.
Có thể chạy độc lập (đơn giản, dùng pandas + mysql-connector) hoặc thay bằng Sqoop
nếu môi trường có sẵn Sqoop:

  sqoop export --connect jdbc:mysql://mysql:3306/telecom_report \
      --username root --password root123 \
      --table top_stations \
      --export-dir /output/5a_top_stations \
      --input-fields-terminated-by ','

Cách dùng script này (chạy trên máy có kết nối tới cả HDFS và MySQL, hoặc trong
container edge-node có hadoop client + python):

  pip install pandas mysql-connector-python pyarrow hdfs
  python export_to_mysql.py --hdfs-path /output/5a_top_stations --table top_stations
"""
import argparse
import subprocess
import tempfile
import os
import pandas as pd
import mysql.connector


def read_hdfs_csv_via_getmerge(hdfs_path):
    """Dùng `hdfs dfs -getmerge` để gộp các file part-* trên HDFS thành 1 CSV cục bộ,
    rồi đọc bằng pandas. Cách này không cần cài thêm thư viện HDFS client Python."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    subprocess.run(["hdfs", "dfs", "-getmerge", hdfs_path, tmp_path], check=True)
    df = pd.read_csv(tmp_path)
    os.remove(tmp_path)
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs-path", required=True, help="Đường dẫn HDFS chứa kết quả, ví dụ /output/5a_top_stations")
    ap.add_argument("--table", required=True, help="Tên bảng MySQL đích")
    ap.add_argument("--mysql-host", default="mysql")
    ap.add_argument("--mysql-user", default="root")
    ap.add_argument("--mysql-password", default="root123")
    ap.add_argument("--mysql-db", default="telecom_report")
    args = ap.parse_args()

    print(f"Đang đọc dữ liệu từ HDFS: {args.hdfs_path}")
    df = read_hdfs_csv_via_getmerge(args.hdfs_path)
    print(f"Đọc được {len(df)} dòng, các cột: {list(df.columns)}")

    conn = mysql.connector.connect(
        host=args.mysql_host, user=args.mysql_user,
        password=args.mysql_password, database=args.mysql_db
    )
    cursor = conn.cursor()

    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {args.table} ({cols}) VALUES ({placeholders})"

    rows = [tuple(x) for x in df.to_numpy()]
    cursor.executemany(insert_sql, rows)
    conn.commit()

    print(f"Đã insert {cursor.rowcount} dòng vào bảng {args.table}.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
