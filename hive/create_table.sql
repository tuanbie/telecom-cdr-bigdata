-- Chạy bằng beeline hoặc hive CLI, kết nối tới hive-server (jdbc:hive2://hive-server:10000)

CREATE DATABASE IF NOT EXISTS telecom;
USE telecom;

-- Bảng external, trỏ trực tiếp vào thư mục HDFS chứa các file CSV thô
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_raw (
    call_id          STRING,
    caller_prefix    STRING,
    receiver_prefix  STRING,
    duration         INT,
    service_type     STRING,
    cost             DOUBLE,
    station_id       STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/cdr';

-- Bảng managed, có thêm partition theo (year, month) để truy vấn theo mốc thời gian D nhanh hơn
-- Ngày/giờ được suy ra từ tên file khi ETL (có thể bổ sung cột date/hour lúc ingest,
-- ở đây minh hoạ bằng cách thêm cột date_str khi copy dữ liệu vào HDFS theo cấu trúc
-- /data/cdr/year=YYYY/month=MM/Telecom-k-YYYYMMDD-hh.csv)

CREATE TABLE IF NOT EXISTS cdr (
    call_id          STRING,
    caller_prefix    STRING,
    receiver_prefix  STRING,
    duration         INT,
    service_type     STRING,
    cost             DOUBLE,
    station_id       STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET;

-- Bật dynamic partition để load nhanh từ bảng raw sang bảng partitioned
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- LƯU Ý QUY MÔ THỰC TẾ (small-file problem):
-- Với 388.800 file gốc dạng Telecom-<StationID>-<YYYYMMDD>-<hh>.csv (mỗi trạm x mỗi giờ),
-- Hive-on-MR (bản 2.3.2/Hadoop 2.7.4 dùng trong project) không thể lập kế hoạch input
-- cho từng đó file nhỏ dù tăng heap tuỳ ý (luôn báo "GC overhead limit exceeded").
-- Hướng xử lý đúng chuẩn small-file problem: GỘP file theo ngày trước khi nạp
-- (45 trạm x 24 giờ -> 1 file/ngày, ví dụ Telecom-ALL-YYYYMMDD.csv), giảm số file
-- cần xử lý từ 388.800 xuống còn ~360, tương ứng số ngày dữ liệu.
-- Xem scripts/merge_by_day.sh để gộp trước khi hdfs dfs -put vào /data/cdr.
--
-- Tên file sau khi gộp có dạng: Telecom-ALL-<YYYYMMDD>.csv
-- Dùng regexp_extract trên input__file__name (biến ảo Hive trả về đường dẫn file gốc)
-- để tách YYYY và MM, tránh phải sửa dữ liệu thô.
INSERT OVERWRITE TABLE cdr PARTITION (year, month)
SELECT
    call_id, caller_prefix, receiver_prefix, duration, service_type, cost, station_id,
    regexp_extract(input__file__name, 'Telecom-ALL-(\\d{4})\\d{4}\\.csv', 1) AS year,
    regexp_extract(input__file__name, 'Telecom-ALL-\\d{4}(\\d{2})\\d{2}\\.csv', 1) AS month
FROM cdr_raw;
