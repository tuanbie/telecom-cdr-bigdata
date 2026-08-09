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

-- Tên file có dạng: Telecom-<StationID>-<YYYYMMDD>-<hh>.csv
-- Dùng regexp_extract trên input__file__name (biến ảo Hive trả về đường dẫn file gốc)
-- để tách YYYY và MM, tránh phải sửa dữ liệu thô.
INSERT OVERWRITE TABLE cdr PARTITION (year, month)
SELECT
    call_id, caller_prefix, receiver_prefix, duration, service_type, cost, station_id,
    regexp_extract(input__file__name, 'Telecom-\\d+-(\\d{4})\\d{4}-\\d{2}\\.csv', 1) AS year,
    regexp_extract(input__file__name, 'Telecom-\\d+-\\d{4}(\\d{2})\\d{2}-\\d{2}\\.csv', 1) AS month
FROM cdr_raw;
