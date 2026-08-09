USE telecom;

-- ===== Đối chiếu 5a: Top K trạm bận rộn nhất (K=10) =====
SELECT station_id, COUNT(*) AS total_records
FROM cdr
GROUP BY station_id
ORDER BY total_records DESC
LIMIT 10;

-- ===== Đối chiếu 5a: Top K thuê bao gọi đi nhiều nhất (K=10) =====
SELECT caller_prefix, COUNT(*) AS total_transactions
FROM cdr
GROUP BY caller_prefix
ORDER BY total_transactions DESC
LIMIT 10;
-- (Lưu ý: cdr.caller_prefix ở đây map với cột caller_prefix_full sinh ra trong Spark job,
--  tên cột được đặt lại cho khớp với DDL bảng Hive nêu trên.)

-- ===== Đối chiếu 5c: Tổng doanh thu theo loại dịch vụ trong năm 2025 =====
SELECT service_type, SUM(cost) AS total_revenue, COUNT(*) AS total_records
FROM cdr
WHERE year = '2025'
GROUP BY service_type
ORDER BY total_revenue DESC;

-- Có thể chạy tương đương bằng SparkSQL (pyspark):
--   df.createOrReplaceTempView("cdr")
--   spark.sql("""
--     SELECT service_type, SUM(cost) AS total_revenue
--     FROM cdr WHERE year = '2025'
--     GROUP BY service_type ORDER BY total_revenue DESC
--   """).show()
