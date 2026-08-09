"""
Bài toán 5b: Thống kê lưu lượng (số bản ghi Call/SMS/Data) và tổng số phút gọi
phát sinh tại thời điểm D (tháng-năm), D truyền vào dạng "YYYY-MM".

Ngày/giờ được lấy từ TÊN FILE (Telecom-<StationID>-<YYYYMMDD>-<hh>.csv) thông qua
hàm input_file_name(), vì dữ liệu CSV gốc không có cột ngày.

Chạy:
  spark-submit --master spark://spark-master:7077 traffic_by_date.py --date 2025-06
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Định dạng YYYY-MM, ví dụ 2025-06")
    ap.add_argument("--input", default="hdfs://namenode:9000/data/cdr")
    args = ap.parse_args()
    year, month = args.date.split("-")

    spark = SparkSession.builder.appName("Traffic_By_Date").getOrCreate()

    schema_cols = ["call_id", "caller_prefix_full", "receiver_prefix_full",
                   "duration", "service_type", "cost", "station_id"]

    df = (spark.read.csv(args.input, header=False, inferSchema=True)
          .toDF(*schema_cols)
          .withColumn("src_file", F.input_file_name())
          .withColumn("file_year", F.regexp_extract("src_file", r"Telecom-\d+-(\d{4})\d{4}-\d{2}\.csv", 1))
          .withColumn("file_month", F.regexp_extract("src_file", r"Telecom-\d+-\d{4}(\d{2})\d{2}-\d{2}\.csv", 1)))

    df_d = df.filter((F.col("file_year") == year) & (F.col("file_month") == month))

    result = (df_d.groupBy("service_type")
                  .agg(F.count("*").alias("total_records"),
                       F.sum(F.when(F.col("service_type") == "Call", F.col("duration")).otherwise(0)).alias("total_call_seconds"))
                  .withColumn("total_call_minutes", F.round(F.col("total_call_seconds") / 60, 2)))

    print(f"=== Lưu lượng & số phút gọi tại {args.date} ===")
    result.show(truncate=False)

    result.write.mode("overwrite").option("header", True) \
        .csv(f"hdfs://namenode:9000/output/5b_traffic_{year}{month}")

    spark.stop()

if __name__ == "__main__":
    main()
