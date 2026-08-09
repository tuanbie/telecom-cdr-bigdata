"""
Bài toán 5d: Tính tổng doanh thu của từng trạm phát sóng (StationID) tại mốc thời gian
D (tháng-năm), để phục vụ tối ưu hạ tầng (biết trạm nào đang "gánh" doanh thu/lưu lượng lớn).

Chạy:
  spark-submit --master spark://spark-master:7077 revenue_by_station.py --date 2025-06
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM")
    ap.add_argument("--input", default="hdfs://namenode:9000/data/cdr")
    args = ap.parse_args()
    year, month = args.date.split("-")

    spark = SparkSession.builder.appName("Revenue_By_Station").getOrCreate()

    schema_cols = ["call_id", "caller_prefix_full", "receiver_prefix_full",
                   "duration", "service_type", "cost", "station_id"]

    df = (spark.read.csv(args.input, header=False, inferSchema=True)
          .toDF(*schema_cols)
          .withColumn("src_file", F.input_file_name())
          .withColumn("file_year", F.regexp_extract("src_file", r"Telecom-\d+-(\d{4})\d{4}-\d{2}\.csv", 1))
          .withColumn("file_month", F.regexp_extract("src_file", r"Telecom-\d+-\d{4}(\d{2})\d{2}-\d{2}\.csv", 1)))

    df_d = df.filter((F.col("file_year") == year) & (F.col("file_month") == month))

    result = (df_d.groupBy("station_id")
                  .agg(F.sum("cost").alias("total_revenue"),
                       F.count("*").alias("total_records"))
                  .orderBy(F.desc("total_revenue")))

    print(f"=== Doanh thu theo trạm phát sóng - {args.date} ===")
    result.show(truncate=False)

    result.write.mode("overwrite").option("header", True) \
        .csv(f"hdfs://namenode:9000/output/5d_revenue_by_station_{year}{month}")

    spark.stop()

if __name__ == "__main__":
    main()
