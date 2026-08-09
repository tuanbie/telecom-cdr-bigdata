"""
Bài toán 5c: Tính tổng doanh thu cước phí theo từng loại dịch vụ (Call/SMS/Data) trong năm.

Chạy:
  spark-submit --master spark://spark-master:7077 revenue_by_service.py --year 2025
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", required=True)
    ap.add_argument("--input", default="hdfs://namenode:9000/data/cdr")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("Revenue_By_Service").getOrCreate()

    schema_cols = ["call_id", "caller_prefix_full", "receiver_prefix_full",
                   "duration", "service_type", "cost", "station_id"]

    df = (spark.read.csv(args.input, header=False, inferSchema=True)
          .toDF(*schema_cols)
          .withColumn("src_file", F.input_file_name())
          .withColumn("file_year", F.regexp_extract("src_file", r"Telecom-\d+-(\d{4})\d{4}-\d{2}\.csv", 1)))

    df_y = df.filter(F.col("file_year") == args.year)

    result = (df_y.groupBy("service_type")
                  .agg(F.sum("cost").alias("total_revenue"),
                       F.count("*").alias("total_records"))
                  .orderBy(F.desc("total_revenue")))

    print(f"=== Doanh thu theo loại dịch vụ - năm {args.year} ===")
    result.show(truncate=False)

    result.write.mode("overwrite").option("header", True) \
        .csv(f"hdfs://namenode:9000/output/5c_revenue_by_service_{args.year}")

    spark.stop()

if __name__ == "__main__":
    main()
