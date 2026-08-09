"""
Bài toán 5a: Top K thuê bao/đầu số gọi đi nhiều nhất, và Top K trạm phát sóng bận rộn nhất.
"Bận rộn" = tổng số bản ghi (Call+SMS+Data) phát sinh tại trạm đó.

Chạy:
  spark-submit --master spark://spark-master:7077 topk_callers_stations.py --k 10
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--input", default="hdfs://namenode:9000/data/cdr")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("TopK_Callers_Stations").getOrCreate()

    schema_cols = ["call_id", "caller_prefix_full", "receiver_prefix_full",
                   "duration", "service_type", "cost", "station_id"]

    df = (spark.read.csv(args.input, header=False, inferSchema=True)
          .toDF(*schema_cols))

    # Top K thuê bao gọi đi nhiều nhất (số lần thực hiện Call/SMS/Data)
    top_callers = (df.groupBy("caller_prefix_full")
                      .agg(F.count("*").alias("total_transactions"),
                           F.sum(F.when(F.col("service_type") == "Call", 1).otherwise(0)).alias("total_calls"))
                      .orderBy(F.desc("total_transactions"))
                      .limit(args.k))

    # Top K trạm phát sóng bận rộn nhất
    top_stations = (df.groupBy("station_id")
                       .agg(F.count("*").alias("total_records"),
                            F.sum("duration").alias("total_duration_sec"))
                       .orderBy(F.desc("total_records"))
                       .limit(args.k))

    print(f"=== Top {args.k} thuê bao gọi đi nhiều nhất ===")
    top_callers.show(truncate=False)

    print(f"=== Top {args.k} trạm phát sóng bận rộn nhất ===")
    top_stations.show(truncate=False)

    top_callers.write.mode("overwrite").option("header", True) \
        .csv("hdfs://namenode:9000/output/5a_top_callers")
    top_stations.write.mode("overwrite").option("header", True) \
        .csv("hdfs://namenode:9000/output/5a_top_stations")

    spark.stop()

if __name__ == "__main__":
    main()
