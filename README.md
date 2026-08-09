# Big Data Platform - Phân tích CDR Viễn thông

Triển khai đề tài "Phân tích cước phí và lưu lượng cuộc gọi viễn thông (Telecom CDR)"
trên Docker, chạy tốt trên máy Intel i7 / RAM 16GB (đã tính `mem_limit` cho từng container).

## 0. Yêu cầu máy
- Docker + Docker Compose
- RAM 16GB: tổng các container ước tính ~11-12GB khi chạy full stack. Nếu máy chậm,
  có thể tắt tạm `metabase` hoặc `spark-worker1` lúc chưa cần dùng
  (`docker compose stop metabase`).

## 1. Dựng cluster (1 master + 2 slave HDFS, YARN, Hive, Spark, MySQL, Metabase)
```bash
cd telecom-cdr-bigdata
docker compose up -d
```
Kiểm tra:
- HDFS Web UI: http://localhost:9870
- YARN Web UI: http://localhost:8088
- Spark Web UI: http://localhost:8080
- Metabase: http://localhost:3000

Kiểm tra 2 datanode đã join cluster (yêu cầu tối thiểu 1 master + 2 slave):
```bash
docker exec -it namenode hdfs dfsadmin -report
```

## 2. Sinh dữ liệu giả lập (N=100 trạm, T=1 giờ)
```bash
# Full: 100 trạm x 365 ngày x 24h = 876,000 file (chạy lâu, cần nhiều dung lượng đĩa)
python3 scripts/generate_telecom_data.py --stations 100 --days 365 --out /home/hduser/data

# Khuyến nghị khi DEV/test pipeline: bộ nhỏ trước
python3 scripts/generate_telecom_data.py --stations 5 --days 2 --out /home/hduser/data
```

## 3. Giả lập nhận dữ liệu real-time (yêu cầu 2)
Copy N file mỗi T giây từ `/home/hduser/data` sang `/home/hduser/realtime-data`:
```bash
python3 scripts/simulate_ingest.py --src /home/hduser/data --dst /home/hduser/realtime-data \
    --stations 100 --interval 5
```
(`--interval 5` để demo nhanh; thực tế T=3600s = 1 giờ)

## 4. ETL đưa dữ liệu lên HDFS (yêu cầu 3)
```bash
docker cp scripts/etl_to_hdfs.sh namenode:/etl_to_hdfs.sh
docker exec -it namenode bash /etl_to_hdfs.sh
```
Có thể chạy script này định kỳ (cron/watch) song song với bước 3 để mô phỏng luồng
streaming ingest liên tục.

## 5. Tạo bảng Hive & load dữ liệu (yêu cầu 4)
```bash
docker exec -it hive-server beeline -u jdbc:hive2://hive-server:10000 \
    -f /opt/hive-scripts/create_table.sql
```
(cần `docker cp hive/create_table.sql hive-server:/opt/hive-scripts/create_table.sql` trước)

## 6. Chạy Spark job cho 4 bài toán (yêu cầu 5)
```bash
docker cp spark/. spark-master:/spark-jobs/

docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
    /spark-jobs/topk_callers_stations.py --k 10

docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
    /spark-jobs/traffic_by_date.py --date 2025-06

docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
    /spark-jobs/revenue_by_service.py --year 2025

docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
    /spark-jobs/revenue_by_station.py --date 2025-06
```
Kết quả được ghi vào HDFS `/output/5a_...`, `/output/5b_...`, `/output/5c_...`, `/output/5d_...`.

## 7. Đối chiếu bằng HiveQL/SparkSQL (yêu cầu 6)
```bash
docker cp hive/verify_queries.sql hive-server:/opt/hive-scripts/verify_queries.sql
docker exec -it hive-server beeline -u jdbc:hive2://hive-server:10000 \
    -f /opt/hive-scripts/verify_queries.sql
```

## 8. ETL kết quả vào MySQL (yêu cầu 7)
```bash
pip install pandas mysql-connector-python --break-system-packages

python3 scripts/export_to_mysql.py --hdfs-path /output/5a_top_stations --table top_stations
python3 scripts/export_to_mysql.py --hdfs-path /output/5c_revenue_by_service_2025 --table revenue_by_service
# tương tự cho top_callers, traffic_by_date, revenue_by_station
```
Schema các bảng MySQL đích: `mysql/init.sql` (đã tự chạy khi container `mysql` khởi động lần đầu).

## 9. BI & Reporting (yêu cầu 8)
1. Mở http://localhost:3000, tạo admin account cho Metabase lần đầu.
2. Add database -> MySQL -> host `mysql`, port `3306`, database `telecom_report`,
   user `root`, password `root123`.
3. Tạo dashboard gồm:
   - Bar chart: Top 10 trạm bận rộn nhất / Top 10 thuê bao gọi nhiều nhất
   - Line/area chart: lưu lượng theo tháng D
   - Pie/bar chart: doanh thu theo loại dịch vụ (Call/SMS/Data) trong năm
   - Bar chart: doanh thu theo từng trạm tại thời điểm D (để xác định trạm cần tối ưu)

## Cấu trúc project
```
telecom-cdr-bigdata/
├── docker-compose.yml          # 1 master + 2 slave HDFS, YARN, Hive, Spark, MySQL, Metabase
├── hadoop.env                  # config chung Hadoop ecosystem
├── scripts/
│   ├── generate_telecom_data.py   # sinh dữ liệu CDR giả lập
│   ├── simulate_ingest.py         # giả lập nhận dữ liệu real-time (yêu cầu 2)
│   ├── etl_to_hdfs.sh              # ETL lên HDFS (yêu cầu 3)
│   └── export_to_mysql.py          # ETL kết quả -> MySQL (yêu cầu 7)
├── hive/
│   ├── create_table.sql            # tạo bảng cdr (yêu cầu 4)
│   └── verify_queries.sql          # đối chiếu 5a, 5c (yêu cầu 6)
├── spark/
│   ├── topk_callers_stations.py    # 5a
│   ├── traffic_by_date.py          # 5b
│   ├── revenue_by_service.py       # 5c
│   └── revenue_by_station.py       # 5d
└── mysql/
    └── init.sql                    # schema báo cáo
```

## Ghi chú quan trọng
- **Small file problem**: 876,000 file nhỏ sẽ làm NameNode tốn nhiều RAM để lưu metadata
  và MapReduce/Spark chạy chậm vì quá nhiều task nhỏ. Khi làm báo cáo thực tế, nên nói rõ
  hướng xử lý: dùng `hadoop archive (HAR)` hoặc gộp file theo ngày trước khi đưa vào HDFS.
- Cột ngày/giờ **không có trong nội dung CSV**, chỉ nằm trong **tên file**
  (`Telecom-k-YYYYMMDD-hh.csv`) — các job Spark 5b/5d dùng `input_file_name()` +
  `regexp_extract` để lấy ra, đúng như đề bài yêu cầu xử lý dữ liệu log gửi theo lô.
- Dữ liệu, đơn giá cước trong `generate_telecom_data.py` là minh hoạ, không phản ánh
  giá thực tế của nhà mạng nào.
# telecom-cdr-bigdata
