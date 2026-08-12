# Giải thích triển khai từng phần

> **Lưu ý mapping domain**: đề bài gốc dùng ngữ cảnh "shop / sản phẩm / bảng Orders",
> project này triển khai theo domain viễn thông: **shop → trạm BTS (`station_id`)**,
> **sản phẩm → loại dịch vụ (`service_type`: Call/SMS/Data)**, **Orders → bảng `cdr`**.
> Các yêu cầu 1-8 vẫn khớp 1-1, chỉ khác tên miền dữ liệu.

## 1. Cluster Hadoop Ecosystem — `docker-compose.yml`
1 master + 2 slave tối thiểu được dựng bằng Docker Compose (bde2020 images):
- **HDFS**: `namenode` (master) + `datanode1`, `datanode2` (2 slave)
- **YARN**: `resourcemanager` + `nodemanager1` quản lý tài nguyên job
- **Hive**: `hive-metastore-postgresql` (lưu metadata) + `hive-metastore` + `hive-server` (HiveServer2/Beeline)
- **Spark**: `spark-master` + `spark-worker1`, đọc/viết trực tiếp HDFS
- **MySQL** + **Metabase** cho phần lưu kết quả & BI

Kiểm tra 2 datanode đã join: `docker exec -it namenode hdfs dfsadmin -report`

## 2. Giả lập nhận dữ liệu từ N shop — `scripts/simulate_ingest.py`
- Dữ liệu 1 năm được sinh trước bằng `generate_telecom_data.py` vào `/home/hduser/data`, tên file dạng `Telecom-<StationID>-<YYYYMMDD>-<hh>.csv` (mỗi file = 1 "shop"/trạm × 1 giờ).
- `simulate_ingest.py` gom N file theo cùng (ngày, giờ) thành 1 batch, rồi mỗi T giây (`--interval`) copy cả batch từ `/home/hduser/data` sang `/home/hduser/realtime-data` — đúng nghĩa "N shop gửi dữ liệu định kỳ mỗi T".
- Dùng `--interval 5` để demo nhanh, thực tế set 3600s (1 giờ).

## 3. ETL sang HDFS — `scripts/etl_to_hdfs.sh`
- Script bash chạy trong container `namenode`: lấy toàn bộ file mới trong `realtime-data`, `hdfs dfs -put` vào `/data/cdr`, sau đó move file đã xử lý vào thư mục `_archived` để tránh nạp trùng lần sau.
- Được thiết kế để chạy định kỳ (cron/watch) song song với bước simulate_ingest → mô phỏng luồng streaming liên tục.

## 4. Bảng "Orders" trên Hive — `hive/create_table.sql`
- `cdr_raw`: bảng **external**, trỏ trực tiếp `/data/cdr` (dữ liệu thô, chưa partition) — đóng vai trò tương đương "Orders" gốc.
- `cdr`: bảng **managed**, partition theo `(year, month)`, format Parquet — tối ưu truy vấn theo mốc thời gian D.
- Vì ngày/giờ **không nằm trong nội dung CSV** mà nằm trong **tên file**, câu lệnh `INSERT OVERWRITE ... SELECT` dùng `regexp_extract(input__file__name, ...)` để tách year/month khi load từ `cdr_raw` sang `cdr` — đây là điểm kỹ thuật quan trọng của đề bài (log theo lô, không có cột thời gian sẵn).

## 5. 4 bài toán Spark — thư mục `spark/`

| Câu | File | Logic |
|---|---|---|
| 5a | `topk_callers_stations.py` | `groupBy(caller_prefix_full/station_id).count()` → `orderBy desc.limit(k)` — top K "sản phẩm" (thuê bao/trạm) bán chạy nhất toàn hệ thống |
| 5b | `traffic_by_date.py` | Trích year/month từ tên file bằng `input_file_name()` + `regexp_extract`, filter theo D, `groupBy(service_type)` — top sản phẩm tại thời điểm D |
| 5c | `revenue_by_service.py` | filter theo năm, `groupBy(service_type).sum(cost)` — doanh thu theo sản phẩm trong năm |
| 5d | `revenue_by_station.py` | filter theo D, `groupBy(station_id).sum(cost)` — doanh thu tại D theo từng "shop" (trạm) |

Mỗi job ghi kết quả ra HDFS `/output/5x_...` dưới dạng CSV — làm input cho bước 7 (ETL vào MySQL).

## 6. Đối chiếu HiveQL/SparkSQL — `hive/verify_queries.sql`
- Chạy lại 5a (top station, top caller) và 5c (doanh thu theo service_type/năm) bằng HiveQL thuần trên bảng `cdr` đã partition, để so sánh số liệu với kết quả Spark job — mục đích kiểm tra tính đúng đắn chéo giữa 2 công cụ.

## 7. ETL kết quả vào MySQL — `scripts/export_to_mysql.py` + `mysql/init.sql`
- `export_to_mysql.py`: dùng `hdfs dfs -getmerge` gộp các file `part-*` trên HDFS thành 1 CSV local, đọc bằng pandas, rồi `INSERT` từng dòng vào MySQL qua `mysql-connector`. Có ghi chú thay bằng Sqoop nếu môi trường có sẵn.
- `mysql/init.sql`: định nghĩa schema đích — 5 bảng tương ứng 4 output (`top_callers`, `top_stations`, `traffic_by_date`, `revenue_by_service`, `revenue_by_station`), tự chạy khi container MySQL khởi động lần đầu (mount vào `docker-entrypoint-initdb.d`).

## 8. BI & Reporting — Metabase
- Kết nối Metabase → MySQL (`telecom_report`), dựng dashboard: bar chart top trạm/thuê bao, line chart lưu lượng theo tháng D, pie/bar doanh thu theo dịch vụ, bar doanh thu theo trạm tại D.

## Ghi chú kỹ thuật quan trọng
- **Small-file problem**: 388.800+ file nhỏ làm NameNode tốn nhiều RAM lưu metadata, Spark/MapReduce chạy chậm vì quá nhiều task nhỏ. Hướng xử lý khi làm báo cáo thực tế: dùng `hadoop archive (HAR)` hoặc gộp file theo ngày trước khi đưa vào HDFS.
- Cột ngày/giờ **không có trong nội dung CSV**, chỉ nằm trong **tên file** — các job Spark 5b/5d và câu lệnh Hive load dữ liệu đều dùng `input_file_name()`/`input__file__name` + `regexp_extract` để lấy ra, đúng với đặc thù dữ liệu log gửi theo lô.
