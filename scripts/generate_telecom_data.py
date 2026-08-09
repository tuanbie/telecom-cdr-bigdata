"""
Sinh dữ liệu giả lập CDR (Call Detail Records) cho N trạm phát sóng (BTS).
Mỗi trạm, mỗi giờ sinh 1 file: Telecom-k-YYYYMMDD-hh.csv
Cấu trúc: CallID,CallerPrefix,ReceiverPrefix,Duration,ServiceType,Cost,StationID

Cách dùng:
  # Sinh dữ liệu 1 năm cho 100 trạm (mặc định) -> N*365*24 file
  python generate_telecom_data.py --stations 100 --days 365 --out /home/hduser/data

  # Sinh nhanh 1 tập nhỏ để dev/test pipeline
  python generate_telecom_data.py --stations 5 --days 2 --out ./data-sample
"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta

# Đầu số viễn thông VN giả lập
PREFIXES = ["032", "033", "034", "035", "036", "037", "038", "039",
            "070", "076", "077", "078", "079", "081", "082", "083",
            "084", "085", "088", "089", "090", "091", "092", "093",
            "094", "096", "097", "098", "099"]

SERVICE_TYPES = ["Call", "SMS", "Data"]
# Xác suất tương đối: gọi thoại nhiều nhất, SMS ít, Data (nếu tính là bản ghi riêng) trung bình
SERVICE_WEIGHTS = [0.55, 0.20, 0.25]

# Đơn giá minh hoạ (VNĐ) theo loại dịch vụ
def calc_cost(service_type, duration):
    if service_type == "Call":
        return round(duration * random.uniform(150, 400) / 60, 0)  # theo phút, ~150-400đ/phút
    elif service_type == "SMS":
        return random.choice([250, 350, 500])
    else:  # Data: Duration ở đây coi như số giây dùng data quy đổi MB
        return round(duration * random.uniform(20, 60), 0)


def random_phone(prefix):
    return prefix + "".join(str(random.randint(0, 9)) for _ in range(7))


def gen_file(path, station_id, num_records):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(num_records):
            caller_prefix = random.choice(PREFIXES)
            receiver_prefix = random.choice(PREFIXES)
            service_type = random.choices(SERVICE_TYPES, weights=SERVICE_WEIGHTS, k=1)[0]
            duration = random.randint(5, 1800) if service_type != "SMS" else 0
            cost = calc_cost(service_type, duration if duration else 1)
            call_id = f"{station_id:03d}{int(datetime.now().timestamp())}{i:05d}"
            writer.writerow([
                call_id,
                random_phone(caller_prefix),
                random_phone(receiver_prefix),
                duration,
                service_type,
                int(cost),
                f"BTS-{station_id:03d}",
            ])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stations", type=int, default=100, help="Số trạm N")
    ap.add_argument("--days", type=int, default=365, help="Số ngày sinh dữ liệu")
    ap.add_argument("--start-date", type=str, default="2025-01-01")
    ap.add_argument("--min-records", type=int, default=50)
    ap.add_argument("--max-records", type=int, default=300)
    ap.add_argument("--out", type=str, default="/home/hduser/data")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")

    total_files = 0
    for d in range(args.days):
        cur_date = start + timedelta(days=d)
        for h in range(24):
            for k in range(1, args.stations + 1):
                fname = f"Telecom-{k}-{cur_date.strftime('%Y%m%d')}-{h:02d}.csv"
                fpath = os.path.join(args.out, fname)
                num_records = random.randint(args.min_records, args.max_records)
                gen_file(fpath, k, num_records)
                total_files += 1
        if d % 10 == 0:
            print(f"Đã sinh xong ngày {d+1}/{args.days} - tổng {total_files} file")

    print(f"HOÀN TẤT. Tổng số file đã sinh: {total_files}")
    print(f"(Kỳ vọng: {args.stations} * {args.days} * 24 = {args.stations*args.days*24})")


if __name__ == "__main__":
    main()
