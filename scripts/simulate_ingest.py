"""
Giả lập ứng dụng nhận dữ liệu từ N trạm BTS.
Định kỳ mỗi T giây, "gửi" (copy) N file (1 file/trạm ứng với 1 giờ dữ liệu)
từ /home/hduser/data sang /home/hduser/realtime-data.

Cách dùng:
  python simulate_ingest.py --src /home/hduser/data --dst /home/hduser/realtime-data \
      --stations 100 --interval 5

  --interval: số giây giữa mỗi lần copy (mô phỏng T=1 giờ thực tế, rút ngắn lại để demo)
"""
import argparse
import glob
import os
import re
import shutil
import time
from collections import defaultdict


def group_files_by_hour(files):
    """Nhóm các file theo cặp (ngày, giờ) dựa vào tên: Telecom-k-YYYYMMDD-hh.csv"""
    pattern = re.compile(r"Telecom-(\d+)-(\d{8})-(\d{2})\.csv")
    groups = defaultdict(list)
    for f in files:
        m = pattern.search(os.path.basename(f))
        if m:
            _, date, hour = m.groups()
            groups[(date, hour)].append(f)
    return groups


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/home/hduser/data")
    ap.add_argument("--dst", default="/home/hduser/realtime-data")
    ap.add_argument("--stations", type=int, default=100, help="N trạm mong đợi mỗi batch")
    ap.add_argument("--interval", type=float, default=5.0, help="T giây giữa mỗi batch")
    ap.add_argument("--max-batches", type=int, default=0, help="0 = chạy tới khi hết dữ liệu")
    args = ap.parse_args()

    os.makedirs(args.dst, exist_ok=True)
    all_files = sorted(glob.glob(os.path.join(args.src, "Telecom-*.csv")))
    groups = group_files_by_hour(all_files)
    ordered_keys = sorted(groups.keys())  # theo (date, hour) tăng dần

    print(f"Tìm thấy {len(all_files)} file, gom thành {len(ordered_keys)} batch (mỗi batch = 1 giờ dữ liệu).")

    batch_count = 0
    for date, hour in ordered_keys:
        batch = groups[(date, hour)]
        for fpath in batch:
            shutil.copy2(fpath, args.dst)
        batch_count += 1
        print(f"[Batch {batch_count}] {date} {hour}h -> copied {len(batch)}/{args.stations} file sang {args.dst}")

        if args.max_batches and batch_count >= args.max_batches:
            print("Đạt max-batches, dừng giả lập.")
            break
        time.sleep(args.interval)

    print(f"Hoàn tất giả lập real-time ingest: {batch_count} batch.")


if __name__ == "__main__":
    main()
