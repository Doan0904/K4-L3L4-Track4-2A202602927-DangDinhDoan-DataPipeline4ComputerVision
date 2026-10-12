<!-- NHÁP: học viên cần đọc lại và viết lại bằng lời của mình -->
# REPORT — Lab Day 9

> Nháp từ kết quả chạy thật. Học viên cần kiểm tra cách diễn đạt, phần Nhìn lại và hiểu từng thí nghiệm trước khi nộp.

| | |
|---|---|
| Họ và tên | Đặng Đỉnh Đoàn |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602927` |
| GitHub | `Doan0904` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02736` |

## Kết quả cuối

- `dataset_version` của bạn: `9e2a61b9153c`
- Số khung hình train / val / test: **775 / 133 / 111**, tổng **1019**; số trip tương ứng **18 / 4 / 4**.
- Pipeline Bước 1–5 đạt; [report/verify.txt](report/verify.txt) có **27/27 checks passed**. Bộ unit test gốc chạy **9 test, OK** ([validation.json](report/evidence/validation.json)).
- DAG từ lake trống và DAG phục hồi đều `success`; bằng chứng: [airflow_baseline.json](report/evidence/airflow_baseline.json), [crash_recovered.json](report/evidence/crash_recovered.json).
- Ảnh giao diện trong [report/screenshots/](report/screenshots/): Graph xanh, MinIO, Metabase, log sập, Graph đỏ, Graph xanh sau phục hồi và hộp thoại Clear.
- Các số cuối lấy từ [run_summary.json](report/evidence/run_summary.json) và [final_observation.json](report/evidence/final_observation.json), sau lần `step all --reset` cuối cùng và chạy lại bốn thí nghiệm.

## Bước 1 · Bronze

### Câu 1

A có 3021 dòng, B có 3234 dòng; ingest lại báo `already present`, lịch sử A chỉ có commit version 0 với 3021 dòng ([bronze_observation.json](report/evidence/bronze_observation.json)).
Khác biệt: `ts_utc` ISO UTC / `timestamp_ms` epoch ms; `speed_mps` m/s / `spd_kmh` km/h; `gas, brake` tỷ lệ / `throttle_pct, brake_pct` phần trăm; `lat, lon, heading_deg` / `latitude, longitude, hdg`; đồng hồ A offset 0 / B offset 1500 ms.
Mỗi nguồn giữ một bảng bronze để bảo toàn schema gốc, truy vết và replay khi quy tắc chuẩn hoá thay đổi.
Các cột dữ liệu CSV đều là string, kể cả ô trống; ép kiểu ở silver giúp tránh mất bản ghi gốc khi gặp giá trị lỗi. Metadata bổ sung có kiểu riêng: `_row_no` int64 và `_ingested_at` timestamp, không phải string.

## Bước 2 · Silver telemetry

### Câu 2

`stg_telemetry_b.sql` đổi tên cột, chia tốc độ cho 3.6, chia bàn đạp cho 100, chuyển `(timestamp_ms - offset_ms)/1000` thành timestamp UTC; offset B là 1500 ms.
Sau chuẩn hoá, A bắt đầu `2025-10-07 08:00:00.014000+00:00`, B bắt đầu `2025-10-08 14:30:00.009000+00:00`: B chỉ lệch 9 ms so với video 14:30:00; tốc độ tối đa làm tròn A/B là 71/100 km/h ([telemetry_observation.json](report/evidence/telemetry_observation.json)).

| Drive | Nhận về | Sạch | Trùng | Thiếu tốc độ | Ngoài khoảng |
| --- | --- | --- | --- | --- | --- |
| drive_a | 3021 | 2968 | 20 | 30 | 3 |
| drive_b | 3234 | 3181 | 20 | 30 | 3 |

Theo `run_summary.json`, mỗi nguồn có 53 dòng cách ly; sạch + cách ly bằng số nhận về. `rejects` giữ dòng gốc và lý do để kiểm toán, đếm lỗi và sửa nguồn, thay vì mất dấu do xoá im lặng.

### Câu 3

`max_gap_ms` có `severity: warn` trong `schema.yml`: mất tín hiệu là hiện tượng cần quan sát, nhưng không làm những mẫu còn lại thành giá trị vô lý; join sau đó gắn cờ khung mất đồng bộ.
`in_range` dùng severity `error` mặc định: tốc độ ngoài 0–70 m/s phải chặn trước Gold.
Nới `max_speed_mps` lên 1000 khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng, `gate_failed=true`.
Tóm tắt khi nới: `PASS=5 WARN=1 ERROR=1 SKIP=0`; khôi phục: `PASS=6 WARN=1 ERROR=0 SKIP=0` ([break_gate.json](report/evidence/break_gate.json)). Cổng test độc lập đã phát hiện dữ liệu xấu lọt qua luật làm sạch bị nới.

## Bước 3 · Silver frames

### Câu 4

Trip `trip-02736` có 60 khung. Hai lần overwrite giữ số dòng toàn bảng 1246 → 1246 và fingerprint `6c7480dfde7e` → `6c7480dfde7e`; `unchanged=true`.
Ghi append hai lần vào bảng thử tạo 120 dòng, bằng 2 × 60 ([idempotency.json](report/evidence/idempotency.json)).
Overwrite với predicate `trip_id` tạo một commit Delta nguyên tử, thay riêng trip đó: reader thấy phiên bản cũ hoặc mới; xoá-rồi-ghi có khoảng trống dữ liệu nếu bị sập giữa hai thao tác.
Fingerprint kiểm tra ID/trip/brightness/blur/dHash, không kiểm tra `_processed_at`; dấu thời gian xử lý có thể đổi khi chạy lại. Bài sập thực tế ở Câu 9 chứng minh phục hồi partition, chưa kiểm tra lỗi đúng giữa một commit Delta.

### Câu 5

Khung đen có brightness < 12 và bị loại; khung tối có 12 ≤ brightness < 40, được gắn `is_dark` và không bị loại chỉ vì tối. Giữ điều kiện đêm/hầm giúp dataset không thiên lệch về ánh sáng tốt.
Theo `run_summary.json`, các cờ có thể chồng nhau, nên không cộng chúng để suy ra tổng bị loại:

| Drive | Tổng | Đen | Mờ | Cờ tối | Trùng | Mất đồng bộ |
| --- | --- | --- | --- | --- | --- | --- |
| drive_a | 602 | 6 | 9 | 4 | 149 | 7 |
| drive_b | 644 | 4 | 0 | 61 | 53 | 7 |

Số khung tối thực sự đủ điều kiện vào Gold A/B là 4/58 (`final_observation.json`); khung tối vẫn bị loại nếu đồng thời trùng, mờ hoặc mất đồng bộ.

| dHash khác tối đa | Số khung trùng A | Số khung trùng B |
| --- | --- | --- |
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Theo [threshold.json](report/evidence/threshold.json), tôi giữ ngưỡng 2 làm mức cân bằng: ngưỡng 6 loại nhiều khung hơn và dễ mất biến đổi nhỏ có ích; ngưỡng 0 giữ lại nhiều khung gần trùng.
Xe chạy chậm khiến khung liên tiếp giống nhau hơn, nên cần đánh giá dHash cùng tốc độ và cảnh. Join ASOF dùng mẫu telemetry gần nhất ở trước hoặc đúng event time, không chọn mẫu tương lai; gap trung bình/max A là 66/2884 ms, B là 73/3112 ms (`frames_observation.json`).

## Bước 4 · Gold

### Câu 6

Có 1019 khung đủ dùng thuộc 26 trip. Chia theo khung làm 26 trip ở nhiều split, trong đó 26 trip có khung ở cả train và test; chia theo trip có 0 trip bị chia cắt ([leakage.json](report/evidence/leakage.json)).
Khung liền nhau rất giống nhau: mô hình học cảnh ở train rồi gặp gần cùng cảnh trong test, khiến metric có vẻ tốt hơn khả năng tổng quát hoá.
Chia theo trip giữ cảnh trong một split và stratify theo drive để cả A/B đều có train, val, test; điều này giảm leakage trong phạm vi trip của bài lab.

### Câu 7

Xuất hai lần và khôi phục MSSV đều trả `9e2a61b9153c`. Trong mã hiện tại, version là 12 ký tự đầu của SHA256 trên danh sách `frame_id|split` đã sắp xếp; không phụ thuộc thời điểm xuất.
MSSV làm salt trong `md5(trip_id || MSSV)`, đổi thứ tự trip của mỗi drive, từ đó đổi split và version.
MSSV `0000000` cho `1166103ed675`, train/val/test = 706/153/160; MSSV của tôi cho 775/133/111. Tổng khung của hai MSSV đều 1019, vì chỉ cách chia đổi ([version_observation.json](report/evidence/version_observation.json)).
Version này định danh tập ID và split; nó không tự phát hiện thay đổi byte ảnh hay nhãn nếu ID/split giữ nguyên. Vì vậy vẫn phải dùng SHA256 nguồn, manifest và phiên bản mã để tái lập.

### Câu 8

Manifest cuối của `9e2a61b9153c` có lineage sau ([final_observation.json](report/evidence/final_observation.json)):

| Bảng Delta | Version trong manifest cuối |
| --- | --- |
| bronze.raw_telemetry_a | 0 |
| bronze.raw_telemetry_b | 0 |
| bronze.trips_raw | 0 |
| bronze.video_files | 1 |
| silver.frames | 25 |
| silver.telemetry | 0 |
| silver.frames_flagged | 0 |
| silver.frame_telemetry | 0 |

SHA256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.
Manifest có luật loại black/blurry/near-duplicate/not-sync-ok và split theo trip. Nó không ghi toàn bộ tham số dbt, MSSV hay commit mã; cần lưu thêm MSSV `2A202602927`, mã nguồn, `.env` cục bộ và cấu hình ([validation.json](report/evidence/validation.json), mã nguồn giữ nguyên so với commit `c8e49c1`): 2 fps, luma 12/40, blur 40, dHash 2, sync 150 ms, split 70/15/15.
Sau 3 tháng, dùng version + Delta versions + SHA256 nguồn + commit mã + tham số/MSSV để dựng lại. Time travel đòi hỏi còn transaction log và file dữ liệu tương ứng; thao tác reset xoá lake nên phải lưu trữ bản sao trước khi cần giữ lịch sử lâu dài.

## Bước 5 · Điều phối và phục vụ

### Câu 9

MSSV có ký tự chữ nên dùng phần số đã xác nhận: `202602927 % 26 = 7`; `list-trips` sắp xếp cho index 7 là `trip-02736`. Đặt `LAB_FAIL_ONCE`, recreate Airflow, rồi trigger `day09_reset` để xoá marker trước run thử sập.
Run `lab9-crash-20261012T195232` có 1 task `failed` (silver_frames map index 7), 7 `upstream_failed`, và 25 mapped task khung `success` ([crash_failed.json](report/evidence/crash_failed.json)).
Trong UI, chọn đúng map index 7; Clear với Downstream/Only Failed để clear task đỏ và 7 task bị chặn phía sau, giữ các mapped task đã thành công. Ảnh: [log sập](report/screenshots/crash_task_log.png), [Graph đỏ](report/screenshots/crash_graph_red.png), [hộp thoại Clear](report/screenshots/crash_clear_dialog.png).
Sau đó toàn bộ 37 task instance xanh; silver có 1246 → 1246 dòng và 1246 ID duy nhất; version vẫn `9e2a61b9153c`, verify 27/27 ([crash_recovered.json](report/evidence/crash_recovered.json), [Graph xanh](report/screenshots/crash_graph_green.png)). `LAB_FAIL_ONCE` đã được gỡ và Airflow recreate.
Lỗi mô phỏng trong mã xảy ra trước decode/ghi partition; không phải bằng chứng một lỗi giữa transaction. Log và `try_number` cho thấy đúng task cần chạy lại.

### Câu 10

Hiện có 1246 khung/26 trip từ 2 drive. Nếu mỗi xe chỉ ghi 5 phút, kịch bản 500 xe/ngày ở 2 fps đã là khoảng 300.000 khung/ngày; đây là ước lượng theo giả định, không phải benchmark.
Ba nút thắt: (1) FFmpeg decode trong một container và `silver_frames` giới hạn 1 task đồng thời → worker phân tán, GPU decode, Ray/Spark; (2) DuckDB đơn máy và một file `.duckdb` → Spark/Trino đọc Delta partition theo xe/ngày; (3) Airflow LocalExecutor parallelism=2, task theo trip → Celery/KubernetesExecutor, giới hạn concurrency theo tài nguyên và gom partition hợp lý.
Liên hệ bài toán tăng quy mô trong slide 12, 22, 23: tách lưu trữ khỏi compute, tăng worker và đo throughput từng stage. MinIO đơn node và Postgres warehouse cũng cần xem xét khi lưu trữ, tải dashboard và khả năng phục hồi tăng.

## Câu hỏi riêng của bạn trên Metabase

Dashboard: [Day 9: video data pipeline](http://localhost:3000/dashboard/2), gồm 6 câu hỏi mẫu và 2 câu hỏi riêng; [ảnh dashboard](report/screenshots/metabase_dashboard.png).

```sql
select drive_id, trip_id, lighting, conditions, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as pct_dropped,
       split
from marts.mart_trip_summary
order by pct_dropped desc, drive_id, trip_id;
```

Trip mất tỷ lệ lớn nhất là `trip-04622` (drive_a): 60 khung, giữ 26, loại 56.7%; metadata lighting=`Day`, conditions=`Clouds` (`final_observation.json`). Đối chiếu cờ của trip này: 34 khung trùng, 0 đen, 0 mờ, 0 mất đồng bộ; tốc độ trung bình 67.1 km/h. Các cờ có thể chồng nhau; metadata ánh sáng/thời tiết chưa đủ để kết luận quan hệ nhân quả.

Câu hỏi vận hành thứ hai:

```sql
select drive_id, rows_received, rows_clean, rows_duplicate,
       rows_speed_missing, rows_speed_out_of_range,
       round(100.0 * (rows_received - rows_clean) / nullif(rows_received, 0), 2)
         as pct_quarantined
from marts.mart_telemetry_quality
order by drive_id;
```

Hai nguồn đều cách ly 53 dòng; A 1.75%, B 1.64%. Dùng tỷ lệ để so sánh chất lượng nguồn khi số dòng nhận về khác nhau.

## Thí nghiệm riêng · Siết dung sai đồng bộ

Chỉ dùng `--vars '{sync_tol_ms: 50}'`, không sửa file dbt. Khi đổi 150 → 50 ms, khung mất đồng bộ A/B đổi 7/7 → 272/328; tổng Gold 1019 → 538.
Split trước: `{'test': 111, 'train': 775, 'val': 133}`; sau: `{'test': 61, 'train': 393, 'val': 84}`. Đã chạy lại vars gốc và kiểm tra số trước/sau khôi phục bằng nhau ([sync_tolerance.json](report/evidence/sync_tolerance.json)); lần chạy cuối dựng lại trạng thái 150 ms.
Siết dung sai tăng độ gần thời gian nhưng làm mất nhiều ví dụ; cần cân đối với tần số lấy mẫu telemetry và độ lệch thời gian thực tế.

## Nhìn lại

*Phần nháp để học viên đối chiếu trải nghiệm và viết lại bằng lời của mình.*
Kết quả đáng chú ý là khung tối không tự bị loại: phải phân biệt điều kiện thật với khung không có thông tin; tăng dHash hay siết sync đều đánh đổi độ đa dạng với chất lượng.
Điều cần tìm hiểu thêm là lưu lịch sử Delta lâu dài khi reset/VACUUM và kiểm thử crash đúng giữa commit, vì bài thử hiện tại sập trước decode.
Pipeline chưa kiểm tra GPS nhảy bất thường, che ống kính hoặc lệch màu; dHash chỉ so khung liền trước trong trip, nên chưa loại trùng xa nhau hay cảnh tương tự giữa hai trip khác nhau.
