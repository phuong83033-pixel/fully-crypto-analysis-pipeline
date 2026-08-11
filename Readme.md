như bạn đang đọc project của tôi thì đường đi của nó sẽ cơ bản là Fetch: Cào dữ liệu giá/volume của Top 20 đồng coin theo từng giờ.

Raw Storage: Lưu JSON gốc nhận được từ API vào MinIO theo cấu trúc thư mục dạng ngày giờ: raw/crypto/YYYY/MM/DD/HH.json.

Transform: Dùng Python/Pydantic đọc file JSON, validate ép kiểu, tính thêm một số chỉ số đơn giản (ví dụ: % thay đổi giá trong 1h), chuyển thành Parquet.

Warehouse: Push vào PostgreSQL (hoặc DuckDB) chia thành 2 bảng:

Bảng dim_coins (ID, Name, Symbol)

Bảng fact_market_prices (Coin_ID, Price, Volume_24h, Timestamp)

Orchestrate: Dùng Prefect lập lịch tự động chạy 1 tiếng/lần.

Bài tập SQL cần tự giải sau khi hoàn thành: Viết query dùng LAG() / LEAD() để tìm coin có mức tăng trưởng mạnh nhất trong 3 tiếng liên tiếp. vẽ cho tôi sơ đồ của project này 

# fully-crypto-analysis-pipeline
