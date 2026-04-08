import duckdb
import os

# 1. Kết nối đến file database của bạn
# Đảm bảo tên file này trùng với cấu hình trong profiles.yml của dbt
con = duckdb.connect('pricing.duckdb')

# 2. Tạo Schema 'raw' nếu chưa có
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

# 3. Danh sách các file cần nạp (nên dùng dấu / để tránh lỗi Windows)
data_files = {
    r"orders": "F:\Projects\Olist\pricing_analytics\data\olist_orders_dataset.csv",
    r"order_items": "F:\Projects\Olist\pricing_analytics\data\olist_order_items_dataset.csv",
    r"order_reviews": "F:\Projects\Olist\pricing_analytics\data\olist_order_reviews_dataset.csv",
    r"products": "F:\Projects\Olist\pricing_analytics\data\olist_products_dataset.csv",
    r"customers": "F:\Projects\Olist\pricing_analytics\data\olist_customers_dataset.csv",
    r"sellers": "F:\Projects\Olist\pricing_analytics\data\olist_sellers_dataset.csv",
    r"category_translation": "F:\Projects\Olist\pricing_analytics\data\product_category_name_translation.csv",
    r"order_payments": "F:\Projects\Olist\pricing_analytics\data\olist_order_payments_dataset.csv"
}

# 4. Vòng lặp nạp dữ liệu
for table_name, file_path in data_files.items():
    if os.path.exists(file_path):
        print(f"Đang nạp {table_name}...")
        query = f"CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
        con.execute(query)
    else:
        print(f"Cảnh báo: Không tìm thấy file tại {file_path}")

# 5. Đóng kết nối để giải phóng file cho dbt sử dụng
con.close()
print("Hoàn thành nạp dữ liệu thô!")