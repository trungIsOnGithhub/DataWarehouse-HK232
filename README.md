#### BTL Kho Dữ Liệu và Hệ hỗ trợ quyết định

### Nguyễn Việt Trung

> Phiên bản Python 3.11.7 và PostgresSQL 12.17, PG Admin 7.8(nếu có)

### Hướng dẫn Setup - Với Decision Suppport System

1. Tải các pip package cần thiết từ terminal/command prompt: ```pip install faker pandas configparser coloredlogs psycopg2 jupyter squarify seaborn scikit-learn```

2. mở commandline - terminal và dùng lệnh ```jupyter notebook```

3. Chọn file Jupyter notebook và chạy như thường

### Hướng dẫn Setup - Với Warehouse

1. Cập nhật thông tin kết nối DB Postgres qua file `local_config.ini` trong thư mục *dwh_pipelines*

2. Tạo sẵn các database theo tên trong `local_config.ini`

3. Tải các pip package cần thiết từ terminal/command prompt: ```pip install faker pandas configparser coloredlogs psycopg2 jupyter squarify seaborn scikit-learn```

4. Chạy lệnh python  ```python gen_staging.py```