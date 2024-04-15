#### BTL Kho Dữ Liệu và Hệ hỗ trợ quyết định

### Nguyễn Việt Trung

> Phiên bản Python 3.11.7 và PostgresSQL 12.17, PG Admin 7.8(nếu có), các
> pip package được liệt kê chi tiết tại tệp tin tùy chỉnh

### Hướng dẫn Setup - Với Decision Suppport System

1. Tải các pip package cần thiết từ terminal/command prompt: ```pip install faker pandas configparser coloredlogs psycopg2 jupyter squarify seaborn scikit-learn```

2. mở commandline - terminal và dùng lệnh ```jupyter notebook```

3. Chọn file Jupyter notebook và chạy như thường

### Hướng dẫn Setup - Với Warehouse

1. Cập nhật thông tin kết nối DB Postgres qua file `config.ini` trong thư mục *dwh_pipelines*

2. Tạo sẵn các database theo tên trong `config.ini`

3. Tải các pip package cần thiết từ terminal/command prompt: ```pip install faker pandas configparser coloredlogs psycopg2 jupyter squarify seaborn scikit-learn```

4. Chạy lệnh python  ```python gen_staging.py``` tạo bảng dimension và ```python gen_fact.py``` tạo bảng fact cho data mart

#### Cấu tạo cơ bản của một Data Warehouse

BTL này được hiện thực theo thiết kế dứoi đây(phương pháp Inmon) và sẽ không đầy đủ chi tiết ở một số lớp
 
![Architecture](arch.png)

### Data pipelines

![Data Pipeline Components](dp.jpg)


### Ví dụ về Data Pipeline trên Đám Mây - Azure Data Factory


> A simple data pipeline of type "Copy Activity" to transform from CSV to Azure SQL table

##### Một số bước đáng chú ý:

1. Tạo tài nguyên Blob Storage trong 1 module quản lý Account Storage

![Screenshot](./azureDF/upload-blob-csv.png)

2. Tạo các Linked Service để kết nối đến các Dataset

![Screenshot](./azureDF/create-linkedservice-and-dataset-1.png)
![Screenshot](./azureDF/create-linkedservice-and-dataset-2.png)

3. Tạo các Dataset Source và Sink(nguồn và đích) cho mỗi Activity

![Screenshot](./azureDF/create-dataset.png)

4. Thêm Activity(Copy) và Import Mapping

![Screenshot](./azureDF/add-copy-activity-and-check-mapping.png)


Tham khảo:

1. [Azure 4 Everyone](https://azure4everyone.com/)
2. [Data piplines with Spotify's Luigi](https://dev.to/mpangrazzi/data-pipelines-with-luigi-87d)
3. [Stephen David William Blog](https://stephendavidwilliams.com/how-i-created-a-postgres-data-warehouse-with-python-sql)
