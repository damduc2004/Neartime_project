# Realtime Data Pipeline Project

## 📋 Mô tả dự án

Dự án xây dựng một **pipeline dữ liệu thời gian thực (Realtime ETL Pipeline)** để theo dõi và phân tích hành vi tương tác của người dùng với các tin tuyển dụng (job tracking). Hệ thống thu thập dữ liệu tracking (click, conversion, qualified, unqualified) từ các nguồn khác nhau, lưu trữ vào Cassandra, sau đó xử lý và chuyển đổi (ETL) để phục vụ phân tích.

## 🏗️ Kiến trúc hệ thống

```
MySQL (Master Data)  ──►  Fake Data Generator  ──►  Cassandra (Tracking Logs)
                                                            │
                                                            ▼
                                                     ETL Pipeline
                                                            │
                                                            ▼
                                                   Data Warehouse / Analytics
```

### Thành phần chính:

| Thành phần | Mô tả |
|---|---|
| **MySQL** | Lưu trữ dữ liệu master: `job`, `master_publisher`, `events` |
| **Apache Cassandra** | Lưu trữ dữ liệu tracking log theo thời gian thực |
| **Fake Data Script** | Sinh dữ liệu giả lập hành vi người dùng liên tục |
| **ETL Pipeline** | Trích xuất, chuyển đổi và tải dữ liệu để phân tích |
| **Docker Compose** | Quản lý và triển khai các dịch vụ |

## 📁 Cấu trúc thư mục

```
Neartime_project/
├── docker-compose.yml              # Cấu hình Docker cho các services
├── pipeline.log                    # Log file tự động sinh khi chạy ETL pipeline
├── data/
│   ├── cassandra/
│   │   ├── _SUCCESS
│   │   └── tracking.csv            # Dữ liệu tracking mẫu
│   └── mysql/
│       ├── events.csv              # Dữ liệu events
│       └── job.csv                 # Dữ liệu job
├── scripts/
│   ├── ETL_pipeline.py             # Script xử lý ETL
│   └── fake_data_script.py         # Script sinh dữ liệu giả lập
```

## 🔧 Yêu cầu hệ thống

### Phần mềm
- **Docker** & **Docker Compose**
- **Python 3.x**

### Thư viện Python
```
cassandra-driver
pandas
numpy
mysql-connector-python
sqlalchemy
```

## 🚀 Hướng dẫn cài đặt và chạy

### 1. Khởi động các services bằng Docker

```bash
cd c:\Neartime_project
docker-compose up -d
```

### 2. Cài đặt các thư viện Python

```bash
pip install cassandra-driver pandas numpy mysql-connector-python sqlalchemy
```

### 3. Cấu hình kết nối

Chỉnh sửa thông tin kết nối trong các file script nếu cần:

| Tham số | Giá trị mặc định |
|---|---|
| MySQL Host | `localhost` |
| MySQL Port | `3306` |
| MySQL Database | `logs` |
| MySQL User | `root` |
| MySQL Password | `1` |
| Cassandra Keyspace | `logs` |
| Cassandra Login | `cassandra` |
| Cassandra Password | `cassandra` |

### 4. Chạy Fake Data Generator

```bash
python scripts/fake_data_script.py
```

Script sẽ liên tục sinh từ **1-20 bản ghi ngẫu nhiên** mỗi **20 giây** vào bảng `tracking` trong Cassandra.

### 5. Chạy ETL Pipeline

```bash
python scripts/ETL_pipeline.py
```

## 📊 Mô hình dữ liệu

### MySQL - Bảng `job`
| Cột | Mô tả |
|---|---|
| `id` | Job ID |
| `campaign_id` | ID chiến dịch |
| `group_id` | ID nhóm |
| `company_id` | ID công ty |

### Cassandra - Bảng `tracking`
| Cột | Mô tả |
|---|---|
| `create_time` | Thời gian tạo (UUID) |
| `bid` | Giá thầu (0 hoặc 1) |
| `campaign_id` | ID chiến dịch |
| `custom_track` | Loại tương tác: `click` (70%), `conversion` (10%), `qualified` (10%), `unqualified` (10%) |
| `group_id` | ID nhóm |
| `job_id` | ID công việc |
| `publisher_id` | ID nhà xuất bản |
| `ts` | Timestamp |

## 📝 Ghi chú

- Fake Data Script chạy liên tục (infinite loop) cho đến khi dừng thủ công (`Ctrl + C`).
- Tỷ lệ phân bổ loại tương tác: **click (70%)**, **conversion (10%)**, **qualified (10%)**, **unqualified (10%)**.
- Dữ liệu mẫu có sẵn trong thư mục `data/` để import ban đầu.
- ETL Pipeline ghi log đồng thời ra **console** và file `pipeline.log` (tự động tạo ở thư mục gốc). Format log: `YYYY-MM-DD HH:MM:SS [LEVEL] message`. Mỗi cycle hiển thị số bản ghi mới và thời gian xử lý (elapsed seconds).

## 👤 Tác giả

Neartime Data Pipeline Project