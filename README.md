# SCRAPE-DATA

**Hệ thống crawl dữ liệu sản phẩm từ các chuỗi cửa hàng (BHX, WinMart)**

## Cài đặt & Chạy

### 1. Cài đặt

```bash
# Clone project
git clone <repo-url>
cd SCRAPE-DATA

# Tạo virtual environment
python -m venv venv
venv\Scripts\activate

# Cài đặt dependencies
pip install -r requirements.txt
```

### 2. Cấu hình

Tạo file `.env`:

```
RABBITMQ_URL=amqp://markendation:password@26.164.48.24:5672//
RABBITMQ_CRAWLING_REQUEST_QUEUE=crawling_requests
RABBITMQ_CRAWLING_RESPONSE_QUEUE=crawling_responses
```

### 3. Chạy hệ thống

**Mở terminal 1 chạy Celery Workers:**

```bash
python worker_manager.py
```

**Mở terminal 2 chạy Crawling Service:**

```bash
python crawling_service.py
```

**Mở terminal 2 chạy Celery beat:**

```bash
celery -A crawling_tasks beat --loglevel=info
```

## Kiến trúc

- **Main Service**: Nhận requests từ API
- **Celery Workers**: Xử lý crawling tasks
- **RabbitMQ**: Message queue
- **MongoDB**: Lưu trữ dữ liệu
