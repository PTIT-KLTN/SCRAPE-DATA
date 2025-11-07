# 🏗️ Kiến trúc Hệ thống Crawling

## 📊 Tổng quan

Hệ thống sử dụng **Kiến trúc Hybrid** để xử lý 2 loại crawling khác nhau:

### 1. **On-Demand Crawling** (Crawl theo yêu cầu)
- **Kích hoạt:** Từ Main Service qua RabbitMQ
- **Xử lý bởi:** `crawling_service.py`
- **Đặc điểm:** Nhanh, real-time, có response về Main Service

### 2. **Scheduled Crawling** (Crawl theo lịch)
- **Kích hoạt:** Tự động theo schedule trong database
- **Xử lý bởi:** Celery Workers + Celery Beat
- **Đặc điểm:** Chạy nền, định kỳ, không cần response ngay

---

## 🔄 Luồng hoạt động

### Luồng 1: On-Demand Crawling

```
Main Service
    │
    ├─ POST /api/crawling/crawl-store
    │  {chain: 'BHX', storeId: 'CH001'}
    │
    ↓
RabbitMQ (crawling_requests queue)
    │
    ↓
crawling_service.py
    │
    ├─ Nhận request
    ├─ Gửi status: "processing"
    ├─ Chạy async crawl
    │  └─ crawl_bhx_store_async() hoặc
    │     crawl_winmart_store_async()
    ├─ Gửi status: "completed" / "failed"
    │
    ↓
RabbitMQ (crawling_responses queue)
    │
    ↓
Main Service nhận kết quả
```

### Luồng 2: Scheduled Crawling

```
Database (schedule_configs collection)
    │
    │ schedule_configs:
    │ - schedule_id: "daily_bhx"
    │ - schedule_type: "daily"
    │ - schedule_config: {hour: 2, minute: 0}
    │ - chains: ["BHX", "WM"]
    │ - is_active: true
    │
    ↓
Celery Beat (check mỗi 60s)
    │
    ├─ Task: check_and_execute_schedules
    ├─ Kiểm tra schedules đến giờ
    ├─ Tránh duplicate (check last_run)
    │
    ↓
execute_scheduled_crawl(schedule_id)
    │
    ├─ Lấy danh sách stores từ DB
    ├─ Submit tasks cho workers
    │
    ↓
Celery Workers
    │
    ├─ crawl_bhx_store_task()
    └─ crawl_winmart_store_task()
        │
        └─ Lưu kết quả vào Database
```

---

## 🧩 Các thành phần

### 1. crawling_service.py
**Vai trò:** Xử lý on-demand crawling từ Main Service

**Chức năng:**
- Listen RabbitMQ queue `crawling_requests`
- Xử lý actions: `ping`, `crawl_store`
- Chạy async crawling trong background thread
- Gửi response về queue `crawling_responses`

**Không làm:**
- Không xử lý scheduled jobs
- Không consume Celery tasks

---

### 2. crawling_tasks.py
**Vai trò:** Định nghĩa Celery tasks cho scheduled jobs

**Celery App config:**
```python
celery_app = Celery('scheduled_crawling', broker=rabbitmq_url)

beat_schedule = {
    'check-and-execute-schedules': {
        'task': 'crawling_tasks.check_and_execute_schedules',
        'schedule': 60.0,  # Mỗi 60 giây
    }
}
```

**Các tasks:**
- `check_and_execute_schedules`: Check DB và execute schedules
- `execute_scheduled_crawl`: Execute crawl cho 1 schedule
- `crawl_bhx_store_task`: Crawl 1 store BHX (scheduled only)
- `crawl_winmart_store_task`: Crawl 1 store WinMart (scheduled only)

**Không làm:**
- Không consume RabbitMQ queue `crawling_requests`
- Không gửi response về Main Service (chỉ ghi DB)

---

### 3. worker_manager.py
**Vai trò:** Quản lý Celery workers

**Chức năng:**
- Start nhiều workers (default: 2)
- Monitor workers
- Stop workers khi Ctrl+C

---

## 🚀 Cách khởi động

### Development (Windows)

```powershell
# Chạy script tự động
.\start.ps1

# Hoặc chạy thủ công từng service:

# Terminal 1: On-demand crawling
python crawling_service.py

# Terminal 2: Celery workers (scheduled jobs)
python worker_manager.py

# Terminal 3: Celery Beat (schedule checker)
celery -A crawling_tasks beat --loglevel=info
```

### Production (Docker)

```bash
# Build và start tất cả services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## 📝 Database Schema

### Collection: schedule_configs

```javascript
{
  _id: ObjectId,
  schedule_id: "daily_bhx_crawl",        // Unique ID
  type: "schedule",                      // Fixed value
  schedule_type: "daily",                // hourly | daily | weekly
  schedule_config: {
    hour: 2,                             // 0-23
    minute: 0,                           // 0-59
    day_of_week: 0                       // 0=Monday ... 6=Sunday (for weekly)
  },
  chains: ["BHX", "WM"],                 // Chains to crawl
  concurrency: 3,                        // Concurrency per store
  is_active: true,                       // Enable/disable
  created_at: ISODate,
  updated_at: ISODate
}
```

### Collection: stores

```javascript
{
  _id: ObjectId,
  store_id: "CH001",
  chain: "BHX",                          // or "winmart", "winmart+"
  provinceId: 3,
  wardId: 4946,
  districtId: 0,
  // ... other fields
}
```

---

## 🔧 Cấu hình

### File .env

```env
# MongoDB
MONGO_URI=mongodb://admin:admin123@100.85.88.111:27017/markendation?authSource=admin

# RabbitMQ
RABBITMQ_URL=amqp://markendation:password123@100.121.57.49:5672/
RABBITMQ_CRAWLING_REQUEST_QUEUE=crawling_requests
RABBITMQ_CRAWLING_RESPONSE_QUEUE=crawling_responses
```

---

## 🧪 Test

### Test On-Demand Crawling

```python
# Từ Main Service
from services.rabbitmq_service import get_rabbitmq_service

rabbitmq = get_rabbitmq_service()

# Test ping
response = rabbitmq.send_request('ping')
print(response)

# Test crawl store
response = rabbitmq.send_request('crawl_store', {
    'task_id': 'test_001',
    'chain': 'BHX',
    'storeId': 'CH001',
    'provinceId': 3,
    'wardId': 4946,
    'concurrency': 2
})
print(response)
```

### Test Scheduled Crawling

```python
# Thêm schedule vào database
from db.db_async import get_sync_db

db = get_sync_db()

db.schedule_configs.insert_one({
    'schedule_id': 'test_hourly',
    'type': 'schedule',
    'schedule_type': 'hourly',
    'schedule_config': {
        'minute': 30  # Chạy vào phút thứ 30 mỗi giờ
    },
    'chains': ['BHX'],
    'concurrency': 2,
    'is_active': True
})

# Celery Beat sẽ tự động chạy khi đến giờ
```

---

## 🐛 Troubleshooting

### On-Demand Crawling không hoạt động

```bash
# Check crawling_service.py
docker-compose logs -f crawling-service

# Check RabbitMQ
curl http://100.121.57.49:15672/api/queues
```

### Scheduled Jobs không chạy

```bash
# Check Celery Beat
docker-compose logs -f celery-beat

# Check Celery Workers
docker-compose logs -f celery-workers

# Check database schedules
mongo --host 100.85.88.111 -u admin -p admin123
> use markendation
> db.schedule_configs.find({is_active: true})
```

---

## ✅ Checklist

Sau khi deploy, kiểm tra:

- [ ] `crawling-service` container đang chạy
- [ ] `celery-workers` container đang chạy
- [ ] `celery-beat` container đang chạy
- [ ] Crawl 1 store từ Main Service hoạt động
- [ ] Scheduled jobs được check mỗi 60s (xem logs beat)
- [ ] Schedule trong DB được execute đúng giờ

---

## 📈 Performance

### Tăng số Celery Workers

```yaml
# docker-compose.yml
celery-workers:
  command: celery -A crawling_tasks worker --concurrency=5
```

### Tăng concurrency cho mỗi store

```python
# Trong request từ Main Service
{
    'concurrency': 5  # Tăng từ 2-3 lên 5
}
```

---

## 🎯 Kết luận

Kiến trúc Hybrid này giải quyết được:

1. ✅ Crawl 1 store nhanh chóng từ Main Service (đã test OK)
2. ✅ Schedule jobs tự động crawl theo lịch
3. ✅ Không conflict giữa 2 luồng
4. ✅ Dễ scale và maintain
5. ✅ Logs riêng biệt cho debugging
