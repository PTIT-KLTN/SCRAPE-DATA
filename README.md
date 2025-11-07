# SCRAPE-DATA

**Hệ thống crawl dữ liệu sản phẩm từ các chuỗi cửa hàng (BHX, WinMart)**

## 🏗️ Kiến trúc Hybrid

Hệ thống sử dụng **kiến trúc Hybrid** để xử lý 2 loại crawling:

1. **On-Demand Crawling**: Crawl 1 store theo yêu cầu từ Main Service qua RabbitMQ
   - Xử lý bởi: `crawling_service.py`
   - Đặc điểm: Nhanh, real-time, có response

2. **Scheduled Crawling**: Crawl tự động theo lịch từ database
   - Xử lý bởi: Celery Workers + Celery Beat
   - Đặc điểm: Chạy nền, định kỳ, không cần response ngay

📖 **Chi tiết:** Xem [ARCHITECTURE.md](./ARCHITECTURE.md)

---

## 🚀 Cài đặt & Chạy

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
RABBITMQ_URL=<RABBITMQ_URL>
RABBITMQ_CRAWLING_REQUEST_QUEUE=<>
RABBITMQ_CRAWLING_RESPONSE_QUEUE=<>
```

### 3. Chạy hệ thống

#### Windows (Development)

**Cách 1: Tự động (khuyến nghị)**
```powershell
.\start.ps1
```

**Cách 2: Thủ công**

Terminal 1 - On-demand crawling:
```bash
python crawling_service.py
```

Terminal 2 - Celery workers (scheduled jobs):
```bash
python worker_manager.py
```

Terminal 3 - Celery Beat (schedule checker):
```bash
celery -A crawling_tasks beat --loglevel=info
```

#### Docker (Production)

```bash
# Build và start
docker-compose up -d

# Xem logs
docker-compose logs -f

# Stop
docker-compose down
```

📖 **Chi tiết Docker:** Xem [DOCKER_DEPLOYMENT.md](./DOCKER_DEPLOYMENT.md)

---

## 🧪 Test

### Test cả 2 tính năng:

```bash
python test_hybrid_system.py
```

Menu test:
1. Test on-demand crawl (RabbitMQ) ✅
2. Test scheduled crawl (Database) ✅
3. Check existing schedules
4. Cleanup test schedule

### Test thủ công:

**Test On-Demand:**
```python
# Từ Main Service, gửi request qua RabbitMQ
{
    "action": "crawl_store",
    "task_id": "test_001",
    "chain": "BHX",
    "storeId": "CH001",
    "concurrency": 2
}
```

**Test Scheduled:**
```python
# Thêm schedule vào database
db.schedule_configs.insert_one({
    'schedule_id': 'test_hourly',
    'type': 'schedule',
    'schedule_type': 'hourly',
    'schedule_config': {'minute': 30},
    'chains': ['BHX'],
    'is_active': True
})
```

---

## 📁 Cấu trúc thư mục

```
SCRAPE-DATA/
├── crawling_service.py        # On-demand crawling service
├── crawling_tasks.py           # Celery tasks cho scheduled jobs
├── worker_manager.py           # Celery workers manager
├── start.ps1                   # Script khởi động (Windows)
├── docker-compose.yml          # Docker config
├── ARCHITECTURE.md             # Chi tiết kiến trúc
├── DOCKER_DEPLOYMENT.md        # Hướng dẫn deploy Docker
├── test_hybrid_system.py       # Test suite
│
├── crawler/
│   ├── bhx/                   # BHX crawlers
│   │   ├── demo.py            # Main BHX crawler
│   │   ├── fetch_store_by_province.py
│   │   └── ...
│   └── winmart/               # WinMart crawlers
│       ├── demo.py            # Main WinMart crawler
│       ├── fetch_branches.py
│       └── ...
│
└── db/
    └── db_async.py             # Database helper
```

---

## 📊 Database Schema

### schedule_configs (Scheduled Jobs)

```javascript
{
  schedule_id: "daily_bhx_crawl",
  type: "schedule",
  schedule_type: "hourly" | "daily" | "weekly",
  schedule_config: {
    hour: 2,           // 0-23 (for daily/weekly)
    minute: 0,         // 0-59
    day_of_week: 0     // 0-6 (for weekly, 0=Monday)
  },
  chains: ["BHX", "WM"],
  concurrency: 3,
  is_active: true
}
```

### stores

```javascript
{
  store_id: "CH001",
  chain: "BHX" | "winmart" | "winmart+",
  provinceId: 3,
  wardId: 4946,
  districtId: 0,
  // ... other fields
}
```

---

## 🔧 Troubleshooting

### On-Demand Crawling không hoạt động

```bash
# Check service
docker-compose logs -f crawling-service

# Check RabbitMQ queues
# Visit: http://100.121.57.49:15672
```

### Scheduled Jobs không chạy

```bash
# Check Beat
docker-compose logs -f celery-beat

# Check Workers
docker-compose logs -f celery-workers

# Check database schedules
mongo --host 100.85.88.111 -u admin -p admin123
> db.schedule_configs.find({is_active: true})
```

---

## 📝 Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f crawling-service
docker-compose logs -f celery-workers
docker-compose logs -f celery-beat
```

---

## 🎯 Features

- ✅ Crawl on-demand từ Main Service qua RabbitMQ
- ✅ Crawl tự động theo lịch từ database
- ✅ Support BHX và WinMart
- ✅ Async crawling với concurrency control
- ✅ Docker support
- ✅ Logs riêng biệt cho debugging
- ✅ Scale horizontal dễ dàng

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/aklc9210/SCRAPE-DATA/issues)
- **Docs:** 
  - [ARCHITECTURE.md](./ARCHITECTURE.md) - Kiến trúc chi tiết
  - [DOCKER_DEPLOYMENT.md](./DOCKER_DEPLOYMENT.md) - Deploy Docker
