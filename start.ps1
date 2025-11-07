# Script khoi dong he thong SCRAPE-DATA (Hybrid Architecture)
# - Crawling Service: Xu ly on-demand crawl tu Main Service (qua RabbitMQ)
# - Celery Workers + Beat: Xu ly scheduled jobs tu database

Write-Host "=== Khoi dong he thong SCRAPE-DATA ===" -ForegroundColor Green

# Kiem tra virtual environment
if (-Not (Test-Path "env\Scripts\Activate.ps1")) {
    Write-Host "Virtual environment khong ton tai. Dang tao..." -ForegroundColor Yellow
    python -m venv env
    & ".\env\Scripts\Activate.ps1"
    Write-Host "Dang cai dat dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt
}
else {
    Write-Host "Kich hoat virtual environment..." -ForegroundColor Cyan
    & ".\env\Scripts\Activate.ps1"
}

# Kiem tra file .env
if (-Not (Test-Path ".env")) {
    Write-Host "CANH BAO: File .env khong ton tai!" -ForegroundColor Red
    Write-Host "Vui long tao file .env voi cac thong tin cau hinh can thiet" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n--- Khoi dong cac services ---" -ForegroundColor Green

# 1. Khoi dong Crawling Service (On-demand crawls)
Write-Host "1. Khoi dong Crawling Service (On-demand)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\env\Scripts\Activate.ps1; python crawling_service.py"

Start-Sleep -Seconds 2

# 2. Khoi dong Celery Workers (Scheduled jobs only)
Write-Host "2. Khoi dong Celery Workers (Scheduled jobs)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\env\Scripts\Activate.ps1; python worker_manager.py"

Start-Sleep -Seconds 3

# 3. Khoi dong Celery Beat (Check schedules every 60s)
Write-Host "3. Khoi dong Celery Beat (Schedule checker)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\env\Scripts\Activate.ps1; celery -A crawling_tasks beat --loglevel=info"

Write-Host "`n=== He thong da duoc khoi dong thanh cong! ===" -ForegroundColor Green
Write-Host "Cac service da duoc khoi dong:" -ForegroundColor Yellow
Write-Host "  [Terminal 1] Crawling Service: Xu ly on-demand crawl tu Main Service" -ForegroundColor White
Write-Host "  [Terminal 2] Celery Workers: Xu ly scheduled jobs tu database" -ForegroundColor White
Write-Host "  [Terminal 3] Celery Beat: Kiem tra schedule moi 60s" -ForegroundColor White
Write-Host "`nKien truc Hybrid - Khong conflict:" -ForegroundColor Cyan
Write-Host "  - On-demand crawls: Main Service -> RabbitMQ -> Crawling Service" -ForegroundColor White
Write-Host "  - Scheduled crawls: Celery Beat -> Check DB -> Celery Workers" -ForegroundColor White
Write-Host "`nDe dung he thong, dong tat ca cac terminal da mo." -ForegroundColor Yellow
