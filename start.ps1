# Script khoi dong toan bo he thong SCRAPE-DATA

Write-Host "=== Khoi dong he thong SCRAPE-DATA ===" -ForegroundColor Green

# Kiem tra virtual environment
if (-Not (Test-Path "venv\Scripts\Activate.ps1")) {
    Write-Host "Virtual environment khong ton tai. Dang tao..." -ForegroundColor Yellow
    python -m venv venv
    & ".\venv\Scripts\Activate.ps1"
    Write-Host "Dang cai dat dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt
}
else {
    Write-Host "Kich hoat virtual environment..." -ForegroundColor Cyan
    & ".\venv\Scripts\Activate.ps1"
}

# Kiem tra file .env
if (-Not (Test-Path ".env")) {
    Write-Host "CANH BAO: File .env khong ton tai!" -ForegroundColor Red
    Write-Host "Vui long tao file .env voi cac thong tin cau hinh can thiet" -ForegroundColor Yellow
}

Write-Host "`n--- Khoi dong cac services ---" -ForegroundColor Green

# Khoi dong Celery Workers (terminal 1)
Write-Host "1. Khoi dong Celery Workers..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\venv\Scripts\Activate.ps1; python worker_manager.py"

# Doi 3 giay de workers khoi dong
Start-Sleep -Seconds 3

# Khoi dong Celery Beat (terminal 2)
Write-Host "2. Khoi dong Celery Beat..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\venv\Scripts\Activate.ps1; celery -A crawling_tasks beat --loglevel=info"

# Doi 2 giay
Start-Sleep -Seconds 2

# Khoi dong Crawling Service (terminal 3)
Write-Host "3. Khoi dong Crawling Service..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; .\venv\Scripts\Activate.ps1; python crawling_service.py"

Write-Host "`n=== He thong da duoc khoi dong thanh cong! ===" -ForegroundColor Green
Write-Host "Cac terminal da duoc mo:" -ForegroundColor Yellow
Write-Host "  - Terminal 1: Celery Workers" -ForegroundColor White
Write-Host "  - Terminal 2: Celery Beat" -ForegroundColor White
Write-Host "  - Terminal 3: Crawling Service" -ForegroundColor White
Write-Host "`nDe dung he thong, dong tat ca cac terminal da mo." -ForegroundColor Yellow
