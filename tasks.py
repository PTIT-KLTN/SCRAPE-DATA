import os
import logging
from datetime import datetime
from celery import Celery
from celery.schedules import crontab
from pymongo import UpdateOne

# Import các fetcher
from crawler.bhx.demo import run_sync as crawl_bhx_all, BHXDataFetcher
from crawler.winmart.demo import run_sync as crawl_wm_all, WinMartFetcher
from db import MongoDB

# Cấu hình logging với TimedRotatingFileHandler
logger = logging.getLogger('crawling')
logger.setLevel(logging.INFO)
handler = logging.handlers.TimedRotatingFileHandler(
    filename=os.getenv('CRAWL_LOG', 'crawling.log'),
    when='midnight', interval=1, backupCount=7
)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s %(module)s: %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Khởi tạo Celery
app = Celery(
    'crawl_tasks',
    broker=os.getenv('RABBITMQ_URL'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'rpc://')
)

# Lên lịch crawling định kỳ hàng ngày
app.conf.beat_schedule = {
    'crawl-bhx-daily': {
        'task': 'tasks.crawl_bhx',
        'schedule': crontab(hour=2, minute=0),
    },
    'crawl-wm-daily': {
        'task': 'tasks.crawl_wm',
        'schedule': crontab(hour=3, minute=0),
    },
}

@app.task(bind=True, max_retries=3, default_retry_delay=300)
def crawl_bhx(self):
    """Task tự động crawl toàn bộ stores của BHX hàng ngày"""
    start_time = datetime.utcnow()
    logger.info('Bắt đầu crawl BHX')
    try:
        stores = crawl_bhx_all()
        end_time = datetime.utcnow()
        # Lưu metadata vào MongoDB
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': 'BHX',
            'start_time': start_time,
            'end_time': end_time,
            'status': 'success',
            'total_stores': len(stores)
        })
        logger.info(f'Hoàn thành crawl BHX: {len(stores)} stores')
    except Exception as exc:
        logger.error(f'Lỗi crawl BHX: {exc}', exc_info=True)
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': 'BHX',
            'start_time': start_time,
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error': str(exc)
        })
        raise self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=300)
def crawl_wm(self):
    """Task tự động crawl toàn bộ stores của WinMart hàng ngày"""
    start_time = datetime.utcnow()
    logger.info('Bắt đầu crawl WinMart')
    try:
        stores = crawl_wm_all()
        end_time = datetime.utcnow()
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': 'WM',
            'start_time': start_time,
            'end_time': end_time,
            'status': 'success',
            'total_stores': len(stores)
        })
        logger.info(f'Hoàn thành crawl WM: {len(stores)} stores')
    except Exception as exc:
        logger.error(f'Lỗi crawl WM: {exc}', exc_info=True)
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': 'WM',
            'start_time': start_time,
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error': str(exc)
        })
        raise self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=300)
def crawl_store(self, chain: str, store_id: int, province_id: int = None, district_id: int = None, ward_id: int = None, onlyOneProduct: bool = False):
    """Task thủ công: crawl 1 store theo yêu cầu"""
    start_time = datetime.utcnow()
    logger.info(f'Start manual crawl_store: chain={chain}, store_id={store_id}')
    try:
        if chain.upper() == 'BHX':
            fetcher = BHXDataFetcher()
            fetcher.init_token()
            result = fetcher.fetch_store_by_id(
                store_id=store_id,
                province_id=province_id or 3,
                district_id=district_id or 0,
                ward_id=ward_id or 0,
                only_one_product=onlyOneProduct
            )
            fetcher.close()
        elif chain.upper() == 'WM':
            fetcher = WinMartFetcher()
            # nếu WinMart cần init token/session
            result = fetcher.fetch_store_by_id(store_id)
            fetcher.close()
        else:
            raise ValueError(f'Unsupported chain: {chain}')

        end_time = datetime.utcnow()
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': chain.upper(),
            'store_id': store_id,
            'start_time': start_time,
            'end_time': end_time,
            'status': 'success'
        })
        return {'store_id': store_id, 'chain': chain, 'status': 'success', 'data': result}

    except Exception as exc:
        logger.error(f'Lỗi manual crawl_store: {exc}', exc_info=True)
        db = MongoDB.get_db()
        db.crawl_jobs.insert_one({
            'chain': chain.upper(),
            'store_id': store_id,
            'start_time': start_time,
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error': str(exc)
        })
        raise self.retry(exc=exc)