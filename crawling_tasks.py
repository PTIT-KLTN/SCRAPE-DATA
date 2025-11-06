from celery import Celery
import os
import asyncio
import sys
from datetime import datetime
import json
import pika
from dotenv import load_dotenv
from db.db_async import get_sync_db
import time

# Import async functions from demo files
from crawler.bhx.demo import crawl_bhx_store_async
from crawler.winmart.demo import crawl_winmart_store_async

load_dotenv()

broker_url = os.getenv('RABBITMQ_URL')
celery_app = Celery('crawling_service', broker=broker_url)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh',
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_connection_retry_on_startup=True,
    worker_pool='solo',
    worker_disable_rate_limits=True,
    task_ignore_result=True,
    worker_send_task_events=False,
    beat_schedule={
        'check-and-execute-schedules': {
            'task': 'crawling_tasks.check_and_execute_schedules',
            'schedule': 60.0,
        },
    }
)

def run_async_safely(async_func, *args, **kwargs):
    """Wrapper để chạy async function trong Celery task"""
    try:
        # Set Windows event loop policy if needed
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Get or create event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # If loop is closed, create new one
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Run the async function
        return loop.run_until_complete(async_func(*args, **kwargs))
        
    except Exception as e:
        print(f"❌ Async execution error: {e}")
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True)
def crawl_bhx_store_task(self, task_id, store_id, province_id=3, ward_id=4946, district_id=0, concurrency=3):
    """Celery task for BHX crawling"""
    print(f"🚀 Starting BHX crawl: {task_id}, store: {store_id}, worker: {self.request.hostname}")
    
    try:
        # Chỉ gửi status update cho user tasks (không phải scheduled tasks)
        is_scheduled = task_id.startswith('scheduled_')
        
        if not is_scheduled:
            send_status_update(task_id, 'processing')
        
        # Gọi async function từ demo.py
        result = run_async_safely(
            crawl_bhx_store_async,
            store_id=store_id,
            province_id=province_id,
            ward_id=ward_id,
            district_id=district_id,
            concurrency=concurrency
        )
        
        if result.get('status') == 'success':
            if not is_scheduled:
                send_status_update(task_id, 'completed', result)
            print(f"✅ BHX crawl completed: {task_id}")
        else:
            if not is_scheduled:
                send_status_update(task_id, 'failed', error=result.get('error'))
            print(f"❌ BHX crawl failed: {task_id}")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ BHX task error: {task_id} - {error_msg}")
        if not task_id.startswith('scheduled_'):
            send_status_update(task_id, 'failed', error=error_msg)
        return {'status': 'error', 'error': error_msg}

@celery_app.task(bind=True)  
def crawl_winmart_store_task(self, task_id, store_code, concurrency=2):
    """Celery task for WinMart crawling"""
    print(f"🚀 Starting WinMart crawl: {task_id}, store: {store_code}, worker: {self.request.hostname}")
    
    try:
        # Chỉ gửi status update cho user tasks (không phải scheduled tasks)
        is_scheduled = task_id.startswith('scheduled_')
        
        if not is_scheduled:
            send_status_update(task_id, 'processing')
        
        # Gọi async function từ demo.py
        result = run_async_safely(
            crawl_winmart_store_async,
            store_code=store_code,
            concurrency=concurrency
        )
        
        if result.get('status') == 'success':
            if not is_scheduled:
                send_status_update(task_id, 'completed', result)
            print(f"✅ WinMart crawl completed: {task_id}")
        else:
            if not is_scheduled:
                send_status_update(task_id, 'failed', error=result.get('error'))
            print(f"❌ WinMart crawl failed: {task_id}")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ WinMart task error: {task_id} - {error_msg}")
        if not task_id.startswith('scheduled_'):
            send_status_update(task_id, 'failed', error=error_msg)
        return {'status': 'error', 'error': error_msg}

@celery_app.task(bind=True)
def check_and_execute_schedules(self):
    """Check database và execute các schedule đến giờ"""
    try:
        db = get_sync_db()
        current_time = datetime.utcnow()
        
        active_schedules = list(db.schedule_configs.find({
            'type': 'schedule', 
            'is_active': True
        }))
        
        executed_count = 0
        
        for schedule in active_schedules:
            schedule_id = schedule['schedule_id']
            schedule_type = schedule['schedule_type']
            config = schedule.get('schedule_config', {})
            
            should_run = False
            
            if schedule_type == 'hourly':
                minute = config.get('minute', 0)
                should_run = current_time.minute == minute and current_time.second < 60
                
            elif schedule_type == 'daily':
                hour = config.get('hour', 0)
                minute = config.get('minute', 0)
                should_run = (current_time.hour == hour and 
                             current_time.minute == minute and 
                             current_time.second < 60)
                             
            elif schedule_type == 'weekly':
                day_of_week = config.get('day_of_week', 0)
                hour = config.get('hour', 0)
                minute = config.get('minute', 0)
                should_run = (current_time.weekday() == day_of_week and
                             current_time.hour == hour and 
                             current_time.minute == minute and 
                             current_time.second < 60)
            
            if should_run:
                # Check duplicate execution
                last_run_key = f"last_run_{schedule_id}"
                current_minute = current_time.strftime("%Y%m%d%H%M")
                
                last_run = db.schedule_configs.find_one({'_id': last_run_key})
                if last_run and last_run.get('last_minute') == current_minute:
                    continue
                
                # Execute schedule
                print(f"🚀 Executing schedule: {schedule_id}")
                execute_scheduled_crawl.delay(schedule_id)
                executed_count += 1
                
                # Update state
                db.schedule_configs.update_one(
                    {'_id': last_run_key},
                    {'$set': {'last_minute': current_minute, 'last_run': current_time}},
                    upsert=True
                )
        
        if executed_count > 0:
            print(f"✅ Executed {executed_count} schedules")
            
        return {'status': 'success', 'checked': len(active_schedules), 'executed': executed_count}
        
    except Exception as e:
        print(f"❌ Error checking schedules: {e}")
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True)
def execute_scheduled_crawl(self, schedule_id):
    """Execute crawl for scheduled task"""
    print(f"🚀 Executing scheduled crawl: {schedule_id}")
    
    try:
        db = get_sync_db()
        schedule = db.schedule_configs.find_one({'schedule_id': schedule_id, 'is_active': True})
        if not schedule:
            print(f"⚠️ Schedule {schedule_id} not found or inactive")
            return {'status': 'error', 'error': 'Schedule not found'}
        
        chains = schedule.get('chains', ['BHX', 'WM'])
        concurrency = schedule.get('concurrency', 2)
        tasks_submitted = 0
        
        for chain in chains:
            if chain == 'BHX':
                stores = list(db.stores.find({'chain': "BHX"}).limit(3))
            else:
                stores = list(db.stores.find({'chain': {'$in': ["winmart", "winmart+"]}}).limit(3))
            
            for store in stores:
                task_id = f"scheduled_{schedule_id}_{chain}_{store['store_id']}_{int(time.time())}"
                
                if chain == 'BHX':
                    crawl_bhx_store_task.delay(
                        task_id=task_id,
                        store_id=store['store_id'],
                        province_id=store.get('provinceId', 3),
                        ward_id=store.get('wardId', 4946),
                        district_id=store.get('districtId', 0),
                        concurrency=concurrency
                    )
                else:
                    crawl_winmart_store_task.delay(
                        task_id=task_id,
                        store_code=str(store['store_id']),
                        concurrency=concurrency
                    )
                
                tasks_submitted += 1
                time.sleep(1)
        
        print(f"✅ Scheduled crawl completed: {tasks_submitted} tasks submitted")
        return {'status': 'success', 'tasks_submitted': tasks_submitted}
        
    except Exception as e:
        print(f"❌ Scheduled crawl error: {e}")
        return {'status': 'error', 'error': str(e)}

def send_status_update(task_id, status, result=None, error=None):
    """Send status update via RabbitMQ"""
    try:
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        response_queue = os.getenv('RABBITMQ_CRAWLING_RESPONSE_QUEUE')
        
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        channel = connection.channel()
        channel.queue_declare(queue=response_queue, durable=True)
        
        status_message = {
            'action': 'task_status_update',
            'task_id': task_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'service': 'celery_worker'
        }
        
        if result:
            status_message['result'] = result
        if error:
            status_message['error'] = error
        
        channel.basic_publish(
            exchange='',
            routing_key=response_queue,
            body=json.dumps(status_message, ensure_ascii=False, default=str),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Status update failed: {task_id} - {e}")