import pika
import json
import os
import asyncio
import threading
import sys
from datetime import datetime
from dotenv import load_dotenv

# Import async functions directly
from crawler.bhx.demo import crawl_bhx_store_async
from crawler.winmart.demo import crawl_winmart_store_async

load_dotenv()

class CeleryCrawlingService:
    def __init__(self):
        self.connection = None
        self.channel = None
        
        # RabbitMQ config
        self.rabbitmq_url = os.getenv('RABBITMQ_URL')
        self.request_queue = os.getenv('RABBITMQ_CRAWLING_REQUEST_QUEUE')
        self.response_queue = os.getenv('RABBITMQ_CRAWLING_RESPONSE_QUEUE')
    
    def setup_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self.rabbitmq_url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.request_queue, durable=True)
        self.channel.queue_declare(queue=self.response_queue, durable=True)
    
    def process_request(self, ch, method, properties, body):
        try:
            request = json.loads(body)
            action = request.get('action')
            
            if action == 'ping':
                # Handle ping directly
                response = {
                    'action': 'ping',
                    'status': 'success', 
                    'message': 'Pong from Celery Crawling Service',
                    'correlationId': request.get('correlationId'),
                    'timestamp': datetime.utcnow().isoformat()
                }
                self.send_response(response)
                
            elif action == 'crawl_store':
                # Send immediate acknowledgment
                correlation_id = request.get('correlationId')
                task_id = request.get('task_id')
                
                # Send processing response
                response = {
                    'action': 'crawl_store',
                    'status': 'processing',
                    'correlationId': correlation_id,
                    'task_id': task_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
                self.send_response(response)
                
                # Process crawl in background thread
                thread = threading.Thread(
                    target=self._process_crawl_async,
                    args=(request,),
                    daemon=True
                )
                thread.start()
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error processing request: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def _process_crawl_async(self, request):
        """Process crawl request in async context"""
        try:
            task_id = request.get('task_id')
            chain = request.get('chain', 'BHX').upper()
            
            # Set up event loop for Windows
            if sys.platform.startswith("win"):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run async crawl
            if chain == 'BHX':
                result = loop.run_until_complete(
                    crawl_bhx_store_async(
                        store_id=request.get('storeId'),
                        province_id=request.get('provinceId', 3),
                        ward_id=request.get('wardId', 4946),
                        district_id=request.get('districtId', 0),
                        concurrency=request.get('concurrency', 3)
                    )
                )
            elif chain == 'WM':
                result = loop.run_until_complete(
                    crawl_winmart_store_async(
                        store_code=str(request.get('storeId')),
                        concurrency=request.get('concurrency', 2)
                    )
                )
            else:
                raise ValueError(f"Unknown chain: {chain}")
            
            loop.close()
            
            # Send status update - completed
            status_response = {
                'action': 'task_status_update',
                'task_id': task_id,
                'status': 'completed',
                'result': result,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.send_response(status_response)
            
            print(f"✅ Crawl completed: {task_id}")
            
        except Exception as e:
            print(f"❌ Crawl failed: {task_id} - {e}")
            
            # Send status update - failed
            error_response = {
                'action': 'task_status_update',
                'task_id': task_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
            self.send_response(error_response)
    
    def send_response(self, response):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.response_queue,
            body=json.dumps(response, ensure_ascii=False, default=str),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    def start(self):
        self.setup_rabbitmq()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.request_queue, on_message_callback=self.process_request)
        print("🚀 Celery Crawling Service started")
        self.channel.start_consuming()

if __name__ == "__main__":
    service = CeleryCrawlingService()
    service.start()