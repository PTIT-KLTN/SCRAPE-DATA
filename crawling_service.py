import pika
import json
from crawling_tasks import crawl_bhx_store_task, crawl_winmart_store_task
import os
from datetime import datetime

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
                # Submit to Celery
                task_id = request.get('task_id')
                chain = request.get('chain', 'BHX').upper()
                
                if chain == 'BHX':
                    crawl_bhx_store_task.delay(
                        task_id=task_id,
                        store_id=request.get('storeId'),
                        province_id=request.get('provinceId', 3),
                        ward_id=request.get('wardId', 4946),
                        district_id=request.get('districtId', 0),
                        concurrency=request.get('concurrency', 3)
                    )
                elif chain == 'WM':
                    crawl_winmart_store_task.delay(
                        task_id=task_id,
                        store_code=str(request.get('storeId')),
                        concurrency=request.get('concurrency', 2)
                    )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error processing request: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
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