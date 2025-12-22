import pika
import json
import os
import asyncio
import threading
import sys
from datetime import datetime
from dotenv import load_dotenv

# Import async crawler functions directly
from crawler.bhx.demo import crawl_bhx_store_async
from crawler.winmart.demo import crawl_winmart_store_async

load_dotenv()


class CeleryCrawlingService:

    def __init__(self) -> None:
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

        # RabbitMQ configuration from environment
        self.rabbitmq_url = os.getenv('RABBITMQ_URL')
        self.request_queue = os.getenv('RABBITMQ_CRAWLING_REQUEST_QUEUE')
        self.response_queue = os.getenv('RABBITMQ_CRAWLING_RESPONSE_QUEUE')

    def setup_rabbitmq(self) -> None:
        """Establish a RabbitMQ connection and declare the request/response queues."""
        params = pika.URLParameters(self.rabbitmq_url)

        params.heartbeat = 600
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.request_queue, durable=True)
        self.channel.queue_declare(queue=self.response_queue, durable=True)

    def process_request(self, ch, method, properties, body) -> None:
        """Callback for incoming messages on the request queue."""
        try:
            request = json.loads(body)
            action = request.get('action')

            if action == 'ping':
                # Directly respond to ping requests.
                response = {
                    'action': 'ping',
                    'status': 'success',
                    'message': 'Pong from Celery Crawling Service',
                    'correlationId': request.get('correlationId'),
                    'timestamp': datetime.utcnow().isoformat(),
                }
                self.send_response(response)

            elif action == 'crawl_store':
                correlation_id = request.get('correlationId')
                task_id = request.get('task_id')

                self.send_response({
                    'action': 'task_status_update',
                    'task_id': task_id,
                    'status': 'processing',
                    'correlationId': correlation_id, 
                    'timestamp': datetime.utcnow().isoformat(),
                })

                thread = threading.Thread(
                    target=self._process_crawl_async,
                    args=(request,),
                    daemon=True,
                )
                thread.start()

            # Always acknowledge the message; we handle errors internally.
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:

            print(f"Error processing request: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _process_crawl_async(self, request: dict) -> None:
        """Run an asynchronous crawl in a thread and send status updates."""
        task_id = request.get('task_id')
        chain = request.get('chain', 'BHX').upper()
        try:

            if sys.platform.startswith('win'):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

            if chain == 'BHX':
                result = asyncio.run(
                    crawl_bhx_store_async(
                        store_id=request.get('storeId'),
                        province_id=request.get('provinceId', 3),
                        ward_id=request.get('wardId', 4946),
                        district_id=request.get('districtId', 0),
                        concurrency=request.get('concurrency', 3),
                    )
                )
            elif chain in ('WM', 'WINMART'):
                result = asyncio.run(
                    crawl_winmart_store_async(
                        store_code=str(request.get('storeId')),
                        concurrency=request.get('concurrency', 2),
                    )
                )
            else:
                raise ValueError(f"Unknown chain: {chain}")

            # Send a completed status with the result payload back to main.
            status_response = {
                'action': 'task_status_update',
                'task_id': task_id,
                'status': 'completed' if result.get('status') == 'success' else 'failed',
                'result': result if result.get('status') == 'success' else None,
                'error': result.get('error') if result.get('status') != 'success' else None,
                'timestamp': datetime.utcnow().isoformat(),
            }
            self.send_response(status_response)
            print(f"✅ Crawl finished: {task_id}")

        except Exception as e:

            print(f"❌ Crawl failed: {task_id} - {e}")
            error_response = {
                'action': 'task_status_update',
                'task_id': task_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
            }
            self.send_response(error_response)

    def send_response(self, response: dict) -> None:
        """Publish a response message on the response queue."""
        self.channel.basic_publish(
            exchange='',
            routing_key=self.response_queue,
            body=json.dumps(response, ensure_ascii=False, default=str),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def start(self) -> None:
        """Connect to RabbitMQ and start consuming crawl requests."""
        self.setup_rabbitmq()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.request_queue, on_message_callback=self.process_request)
        print("🚀 Celery Crawling Service started")
        self.channel.start_consuming()


if __name__ == '__main__':
    service = CeleryCrawlingService()
    service.start()