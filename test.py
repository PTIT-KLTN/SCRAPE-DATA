import pika
import json
import os
from dotenv import load_dotenv

load_dotenv()

def test_rabbitmq_connection():
    """Test basic RabbitMQ connection"""
    print("🔍 Testing RabbitMQ connection...")
    
    try:
        # Connection parameters
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        request_queue = os.getenv('CRAWLING_REQUEST_QUEUE', 'crawling_requests')
        response_queue = os.getenv('CRAWLING_RESPONSE_QUEUE', 'crawling_responses')
        
        print(f"   RabbitMQ URL: {rabbitmq_url}")
        print(f"   Request Queue: {request_queue}")
        print(f"   Response Queue: {response_queue}")
        
        # Create connection
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        channel = connection.channel()
        
        print("✅ Connected to RabbitMQ successfully!")
        
        # Declare queues
        channel.queue_declare(queue=request_queue, durable=True)
        channel.queue_declare(queue=response_queue, durable=True)
        
        print("✅ Queues declared successfully!")
        
        # Send test message
        test_message = {
            'action': 'ping',
            'message': 'Hello RabbitMQ!',
            'timestamp': '2025-01-01T00:00:00Z'
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=request_queue,
            body=json.dumps(test_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        print("✅ Test message sent successfully!")
        
        # Close connection
        connection.close()
        print("✅ Connection closed successfully!")
        
        print("\n🎉 RabbitMQ is working properly!")
        print("\n📋 Next steps:")
        print("   1. Check Management UI: http://localhost:15672")
        print("   2. Username: admin, Password: admin123")
        print("   3. Go to 'Queues' tab to see your queues")
        
        return True
        
    except Exception as e:
        print(f"❌ RabbitMQ connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("   1. Make sure RabbitMQ is running: docker ps | grep rabbitmq")
        print("   2. Check if port 5672 is accessible: telnet localhost 5672")
        print("   3. Verify credentials in .env file")
        return False

if __name__ == "__main__":
    print("🚀 Simple RabbitMQ Test")
    print("=" * 40)
    test_rabbitmq_connection()