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


class _AsyncWorker:
    """Owns a single asyncio event loop running in a dedicated thread.

    Why: creating/closing a fresh loop per request on Windows often leaves
    background tasks (aiohttp/motor) that later try to run callbacks on a
    closed loop => 'Event loop is closed'.
    """

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="async-worker", daemon=True)
        self._thread.start()
        self._started.wait(timeout=10)

    def _run(self) -> None:
        # IMPORTANT: Do NOT force WindowsSelectorEventLoopPolicy here.
        # Playwright requires subprocess support (Proactor on Windows).
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._started.set()
        try:
            self.loop.run_forever()
        finally:
            try:
                pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception:
                pass
            try:
                asyncio.set_event_loop(None)
            except Exception:
                pass
            try:
                self.loop.close()
            except Exception:
                pass

    def submit(self, coro, done_cb=None):
        if not self.loop:
            raise RuntimeError("Async worker loop is not started")
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        if done_cb is not None:
            fut.add_done_callback(done_cb)
        return fut

    def stop(self) -> None:
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)


class CeleryCrawlingService:
    def __init__(self) -> None:
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

        self.rabbitmq_url = os.getenv("RABBITMQ_URL")
        self.request_queue = os.getenv("RABBITMQ_CRAWLING_REQUEST_QUEUE")
        self.response_queue = os.getenv("RABBITMQ_CRAWLING_RESPONSE_QUEUE")

        self._worker = _AsyncWorker()

    def setup_rabbitmq(self) -> None:
        params = pika.URLParameters(self.rabbitmq_url)
        params.heartbeat = 600
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.request_queue, durable=True)
        self.channel.queue_declare(queue=self.response_queue, durable=True)

    # -------- publishing (thread-safe) --------
    def _publish(self, response: dict) -> None:
        assert self.channel is not None
        self.channel.basic_publish(
            exchange="",
            routing_key=self.response_queue,
            body=json.dumps(response, ensure_ascii=False, default=str),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def send_response(self, response: dict) -> None:
        """Thread-safe publish to RabbitMQ.

        process_request runs on the pika IO thread.
        async crawler runs on async-worker thread.
        Pika BlockingConnection/channel is NOT thread-safe, so we schedule the publish
        back onto the pika thread using add_callback_threadsafe.
        """
        if not self.connection or self.connection.is_closed:
            return

        try:
            self.connection.add_callback_threadsafe(lambda: self._publish(response))
        except Exception:
            # As a fallback (best-effort), try direct publish if we're already on pika thread.
            try:
                self._publish(response)
            except Exception:
                pass

    # -------- request handling --------
    def process_request(self, ch, method, properties, body) -> None:
        try:
            request = json.loads(body)
            action = request.get("action")

            if action == "ping":
                response = {
                    "action": "ping",
                    "status": "success",
                    "message": "Pong from Celery Crawling Service",
                    "correlationId": request.get("correlationId"),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                self.send_response(response)

            elif action == "crawl_store":
                correlation_id = request.get("correlationId")
                task_id = request.get("task_id")

                self.send_response(
                    {
                        "action": "task_status_update",
                        "task_id": task_id,
                        "status": "processing",
                        "correlationId": correlation_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

                # Run crawl in the dedicated asyncio loop (no per-request loop creation).
                coro = self._crawl_and_build_response(request)
                self._worker.submit(coro, done_cb=lambda f: self._on_crawl_done(task_id, f))

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"Error processing request: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_crawl_done(self, task_id: str, future) -> None:
        try:
            response = future.result()
        except Exception as e:
            response = {
                "action": "task_status_update",
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }
        self.send_response(response)

    async def _crawl_and_build_response(self, request: dict) -> dict:
        task_id = request.get("task_id")
        chain = (request.get("chain", "BHX") or "BHX").upper()

        try:
            if chain == "BHX":
                result = await crawl_bhx_store_async(
                    store_id=request.get("storeId"),
                    province_id=request.get("provinceId", 3),
                    ward_id=request.get("WardId", 4946)
                    if request.get("WardId") is not None
                    else request.get("wardId", 4946),
                    district_id=request.get("districtId", 0),
                    concurrency=request.get("concurrency", 3),
                )
            elif chain in ("WM", "WINMART"):
                result = await crawl_winmart_store_async(
                    store_code=str(request.get("storeId")),
                    concurrency=request.get("concurrency", 2),
                )
            else:
                raise ValueError(f"Unknown chain: {chain}")

            if not isinstance(result, dict):
                raise RuntimeError(f"Crawler returned non-dict result: {result}")

            status = "completed" if result.get("status") == "success" else "failed"
            response: dict[str, any] = {
                "action": "task_status_update",
                "task_id": task_id,
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
            }
            if status == "completed":
                response["result"] = result
            else:
                response["error"] = result.get("error") or "Unknown error"
            print(f"✅ Crawl finished: {task_id} (status={status})")
            return response

        except Exception as e:
            print(f"❌ Crawl failed: {task_id} - {e}")
            return {
                "action": "task_status_update",
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def start(self) -> None:
        self.setup_rabbitmq()
        self._worker.start()

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.request_queue, on_message_callback=self.process_request)
        print("🚀 Celery Crawling Service started")
        try:
            self.channel.start_consuming()
        finally:
            self._worker.stop()


if __name__ == "__main__":
    service = CeleryCrawlingService()
    service.start()
