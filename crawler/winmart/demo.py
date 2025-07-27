import asyncio
import sys
import time
import logging
from pymongo import UpdateOne
from tqdm.asyncio import tqdm
from crawler.winmart.fetch_branches import fetch_branches
from crawler.winmart.fetch_category import fetch_categories
from crawler.winmart.fetch_product import fetch_products_by_store
from crawler.winmart.data_processor import process_products_batch
from db.db_async import get_db

# Cấu hình logger
logging.basicConfig(
    filename='winmart_crawl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class WinMartFetcher:
    def __init__(self, concurrency: int = 10):
        self.branches = None
        self.categories = None
        self.db = None
        self.sem = asyncio.Semaphore(concurrency)

    async def init(self):
        self.branches = await fetch_branches()
        self.categories = await fetch_categories()
        self.db = get_db()

    async def sem_wrap(self, coro, *args):
        async with self.sem:
            try:
                return await coro(*args)
            except asyncio.TimeoutError:
                logger.error(f"TimeoutError: {args[0] if args else 'Unknown'}")
            except Exception as e:
                logger.error(f"Exception while processing {args[0] if args else 'Unknown'}: {e}")


    async def crawl_store(self, store: dict):
        sid = store.get("code")

        # Fetch raw products for this store
        raws = await fetch_products_by_store(sid, self.categories)

        # Process raw items into normalized records
        records = await process_products_batch(raws, self.db)
        if not records:
            logger.error(f"No valid records for store {sid}, skipping.")
            return

        # Build category groups
        category_groups = {}
        for rec in records:
            coll = rec.get("category", "general").replace(" ", "_").lower()
            category_groups.setdefault(coll, []).append(rec)

        # Bulk upsert per collection
        for coll_name, recs in tqdm(
            category_groups.items(),
            desc=f"  Store {sid}",
            unit="category",
            leave=False
        ):
            operations = [
                UpdateOne(
                    {"sku": r["sku"], "store_id": r["store_id"]},
                    {"$set": r},
                    upsert=True
                ) for r in recs
            ]
            if not operations:
                continue

            bulk_result = await self.db[coll_name].bulk_write(
                operations,
                ordered=False
            )
            logger.info(f"[{coll_name}] upserted: {bulk_result.upserted_count}")

    async def run(self):
        await self.init()

        start_time = time.time()

        # test 1 store
        self.branches = self.branches[6:9]

        tasks = [self.sem_wrap(self.crawl_store, store) for store in self.branches]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Store task {i} failed with: {result}")

        elapsed = time.time() - start_time
        logger.info(f"✅ Total time: {elapsed:.2f} seconds")

async def main(concurrency):
    fetcher = WinMartFetcher(concurrency)
    await fetcher.init()
    await fetcher.run()

def run_sync(concurrency):
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(main(concurrency))
