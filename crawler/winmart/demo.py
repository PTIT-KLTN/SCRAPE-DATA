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
    def __init__(self, concurrency: int = 3):
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
            coll = rec.get("category").replace(" ", "_").lower()
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

    async def crawl_single_store(self, store_code: str):
        """Crawl một store cụ thể theo store_code"""
        try:
            # Find store by code
            target_store = None
            for store in self.branches:
                if store.get("code") == store_code:
                    target_store = store
                    break
            
            if not target_store:
                logger.error(f"Store with code {store_code} not found")
                return {
                    'status': 'error',
                    'store_code': store_code,
                    'error': f'Store with code {store_code} not found'
                }
            
            # Crawl the specific store
            start_time = time.time()
            await self.crawl_store(target_store)
            end_time = time.time()
            
            elapsed = end_time - start_time
            logger.info(f"✅ Store {store_code} crawled in {elapsed:.2f} seconds")
            
            return {
                'status': 'success',
                'store_code': store_code,
                'store_name': target_store.get('name', ''),
                'processing_time': elapsed,
                'categories_count': len(self.categories) if self.categories else 0
            }
            
        except Exception as e:
            logger.error(f"❌ Error crawling store {store_code}: {e}")
            return {
                'status': 'error',
                'store_code': store_code,
                'error': str(e)
            }

    async def run(self, store_code=None):
        """Run crawling - updated to support specific store_code"""
        await self.init()

        start_time = time.time()

        if store_code:
            # Crawl specific store
            result = await self.crawl_single_store(store_code)
            return result
        else:
            # Original logic - crawl multiple stores
            # test 1 store
            test_branches = self.branches[6:9]

            tasks = [self.sem_wrap(self.crawl_store, store) for store in test_branches]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"Store task {i} failed with: {result}")

            elapsed = time.time() - start_time
            logger.info(f"✅ Total time: {elapsed:.2f} seconds")
            
            return {
                'status': 'success',
                'stores_count': len(test_branches),
                'processing_time': elapsed
            }


async def main(concurrency, store_code=None):
    """Main function - updated to support specific store_code"""
    fetcher = WinMartFetcher(concurrency)
    result = await fetcher.run(store_code)
    return result


def run_sync(concurrency=3, store_code=None):
    """Sync wrapper - updated to support store_code"""
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    return asyncio.run(main(concurrency, store_code))


# Async wrapper function for RabbitMQ integration
async def crawl_winmart_store_async(store_code: str, concurrency: int = 3):
    """Async function to be called from crawling_service.py"""
    return await main(concurrency, store_code)

# Sync wrapper function for RabbitMQ integration
def crawl_winmart_store(store_code: str, concurrency: int = 3):
    """Function to be called from crawling_service.py"""
    return run_sync(concurrency, store_code)