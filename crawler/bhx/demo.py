import asyncio
import time
import aiohttp
import sys
import logging
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.fetch_store_by_province import fetch_stores_async
from crawler.bhx.fetch_full_location import fetch_full_location_data
from crawler.bhx.fetch_menus_for_store import fetch_menus_for_store
from db.db_async import get_db

from crawler.bhx.process_data import process_product_data
from crawler.process_data.process import CATEGORIES_MAPPING_BHX
from tqdm import tqdm
from asyncio import Semaphore

# Cấu hình logger
logging.basicConfig(
    filename='bhx_crawl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class BHXDataFetcher:
    def __init__(self, concurrency: int = 5):
        self.token = None
        self.deviceid = None
        self.interceptor = None
        self.session = None
        self.db = get_db()

        # semaphore để giới hạn số store chạy song song
        self.sem = Semaphore(concurrency)
    
    async def init(self):
        ti = BHXTokenInterceptor()
        self.token, self.deviceid = await ti.init_and_get_token()
        await ti.close()
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def close(self):
        await self.session.close()

    async def fetch_categories(self, province, ward, store):
        raw = await fetch_menus_for_store(province, ward, store,
                                          self.token, self.deviceid)
        cats = []
        for m in raw:
            for c in m.get("childrens", []):
                eng = CATEGORIES_MAPPING_BHX.get(c["name"])
                if eng:
                    cats.append({"name":eng, "link":c["url"]})
        # group by name
        grouped = {}
        for c in cats:
            grouped.setdefault(c["name"], []).append(c["link"])
        return [{"name":n, "links":list(set(ls))} for n,ls in grouped.items()]

    async def crawl_store(self, store, categories, province):
        store_id = store["storeId"]
        ward_id  = store.get("wardId",0)
        dist_id  = store.get("districtId",0)

        bar = tqdm(categories, desc=f"Store {store_id}", leave=True)
        for cat in categories:
            for url in cat["links"]:
                await self.sem_wrap(self.fetch_api_and_save(
                    store_id, ward_id, dist_id, province, cat, url
                ))
            bar.update(1)
        bar.close()

    async def fetch_api_and_save(self, store_id, ward, dist, prov, cat, url):
        # fetch all pages
        allp = []
        page=1; size=20
        with tqdm(desc=f"Store {store_id} API", unit="page") as pbar:
            while True:
                api = (f"https://apibhx.tgdd.vn/Category/V2/GetCate?"
                    f"provinceId={prov}&wardId={ward}&districtId={dist}"
                    f"&storeId={store_id}&categoryUrl={url}"
                    f"&isMobile=true&pageSize={size}&page={page}")
                h = get_headers(self.token, self.deviceid)

                # Headers với token
                h.update({
                    "referer": f"https://www.bachhoaxanh.com/{cat}",
                    "accept": "application/json, text/plain, */*"
                })

                try:
                    async with self.session.get(api, headers=h, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        js = await resp.json()
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout at page {page} for store {store_id}")
                    break
                except aiohttp.ClientError as e:
                    logger.error(f"Network error for store {store_id}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error while fetching store {store_id}: {e}")
                    break

                batch = js["data"].get("products", [])
                total = js["data"].get("total", 0)

                if not batch: break

                allp.extend(batch)   
                if len(allp) >= total: 
                    break

                page+=1
                pbar.update(1)
                await asyncio.sleep(0.5)

        # build and write ops
        ops = await process_product_data(allp, cat["name"], store_id, self.db)
        if ops:
            coll = self.db[cat["name"].replace(" ","_").lower()]
            result = await coll.bulk_write(ops, ordered=False)
            logger.info(f"Store {store_id}｜{cat['name']}: upserted {result.upserted_count}, mod {result.modified_count}")

    async def sem_wrap(self, coro):
        async with self.sem:
            try:
                return await coro
            except asyncio.TimeoutError:
                logger.warning("TimeoutError while executing a task")
            except aiohttp.ClientError as e:
                logger.error(f"Network error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
        

async def main(concurrency: int = 4):
    fetcher = BHXDataFetcher(concurrency=concurrency)
    await fetcher.init()
    start = None
    end = None
    try:
        # 1. Categories from any sample store
        prov, ward, store0 = 3, 4946, 2087
        categories = await fetcher.fetch_categories(prov, ward, store0)
    
        # 2. Get stores in HCM
        full = await fetch_full_location_data(fetcher.token, fetcher.deviceid)
        provinces = [p for p in full.get("provinces",[]) if p["name"].strip() == "TP. Hồ Chí Minh"]
        if not provinces:
            logger.error("No provinces found for TP. Hồ Chí Minh")
            return
        
        stores = await fetch_stores_async(provinces[0]["id"],
                                        fetcher.token, fetcher.deviceid)
        
        # test thử 1 store
        stores = stores[450:]

        # 3. Crawl product
        start = time.time()
        await asyncio.gather(
            *[ fetcher.crawl_store(s, categories, prov)
            for s in stores ]
        )

        end = time.time()
        logger.info(f"✅ Total time: {(end - start):.2f} minutes")
    finally:
        await fetcher.close()

def run_sync(concurrency: int = 4):
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(main(concurrency))
