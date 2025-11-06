import asyncio
import aiohttp
import json
import time
from datetime import datetime
from crawler.bhx.token_interceptor import BHXTokenInterceptor, get_headers
from crawler.bhx.process_data import CATEGORIES_MAPPING
from tqdm import tqdm

class BHXAPIExporter:
    def __init__(self, store_id: int, province_id: int = 3, ward_id: int = 0, district_id: int = 0):
        self.store_id = store_id
        self.province_id = province_id
        self.ward_id = ward_id
        self.district_id = district_id
        self.token = None
        self.deviceid = None
        self.session = None
        
    async def init(self):
        """Initialize token and session"""
        print("Initializing BHX token...")
        ti = BHXTokenInterceptor()
        self.token, self.deviceid = await ti.init_and_get_token()
        await ti.close()
        self.session = aiohttp.ClientSession()
        print(f"Token initialized: {self.token[:20]}...")
        
    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()
    
    async def fetch_category_data(self, category_url: str, category_name: str, eng_category: str):
        """Fetch all products from a category"""
        print(f"Fetching data for category: {category_name} ({category_url})")
        
        all_products = []
        page = 1
        page_size = 50  # Increase page size for efficiency
        
        with tqdm(desc=f"Category: {category_name}", unit="page") as pbar:
            while True:
                api_url = (
                    f"https://apibhx.tgdd.vn/Category/V2/GetCate?"
                    f"provinceId={self.province_id}&wardId={self.ward_id}&districtId={self.district_id}"
                    f"&storeId={self.store_id}&categoryUrl={category_url}"
                    f"&isMobile=true&isV2=true&pageSize={page_size}&page={page}"
                )
                
                headers = get_headers(self.token, self.deviceid)
                headers.update({
                    "referer": f"https://www.bachhoaxanh.com/{category_url}",
                    "accept": "application/json, text/plain, */*"
                })
                
                try:
                    async with self.session.get(api_url, headers=headers) as response:
                        if response.status != 200:
                            print(f"Error fetching page {page}: HTTP {response.status}")
                            break
                            
                        data = await response.json()
                        
                        if "data" not in data:
                            print(f"No data field in response for page {page}")
                            break
                            
                        products = data["data"].get("products", [])
                        total = data["data"].get("total", 0)
                        
                        if not products:
                            print(f"No more products found at page {page}")
                            break
                        
                        # Extract only required fields from products
                        filtered_products = []
                        for product in products:
                            filtered_product = {
                                "name": product.get("name", ""),
                                "image": product.get("avatar", ""),
                                "unit": product.get("unit", ""),
                                "category": eng_category
                            }
                            filtered_products.append(filtered_product)
                            
                        all_products.extend(filtered_products)
                        pbar.set_postfix({
                            'products': len(all_products),
                            'total': total
                        })
                        
                        # Check if we've got all products
                        if len(all_products) >= total:
                            print(f"Fetched all {total} products for {category_name}")
                            break
                            
                        page += 1
                        pbar.update(1)
                        
                        # Add small delay to be respectful
                        await asyncio.sleep(0.3)
                        
                except Exception as e:
                    print(f"Error fetching page {page} for {category_name}: {e}")
                    break
        
        print(f"Total products fetched for {category_name}: {len(all_products)}")
        return all_products
    
    async def fetch_menu_categories(self):
        """Fetch category URLs from BHX Menu API"""
        print("Fetching menu categories from BHX API...")
        
        menu_api_url = (
            f"https://apibhx.tgdd.vn/Menu/GetMenuV2?"
            f"ProvinceId={self.province_id}&WardId={self.ward_id}&StoreId={self.store_id}"
        )
        
        headers = get_headers(self.token, self.deviceid)
        headers.update({
            "accept": "application/json, text/plain, */*",
            "referer": "https://www.bachhoaxanh.com/"
        })
        
        try:
            async with self.session.get(menu_api_url, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch menu: HTTP {response.status}")
                
                data = await response.json()
                
                if data.get("code") != 0:
                    raise Exception(f"API returned error code: {data.get('code')}")
                
                categories = []
                menus = data.get("data", {}).get("menus", [])
                
                for menu in menus:
                    menu_name = menu.get("name", "")
                    childrens = menu.get("childrens", [])
                    
                    for child in childrens:
                        child_name = child.get("name", "")
                        child_url = child.get("url", "")
                        
                        if child_url:
                            # Map to English category using CATEGORIES_MAPPING
                            eng_category = CATEGORIES_MAPPING.get(child_name, "Other")
                            
                            # Skip "Other" category
                            if eng_category == "Other":
                                continue
                            
                            categories.append({
                                "vn_name": child_name,
                                "eng_category": eng_category,
                                "url": child_url,
                                "parent_menu": menu_name
                            })
                
                print(f"Found {len(categories)} categories from API")
                return categories
                
        except Exception as e:
            print(f"Error fetching menu categories: {e}")
            raise
    
    async def export_all_store_data(self, output_file: str = None):
        """Export all data for the store"""
        if not output_file:
            output_file = f"bhx_store_{self.store_id}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        print(f"Starting export for store_id: {self.store_id}")
        print(f"Province: {self.province_id}, Ward: {self.ward_id}, District: {self.district_id}")
        
        await self.init()
        
        store_data = {
            "store_id": self.store_id,
            "province_id": self.province_id,
            "ward_id": self.ward_id,
            "district_id": self.district_id,
            "exported_at": datetime.utcnow().isoformat(),
            "categories": {},
            "summary": {
                "total_products": 0,
                "categories_count": 0,
                "successful_categories": [],
                "failed_categories": []
            }
        }
        
        total_products = 0
        
        try:
            # First, get all categories from Menu API
            categories = await self.fetch_menu_categories()
            
            if not categories:
                print("No categories found from Menu API")
                return store_data
            
            # Group categories by English name
            category_groups = {}
            for cat in categories:
                eng_name = cat['eng_category']
                if eng_name not in category_groups:
                    category_groups[eng_name] = []
                category_groups[eng_name].append(cat)
            
            print(f"Found {len(category_groups)} category groups:")
            for eng_name, cats in category_groups.items():
                print(f"  {eng_name}: {len(cats)} subcategories")
            
            for eng_category, category_list in category_groups.items():
                print(f"\n=== Processing category: {eng_category} ===")
                
                category_products = []
                
                for cat_info in category_list:
                    vn_name = cat_info['vn_name']
                    url = cat_info['url']
                    parent_menu = cat_info['parent_menu']
                    
                    try:
                        products = await self.fetch_category_data(url, vn_name, eng_category)
                        if products:
                            category_products.extend(products)
                            print(f"  ✓ {vn_name}: {len(products)} products")
                        else:
                            print(f"  - {vn_name}: No products")
                    except Exception as e:
                        print(f"  ✗ Error fetching {vn_name} ({url}): {e}")
                        store_data["summary"]["failed_categories"].append({
                            'vn_name': vn_name,
                            'eng_name': eng_category,
                            'url': url,
                            'parent_menu': parent_menu,
                            'error': str(e)
                        })
                
                if category_products:
                    store_data["categories"][eng_category] = {
                        "product_count": len(category_products),
                        "products": category_products
                    }
                    total_products += len(category_products)
                    store_data["summary"]["successful_categories"].append(eng_category)
                    print(f"✓ {eng_category}: {len(category_products)} total products")
                else:
                    print(f"✗ {eng_category}: No products found")
        
        finally:
            await self.close()
        
        # Update summary
        store_data["summary"]["total_products"] = total_products
        store_data["summary"]["categories_count"] = len(store_data["categories"])
        
        # Write to JSON file
        print(f"\nWriting data to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(store_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Export completed!")
        print(f"Total products exported: {total_products}")
        print(f"Successful categories: {len(store_data['summary']['successful_categories'])}")
        print(f"Failed categories: {len(store_data['summary']['failed_categories'])}")
        print(f"Output file: {output_file}")
        
        return store_data

async def main():
    # Export data for store_id = 2546
    exporter = BHXAPIExporter(
        store_id=2546,
        province_id=3,  # TP. Hồ Chí Minh
        ward_id=0,
        district_id=0
    )
    
    start_time = time.time()
    
    try:
        store_data = await exporter.export_all_store_data()
        
        end_time = time.time()
        print(f"\nTotal export time: {(end_time - start_time):.2f} seconds")
        
    except Exception as e:
        print(f"Export failed: {e}")
        await exporter.close()

def run_export():
    """Synchronous wrapper to run the async export"""
    asyncio.run(main())

if __name__ == "__main__":
    run_export()