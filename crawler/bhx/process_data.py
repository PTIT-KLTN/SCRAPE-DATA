from typing import List
from datetime import datetime
from pymongo import UpdateOne
from crawler.process_data.process import *

async def extract_best_price(product: dict) -> dict:
    
    base_price_info = product.get("productPrices", [])
    campaign_info = product.get("lstCampaingInfo", [])
    
    def build_result(info: dict):
        return {
            "price": info.get("price"),
            "sys_price": info.get("sysPrice"),
            "discount_percent": info.get("discountPercent"),
            "date_begin": info.get("startTime") or info.get("poDate"),
            "date_end": info.get("dueTime") or info.get("poDate"),
        }
    
    # Ưu tiên giá khuyến mãi
    if campaign_info:
        return build_result(campaign_info[0].get("productPrice", {}))
    
    # Giá gốc
    if base_price_info:
        return build_result(base_price_info[0])
    
    # Không có thông tin giá
    return {
        "price": None,
        "sys_price": None,
        "discount_percent": None,
        "date_begin": None,
        "date_end": None,
    }

async def process_product_data(raw: List[dict], category: str, store_id: int, db) -> List[UpdateOne]:

    coll_name = category.replace(" ", "_").lower()

    # nếu coll_name chưa có trong db
    if coll_name not in await db.list_collection_names():
        await db.create_collection(coll_name)

    coll = db[coll_name]
    ops = []
    
    for prod in raw:
        sku = prod.get("id")
        if not sku:
            continue

        name = prod.get("name", "")

        # 1. Tìm doc bất kỳ có cùng 'name' để tái sử dụng name_en và token_ngrams
        trans_doc = await coll.find_one(
            {"name": name},
            {"name_en": 1, "token_ngrams": 1}
        )

        price_info = await extract_best_price(prod)
        unit_info  = await process_unit_and_net_value(prod)

        # 2. Khởi tạo document upsert
        upd = {
            "sku": sku,
            "store_id": store_id,
            "category": category,
            "url": f"https://www.bachhoaxanh.com{prod.get('url', '')}",
            "image": prod.get("avatar", ""),
            "promotion": prod.get("promotionText", ""),
            "crawled_at": datetime.utcnow().isoformat(),
            "chain": "BHX",
            **unit_info,
            **price_info,
        }

        if trans_doc:
            # Nếu có sản phẩm cùng name, giữ nguyên bản dịch và ngrams
            upd.update({
                "name":           name,
                "name_en":        trans_doc["name_en"],
                "token_ngrams":   trans_doc["token_ngrams"],
            })
        else:
            # Chưa có → dịch mới rồi sinh ngrams
            name_en = await translate_vi2en(name)
            ngram   = await generate_token_ngrams(name_en, 2)
            upd.update({
                "name":         name,
                "name_en":      name_en,
                "token_ngrams": ngram,
            })

        # 3. Upsert theo (sku, store_id)
        filt = {"sku": sku, "store_id": store_id}
        ops.append(UpdateOne(filt, {"$set": upd}, upsert=True))

    return ops
