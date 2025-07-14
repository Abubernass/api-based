# # routes.py

# import numpy as np
# # from .utils import get_branchWiseItem_collection
# from fastapi import (
#     APIRouter,
#     Body,
#     HTTPException,
#     Query,
#     WebSocket,
#     logger,
    
# )   
# from typing import List, Optional, Dict, Any
# from bson import ObjectId
# from motor.motor_asyncio import AsyncIOMotorClient

# # from orderType.utils import get_orderType_collection
# from .models import BranchwiseItem, BranchwiseItemPost, ItemUpdate,BranchwiseItemPatch
# from stockClossin.utils import get_stockclosing_collection
# import pandas as pd
# import io
# from pymongo import ReturnDocument, UpdateOne
# from io import BytesIO
# from pathlib import Path
# from datetime import datetime, timedelta
# from fastapi.responses import FileResponse, JSONResponse
# from pydantic import BaseModel, parse_obj_as, create_model, Field
# from typing import List, Union, Optional
# from fastapi.encoders import jsonable_encoder
# from fastapi.responses import StreamingResponse
# import re
# from asyncio import sleep
# from fastapi import APIRouter, status
# import re

# from typing import List, Optional, Dict, Any
# from bson import ObjectId       
# from motor.motor_asyncio import AsyncIOMotorClient
# from .models import BranchwiseItem,ItemUpdate
# import pandas as pd8
# from fastapi import WebSocket

# from fastapi import APIRouter, Query, HTTPException
# from typing import Optional, List
# import re
# from reportlab.lib.pagesizes import A4
# from reportlab.pdfgen import canvas

# router = APIRouter()


# from fastapi.responses import StreamingResponse
# import pandas as pd
# import io




# # from pyzbar.pyzbar import decode
    
# router = APIRouter()


# connections: list[WebSocket] = []

# @router.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     connections.append(websocket)
#     try:
#         while True:
#             await websocket.receive_text()
#     except Exception:
#         connections.remove(websocket)

# # Function to send updates to all WebSocket clients
# async def send_updates(data):
#     for connection in connections:
#         await connection.send_text(data)

#         # Function to convert string 'Y'/'N' to bool
# def convert_to_bool(value):
#     if isinstance(value, str):
#         if value.lower() == "y":
#             return True
#         elif value.lower() == "n":
#             return False
#     return value






# # routes.py

# import asyncio
# import gc
# import json
# from math import ceil

# #import httpx
# import numpy as np
# #from branches.utils import get_branch_collection
# from fastapi import (
#     APIRouter,
#     HTTPException,
#     Query,
# )


# router = APIRouter()
# mongo_client = AsyncIOMotorClient("mongodb://admin:YenE580nOOUE6cDhQERP@194.233.78.90:27017/admin?appName=mongosh+2.1.1&authSource=admin&authMechanism=SCRAM-SHA-256&replicaSet=yenerp-cluster")
# db = mongo_client["admin2"]
# item_collection = db["fortest"]
# branchwiseitem_collection = db["fortest"]

# branchwise_items_collection = db["fortest"]
# variances_collection = db["variances"]
# items_collection23 = db["items"]  # Items collection




# class BranchwiseItem(BaseModel):
#     id: str = Field(..., alias="_id")
#     varianceitemCode: Optional[str]= None
#     itemName: Optional[str]= None
#     varianceName: Optional[str]= None
#     category: Optional[str]= None
#     subCategory: Optional[str]= None
#     itemGroup: Optional[str]= None
#     ItemType: Optional[Union[str, None]] = None
#     varianceName_Uom: Optional[str]= None
#     item_Uom: Optional[str]= None
#     tax: Optional[Union[int, float, None]]
#     item_Defaultprice: Optional[Union[int, float, None]]
#     variance_Defaultprice: Optional[Union[int, float, None]]
#     description: Optional[Union[str, None]] = None
#     hsnCode: Optional[Union[int, str, None]]
#     shelfLife: Optional[Union[int, float, None]]
#     reorderLevel: Optional[Union[int, float, None]]
#     itemid:Optional[str]=None
#     dynamicFields: Dict[str, Any] = {}



# @router.post("/", response_model=str,status_code=status.HTTP_201_CREATED)
# async def create_item(item: BranchwiseItemPost):
#     result = await item_collection.insert_one(item.dict())
#     return str(result.inserted_id)

# @router.get("/items/export-csv", summary="Export filtered items as CSV")
# async def export_items_csv(
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
# ):
#     filters = {}

#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}

#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))

#     # Fetch filtered data
#     data = await branchwise_items_collection.find(filters).to_list(length=None)

#     rows = [
#         {    "Category": d.get("category", ""),
#             "Subcategory": d.get("subCategory", ""),
#             "Item Name": d.get("itemName", ""),
#             "Variance": d.get("varianceName", ""),
#             "branchwise": d.get("branchwise", ""),
#             "Physical Stock": d.get("physicalStock", 0),
#             "System Stock": d.get("systemStock", 0),
           
#         }
#         for d in data
#     ]

#     df = pd.DataFrame(rows)
#     output = io.StringIO()
#     df.to_csv(output, index=False)

#     output.seek(0)
#     return StreamingResponse(
#         iter([output.getvalue()]),
#         media_type="text/csv",
#         headers={"Content-Disposition": "attachment; filename=filtered_items.csv"},
#     )
# @router.get("/items/export-pdf", summary="Export filtered items as PDF")
# async def export_items_pdf(
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
# ):
#     filters = {}

#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}

#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))

#     data = await branchwise_items_collection.find(filters).to_list(length=None)

#     # PDF generation
#     buffer = io.BytesIO()
#     p = canvas.Canvas(buffer, pagesize=A4)
#     width, height = A4

#     x = 40
#     y = height - 40
#     p.setFont("Helvetica-Bold", 10)
#     headers = ["Category", "Subcategory","Item Name", "Variance", "Branch Name", "Physical Stock", "System Stock" ]
#     for i, header in enumerate(headers):
#         p.drawString(x + i * 80, y, header)

#     p.setFont("Helvetica", 9)
#     y -= 20
#     for item in data:
#         values = [

#             item.get("category", ""),
#             item.get("subCategory", ""),
#             item.get("itemName", ""),
#             item.get("varianceName", ""),
#             item.get("branchName", ""),
#             str(item.get("physicalStock", 0)),
#             str(item.get("systemStock", 0)),
           
#         ]
#         for i, value in enumerate(values):
#             p.drawString(x + i * 80, y, str(value))
#         y -= 20
#         if y < 40:
#             p.showPage()
#             y = height - 40
#             p.setFont("Helvetica", 9)

#     p.save()
#     buffer.seek(0)

#     return StreamingResponse(
#         buffer,
#         media_type="application/pdf",
#         headers={"Content-Disposition": "attachment; filename=filtered_items.pdf"},
#     )

    

# # @router.get("/items", summary="Filtered & paginated items with filterOptions pagination")
# # async def get_items(
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(10, ge=1, le=100),

# #     category: Optional[str] = None,
# #     subCategory: Optional[str] = None,
# #     itemName: Optional[str] = None,
# #     varianceName: Optional[str] = None,

# #     categoryPage: int = Query(1, ge=1),
# #     categoryLimit: int = Query(20, ge=1, le=100),
# #     subCategoryPage: int = Query(1, ge=1),
# #     subCategoryLimit: int = Query(20, ge=1, le=100),
# #     itemNamePage: int = Query(1, ge=1),
# #     itemNameLimit: int = Query(20, ge=1, le=100),
# #     varianceNamePage: int = Query(1, ge=1),
# #     varianceNameLimit: int = Query(20, ge=1, le=100),
# # ):
# #     filters = {}

# #     def create_in_filter(values: str, field: str):
# #         terms = [v.strip() for v in values.split(",") if v.strip()]
# #         return {field: {"$in": terms}}

# #     if category:
# #         filters.update(create_in_filter(category, "category"))
# #     if subCategory:
# #         filters.update(create_in_filter(subCategory, "subCategory"))
# #     if itemName:
# #         filters.update(create_in_filter(itemName, "itemName"))
# #     if varianceName:
# #         filters.update(create_in_filter(varianceName, "varianceName"))

# #     # Step 1: Paginate the main items
# #     skip = (page - 1) * limit
# #     total = await branchwise_items_collection.count_documents(filters)
# #     paginated_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)

# #     items = [
# #         {
# #             "category": itm.get("category"),
# #             "subCategory": itm.get("subCategory"),
# #             "itemName": itm.get("itemName"),
# #             "varianceName": itm.get("varianceName"),
# #         }
# #         for itm in paginated_items
# #     ]

# #     # Step 2: Paginate each filterOption
# #     async def get_paginated_filter(field, exclude_field, page_num, limit_num):
# #         temp_filters = {k: v for k, v in filters.items() if k != exclude_field}
# #         values = await branchwise_items_collection.distinct(field, filter=temp_filters)
# #         values = sorted(filter(None, values))
# #         total = len(values)
# #         start = (page_num - 1) * limit_num
# #         end = start + limit_num
# #         return {
# #             "values": values[start:end],
# #             "total": total,
# #             "page": page_num,
# #             "limit": limit_num,
# #             "count": len(values[start:end])
# #         }

# #     filter_options = {
# #         "category": await get_paginated_filter("category", "category", categoryPage, categoryLimit),
# #         "subCategory": await get_paginated_filter("subCategory", "subCategory", subCategoryPage, subCategoryLimit),
# #         "itemName": await get_paginated_filter("itemName", "itemName", itemNamePage, itemNameLimit),
# #         "varianceName": await get_paginated_filter("varianceName", "varianceName", varianceNamePage, varianceNameLimit),
# #     }

# #     return {
# #         "filterOptions": filter_options,
# #         "filteredItems": {
# #             "total": total,
# #             "page": page,
# #             "limit": limit,
# #             "count": len(items),
# #             "items": items
# #         }
# #     }


# @router.patch("/update-item/{item_name}")
# async def update_item_by_name(item_name: str, item_update: ItemUpdate):
#     query = {"itemName": item_name}
#     update_data = {
#         "$set": {
#             key: val for key, val in item_update.updates.items() if val is not None
#         }
#     }

#     result = await branchwise_items_collection.update_one(query, update_data)
#     if result.modified_count == 0:
#         raise HTTPException(status_code=404, detail="Item not found or no update made")

#     updated_item = await branchwise_items_collection.find_one(query)
#     updated_item["branchwiseItemId"] = str(updated_item.pop("_id"))
#     return updated_item





# @router.get("/items", summary="Filtered & paginated items with filterOptions pagination")
# async def get_items(
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
#     branch: Optional[str] = None,
#     categoryPage: int = Query(1, ge=1),
#     categoryLimit: int = Query(20, ge=1, le=100),
#     subCategoryPage: int = Query(1, ge=1),
#     subCategoryLimit: int = Query(20, ge=1, le=100),
#     itemNamePage: int = Query(1, ge=1),
#     itemNameLimit: int = Query(20, ge=1, le=100),
#     varianceNamePage: int = Query(1, ge=1),
#     varianceNameLimit: int = Query(20, ge=1, le=100),
# ):
#     import logging
#     logging.basicConfig(level=logging.DEBUG)
#     logger = logging.getLogger(__name__)

#     filters = {}

#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}

#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))

#     # Step 1: Paginate the main items
#     skip = (page - 1) * limit
#     total = await branchwise_items_collection.count_documents(filters)
#     paginated_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)
#     logger.debug(f"Found {total} items matching filters: {filters}")

#     # Step 2: Load stock closing from sync Mongo (latest entry for branch)
#     stock_map = {}
#     if branch:
#         stock_closing_coll = get_stockclosing_collection()
#         latest_stock_doc = stock_closing_coll.find_one(
#             {"branch": branch},
#             sort=[("date", -1)]
#         )
#         if latest_stock_doc:
#             item_names = latest_stock_doc.get("itemName", []) or []
#             closing_qtys = latest_stock_doc.get("closingQty", []) or []
#             min_len = min(len(item_names), len(closing_qtys))
#             logger.debug(f"Latest stock doc for branch '{branch}': {item_names[:min_len]} with quantities {closing_qtys[:min_len]}")
            
#             stock_map = {
#                 item_names[i].strip().upper(): closing_qtys[i]
#                 for i in range(min_len)
#                 if item_names[i] and isinstance(closing_qtys[i], (int, float, str))
#             }
#             logger.debug(f"Stock map created: {stock_map}")
#         else:
#             logger.warning(f"No stock document found for branch: {branch}")
#     else:
#         logger.warning("No branch provided, skipping stock retrieval")

#     # Step 3: Add StockQty to response
#     items = []
#     for itm in paginated_items:
#         item_name = itm.get("itemName", "").strip().upper()
#         stock_qty = stock_map.get(item_name, "0")
#         items.append({
#             "category": itm.get("category"),
#             "subCategory": itm.get("subCategory"),
#             "itemName": itm.get("itemName"),
#             "varianceName": itm.get("varianceName"),
#             "StockQty": stock_qty
#         })
#         logger.debug(f"Item: {itm.get('itemName')} -> StockQty: {stock_qty}")

#     # Step 4: Paginate each filterOption
#     async def get_paginated_filter(field, exclude_field, page_num, limit_num):
#         temp_filters = {k: v for k, v in filters.items() if k != exclude_field}
#         values = await branchwise_items_collection.distinct(field, filter=temp_filters)
#         values = sorted(filter(None, values))
#         total = len(values)
#         start = (page_num - 1) * limit_num
#         end = start + limit_num
#         return {
#             "values": values[start:end],
#             "total": total,
#             "page": page_num,
#             "limit": limit_num,
#             "count": len(values[start:end])
#         }

#     filter_options = {
#         "category": await get_paginated_filter("category", "category", categoryPage, categoryLimit),
#         "subCategory": await get_paginated_filter("subCategory", "subCategory", subCategoryPage, subCategoryLimit),
#         "itemName": await get_paginated_filter("itemName", "itemName", itemNamePage, itemNameLimit),
#         "varianceName": await get_paginated_filter("varianceName", "varianceName", varianceNamePage, varianceNameLimit),
#     }

#     return {
#         "filterOptions": filter_options,
#         "filteredItems": {
#             "total": total,
#             "page": page,
#             "limit": limit,
#             "count": len(items),
#             "items": items
#         }
#     }



# #http://192.168.1.112:8888/fastapi/branchwiseitems/update_physicalstock/?variance_names=BUN%20PACK&variance_names=BABIES%20BUN&branch_aliases=AR&branch_aliases=SB




# # @router.get("/getbyid/{item_id}")
# # async def get_item(item_id: str):
# #     try:
# #         # Fetch the item from the MongoDB collection by ObjectId
# #         collection = get_branchWiseItem_collection()
# #         item = collection.find_one({"_id": ObjectId(item_id)})

# #         if not item:
# #             raise HTTPException(status_code=404, detail="Item not found.")

# #         # Clean the item to handle NaN values
# #         item = {k: (None if pd.isna(v) else v) for k, v in item.items()}

# #         # Create transformed_item dictionary with necessary fields
# #         item_name = item.get("itemName")
# #         variance_name = item.get("varianceName")
# #         transformed_item = {
# #             "item": {
# #                 "branchwiseItemId": str(item.get("_id")),
# #                 "itemName": item.get("itemName"),
# #                 "varianceName": item.get("varianceName"),
# #                 "category": item.get("category"),
# #                 "subcategory": item.get("subCategory"),
# #                 "itemGroup": item.get("itemGroup"),
# #                 "ItemType": item.get("ItemType"),
# #                 "itemUom": item.get("item_Uom"),
# #                 "tax": item.get("tax"),
# #                 "itemDefaultprice": item.get("item_Defaultprice"),
# #                 "description": item.get("description"),
# #                 "hsnCode": item.get("hsnCode"),
# #                 "itemgroup": None,
# #                 "status": None,
# #                 "create_item_date": None,
# #                 "updated_item_date": None,
# #                 "netPrice": None,
# #             },
# #             "variance": {},
# #         }

# #         # Create the variance info for the item
# #         variance_info = {
# #             "
# # 

# #varianceitemCode": item.get("varianceitemCode"),
# #             "varianceName": variance_name,
# #             "variance_Defaultprice": item.get("variance_Defaultprice"),
# #             "variance_Uom": item.get("variance_Uom"),
# #             "varianceStatus": "Active",  # Assuming status is active
# #             "qrCode": None,
# #             "shelfLife": item.get("shelfLife"),
# #             "reorderLevel": item.get("reorderLevel"),
# #             "orderType": {},
# #             "branchwise": {},
# #         }

# #         # Extract order types (TakeAway, Dinning, etc.)
# #         order_types = set(
# #             [
# #                 key.split("_")[0]
# #                 for key in item.keys()
# #                 if key.endswith("_Price") or key.endswith("_Enable")
# #             ]
# #         )

# #         # Populate order type info
# #         for order_type in order_types:
# #             variance_info["orderType"][order_type] = {
# #                 f"{order_type}_price": item.get(f"{order_type}_Price"),
# #                 f"{order_type}_enable": convert_to_bool(item.get(f"{order_type}_Enable")),
# #             }

# #         # Extract branch information (GH, SB, etc.)
# #         branches = set(
# #             [
# #                 key.split("_")[1]
# #                 for key in item.keys()
# #                 if key.startswith("Price_") or key.startswith("EnablePrice_") or key.startswith("branchwise_item_status_")
# #             ]
# #         )

# #         # Populate branch-wise information
# #         for branch in branches:
# #             variance_info["branchwise"][branch] = {
# #                 f"Price_{branch}": item.get(f"Price_{branch}"),
# #                 f"EnablePrice_{branch}": convert_to_bool(item.get(f"EnablePrice_{branch}")),
# #                 f"itemStatus_{branch}": convert_to_bool(item.get(f"branchwise_item_status_{branch}")),
# #                 f"availableStock_{branch}": item.get(f"availableStock_{branch}", 0),
# #             }

# #         # Add variance info to the transformed item
# #         transformed_item["variance"][variance_name] = variance_info

# #         return transformed_item

# #     except Exception as exc:
# #         logger.exception(f"Error occurred while fetching item {item_id}: {exc}")
# #         raise HTTPException(
# #             status_code=500, detail=f"An error occurred while fetching the item: {exc}"
# #         )

# # @router.get("/")
# # async def get_all_branchwise_items(
# #     branch_alias: str = Query(None, alias="branch_alias"),
# #     order_type: str = Query(None, alias="order_type"),
# # ):
# #     try:
# #         collection = get_branchWiseItem_collection()
# #         cursor = collection.find({}, {"_id": False})
# #         items = list(cursor)  # ✅ No 'await' needed
# #     except Exception as e:
# #         logger.exception("Error fetching items from MongoDB")
# #         raise HTTPException(status_code=500, detail=str(e))

# #     categories = set()
# #     result = {}

# #     for item in items:
# #         item_name = item.get("itemName", "Unnamed")
# #         cleaned_item = {
# #             k: (None if isinstance(v, float) and np.isnan(v) else v)
# #             for k, v in item.items()
# #         }

# #         if item_name not in result:
# #             result[item_name] = {"item": {}, "variance": {}}

# #         category = cleaned_item.get("category", "Uncategorized")
# #         categories.add(category)

# #         item_attributes = [
# #             "branchwiseItemId", "itemName", "category", "subcategory", "itemGroup",
# #             "ItemType", "item_Uom", "tax", "item_Defaultprice", "description",
# #             "hsnCode", "status", "create_item_date", "updated_item_date",
# #             "netPrice", "itemid",
# #         ]
# #         item_info = {k: cleaned_item[k] for k in item_attributes if k in cleaned_item}
# #         result[item_name]["item"].update(item_info)

# #         variance_attributes = [
# #             "varianceid", "varianceitemCode", "varianceName", "variance_Defaultprice",
# #             "variance_Uom", "varianceStatus", "qrCode", "selfLife", "reorderLevel",
# #         ]
# #         variance_info = {k: cleaned_item[k] for k in variance_attributes if k in cleaned_item}

# #         branchwise_nested_info = {}
# #         for key, value in cleaned_item.items():
# #             if (
# #                 key.startswith("Price_")
# #                 or key.startswith("EnablePrice_")
# #                 or key.startswith("systemStock_")
# #                 or key.startswith("physicalStock_")
# #             ):
# #                 try:
# #                     branch = key.split("_")[1].strip()
# #                     attribute = key.split("_", 1)[0].strip()
# #                 except IndexError:
# #                     continue

# #                 if branch_alias and branch.lower() != branch_alias.strip().lower():
# #                     continue

# #                 if branch not in branchwise_nested_info:
# #                     branchwise_nested_info[branch] = {}
# #                 branchwise_nested_info[branch][f"{attribute}_{branch}"] = value

# #         order_type_info = {}
# #         if "TakeAway_Price" in cleaned_item and "TakeAway_Enable" in cleaned_item:
# #             order_type_info["TakeAway"] = {
# #                 "TakeAway_Price": cleaned_item.get("TakeAway_Price"),
# #                 "TakeAway_Enable": cleaned_item.get("TakeAway_Enable"),
# #             }
# #         if "Dinning_Price" in cleaned_item and "Dinning_Enable" in cleaned_item:
# #             order_type_info["Dinning"] = {
# #                 "Dinning_Price": cleaned_item.get("Dinning_Price"),
# #                 "Dinning_Enable": cleaned_item.get("Dinning_Enable"),
# #             }

# #         if order_type:
# #             order_type_info = {
# #                 k: v for k, v in order_type_info.items() if k.lower() == order_type.lower()
# #             }

# #         if not branchwise_nested_info and not order_type_info:
# #             continue

# #         variance_name = cleaned_item.get("varianceName", "Default")
# #         if variance_name not in result[item_name]["variance"]:
# #             result[item_name]["variance"][variance_name] = variance_info
# #             result[item_name]["variance"][variance_name].update(
# #                 {
# #                     "orderType": order_type_info,
# #                     "branchwise": branchwise_nested_info,
# #                 }
# #             )

# #     if not result:
# #         raise HTTPException(status_code=404, detail="No items found")

# #     return {"categories": list(categories), "data": result}

# # @router.patch("/update_physicalstock/")
# # async def update_physical_stock(
# #     variance_names: List[str] = Query(..., description="List of variance names of the items to update"),
# #     branch_aliases: List[str] = Query(..., description="List of branch aliases like AR, SB"),
# #     new_physical_stocks: List[int] = Body(..., description="List of new physical stock counts to update")  
# # ):
# #     if len(variance_names) != len(branch_aliases) or len(variance_names) != len(new_physical_stocks):
# #         raise HTTPException(status_code=400, detail="The lengths of variance names, branch aliases, and physical stocks must match")
    
# #     update_responses = []
# #     for variance_name, branch_alias, new_physical_stock in zip(variance_names, branch_aliases, new_physical_stocks):
# #         # Update the physical stock
# #         query = {"varianceName": variance_name}
# #         update_data = {
# #             "$set": {
# #                 f"physicalStock_{branch_alias}": new_physical_stock,
# #                 f"systemStock_{branch_alias}": new_physical_stock  # Optionally update system stock
# #             }
# #         }
# #         collection=get_branchWiseItem_collection()

# #         # Perform the update operation
# #         update_result = collection.update_one(query, update_data)
        
# #         if update_result.modified_count == 0:
# #             # No document was updated
# #             update_responses.append({
# #                 "varianceName": variance_name,
# #                 "branchAlias": branch_alias,
# #                 "updatedPhysicalStock": None,
# #                 "updatedSystemStock": None,
# #                 "error": "Item not found or no update made"
# #             })
# #             continue
# #         collection=get_branchWiseItem_collection()
# #         # Retrieve the updated stock details
# #         updated_item = collection.find_one(query, {'_id': False})
        
# #         updated_stock_details = {
# #             "varianceName": variance_name,
# #             "branchAlias": branch_alias,
# #             "updatedPhysicalStock": updated_item.get(f"physicalStock_{branch_alias}"),
# #             "updatedSystemStock": updated_item.get(f"systemStock_{branch_alias}")
# #         }
        
# #         update_responses.append(updated_stock_details)

# #     return update_responses
    
    

#     # @router.get("/items", summary="Filtered & paginated items for table")
# # async def get_items(
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(10, ge=1, le=100),
# #     category: Optional[str] = None,
# #     subCategory: Optional[str] = None,
# #     itemName: Optional[str] = None,
# #     varianceName: Optional[str] = None,
# # ):
# #     filters = {}

# #     def create_in_filter(values: str, field: str):
# #         terms = [v.strip() for v in values.split(",") if v.strip()]
# #         return {field: {"$in": terms}}

# #     if category:
# #         filters.update(create_in_filter(category, "category"))
# #     if subCategory:
# #         filters.update(create_in_filter(subCategory, "subCategory"))
# #     if itemName:
# #         filters.update(create_in_filter(itemName, "itemName"))
# #     if varianceName:
# #         filters.update(create_in_filter(varianceName, "varianceName"))

# #     # 1. Get all filtered items
# #     filtered_items = await branchwise_items_collection.find(filters).to_list(length=None)
# #     total = len(filtered_items)

# #     # 2. Pagination logic
# #     start = (page - 1) * limit
# #     end = start + limit
# #     paginated_items = filtered_items[start:end]

# #     items = [
# #         {
# #             "category": itm.get("category"),
# #             "subCategory": itm.get("subCategory"),
# #             "varianceName": itm.get("varianceName"),
# #             "itemName": itm.get("itemName"),
# #         }
# #         for itm in paginated_items
# #     ]

# #     # 3. Filter dropdowns - based on current filter selections
# #     distinct_fields = ["category", "subCategory", "itemName", "varianceName"]
# #     filter_options = {}

# #     for field in distinct_fields:
# #         # Create a copy of current filters without the current field
# #         temp_filters = {k: v for k, v in filters.items() if k != field}
# #         values = await branchwise_items_collection.distinct(field, filter=temp_filters)
# #         filter_options[field] = sorted(filter(None, values))

# #     return {
# #         "filterOptions": filter_options,
# #         "filteredItems": {
# #             "total": total,
# #             "page": page,
# #             "limit": limit,
# #             "count": len(items),
# #             "items": items
# #         }
# #     } 


# # @router.get("/items", summary="Filtered & paginated items for table")
# # async def get_items(
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(10, ge=1, le=100),
# #     category: Optional[str] = None,
# #     subCategory: Optional[str] = None,
# #     itemName: Optional[str] = None,
# #     varianceName: Optional[str] = None,
# # ):
# #     filters = {}

# #     def create_in_filter(values: str, field: str):
# #         terms = [v.strip() for v in values.split(",") if v.strip()]
# #         return {field: {"$in": terms}}

# #     if category:
# #         filters.update(create_in_filter(category, "category"))
# #     if subCategory:
# #         filters.update(create_in_filter(subCategory, "subCategory"))
# #     if itemName:
# #         filters.update(create_in_filter(itemName, "itemName"))
# #     if varianceName:
# #         filters.update(create_in_filter(varianceName, "varianceName"))

# #     # 1. Get all filtered items
# #     filtered_items = await branchwise_items_collection.find(filters).to_list(length=None)
# #     total = len(filtered_items)

# #     # 2. Pagination logic
# #     start = (page - 1) * limit
# #     end = start + limit
# #     paginated_items = filtered_items[start:end]

# #     items = [
# #         {
# #             "category": itm.get("category"),
# #             "subCategory": itm.get("subCategory"),
# #             "varianceName": itm.get("varianceName"),
# #             "itemName": itm.get("itemName"),
# #         }
# #         for itm in paginated_items
# #     ]

# #     # 3. Filter dropdowns - based on current filter selections
# #     distinct_fields = ["category", "subCategory", "itemName", "varianceName"]
# #     filter_options = {}

# #     for field in distinct_fields:
# #         # Create a copy of current filters without the current field
# #         temp_filters = {k: v for k, v in filters.items() if k != field}
# #         values = await branchwise_items_collection.distinct(field, filter=temp_filters)
# #         filter_options[field] = sorted(filter(None, values))

# #     return {
# #         "filterOptions": filter_options,
# #         "filteredItems": {
# #             "total": total,
# #             "page": page,
# #             "limit": limit,
# #             "count": len(items),
# #             "items": items
# #         }
# #     } 



# # @router.get("/items", summary="Filtered & paginated items")
# # async def get_items(
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(10, ge=1, le=100),
# #     category: Optional[str] = Query(None, description="Comma-separated values"),
# #     subCategory: Optional[str] = Query(None),
# #     itemName: Optional[str] = Query(None),
# #     varianceName: Optional[str] = Query(None),
# # ):
# #     filters = {}
# #     def create_or_regex(values: str, field: str):
# #         terms = [v.strip() for v in values.split(",") if v.strip()]
# #         return {
# #             "$or": [
# #                 {field: {"$regex": f"^{re.escape(term)}", "$options": "i"}}
# #                 for term in terms
# #             ]
# #         }

# #     if category:
# #         filters = create_or_regex(category, "category")
# #     elif subCategory:
# #         filters = create_or_regex(subCategory, "subCategory")
# #     elif itemName:
# #         filters = create_or_regex(itemName, "itemName")
# #     elif varianceName:
# #         filters = create_or_regex(varianceName, "varianceName")

# #     # Apply filtering to ALL items first
# #     filtered_items = await branchwise_items_collection.find(filters).to_list(length=None)

# #     total = len(filtered_items)
# #     if total == 0:
# #         raise HTTPException(status_code=404, detail="No items found matching the filter.")

# #     # Now apply pagination
# #     start = (page - 1) * limit
# #     end = start + limit
# #     paginated_items = filtered_items[start:end]

# #     items = [
# #         {
# #             "category": itm.get("category"),
# #             "subCategory": itm.get("subCategory"),
# #             "varianceName": itm.get("varianceName"),
# #             "itemName": itm.get("itemName"),
# #         }
# #         for itm in paginated_items
# #     ]

# #     return {
# #         "total": total,
# #         "page": page,
# #         "limit": limit,
# #         "count": len(items),
# #         "filterUsed": filters,
# #         "items": items,
# #     }
    

# # @router.get("/itemssd", summary="Filtered & paginated items")
# # async def get_items(
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(10, ge=1, le=100),
# #     category: Optional[str] = Query(None),
# #     subCategory: Optional[str] = Query(None),
# #     itemName: Optional[str] = Query(None),
# #     varianceName: Optional[str] = Query(None),
# #     branch: str = Query(...),
# #     date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
# # ):
# #     filters = {}

# #     def create_or_regex(values: str, field: str):
# #         terms = [v.strip() for v in values.split(",") if v.strip()]
# #         return {
# #             "$or": [
# #                 {field: {"$regex": f"^{re.escape(term)}", "$options": "i"}}
# #                 for term in terms
# #             ]
# #         }

# #     # Apply one of the filters
# #     if category:
# #         filters = create_or_regex(category, "category")
# #     elif subCategory:
# #         filters = create_or_regex(subCategory, "subCategory")
# #     elif itemName:
# #         filters = create_or_regex(itemName, "itemName")
# #     elif varianceName:
# #         filters = create_or_regex(varianceName, "varianceName")

# #     # Get filtered items from branchwise_items_collection
# #     filtered_items = await branchwise_items_collection.find(filters).to_list(length=None)

# #     if not filtered_items:
# #         raise HTTPException(status_code=404, detail="No items found matching the filter.")

# #     # Get stockclosing collection
# #     stock_collection = get_stockclosing_collection()

# #     # Build query to fetch stock for the date (full day range)
# #     stock_query = {"branch": branch}
# #     stock_query_text = f"Stock search for branch='{branch}'"

# #     if date:
# #         try:
# #             parsed_date = datetime.fromisoformat(date)
# #             next_day = parsed_date + timedelta(days=1)
# #             stock_query["date"] = {"$gte": parsed_date, "$lt": next_day}
# #             stock_query_text += f", date between {parsed_date} and {next_day}"
# #         except ValueError:
# #             raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

# #     print("Stock query:", stock_query_text)

# #     # Try to find stock document for the date
# #     latest_stock =  stock_collection.find_one(
# #         stock_query,
# #         sort=[("date", -1)] if not date else None
# #     )

# #     # Fallback: try latest stock if no matching date entry found
# #     if not latest_stock and date:
# #         fallback_query = {"branch": branch}
# #         latest_stock =  stock_collection.find_one(fallback_query, sort=[("date", -1)])

# #     if not latest_stock:
# #         raise HTTPException(status_code=404, detail="No stock data found for the given branch/date.")

# #     stock_items = latest_stock.get("itemName", [])
# #     closing_qtys = latest_stock.get("closingQty", [])

# #     def get_closing_qty(variance: str) -> Optional[float]:
# #         try:
# #             index = stock_items.index(variance)
# #             return closing_qtys[index]
# #         except (ValueError, IndexError):
# #             return None

# #     # Pagination
# #     total = len(filtered_items)
# #     start = (page - 1) * limit
# #     end = start + limit
# #     paginated_items = filtered_items[start:end]

# #     # Build response
# #     items = [
# #         {
# #             "category": itm.get("category"),
# #             "subCategory": itm.get("subCategory"),
# #             "varianceName": itm.get("varianceName"),
# #             "itemName": itm.get("itemName"),
# #             "closingQty": get_closing_qty(itm.get("varianceName", ""))
# #         }
# #         for itm in paginated_items
# #     ]

# #     return {
# #         "total": total,
# #         "page": page,
# #         "limit": limit,
# #         "count": len(items),
# #         "branch": branch,
# #         "stockDate": latest_stock.get("date"),
# #         "items": items,
# #     }

# # @router.get("/distinct-valuesssss/{field}", summary="Get paginated distinct values")
# # async def get_distinct_field_values(
# #     field: str,
# #     page: int = Query(1, ge=1),
# #     limit: int = Query(20, ge=1, le=100)
# # ):
# #     if field not in ["itemName", "varianceName", "category", "subCategory"]:
# #         raise HTTPException(status_code=400, detail="Invalid field")

# #     values = await branchwise_items_collection.distinct(field)
# #     sorted_values = sorted(set(values))
# #     start = (page - 1) * limit
# #     end = start + limit
# #     paginated = sorted_values[start:end]

# #     return {
# #         "values": paginated,
# #         "total": len(sorted_values),
# #         "page": page,
# #         "limit": limit,
# #     }
    

# # @router.get("/distinct-values", summary="Get distinct values for filters")
# # async def get_distinct_values():
# #     categories = await branchwise_items_collection.distinct("category")
# #     sub_categories = await branchwise_items_collection.distinct("subCategory")
# #     item_names = await branchwise_items_collection.distinct("itemName")
# #     variance_names = await branchwise_items_collection.distinct("varianceName")
# #     return {
# #         "categories": sorted(categories),
# #         "subCategories": sorted(sub_categories),
# #         "itemNames": sorted(item_names),
# #         "varianceNames": sorted(variance_names),
# #     }








# Enhanced routes.py with improved stock closing integration

import asyncio
import gc
import json
from math import ceil
import logging
import numpy as np
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    status,
)
from typing import List, Optional, Dict, Any, Union
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import pandas as pd
import io
from fastapi.responses import StreamingResponse
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from fastapi.encoders import jsonable_encoder


# Import your existing utilities
from stockClossin.utils import get_stockclosing_collection
from .models import BranchwiseItem, BranchwiseItemPost, ItemUpdate, BranchwiseItemPatch

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter()

# Database connection
mongo_client = AsyncIOMotorClient("mongodb://admin:YenE580nOOUE6cDhQERP@194.233.78.90:27017/admin?appName=mongosh+2.1.1&authSource=admin&authMechanism=SCRAM-SHA-256&replicaSet=yenerp-cluster")
db = mongo_client["admin2"]
branchwise_items_collection = db["fortest"]

class EnhancedBranchwiseItem(BaseModel):
    category: Optional[str] = None
    subCategory: Optional[str] = None
    itemName: Optional[str] = None
    varianceName: Optional[str] = None
    stockQty: Optional[Union[str, int, float]] = "0"
    branch: Optional[str] = None
    lastStockUpdate: Optional[str] = None
    stockStatus: Optional[str] = "unknown"  # available, low, out_of_stock

def get_stock_mapping(branch: str, date: Optional[str] = None) -> Dict[str, Any]:
    """
    Get stock mapping for a specific branch and date.
    Returns a dictionary with item names as keys and stock info as values.
    """
    try:
        stock_closing_coll = get_stockclosing_collection()
        
        # Build query
        query = {"branch": branch}
        if date:
            try:
                target_date = datetime.fromisoformat(date)
                next_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                query["date"] = {"$gte": target_date.isoformat(), "$lt": next_day.isoformat()}
            except ValueError:
                logger.warning(f"Invalid date format: {date}")
        
        # Get the latest stock document for the branch
        latest_stock_doc = stock_closing_coll.find_one(
            query,
            sort=[("date", -1)]
        )
        
        if not latest_stock_doc:
            logger.warning(f"No stock document found for branch: {branch}")
            return {}
        
        item_names = latest_stock_doc.get("itemName", []) or []
        closing_qtys = latest_stock_doc.get("closingQty", []) or []
        require_qtys = latest_stock_doc.get("requireQty", []) or []
        
        # Ensure all lists have the same length
        min_len = min(len(item_names), len(closing_qtys))
        
        stock_map = {}
        for i in range(min_len):
            if item_names[i] and isinstance(closing_qtys[i], (int, float, str)):
                # Create normalized key for matching
                normalized_key = item_names[i].strip().upper()
                
                # Determine stock status
                closing_qty = float(closing_qtys[i]) if str(closing_qtys[i]).replace('.', '').isdigit() else 0
                require_qty = float(require_qtys[i]) if i < len(require_qtys) and str(require_qtys[i]).replace('.', '').isdigit() else 0
                
                status = "available"
                if closing_qty <= 0:
                    status = "out_of_stock"
                elif require_qty > 0 and closing_qty <= require_qty:
                    status = "low"
                
                stock_map[normalized_key] = {
                    "stockQty": closing_qtys[i],
                    "requireQty": require_qtys[i] if i < len(require_qtys) else 0,
                    "status": status,
                    "lastUpdate": latest_stock_doc.get("date"),
                    "originalItemName": item_names[i]
                }
        
        logger.debug(f"Stock map created for branch '{branch}': {len(stock_map)} items")
        return stock_map
        
    except Exception as e:
        logger.error(f"Error getting stock mapping: {e}")
        return {}

def match_item_with_stock(item: Dict, stock_map: Dict[str, Any]) -> Dict:
    """
    Match a branchwise item with stock closing data.
    Tries multiple matching strategies.
    """
    # Strategy 1: Match by itemName
    item_name = item.get("itemName", "").strip().upper()
    if item_name in stock_map:
        stock_info = stock_map[item_name]
        item["stockQty"] = stock_info["stockQty"]
        item["requireQty"] = stock_info["requireQty"]
        item["stockStatus"] = stock_info["status"]
        item["lastStockUpdate"] = stock_info["lastUpdate"]
        item["matchedBy"] = "itemName"
        return item
    
    # Strategy 2: Match by varianceName
    variance_name = item.get("varianceName", "").strip().upper()
    if variance_name in stock_map:
        stock_info = stock_map[variance_name]
        item["stockQty"] = stock_info["stockQty"]
        item["requireQty"] = stock_info["requireQty"]
        item["stockStatus"] = stock_info["status"]
        item["lastStockUpdate"] = stock_info["lastUpdate"]
        item["matchedBy"] = "varianceName"
        return item
    
    # Strategy 3: Fuzzy matching (contains)
    for stock_key, stock_info in stock_map.items():
        if item_name and item_name in stock_key:
            item["stockQty"] = stock_info["stockQty"]
            item["requireQty"] = stock_info["requireQty"]
            item["stockStatus"] = stock_info["status"]
            item["lastStockUpdate"] = stock_info["lastUpdate"]
            item["matchedBy"] = "fuzzy_itemName"
            return item
        elif variance_name and variance_name in stock_key:
            item["stockQty"] = stock_info["stockQty"]
            item["requireQty"] = stock_info["requireQty"]
            item["stockStatus"] = stock_info["status"]
            item["lastStockUpdate"] = stock_info["lastUpdate"]
            item["matchedBy"] = "fuzzy_varianceName"
            return item
    
    # No match found
    item["stockQty"] = "0"
    item["requireQty"] = "0"
    item["stockStatus"] = "unknown"
    item["lastStockUpdate"] = None
    item["matchedBy"] = "none"
    return item


# @router.get("/branchwise-items/closing-quantity", summary="Get closing quantities for branchwise items")
# async def get_branchwise_items_closing_quantity(
#     branch: str = Query(..., description="Branch name is required"),
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
#     date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
#     onlyWithStock: bool = Query(False, description="Only return items with stock > 0"),
# ):
#     """
#     Get branchwise items with their closing quantities from stock closing data.
#     This endpoint focuses specifically on showing closing quantities for branchwise items.
#     """
    
#     # Build filters for branchwise items
#     filters = {}
    
#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}
    
#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))
    
#     # Get stock closing data for the branch
#     stock_closing_coll = get_stockclosing_collection()
    
#     # Build stock closing query
#     stock_query = {"branch": branch}
#     if date:
#         try:
#             target_date = datetime.fromisoformat(date)
#             next_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
#             stock_query["date"] = {"$gte": target_date.isoformat(), "$lt": next_day.isoformat()}
#         except ValueError:
#             logger.warning(f"Invalid date format: {date}")
    
#     # Get the latest stock closing document
#     latest_stock_doc = stock_closing_coll.find_one(
#         stock_query,
#         sort=[("date", -1)]
#     )
    
#     if not latest_stock_doc:
#         return {
#             "items": [],
#             "pagination": {
#                 "total": 0,
#                 "page": page,
#                 "limit": limit,
#                 "count": 0
#             },
#             "message": f"No stock closing data found for branch: {branch}",
#             "branch": branch,
#             "date": date
#         }
    
#     # Create stock mapping
#     item_names = latest_stock_doc.get("itemName", []) or []
#     closing_qtys = latest_stock_doc.get("closingQty", []) or []
#     require_qtys = latest_stock_doc.get("requireQty", []) or []
    
#     # Create a comprehensive stock mapping
#     stock_map = {}
#     min_len = min(len(item_names), len(closing_qtys))
    
#     for i in range(min_len):
#         if item_names[i]:
#             # Create multiple keys for better matching
#             original_name = item_names[i].strip()
#             normalized_name = original_name.upper()
            
#             closing_qty = closing_qtys[i]
#             require_qty = require_qtys[i] if i < len(require_qtys) else 0
            
#             # Convert to numeric for comparison
#             try:
#                 closing_qty_num = float(closing_qty) if str(closing_qty).replace('.', '').replace('-', '').isdigit() else 0
#                 require_qty_num = float(require_qty) if str(require_qty).replace('.', '').replace('-', '').isdigit() else 0
#             except (ValueError, TypeError):
#                 closing_qty_num = 0
#                 require_qty_num = 0
            
#             stock_info = {
#                 "closingQty": closing_qty,
#                 "requireQty": require_qty,
#                 "closingQtyNumeric": closing_qty_num,
#                 "requireQtyNumeric": require_qty_num,
#                 "originalName": original_name,
#                 "stockDate": latest_stock_doc.get("date"),
#                 "postedBy": latest_stock_doc.get("postedBy")
#             }
            
#             # Store with multiple keys for matching
#             stock_map[normalized_name] = stock_info
#             stock_map[original_name] = stock_info
    
#     # Get paginated branchwise items
#     skip = (page - 1) * limit
#     total = await branchwise_items_collection.count_documents(filters)
#     paginated_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)
    
#     # Match branchwise items with closing quantities
#     items_with_closing_qty = []
    
#     for item in paginated_items:
#         item_name = item.get("itemName", "").strip()
#         variance_name = item.get("varianceName", "").strip()
        
#         # Create response item
#         result_item = {
#             "branchwiseItemId": str(item.get("_id", "")),
#             "category": item.get("category"),
#             "subCategory": item.get("subCategory"),
#             "itemName": item_name,
#             "varianceName": variance_name,
#             "closingQty": None,
#             "requireQty": None,
#             "closingQtyNumeric": 0,
#             "requireQtyNumeric": 0,
#             "stockDate": None,
#             "postedBy": None,
#             "matchedBy": None,
#             "hasStock": False
#         }
        
#         # Try to match with stock closing data
#         stock_info = None
        
#         # Strategy 1: Match by exact itemName
#         if item_name and item_name in stock_map:
#             stock_info = stock_map[item_name]
#             result_item["matchedBy"] = "itemName_exact"
        
#         # Strategy 2: Match by exact varianceName
#         elif variance_name and variance_name in stock_map:
#             stock_info = stock_map[variance_name]
#             result_item["matchedBy"] = "varianceName_exact"
        
#         # Strategy 3: Match by normalized itemName
#         elif item_name and item_name.upper() in stock_map:
#             stock_info = stock_map[item_name.upper()]
#             result_item["matchedBy"] = "itemName_normalized"
        
#         # Strategy 4: Match by normalized varianceName
#         elif variance_name and variance_name.upper() in stock_map:
#             stock_info = stock_map[variance_name.upper()]
#             result_item["matchedBy"] = "varianceName_normalized"
        
#         # Strategy 5: Fuzzy matching
#         else:
#             for stock_key, stock_data in stock_map.items():
#                 if (item_name and item_name.upper() in stock_key.upper()) or \
#                    (variance_name and variance_name.upper() in stock_key.upper()):
#                     stock_info = stock_data
#                     result_item["matchedBy"] = "fuzzy_match"
#                     break
        
#         # Apply stock information if found
#         if stock_info:
#             result_item.update({
#                 "closingQty": stock_info["closingQty"],
#                 "requireQty": stock_info["requireQty"],
#                 "closingQtyNumeric": stock_info["closingQtyNumeric"],
#                 "requireQtyNumeric": stock_info["requireQtyNumeric"],
#                 "stockDate": stock_info["stockDate"],
#                 "postedBy": stock_info["postedBy"],
#                 "hasStock": stock_info["closingQtyNumeric"] > 0
#             })
        
#         # Filter by stock if requested
#         if onlyWithStock and not result_item["hasStock"]:
#             continue
        
#         items_with_closing_qty.append(result_item)
    
#     # Calculate summary statistics
#     total_items = len(items_with_closing_qty)
#     items_with_stock = len([item for item in items_with_closing_qty if item["hasStock"]])
#     matched_items = len([item for item in items_with_closing_qty if item["matchedBy"]])
    
#     return {
#         "items": items_with_closing_qty,
#         "pagination": {
#             "total": total,
#             "page": page,
#             "limit": limit,
#             "count": total_items
#         },
#         "summary": {
#             "totalItemsReturned": total_items,
#             "itemsWithStock": items_with_stock,
#             "itemsWithoutStock": total_items - items_with_stock,
#             "matchedWithStockClosing": matched_items,
#             "unmatchedItems": total_items - matched_items
#         },
#         "stockClosingInfo": {
#             "branch": branch,
#             "date": date,
#             "stockDate": latest_stock_doc.get("date"),
#             "totalStockItems": len(set(item_names)),
#             "postedBy": latest_stock_doc.get("postedBy")
#         }
#     }

# @router.get("/items", summary="Enhanced items endpoint with stock integration")
# async def get_items(
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
#     branch: Optional[str] = None,
#     includeStock: bool = Query(True, description="Include stock quantities"),
#     categoryPage: int = Query(1, ge=1),
#     categoryLimit: int = Query(20, ge=1, le=100),
#     subCategoryPage: int = Query(1, ge=1),
#     subCategoryLimit: int = Query(20, ge=1, le=100),
#     itemNamePage: int = Query(1, ge=1),
#     itemNameLimit: int = Query(20, ge=1, le=100),
#     varianceNamePage: int = Query(1, ge=1),
#     varianceNameLimit: int = Query(20, ge=1, le=100),
# ):
#     """
#     Enhanced version of your existing get_items endpoint with improved stock integration.
#     """
    
#     filters = {}
    
#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}
    
#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))
    
#     # Get paginated items
#     skip = (page - 1) * limit
#     total = await branchwise_items_collection.count_documents(filters)
#     paginated_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)
    
#     # Get stock mapping if branch is provided and stock is requested
#     stock_map = {}
#     if branch and includeStock:
#         stock_map = get_stock_mapping(branch)
    
#     # Process items
#     items = []
#     for item in paginated_items:
#         processed_item = {
#             "category": item.get("category"),
#             "subCategory": item.get("subCategory"),
#             "itemName": item.get("itemName"),
#             "varianceName": item.get("varianceName"),
#         }
        
#         # Add stock information if available
#         if includeStock and stock_map:
#             processed_item = match_item_with_stock(processed_item, stock_map)
#         elif includeStock:
#             processed_item["stockQty"] = "0"
#             processed_item["stockStatus"] = "unknown"
        
#         items.append(processed_item)
    
#     # Get filter options (your existing code)
#     async def get_paginated_filter(field, exclude_field, page_num, limit_num):
#         temp_filters = {k: v for k, v in filters.items() if k != exclude_field}
#         values = await branchwise_items_collection.distinct(field, filter=temp_filters)
#         values = sorted(filter(None, values))
#         total = len(values)
#         start = (page_num - 1) * limit_num
#         end = start + limit_num
#         return {
#             "values": values[start:end],
#             "total": total,
#             "page": page_num,
#             "limit": limit_num,
#             "count": len(values[start:end])
#         }
    
#     filter_options = {
#         "category": await get_paginated_filter("category", "category", categoryPage, categoryLimit),
#         "subCategory": await get_paginated_filter("subCategory", "subCategory", subCategoryPage, subCategoryLimit),
#         "itemName": await get_paginated_filter("itemName", "itemName", itemNamePage, itemNameLimit),
#         "varianceName": await get_paginated_filter("varianceName", "varianceName", varianceNamePage, varianceNameLimit),
#     }
    
#     return {
#         "filterOptions": filter_options,
#         "filteredItems": {
#             "total": total,
#             "page": page,
#             "limit": limit,
#             "count": len(items),
#             "items": items
#         },
#         "stockInfo": {
#             "branch": branch,
#             "includeStock": includeStock,
#             "stockItemsFound": len(stock_map) if stock_map else 0
#         }
#     }
    


# def convert_objectid(item: Dict) -> Dict:
#     return {
#         k: str(v) if isinstance(v, ObjectId) else v
#         for k, v in item.items()
#     }

@router.get("/items", summary="Minimal filtered items with stock")
async def get_items(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    branch: str = Query(...),
    category: Optional[str] = None,
    subCategory: Optional[str] = None,
    itemName: Optional[str] = None,
    varianceName: Optional[str] = None,
    includeStock: bool = Query(True),
    onlyWithStock: bool = Query(False),
    categoryPage: int = Query(1),
    categoryLimit: int = Query(20),
    subCategoryPage: int = Query(1),
    subCategoryLimit: int = Query(20),
    itemNamePage: int = Query(1),
    itemNameLimit: int = Query(20),
    varianceNamePage: int = Query(1),
    varianceNameLimit: int = Query(20),
):
    filters = {}

    def create_in_filter(val: Optional[str], field: str):
        if val:
            return {field: {"$in": [v.strip() for v in val.split(",") if v.strip()]}}
        return {}

    filters.update(create_in_filter(category, "category"))
    filters.update(create_in_filter(subCategory, "subCategory"))
    filters.update(create_in_filter(itemName, "itemName"))
    filters.update(create_in_filter(varianceName, "varianceName"))

    # Pagination
    skip = (page - 1) * limit
    total = await branchwise_items_collection.count_documents(filters)
    raw_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)

    # Stock Map
    stock_map = {}
    if includeStock:
        stock_doc =  get_stockclosing_collection().find_one(
            {"branch": branch}, sort=[("date", -1)]
        )
        if stock_doc:
            for i, name in enumerate(stock_doc.get("itemName", [])):
                qty = stock_doc.get("closingQty", [])[i]
                key = name.strip().upper()
                status = "available"
                q = float(qty) if str(qty).replace('.', '').isdigit() else 0
                if q <= 0:
                    status = "out_of_stock"
                stock_map[key] = {"qty": qty, "status": status}

    # Final Items
    items = []
    for item in raw_items:
        result = {
            "category": item.get("category"),
            "subCategory": item.get("subCategory"),
            "itemName": item.get("itemName"),
            "varianceName": item.get("varianceName"),
            "stockQty": "0",
            "stockStatus": "unknown"
        }
        key1 = item.get("itemName", "").strip().upper()
        key2 = item.get("varianceName", "").strip().upper()
        stock_info = stock_map.get(key1) or stock_map.get(key2)

        if stock_info:
            result["stockQty"] = stock_info["qty"]
            result["stockStatus"] = stock_info["status"]

        if onlyWithStock and result["stockStatus"] != "available":
         continue

        items.append(result)

    # Filter dropdowns
    async def filter_field(field: str, exclude: str, page: int, limit: int):
        f = {k: v for k, v in filters.items() if k != exclude}
        values = await branchwise_items_collection.distinct(field, filter=f)
        values = sorted(filter(None, values))
        start = (page - 1) * limit
        return {
            "values": values[start:start + limit],
            "total": len(values),
            "page": page,
            "limit": limit,
            "count": len(values[start:start + limit])
        }

    return {
        "filterOptions": {
            "category": await filter_field("category", "category", categoryPage, categoryLimit),
            "subCategory": await filter_field("subCategory", "subCategory", subCategoryPage, subCategoryLimit),
            "itemName": await filter_field("itemName", "itemName", itemNamePage, itemNameLimit),
            "varianceName": await filter_field("varianceName", "varianceName", varianceNamePage, varianceNameLimit),
        },
        "filteredItems": {
            "total": total,
            "page": page,
            "limit": limit,
            "count": len(items),
            "items": items
        }
    }

    # @router.get("/items-with-stock", summary="Get items with stock quantities")
# async def get_items_with_stock(
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
#     branch: str = Query(..., description="Branch name is required"),
#     date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
#     category: Optional[str] = None,
#     subCategory: Optional[str] = None,
#     itemName: Optional[str] = None,
#     varianceName: Optional[str] = None,
#     stockStatus: Optional[str] = Query(None, description="Filter by stock status: available, low, out_of_stock"),
#     includeStockDetails: bool = Query(True, description="Include detailed stock information"),
# ):
#     """
#     Enhanced endpoint to get branchwise items with stock closing quantities.
#     Supports multiple matching strategies and detailed stock information.
#     """
    
#     # Build filters for branchwise items
#     filters = {}
    
#     def create_in_filter(values: str, field: str):
#         terms = [v.strip() for v in values.split(",") if v.strip()]
#         return {field: {"$in": terms}}
    
#     if category:
#         filters.update(create_in_filter(category, "category"))
#     if subCategory:
#         filters.update(create_in_filter(subCategory, "subCategory"))
#     if itemName:
#         filters.update(create_in_filter(itemName, "itemName"))
#     if varianceName:
#         filters.update(create_in_filter(varianceName, "varianceName"))
    
#     # Get stock mapping for the branch
#     stock_map = get_stock_mapping(branch, date)
    
#     # Get paginated items
#     skip = (page - 1) * limit
#     total = await branchwise_items_collection.count_documents(filters)
#     paginated_items = await branchwise_items_collection.find(filters).skip(skip).limit(limit).to_list(length=limit)
    
#     # Match items with stock data
#     enriched_items = []
#     for item in paginated_items:
#         enriched_item = match_item_with_stock(item, stock_map)
        
#         # Apply stock status filter if specified
#         if stockStatus and enriched_item.get("stockStatus") != stockStatus:
#             continue
            
#         # Add branch info
#         enriched_item["branch"] = branch
        
#         # Clean up the response
#         if not includeStockDetails:
#             # Remove detailed stock info if not requested
#             keys_to_remove = ["requireQty", "matchedBy", "lastStockUpdate"]
#             for key in keys_to_remove:
#                 enriched_item.pop(key, None)
        
#         enriched_items.append(enriched_item)
    
#     # Calculate stock statistics
#     stock_stats = {
#         "totalItems": len(enriched_items),
#         "matchedItems": len([item for item in enriched_items if item.get("matchedBy") != "none"]),
#         "unmatchedItems": len([item for item in enriched_items if item.get("matchedBy") == "none"]),
#         "stockStatus": {
#             "available": len([item for item in enriched_items if item.get("stockStatus") == "available"]),
#             "low": len([item for item in enriched_items if item.get("stockStatus") == "low"]),
#             "out_of_stock": len([item for item in enriched_items if item.get("stockStatus") == "out_of_stock"]),
#             "unknown": len([item for item in enriched_items if item.get("stockStatus") == "unknown"])
#         }
#     }
    
#     return {
#         "items": enriched_items,
#         "pagination": {
#             "total": total,
#             "page": page,
#             "limit": limit,
#             "count": len(enriched_items)
#         },
#         "stockStats": stock_stats,
#         "branch": branch,
#         "date": date,
#         "lastStockUpdate": stock_map.get(list(stock_map.keys())[0], {}).get("lastUpdate") if stock_map else None
#     }

# @router.get("/stock-comparison", summary="Compare stock closing with branchwise items")
# async def get_stock_comparison(
#     branch: str = Query(..., description="Branch name is required"),
#     date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
#     showUnmatched: bool = Query(True, description="Show unmatched items from both sides"),
# ):
#     """
#     Compare stock closing data with branchwise items to identify matches and mismatches.
#     """
    
#     # Get stock mapping
#     stock_map = get_stock_mapping(branch, date)
    
#     # Get all branchwise items
#     all_items = await branchwise_items_collection.find({}).to_list(length=None)
    
#     # Create item mapping for reverse lookup
#     item_map = {}
#     for item in all_items:
#         item_name = item.get("itemName", "").strip().upper()
#         variance_name = item.get("varianceName", "").strip().upper()
        
#         if item_name:
#             item_map[item_name] = item
#         if variance_name and variance_name != item_name:
#             item_map[variance_name] = item
    
#     # Find matches and mismatches
#     matched_items = []
#     stock_only_items = []
#     items_only = []
    
#     # Process stock items
#     for stock_key, stock_info in stock_map.items():
#         if stock_key in item_map:
#             # Match found
#             item = item_map[stock_key].copy()
#             item["stockQty"] = stock_info["stockQty"]
#             item["requireQty"] = stock_info["requireQty"]
#             item["stockStatus"] = stock_info["status"]
#             item["lastStockUpdate"] = stock_info["lastUpdate"]
#             item["matchedBy"] = "exact"
#             matched_items.append(item)
#         else:
#             # Stock item without corresponding branchwise item
#             if showUnmatched:
#                 stock_only_items.append({
#                     "originalItemName": stock_info["originalItemName"],
#                     "stockQty": stock_info["stockQty"],
#                     "requireQty": stock_info["requireQty"],
#                     "stockStatus": stock_info["status"],
#                     "lastStockUpdate": stock_info["lastUpdate"]
#                 })
    
#     # Find branchwise items without stock data
#     if showUnmatched:
#         for item in all_items:
#             item_name = item.get("itemName", "").strip().upper()
#             variance_name = item.get("varianceName", "").strip().upper()
            
#             if item_name not in stock_map and variance_name not in stock_map:
#                 item_copy = item.copy()
#                 item_copy["stockQty"] = "0"
#                 item_copy["stockStatus"] = "unknown"
#                 items_only.append(item_copy)
    
#     comparison_stats = {
#         "totalStockItems": len(stock_map),
#         "totalBranchwiseItems": len(all_items),
#         "matchedItems": len(matched_items),
#         "stockOnlyItems": len(stock_only_items),
#         "branchwiseOnlyItems": len(items_only),
#         "matchPercentage": (len(matched_items) / len(stock_map) * 100) if stock_map else 0
#     }
    
#     return {
#         "comparison": {
#             "matched": matched_items,
#             "stockOnly": stock_only_items if showUnmatched else [],
#             "branchwiseOnly": items_only if showUnmatched else []
#         },
#         "stats": comparison_stats,
#         "branch": branch,
#         "date": date
#     }