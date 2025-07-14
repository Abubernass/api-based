from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, status, Query
from bson import ObjectId

# Assuming you have these utility functions to get MongoDB collections
from .utils import get_stockclosing_collection
from .models import StockclossingPost, Stockclossing, get_iso_datetime  # Update the import from Stickercount to Stockclossing

router = APIRouter()

# FastAPI route
@router.post("/", response_model=Stockclossing, status_code=status.HTTP_201_CREATED)
async def create_stockclosing(stockclosing: StockclossingPost):
    new_stockclosing_data = stockclosing.model_dump()

    # Assign current date and time in ISO 8601 format if 'date' is not provided
    if not new_stockclosing_data.get('date'):
        new_stockclosing_data['date'] = get_iso_datetime()

    # Convert date string (in 'DD-MM-YYYY') to ISO 8601 datetime before inserting into MongoDB
    if 'date' in new_stockclosing_data and isinstance(new_stockclosing_data['date'], str):
        try:
            # If provided in 'DD-MM-YYYY', parse and convert to ISO 8601 format
            new_stockclosing_data['date'] = datetime.strptime(new_stockclosing_data['date'], '%d-%m-%Y').isoformat()
        except ValueError:
            # If parsing fails, ensure it's already in ISO 8601 format
            try:
                datetime.fromisoformat(new_stockclosing_data['date'])
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'DD-MM-YYYY' or ISO 8601 format.")

    # Insert the data into MongoDB
    result = get_stockclosing_collection().insert_one(new_stockclosing_data)

    # Create the Stockclosing response model with the inserted ID
    return Stockclossing(
        stockclosingId=str(result.inserted_id),  # Assign the inserted ID
        itemName=new_stockclosing_data.get('itemName'),
        date=new_stockclosing_data.get('date'),
        branch=new_stockclosing_data.get('branch'),
        closingQty=new_stockclosing_data.get('closingQty'),
        requireQty=new_stockclosing_data.get('requireQty'),
        postedBy=new_stockclosing_data.get('postedBy'),
    )



class ItemStock(BaseModel):
    itemName: str
    stockQty: str

@router.get("/stock-summary", response_model=List[ItemStock])
async def get_stock_summary(
    branch: Optional[str] = Query(None),
    date: Optional[str] = Query(None)  # format: YYYY-MM-DD
):
    query = {}

    if branch:
        query["branch"] = branch

    if date:
        try:
            # Convert to ISO datetime format range for filtering
            target_date = datetime.fromisoformat(date)
            next_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            query["date"] = {"$gte": target_date.isoformat(), "$lt": next_day.isoformat()}
        except ValueError:
            return []

    document =  get_stockclosing_collection().find_one(query)
    if not document:
        return []

    item_names = document.get("itemName", [])
    closing_qtys = document.get("closingQty", [])

    return [
        {"itemName": name, "stockQty": qty}
        for name, qty in zip(item_names, closing_qtys)
    ]

@router.get("/{stockclosing_id}", response_model=Stockclossing)
async def get_stockclosing_by_id(stockclosing_id: str):
    stockclosing_data = get_stockclosing_collection().find_one({"_id": ObjectId(stockclosing_id)})
    if stockclosing_data:
        return Stockclossing(**convert_document(stockclosing_data))
    else:
        raise HTTPException(status_code=404, detail="Stockclosing entry not found")

@router.patch("/{stockclosing_id}", response_model=Stockclossing)
async def patch_stockclosing(stockclosing_id: str, stockclosing_patch: StockclossingPost):
    existing_stockclosing_data = get_stockclosing_collection().find_one({"_id": ObjectId(stockclosing_id)})
    if not existing_stockclosing_data:
        raise HTTPException(status_code=404, detail="Stockclosing entry not found")

    # Update the document with only the fields that are set in stockclosing_patch
    updated_fields = {k: v for k, v in stockclosing_patch.dict(exclude_unset=True).items() if v is not None}

    # Convert date string to datetime if present
    if 'date' in updated_fields and updated_fields['date']:
        updated_fields['date'] = datetime.strptime(updated_fields['date'], '%d-%m-%Y')

    if updated_fields:
        result = get_stockclosing_collection().update_one({"_id": ObjectId(stockclosing_id)}, {"$set": updated_fields})
        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update Stockclosing entry")

    # Fetch the updated document to return as response
    updated_stockclosing = get_stockclosing_collection().find_one({"_id": ObjectId(stockclosing_id)})
    if updated_stockclosing:
        return Stockclossing(**convert_document(updated_stockclosing))
    else:
        raise HTTPException(status_code=404, detail="Updated Stockclosing entry not found")

# @router.delete("/{stockclosing_id}")
# async def delete_stockclosing(stockclosing_id: str):
#     result = get_stockclosing_collection().delete_one({"_id": ObjectId(stockclosing_id)})
#     if result.deleted_count == 0:
#         raise HTTPException(status_code=404, detail="Stockclosing entry not found")
#     return {"message": "Stockclosing deleted successfully"}

def convert_document(document):
    document['stockclosingId'] = str(document.pop('_id'))
    return document
