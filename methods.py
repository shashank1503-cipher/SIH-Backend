from fastapi import APIRouter, HTTPException, File, UploadFile, Form

import configs

client = configs.client

router = APIRouter()

@router.get('/indices')
async def indices():
    
    data = client.indices.get_alias(index="*")
    final_data = []
    for key in data.keys():
        if(key[0] == '.'):
            pass
        else:
            final_data.append(key)

    print(final_data)
    return {'data': final_data}


@router.get('/index')
async def index(q: str):

    if not client.indices.exists(index=q):
        raise HTTPException(status_code=404, detail="Index not found")

    data = client.indices.get(index=q)
    
    return {"data": data[q]}

@router.get('/count')
async def count(q:str):

    if not client.indices.exists(index=q):
        raise HTTPException(status_code=404, detail="Index not found")

    data = client.count(index=q)
    print(data)

    return {'count': data['count']}