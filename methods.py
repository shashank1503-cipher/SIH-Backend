from fastapi import APIRouter, HTTPException, File, UploadFile, Form, Request

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

@router.post('/delete')
async def delete(req: Request):
    data = await req.json()
    index = data['index']

    print(index)
    try:
        data = await client.indices.delete(index=index)
        # print(data)
        return {"status": 1}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/doc/{index_name}/{doc_id}')
def getDoc(index_name,doc_id):
    print(doc_id)
    print(index_name)
    data = client.get(index=index_name, id=doc_id)

    final_data = {
        "index": data["_index"],
        "id": data['_id'],
        "source": data["_source"],
    }

    print(final_data)

    return {"data": final_data}