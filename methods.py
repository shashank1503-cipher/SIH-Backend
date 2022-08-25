from fastapi import APIRouter, HTTPException, File, UploadFile, Form, Request

import configs
from utils import convert_bytes

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

@router.get('/stats')
async def stats():
    cluster_health = client.cluster.health()
    data = {}
    data['cluster_health'] = cluster_health
    total_documents = client.count(index="*")['count']
    data['total_documents'] = total_documents
    total_images = client.count(index="*", body={
        "query": {
            "match": {
                "doc_type": "image"
            }
        }
    }
)['count']
    total_audio = client.count(index="*", body={
        "query": {
            "match": {
                "doc_type": "sound"
            }
        }
    }
)['count']
    total_text = client.count(index="*", body={
        "query": {
            "match": {
                "doc_type": "text"
            }
        }
    }
)['count']
    total_pdf = client.count(index="*", body={
        "query": {
            "match": {
                "doc_type": "pdf"
            }
        }
    }
)['count']
    total_docx = client.count(index="*", body={
        "query": {
            "match": {
                "doc_type": "doc"
            }   
        }
    }
)['count']
    data['total_images'] = total_images
    data['total_audio'] = total_audio
    data['total_text'] = total_text
    data['total_docx'] = total_docx
    data['total_pdf'] = total_pdf
    data['total_indexes'] = len(client.indices.get_alias(index="*").keys())
    data['memory_usage'] = client.cluster.stats()['nodes']['fs']
    data['memory_usage_pretty'] = {
        'total':convert_bytes(data['memory_usage']['total_in_bytes']),
        'free': convert_bytes(data['memory_usage']['available_in_bytes']),
        'available': convert_bytes(data['memory_usage']['available_in_bytes']),
    }

    return {'data':data }


@router.post('/delete')
async def delete(req: Request):
    data = await req.json()
    index = data['index']

    print(index)
    try:
        data = client.options(ignore_status=[400,404]).indices.delete(index=index)
        print(data)
        return {"status": 1}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/doc/{index_name}/{doc_id}')
def getDoc(index_name,doc_id):
    print(doc_id)
    print(index_name)
    data = client.get(index=index_name, id=doc_id)
    print(data)
    final_data = {
        "index": data["_index"],
        "id": data['_id'],
        "source": data["_source"],
    }

    print(final_data)

    return {"data": final_data}