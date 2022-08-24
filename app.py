from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI()


import add_data
import configs
import methods


app = FastAPI()

client = configs.client


origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(add_data.router, prefix="/add_data")
app.include_router(methods.router, prefix='/get')

@app.get("/")
async def get_routes():
    routes = {
        "Search API":"/search",
        "Add Data": "/add_data",
        "Swagger_Docs": "/docs",
    }
    return routes

@app.get("/search")
async def search(q: str, page: Optional[int] = 1, per_page: Optional[int] = 10, filters: Optional[str] = ""):
    result = {}
    print(filters)
    filters = json.loads(filters)
    print(filters)
    print(q)
    if not q:
            raise HTTPException(status_code=400, detail="Query not found")
    if len(filters['index']) > 0:
        if not client.indices.exists(index=filters['index']):
            raise HTTPException(status_code=400, detail="Index not found")
    try:
        query = {"from":(page-1)*per_page,"size":per_page,"query": {}}
        print("doc length", len(filters['doc']))
        if len(filters['doc']) > 0:
            print(query)
            query['query']['bool'] = {}
            query['query']['bool']['must'] = [{
                'bool': {
                    'should': []
                }},
                {
                    'query_string': {'query': q}
                }
            ]
            print(query)
            for doc in filters['doc']:
                print('DOC TYPE', doc)
                query['query']['bool']['must'][0]['bool']['should'].append({
                    
                    'match': {
                        'doc_type': doc
                    }
                    
                })
        else:
            query = {"from":(page-1)*per_page,"size":per_page,"query": {'query_string': {'query': q}}}

        print(query)
            

        if len(filters['index']) > 0:
            resp = client.search(body=query,index=filters['index'])
        else:
            resp = client.search(body=query)
        data = resp["hits"]["hits"]
        result['data'] = data
        result['meta'] = {'total':resp["hits"]["total"]["value"]}
        print(result["meta"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result

