from typing import Optional
from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
import json
from elasticsearch import Elasticsearch, helpers
from PyPDF2 import PdfReader
import validators
import add_data
import configs


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

@app.get("/")
async def get_routes():
    routes = {
        "Search API":"/search",
        "Add Data": "/add_data",
        "Swagger_Docs": "/docs"
    }
    return routes

@app.get("/search")
async def search(q: str, page: Optional[int] = 1, per_page: Optional[int] = 10):
    result = {}
    try:
        resp = client.search(body={"from":(page-1)*per_page,"size":per_page,"query": {"query_string": {"query": q}}})
        data = resp["hits"]["hits"]
        result['data'] = data
        result['meta'] = {'total':resp["hits"]["total"]["value"]}
        print(result["meta"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result

