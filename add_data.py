import json
from typing import Optional
import uuid
from fastapi import APIRouter, HTTPException, File, UploadFile, Form,Request
import validators
import os
from elasticsearch import helpers
from google.cloud import vision
import cloudinary
from pandas import read_csv
from urllib.request import urlopen

import configs
import utils

import cloudinary.uploader
import cloudinary.api

# cloud vision api creds
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "copper-guide-359913-dd3e59666dc7.json"

client = configs.client
router = APIRouter()
        

@router.post("/texttoindex")
async def add_data_to_index(req: Request):
    data = await req.json()
    fetch_index = data.get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = data.get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'text':
        fetch_data = data.get('data',None)
        if not fetch_data:
            raise HTTPException(status_code=400, detail="Data not found")
        try:
            fetch_data['doc_type'] = fetch_doc_type
            client.index(index=fetch_index,body=fetch_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {"message":"Data added to index","data":data}
    else:
        raise HTTPException(status_code=400, detail="Doc Type not supported for this endpoint")

@router.post("/pdftoindex")
async def add_pdf_to_index(req:Request):
    data = await req.json()
    fetch_url = json.loads(data).get('url',None)
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = json.loads(data).get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = json.loads(data).get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'pdf':
        try:
            fetch_path = utils.download_data_from_cloudinary(fetch_url)
        except Exception as e:
            raise HTTPException(status_code=500,detail="Error Downloading File from Cloud on Server " + str(e))
        try:
            content = utils.get_data_from_pdf(fetch_path)
            meta = utils.get_meta_data_from_doc(fetch_path,'pdf')
        except Exception as e:
            os.remove(fetch_path)
            raise HTTPException(status_code=500,detail="Error Fetching Data From PDF " + str(e))
        data = {
        'doc_type':fetch_doc_type,
        'meta':meta,
        'content':content,
        'url':fetch_url
        }
        try:
             client.index(index=fetch_index,body=data)
        except Exception as e:
            os.remove(fetch_path)
            raise HTTPException(status_code=500,detail="Error Adding Data to Index " + str(e))
        os.remove(fetch_path)
        return {"message":"Data added to index","data":data}
    
    else:
        raise HTTPException(status_code=400, detail="Doc Type not supported for this endpoint")

@router.post("/wordtoindex")
async def add_word_to_index(req:Request):
    data = await req.json()
    fetch_url = json.loads(data).get('url',None)
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = json.loads(data).get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = json.loads(data).get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'doc':
        try:
            fetch_path = utils.download_data_from_cloudinary(fetch_url)
        except Exception as e:
            raise HTTPException(status_code=500,detail="Error Downloading File from Cloud on Server " + str(e))
        try:
            content = utils.extract_data_from_doc(fetch_path)
            meta = utils.get_meta_data_from_doc(fetch_path,'doc')
        except Exception as e:
            os.remove(fetch_path)
            raise HTTPException(status_code=500,detail="Error Fetching Data From Doc " + str(e))
        data = {
            'doc_type':fetch_doc_type,
            'meta':meta,
            'content':content,
            'url':fetch_url
        }
        try:
             client.index(index=fetch_index,body=data)
        except Exception as e:
            os.remove(fetch_path)
            raise HTTPException(status_code=500,detail="Error Adding Data to Index " + str(e))
        os.remove(fetch_path)
        return {"message":"Data added to index","data":data}
    else:
        raise HTTPException(status_code=400, detail="Doc Type not supported for this endpoint")
        
@router.post("/sqltoindex")
async def add(file: UploadFile= File(...), name: str = Form()):
    try:
        contents = await file.read()
        try:
            f = open('sql.sql', 'wb')
            f.write(contents)
            f.close()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        path = os.path()
        cmd = f"sqldump-to -i {path}/sql.sql > j.json"
        os.system(cmd)

        def generate_docs():

            try:
                f = open('j.json', encoding='utf8')
                data = json.load(f)
                new_data = data
            except:
                f = open('j.json', encoding='utf8')
                data = f.readlines()
                new_data = []
                for row in data:
                    dict_obj = json.loads(row)
                    new_data.routerend(dict_obj)  

            for row in new_data:
                row['doc_type']= 'text'
                doc = {
                        "_index": name,
                        "_id": uuid.uuid4(),
                        "_source": row,
                    }
                yield doc
    
        helpers.bulk(client, generate_docs())
        print("Done")
        os.remove('j.json')
        os.remove('sql.sql')        
        return {"status": 1}
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
    
@router.post("/cloudimagetoindex")
async def add_cloud_image_to_index(prefix: str, maxSize : Optional[int] = 2000):
    try:
        images = cloudinary.api.resources(
        type = "upload", 
        prefix = prefix,
        max_results = maxSize)

        rateLimitCloudinary = maxSize if maxSize < images.rate_limit_allowed else images.rate_limit_allowed
        rateLimitCloudVision = maxSize if maxSize < 10 else 10
        
        next_cursor = None if rateLimitCloudinary == maxSize else images["next_cursor"]
        for j in range(maxSize // rateLimitCloudinary):
            if j != 0:
                images = cloudinary.api.resources(
                    type = "upload",
                    prefix = prefix,
                    max_results = maxSize,
                    next_cursor = next_cursor
                )
                next_cursor = images["next_cursor"]
                
            imageURLs = [i["url"] for i in images["resources"]]
            totalSize = len(imageURLs)

            for i in range(totalSize // rateLimitCloudVision):
                try:
                    helpers.bulk(client, utils.getImageData(imageURLs, rateLimitCloudVision * i, rateLimitCloudVision, index = "sample_dataset_3"))
                except Exception as e:
                    raise HTTPException(status_code=500,detail=str(e))    

            print(j, "~^" * 30)
            
            return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@router.post("/csvimagetoindex")
async def add_csv_image_to_index(file: UploadFile = File(...), url_prop: Optional[str] = "photo_image_url"):
    images = read_csv(file.file, sep = "\t")
    imageURLs = images[url_prop]
    
    totalSize = imageURLs.size
    
    try:
        rateLimitCloudVision = totalSize if totalSize < 10 else 10
         
        for i in range(totalSize // rateLimitCloudVision):
            try:
                helpers.bulk(client, utils.getImageData(imageURLs, rateLimitCloudVision * i, rateLimitCloudVision, index = "sample_dataset_4"))
            except Exception as e:
                raise HTTPException(status_code=500,detail=str(e))    

        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@router.post("/singleimagefiletoindex")
async def add_single_image_file_to_index(index: str, file: bytes = File()):
    try:
        file_url = cloudinary.uploader.upload(file, folder = "textual_images")

        resp = utils.getIndividualImageData(file_url["url"], client, index)
        return resp
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))        

@router.post("/singleimageurltoindex")
async def add_single_image_url_to_index(image_url: str, index: str):
    try:
        urlOpen = urlopen(image_url) 
        img = urlOpen.read()
        file_url = cloudinary.uploader.upload(img, folder = "textual_images")
        resp = utils.getIndividualImageData(file_url["url"], client, index)
        return resp
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))        


# @router.post("/testing")
# async def testing(url: Optional[str] = "https://res.cloudinary.com/dikr8bxj7/image/upload/v1660945000/textual_images/mzsurkkdmw376atg2enp.jpg"):
#     # return(utils.get_meta_data_from_doc(url, "image"))
#     print(client.options(ignore_status=[400,404]).indices.delete(index='image_dataset'))
