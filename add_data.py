import io
import json
from typing import Optional
import uuid
from zipfile import ZipFile
from fastapi import APIRouter, HTTPException, File, UploadFile, Form,Request
import validators
import os
from elasticsearch import helpers
import subprocess
import csv
from io import StringIO, BytesIO
from PIL import Image as PILImage
import requests
from pandas import read_csv

import configs
import utils



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
    print(data)
    fetch_url = data.get('url',None)
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = data.get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = data.get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'pdf':
        try:
            fetch_path = utils.download_data_from_FTP(fetch_url)
        except Exception as e:
            raise HTTPException(status_code=500,detail="Error Downloading File from FTP Server " + str(e))
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
    fetch_url = data.get('url',None)
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = data.get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = data.get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'doc':
        try:
            fetch_path = utils.download_data_from_FTP(fetch_url)
        except Exception as e:
            raise HTTPException(status_code=500,detail="Error Downloading File from FTP Server " + str(e))
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
        print(name)


        contents = await file.read()

        # contents.replace(' IF NOT NULL', '')

        print("Working")
        try:
            f = open('sql.sql', 'wb')
            f.write(contents)
            f.close()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # print("path: ", os.getcwd())
        path = os.getcwd()
        print(path)
        cmd = f"sqldump-to -i {path}/sql.sql > j.json"
        print(cmd)
        try:
            # os.system(cmd)
            subprocess.run(['sqldump-to', '-i', f'{path}/sql.sql', '>', 'j.json'], check=True, shell=True)
        except subprocess.CalledProcessError:
            raise HTTPException(status_code=400, detail=str(e))

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
                    new_data.append(dict_obj)

            if not len(new_data):
                raise HTTPException(status_code=400, detail="error")

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

@router.post("/csvimagetoindex")
async def add_csv_file_images_to_index(file: UploadFile = File(...), url_prop: Optional[str] = "photo_image_url"):
    images = read_csv(file.file)
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
async def add_single_image_file_to_index(req:Request):
    data = await req.json()
    fetch_url = data.get('url',None)
    print("Url",fetch_url)
    # with BytesIO() as f:
    #     im = PILImage.open(requests.get(fetch_url, stream=True).raw)
    #     im = im.convert("RGB")
    #     im = im.resize((500, 500))
    #     im.save(f, format='JPEG')
    #     val = f.getvalue()
        
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = data.get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = data.get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    
    if fetch_doc_type == 'image':
        # try:
        #     resp = utils.getIndividualImageData(fetch_url, client, fetch_index)
        #     return resp
        # except Exception as e:
        #     raise HTTPException(status_code=500,detail=str(e))
        resp = utils.getIndividualImageData(fetch_url, client, fetch_index)
        return resp
    else:
        raise HTTPException(status_code=400, detail="Doc Type not supported for this endpoint")        

@router.post("/zipimagetoindex")
async def add_zip_file_images_to_index(file: UploadFile = File(...), index: str = Form()):
    with ZipFile(io.BytesIO((file.file).read()), 'r') as zip:
        listOfFileNames = zip.namelist()
        for fileName in listOfFileNames:
            res = zip.open(fileName)
            try:
                with BytesIO() as f:
                    # im = PILImage.open(requests.get(file_url["url"], stream=True).raw)
                    im = PILImage.open(BytesIO(res.read()))
                    im = im.convert("RGB")
                    im.save(f, format='JPEG')
                    val = f.getvalue()
                    file_url = cloudinary.uploader.upload(val, folder = "textual_images")
    
                resp = utils.getIndividualImageData(file_url["url"], client, index, val)

            except Exception as e:
                raise HTTPException(status_code=500,detail=str(e))

        return {"success": True}

@router.post("/jsontoindex")
async def add_json_data(file: UploadFile= File(...), name: str = Form()):
    try:
        contents = await file.read()
        try:
            f = open('j.json', 'wb')
            f.write(contents)
            f.close()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        def generate_docs():
            f = open('j.json', encoding='utf8')
            data = json.load(f)
            new_data = []
            if type(data) == list:
                new_data = data
            else:
                for i in data:
                    if (type(data[i]) == list):
                        new_data = data[i]
                        break
            # print(new_data)
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
        return {"status": 1}
    except Exception as e:
        print("Error", e)
        raise HTTPException(status_code=500,detail=str(e))

@router.post("/soundtoindex")
async def add_sound(req:Request):
    data = await req.json()
    fetch_url = data.get('url',None)
    if not fetch_url:
        raise HTTPException(status_code=400, detail="URL not found")
    if not validators.url(fetch_url):
        raise HTTPException(status_code=400, detail="Invalid URL")
    fetch_index = data.get('index',None)
    if not fetch_index:
        raise HTTPException(status_code=400, detail="Index not found")
    fetch_doc_type = data.get('doc_type',None)
    if not fetch_doc_type:
        raise HTTPException(status_code=400, detail="Doc Type not found")
    if fetch_doc_type == 'sound':
        
        try:
            data = utils.extract_from_sound(fetch_url)
            if data["success"] == False:
                raise HTTPException(status_code=500, detail=data["errors"])
            data = data["data"]
            content = data.get('content',None)
            language = data.get('language',None)
        except Exception as e:
            # os.remove(fetch_url)
            raise HTTPException(status_code=500,detail="Error Fetching Data From Sound " + str(e))
        data = {
        'doc_type':fetch_doc_type,
        'content':content,
        'url':fetch_url,
        'original_language':language
        }
        try:
             client.index(index=fetch_index,body=data)
        except Exception as e:
            raise HTTPException(status_code=500,detail="Error Adding Data to Index " + str(e))
        return {"message":"Data added to index","data":data}

@router.post('/csvtoindex')
async def csvtoindex(file: UploadFile = File(...), name: str = Form()):
    try:
        contents = file.file.read()
        # print(contents)
        buffer = StringIO(contents.decode('utf-8'))
        csvReader = csv.DictReader(buffer)
        
        out = list(csvReader)
        
        def generate_docs():

            for row in out:
                print(row)
                row['doc_type'] = 'text'
                doc = {
                    "_index": name,
                    "_id": uuid.uuid4(),
                    "_source": row
                }
                print(doc)
                yield doc
        
        # print("out", out)
        helpers.bulk(client, generate_docs())

        return {'status': 1}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
