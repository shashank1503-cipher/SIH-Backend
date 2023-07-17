from urllib.request import urlopen
import datetime
from fastapi import HTTPException
from PyPDF2 import PdfReader,PdfFileReader
import requests
import os
from urllib.parse import urlparse
import textract
from pydub import AudioSegment
import audioread
import whisper
import math
from win32api import GetShortPathName
import aspose.words as aw
model = whisper.load_model("small")

import json

from exif import Image

def decimal_coords(coords, ref):
 decimal_degrees = coords[0] + coords[1] / 60 + coords[2] / 3600
 if ref =="S" or ref == "W":
     decimal_degrees = -decimal_degrees
 return decimal_degrees
def image_coordinates(image_path):
    urlOpen = urlopen(image_path) 
    img = urlOpen.read()
    img_info = urlOpen.info()
    file_length = img_info["Content-Length"]
    file_format = img_info["Content-Type"]
    img = Image(img)     
    if img.has_exif:
        try:
            if(img.get("gps_latitude")) != None:
                coords = (decimal_coords(img.gps_latitude,
                        img.gps_latitude_ref),
                        decimal_coords(img.gps_longitude,
                        img.gps_longitude_ref))
                return {"success": True, "data": [file_format, image_path.split("/")[-1], file_length,coords,img.get('gps_datestamp')]}
            else:
                return {"success": False, "data": []}   
        except AttributeError:
            print ('No Coordinates')
    else:
        print ('The Image has no EXIF information')   
        return {"success": False}


def get_data_from_pdf(path):
    reader = PdfReader(path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    text = text.strip()
    text = text.replace("\n", " ")
    print(text)
    return text
    
def download_data_from_FTP(url):
    a = urlparse(url)
    name = os.path.basename(a.path)                     
    request_obj = requests.get(url)
    open(name, 'wb').write(request_obj.content) 
    return name
def extract_data_from_doc(path):
    if path.lower().endswith(".doc"):
        doc = aw.Document(path)
        doc.save("Output.docx")
        text = textract.process("Output.docx")
        text =  text.decode()
        text = text.strip()
        text = text.replace("\n", " ")
        text = text.replace("Evaluation Only. Created with Aspose.Words. Copyright 2003-2023 Aspose Pty Ltd.","")
        text = text.replace("This document was truncated here because it was created in the Evaluation Mode.  Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/","")
        text = text.replace("Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/","")
        return text

    text = textract.process(path)
    text =  text.decode()
    text = text.strip()
    text = text.replace("\n", " ")
    return text

def get_meta_data_from_doc(path,type):
    if type == 'pdf':
        with open(path, 'rb') as f:
            pdf = PdfFileReader(f)
            info = pdf.getDocumentInfo()
        meta_data = {}
        meta_data['name'] = info.title
        meta_data['author'] = info.author
        meta_data['creator'] = info.creator
        meta_data['producer'] = info.producer
        meta_data['subject'] = info.subject
        meta_data['title'] = info.title
        meta_data['date_created'] = datetime.datetime.now() 
        return meta_data
    if type == 'doc':
        meta_data = {}
        meta_data['name'] = path
        meta_data['file_size'] = os.path.getsize(path)
        meta_data['date_created'] = datetime.datetime.now()
        return meta_data
    if type == 'image':
        meta_data = {}
        val = image_coordinates(path)
        if(val["success"] == True):
            file_format = val["data"][0]
            file_name = val["data"][1]
            file_size = val["data"][2]
            lat,long = val["data"][3]
            dateStamp = val["data"][4]
            meta_data['coordinates'] = {'lat':lat,'long':long}
            meta_data['name'] = file_name
            meta_data['file_size'] = file_size
            meta_data['date'] = dateStamp
            meta_data['format'] = file_format 
        
        else:
            return meta_data    
        return meta_data


# # creating a list of requests for making batch requests to cloud vision
# def constructReqs(start, imageURLs, rateLimitCloudVision):
#     reqs = []
#     try:
#         for i in range(start, start + rateLimitCloudVision):
#             visionClient = vision.ImageAnnotatorClient()
#             source_image = vision.ImageSource(image_uri=imageURLs[i])
#             request = vision.AnnotateImageRequest(
#                 image = vision.Image(source = source_image),
#                 features = [
#                     {"type_": vision.Feature.Type.LABEL_DETECTION},
#                     {"type_": vision.Feature.Type.TEXT_DETECTION},
#                     {"type_": vision.Feature.Type.OBJECT_LOCALIZATION},
#                     {"type_": vision.Feature.Type.LOGO_DETECTION}
#                 ],
#             )
#             reqs.append(request)
#         response = visionClient.batch_annotate_images(requests = reqs)
#         return response
#     except Exception as e:
#         raise HTTPException(status_code=500,detail=str(e))

# extracting data from constructReqs response
# def getImageData(imageURLs, start, rateLimitCloudVision, index):
#     response = constructReqs(start, imageURLs, rateLimitCloudVision)
#     response = response.responses
    
#     for i in range(start, start + rateLimitCloudVision):
#         indObj = {}
#         indObj["doc_type"] = "image"
#         indObj["url"] = imageURLs[i]
#         indObj["metadata"] = get_meta_data_from_doc(indObj["url"], "image")
#         indObj["labels"] = []
#         indObj["text_data"] = {"translated": [], "original": []}

#         # labels
#         for label in response[i - start].label_annotations:
#             val = label.description
#             indObj["labels"].append(val)
        
#         # objects
#         for object in response[i - start].localized_object_annotations:
#             val = object.name
#             indObj["objects"].append(val)

#         # logos
#         for logo in response[i - start].logo_annotations:
#             val = logo.description
#             indObj["logos"].append(val)
        
#         # texts 
#         responseSize = len(response[i - start].text_annotations)
#         for j in range (1, responseSize):
#             try:
#                 val = response[i - start].text_annotations[j].description
#                 engTranslate = translate_client.translate(val, target_language = "en")
#                 if(engTranslate["translatedText"].lower() != engTranslate["input"].lower()):
#                     indObj["text_data"]["translated"].append(engTranslate["translatedText"])

#                 indObj["text_data"]["original"].append(val)
#             except:
#                 continue    
        
#         doc = {
#             "_index": index,
#             "_source": indObj
#         }
        
#         print(i, doc)
#         print("=" * 30)
#         yield doc

#individual_image_data_collection
def getIndividualImageData(image_url, client, index, content=None):
    data = {
    'url': image_url,
}
    response = requests.post(url="http://127.0.0.1:4500/getdata", json=data)
    response = response.json()
    print(response)
    indObj = {}
    indObj["doc_type"] = "image"
    indObj["url"] = image_url
    indObj["metadata"] = get_meta_data_from_doc(indObj["url"], "image")
    indObj["labels"] = []
    indObj["text_data"] = {"translated":[],"original":[]}
    indObj["objects"] = []
    # indObj["logos"] = []
    
    # labels
    for label in response["data"]["labels"]:
        indObj["labels"].append(label)
        
    # objects
    for object in response["data"]["objects"]:
        indObj["objects"].append(object['Object type'])
            
    # # logos
    # for logo in response.logo_annotations:
    #     val = logo.description
    #     indObj["logos"].append(val)
            
    # texts 
    for textBlock in response["data"]["texts"]:
        indObj["text_data"]["original"].append(textBlock["text"])

    print(indObj)
    with open('image_logs.txt','w') as f:
        f.write(image_url)
        f.write("\n")
        f.write(json.dumps(indObj)) 
        f.write("\n")
    
    errors = ""
    for error in response["meta"]["error"]:
        errors += error["task"] + ","
    
    if(errors != ""):
        errors = errors[:-1]
    
    
    client.index(index = index, document = indObj)
    
    return {"success": True, "data": indObj, "errors": errors}

def extract_from_sound(path):
    data = {
        'url': path,
    }

    response = requests.post(url="http://127.0.0.1:6500/asr", json=data)
    response = response.json()
    print(response)
    if response["meta"]["error"] != []:
        return {"success": False, "data": {}, "errors": response["meta"]["error"]}
    
    transcription = response["data"]["transcription"]
    language = response["data"]["language"]
    
    return {"success": True, "data": {"content": transcription, "language": language}, "errors": []}

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def is_feasible_audio(path):
    with audioread.audio_open(path) as f:
        totalsec = f.duration
        totalsec = int(totalsec)
        if totalsec >= 60:
            return False
        else:
            return True
        
def upload_to_ftp(filename):
    url = "http://127.0.0.1:5500/upload"   
    with open(filename, 'rb') as f:
        files = {'file': (filename, f)}
        r = requests.post(url, files=files)
        if r.status_code == 200:
            return r.json()['url']
        else:
            return None