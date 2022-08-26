from urllib.request import urlopen
import datetime
from fastapi import HTTPException
from PyPDF2 import PdfReader,PdfFileReader
import requests
import os
from urllib.parse import urlparse
import textract
from google.cloud import vision, translate_v2 as translate
from pydub import AudioSegment
import audioread
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "copper-guide-359913-dd3e59666dc7.json"

# google cloud translate api
translate_client = translate.Client()

"""Helper Function for Image Location"""
from exif import Image
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="MyApp")
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
    return text
    
def download_data_from_cloudinary(url):
    a = urlparse(url)
    name = os.path.basename(a.path)                     
    request_obj = requests.get(url)
    open(name, 'wb').write(request_obj.content) 
    return name
def extract_data_from_doc(path):
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
        extension = path.split(".")[1]
        if extension != "jpg" or extension != "jpeg":
            return {}
        meta_data = {}
        val = image_coordinates(path)
        if(val["success"] == True):
            file_format = val["data"][0]
            file_name = val["data"][1]
            file_size = val["data"][2]
            lat,long = val["data"][3]
            dateStamp = val["data"][4]
            location = geolocator.reverse(str(lat)+','+str(long))

            meta_data['coordinates'] = {'lat':lat,'long':long}
            meta_data['name'] = file_name
            meta_data['location'] = (location.raw)["display_name"]
            meta_data['file_size'] = file_size
            meta_data['date'] = dateStamp
            meta_data['format'] = file_format 
        
        else:
            return meta_data    
        return meta_data


# creating a list of requests for making batch requests to cloud vision
def constructReqs(start, imageURLs, rateLimitCloudVision):
    reqs = []
    try:
        for i in range(start, start + rateLimitCloudVision):
            visionClient = vision.ImageAnnotatorClient()
            source_image = vision.ImageSource(image_uri=imageURLs[i])
            request = vision.AnnotateImageRequest(
                image = vision.Image(source = source_image),
                features = [
                    {"type_": vision.Feature.Type.LABEL_DETECTION},
                    {"type_": vision.Feature.Type.TEXT_DETECTION},
                    {"type_": vision.Feature.Type.OBJECT_LOCALIZATION},
                    {"type_": vision.Feature.Type.LOGO_DETECTION}
                ],
            )
            reqs.append(request)
        response = visionClient.batch_annotate_images(requests = reqs)
        return response
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

# extracting data from constructReqs response
def getImageData(imageURLs, start, rateLimitCloudVision, index):
    response = constructReqs(start, imageURLs, rateLimitCloudVision)
    response = response.responses
    
    for i in range(start, start + rateLimitCloudVision):
        indObj = {}
        indObj["doc_type"] = "image"
        indObj["url"] = imageURLs[i]
        indObj["metadata"] = get_meta_data_from_doc(indObj["url"], "image")
        indObj["labels"] = []
        indObj["text_data"] = {"translated": [], "original": []}

        # labels
        for label in response[i - start].label_annotations:
            val = label.description
            indObj["labels"].append(val)
        
        # objects
        for object in response[i - start].localized_object_annotations:
            val = object.name
            indObj["objects"].append(val)

        # logos
        for logo in response[i - start].logo_annotations:
            val = logo.description
            indObj["logos"].append(val)
        
        # texts 
        responseSize = len(response[i - start].text_annotations)
        for j in range (1, responseSize):
            try:
                val = response[i - start].text_annotations[j].description
                engTranslate = translate_client.translate(val, target_language = "en")
                if(engTranslate["translatedText"].lower() != engTranslate["input"].lower()):
                    indObj["text_data"]["translated"].append(engTranslate["translatedText"])

                indObj["text_data"]["original"].append(val)
            except:
                continue    
        
        doc = {
            "_index": index,
            "_source": indObj
        }
        
        print(i, doc)
        print("=" * 30)
        yield doc

#individual_image_data_collection
def getIndividualImageData(image_url, client, index):
    visionClient = vision.ImageAnnotatorClient()
    image = vision.Image()
    image.source.image_uri = image_url
    request = {
        "image": image,
        "features": [
            {"type_": vision.Feature.Type.LABEL_DETECTION},
            {"type_": vision.Feature.Type.TEXT_DETECTION},
            {"type_": vision.Feature.Type.OBJECT_LOCALIZATION},
            {"type_": vision.Feature.Type.LOGO_DETECTION}
        ],
    }

    response = visionClient.annotate_image(request)
    indObj = {}
    indObj["doc_type"] = "image"
    indObj["url"] = image_url
    indObj["metadata"] = get_meta_data_from_doc(indObj["url"], "image")
    indObj["labels"] = []
    indObj["text_data"] = {"translated": [], "original": []}
    indObj["objects"] = []
    indObj["logos"] = []
    
    # labels
    for label in response.label_annotations:
        val = label.description
        indObj["labels"].append(val)
        
    # objects
    for object in response.localized_object_annotations:
        val = object.name
        indObj["objects"].append(val)
            
    # logos
    for logo in response.logo_annotations:
        val = logo.description
        indObj["logos"].append(val)
            
    # texts 
    responseSize = len(response.text_annotations)
    for j in range (1, responseSize):
        try:
            val = response.text_annotations[j].description
            engTranslate = translate_client.translate(val, target_language = "en")
            if(engTranslate["translatedText"].lower() != engTranslate["input"].lower()):
                indObj["text_data"]["translated"].append(engTranslate["translatedText"])
        except:
            continue
        indObj["text_data"]["original"].append(val)
        
    print(indObj)
    client.index(index = index, document = indObj)
    
    return {"success": True, "data": indObj}


def extract_from_sound(path):
     
    extension = path.split(".")[1]
    if extension !="wav":
        src = path
        path = path.split(".")[0] + ".wav"
        sound = AudioSegment.from_file(src)
        sound.export(path, format="wav")
    text = textract.process(path)
    text =  text.decode()
    text = text.strip()
    text = text.replace("\n", " ")
    os.remove(path)
    return text
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