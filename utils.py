import datetime
from fastapi import HTTPException
from PyPDF2 import PdfReader,PdfFileReader
import requests
import os
from urllib.parse import urlparse
import textract
from google.cloud import vision
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "copper-guide-359913-dd3e59666dc7.json"
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
    with open(image_path, 'rb') as src:
        img = Image(src)
    if img.has_exif:
        try:
            img.gps_longitude
            coords = (decimal_coords(img.gps_latitude,
                    img.gps_latitude_ref),
                    decimal_coords(img.gps_longitude,
                    img.gps_longitude_ref))
        except AttributeError:
            print ('No Coordinates')
    else:
        print ('The Image has no EXIF information')   
    return coords,img.get('gps_datestamp')


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
        meta_data = {}
        lat,long = image_coordinates(path)
        location = geolocator.reverse(str(lat)+','+str(long))
        meta_data['coordinates'] = {'lat':lat,'long':long}
        meta_data['name'] = path
        meta_data['address'] = location
        meta_data['file_size'] = os.path.getsize(path)
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
        indObj["datatype"] = "image"
        indObj["url"] = imageURLs[i]
        indObj["metadata"] = {}
        indObj["labels"] = []
        indObj["texts"] = []
        
        # ~~utils wala metadata connect krlena yahan~~ indObj["metadata"]

        # labels
        for label in response[i - start].label_annotations:
            val = label.description
            indObj["labels"].append(val)
         
        # texts 
        responseSize = len(response[i - start].text_annotations)
        for j in range (1, responseSize):
            val = response[i - start].text_annotations[j].description
            indObj["texts"].append(val)
        
        doc = {
            "_index": index,
            "_source": indObj
        }
        
        print(i, doc)
        print("=" * 30)
        yield doc
