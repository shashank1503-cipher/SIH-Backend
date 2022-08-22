import os
from elasticsearch import Elasticsearch
import cloudinary
from dotenv import load_dotenv
load_dotenv()

ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

# Found in the 'Manage Deployment' page
CLOUD_ID = os.getenv("CLOUD_ID")
# Create the client instance
client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD),
)

# cloudinary keys
CLOUD_NAME = os.getenv("CLOUD_NAME")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")


cloudinary.config( 
  cloud_name = CLOUD_NAME, 
  api_key = API_KEY, 
  api_secret = API_SECRET,
  secure = True
)