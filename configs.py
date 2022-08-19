import os
from elasticsearch import Elasticsearch
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
