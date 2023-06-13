import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
load_dotenv()

ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

# Create the client instance
client = Elasticsearch(
    "https://localhost:9200",
    ca_certs="configs/certs/http_ca.crt",
    basic_auth=("elastic", ELASTIC_PASSWORD)
)
