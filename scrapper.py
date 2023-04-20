from Bio import Entrez
import pandas as pd
from google.cloud import storage
import urllib.request
import glob

from medhro_user_definition import *
from datetime import datetime, date


# set this for airflow errors. https://github.com/apache/airflow/discussions/24463
os.environ["no_proxy"] = "*"


def xml_extract_upload(email, search_query_list, max_articles, bucket_name, service_account_key_file, gcs_path):
    '''
    This function is used to query the papers in PubMed and upload info into GCP bucket as xml files
    '''

    storage_client = storage.Client.from_service_account_json(
        service_account_key_file)

    bucket = storage_client.bucket(bucket_name)

    # will replace with environmental variable for privacy
    Entrez.email = email
    Entrez.timeout = 3600

    for search_query in search_query_list:
        # Search PubMed
        handle = Entrez.esearch(
            db="pubmed", term=search_query, retmax=max_articles)
        record = Entrez.read(handle)

        # get list of PubMed IDs
        id_list = record["IdList"]

        # Use the Entrez.efetch() function to download the XML file for each PubMed ID
        for pmid in id_list:
            handle = Entrez.efetch(db="pubmed", id=pmid,
                                   rettype="xml", retmode="text")
            xml_data = handle.read()
            blob = bucket.blob(f"{gcs_path}{search_query}_{pmid}.xml")
            blob.upload_from_string(xml_data)
