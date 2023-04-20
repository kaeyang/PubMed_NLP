
import os
from Bio import Entrez

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
med_email = os.environ.get("PUBMED_EMAIL")
