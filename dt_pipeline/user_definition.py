import datetime
import os

### date and relevant API call info ###

today = datetime.date.today()
tool = 'MedRho'
email = os.environ.get("PUBMED_EMAIL")
list_of_queries = ['NSCLC', 'breast+cancer', 'brain+cancer', 'blood+cancer', 'stomach+cancer']
max_results = 1000

### gcs bucket data ###

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")