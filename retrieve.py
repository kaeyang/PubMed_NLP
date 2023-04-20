import pandas as pd
from google.cloud import storage
import xml.etree.cElementTree as et
from nltk.stem.porter import *
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import string
import nltk

from medhro_user_definition import *
# set this for airflow errors. https://github.com/apache/airflow/discussions/24463
os.environ["no_proxy"] = "*"


def extract_xml(folder, search_query, xml_tag, bucket_name, service_account_key_file):
    '''
    This function is used to extract the xml files from GCP
    '''
    storage_client = storage.Client.from_service_account_json(
        service_account_key_file)

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f'{folder}{search_query}', delimiter='/')

    string_list = ''

    for blob in blobs:
        content = blob.download_as_string()
        tree = et.fromstring(content)
        for node in tree.iter(xml_tag):
            for elem in node.iter(xml_tag):
                #print("{}: {}".format(elem.tag, elem.text))
                if elem.text is not None:
                    string_list += elem.text

    return string_list


def tokenize(text):
    '''
    This function is tokenize the text
    '''
    text = text.lower()
    text = re.sub('[' + string.punctuation + '0-9\\r\\t\\n]', ' ', text)
    tokens = nltk.word_tokenize(text)
    tokens = [w for w in tokens if len(w) > 2]  # ignore a, an, to, at, be, ...
    tokens = [word for word in tokens if not word in ENGLISH_STOP_WORDS]
    return tokens