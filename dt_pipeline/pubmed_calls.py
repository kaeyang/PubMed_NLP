import re
import requests
import pandas as pd
import concurrent.futures

from bs4 import BeautifulSoup
from pymed import PubMed
from google.cloud import storage

from user_definition import *

def initiate_pubmed_api(tool, email):
    """
    Initiate and return PubMed API Wrapper for query access.
    
    tool = name of project for reference to API (string)
    email = email to be used for reference by API (string)
    """
    
    pubmed = PubMed(tool=tool, email=email)
    return pubmed

def collect_data_from_api_query(pubmed, query, max_results):
    """
    Return at most max_results from a specified query to the PubMed API. Returns dataframe of
    articles and the following attributes: PubMed ID, author(s), title, abstract, keywords,
    and copyrights (if any do not exist then it is filled as None).
    
    pubmed = PubMed API Wrapper instance (class)
    query = query for PubMed API (string)
    max_results = max # of results (int)
    """
    
    results = pubmed.query(query=query, max_results=max_results) # iterable result from API #
    article_list = [[article.pubmed_id.split('\n')[0], # this list comprehension allows for None
                    article.authors,                   # values to fill in if the attribute
                    article.title,                     # does not exist
                    article.abstract,
                    article.keywords,
                    article.copyrights,
                    query] if hasattr(article, 'pubmed_id') and hasattr(article, 'authors') 
                    and hasattr(article, 'title') and hasattr(article, 'abstract')
                    and hasattr(article, 'keywords') and hasattr(article, 'copyrights')
                    else [None, None, None, None, None, None, query] for article in results]
    
    df = pd.DataFrame(article_list, columns=['pubmed_id', 'authors', 'title', 'abstract', 'keywords', 'copyright', 'query'])
    return df

def collect_data_from_multiple_queries(list_of_queries, pubmed, max_results):
    """
    Returns single dataframe of article data from a list of queries specified.
    
    list_of_queries = list of queries specified; default type for each query is string (list)
    """
    
    list_of_df = [collect_data_from_api_query(pubmed, query, max_results) for query in list_of_queries]
    return pd.concat(list_of_df)

# def collect_article_text_from_pmcid(df):
#     """
#     Returns article text for all ids in dataframe (if they exist, else None).
    
#     df = dataframe of article id's and query; assumed the column 'pubmed_id'
#     is present
#     """
    
#     pmc_list = []
#     for id_, query in zip(df['pubmed_id'], df['query']):
#         try:
#             url = f'https://pubmed.ncbi.nlm.nih.gov/{int(id_)}/' # get webpage of the article to find pmcid
#             html = requests.get(url)                             # a separate id from pubmed id
#             soup = BeautifulSoup(html.content, 'html.parser')
#             try:
#                 list_of_links = soup.find_all('a', {'ref':'linksrc=references_link&ordinalpos=3'}) # links that contain pmcid
#             except:
#                 list_of_links = None
#         except:
#             list_of_links = None
#         pmc_list += [(link.get('href').split('/')[-2], id_, query) for link in list_of_links]
        
#     doc_list = []
#     for pmcid, id_, query in pmc_list:
#         if re.match('[0-9]*$', pmcid) is not None:                                            # removes non-matching ids
#             pmc_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmcid}' # link to pmcid xml
#             xml = requests.get(pmc_url)
#             soup = BeautifulSoup(xml.content, features='xml')
#             xml_text = '\n'.join([str(x) for x in soup.find_all('sec')])
#             if xml_text != '':                                                                # removes ids with no text
#                 doc_list.append((id_, pmcid, BeautifulSoup(xml_text, 'html.parser').get_text(), query))
                
#     return pd.DataFrame(doc_list, columns=['pubmed_id', 'pmcid', 'article_text', 'query'])

def process_id_query(id_, query):
    try:
        url = f'https://pubmed.ncbi.nlm.nih.gov/{int(id_)}/' # get webpage of the article to find pmcid
        html = requests.get(url)                             # a separate id from pubmed id
        soup = BeautifulSoup(html.content, 'lxml')
        list_of_links = soup.find_all('a', {'ref':'linksrc=references_link&ordinalpos=3'}) # links that contain pmcid
    except:
        list_of_links = []
    
    pmc_list = []
    for link in list_of_links:
        pmcid = link.get('href').split('/')[-2]
        if re.match('[0-9]*$', pmcid) is not None: # removes non-matching ids
            pmc_list.append((pmcid, id_, query))
            
    return pmc_list

def process_pmcid(id_, pmcid, query):
    pmc_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmcid}' # link to pmcid xml
    xml = requests.get(pmc_url)
    soup = BeautifulSoup(xml.content, 'lxml')
    xml_text = '\n'.join([str(x) for x in soup.find_all('sec')])
    if xml_text != '': # removes ids with no text
        return (id_, pmcid, BeautifulSoup(xml_text, 'lxml').get_text(), query)
    else:
        return None

def get_article_data(df):
    pmc_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for id_, query in zip(df['pubmed_id'], df['query']):
            futures.append(executor.submit(process_id_query, id_, query))
            
        for future in tqdm(concurrent.futures.as_completed(futures)):
            pmc_list.extend(future.result())
    
    doc_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for pmcid, id_, query in pmc_list:
            futures.append(executor.submit(process_pmcid, id_, pmcid, query))
            
        for future in tqdm(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result is not None:
                doc_list.append(result)
                
    return pd.DataFrame(doc_list, columns=['pubmed_id', 'pmcid', 'article_text', 'query'])

def write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df):
    '''
    Writes the dataframe (df) as a CSV file to the GCS bucket.
    
    bucket_name = name of GCS bucket (string)
    blob_name = name of blob (string)
    service_account_key_file = json key for bucket access (string)
    df = dataframe of article data (df)
    '''
    
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    with blob.open('w') as f:
        df.to_csv(f, index=False)