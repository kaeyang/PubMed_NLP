import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from pubmed_calls import *
from user_definition import *

def _download_pubmed_data():
    """
    Create PubMed API instance to query and collect data, then write to the
    gcs bucket.
    """
    
    pubmed = initiate_pubmed_api(tool, email)
    
    article_blob = f'{today}_articles.csv' # names for the files
    text_blob = f'{today}_text.csv'
    
    article_df = collect_data_from_multiple_queries(list_of_queries, pubmed, max_results)
    text_df = get_article_data(article_df)

    write_csv_to_gcs(bucket_name, article_blob, service_account_key_file, article_df)
    write_csv_to_gcs(bucket_name, text_blob, service_account_key_file, text_df)

with DAG(
    dag_id="pubmed_dag",
    schedule_interval='@daily',
    start_date=datetime.datetime(2023, 4, 1),
    end_date=datetime.datetime(2023, 6, 30),
    catchup=False
    ) as dag:
    
    download_pubmed_data = PythonOperator(task_id='download_pubmed_data',
                                           python_callable=_download_pubmed_data,
                                         dag=dag)
    download_pubmed_data