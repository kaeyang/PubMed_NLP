import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scrapper import *
from medhro_user_definition import *


search_list = ["lung cancer", "diabetes",
               "pulmonary hypertension", "liver cirrhosis", "hypothyroidism"]
max_articles = 500
gcs_path = 'data/'


with DAG(dag_id="medhro_scraping",
         start_date=datetime(2023, 4, 20),
         schedule_interval='@daily') as dag:

    os.environ["no_proxy"] = "*"

    t1 = PythonOperator(task_id="pubmed_scrape_and_upload",
                        python_callable=xml_extract_upload,
                        op_kwargs={"email": med_email,
                        		 "search_query_list": search_list,
                                   "max_articles": max_articles,
                                   "bucket_name": bucket_name,
                                   "service_account_key_file": service_account_key_file,
                                   "gcs_path": gcs_path})

    t1
