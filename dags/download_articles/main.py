import os
import sys
import pandas as pd
from airflow import DAG
from pendulum import datetime
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(DAG_FOLDER,'data')

## Custom modules
sys.path.append(DAG_FOLDER)
from modules.extract import extract_pcmid_from_pubmed
from modules.download import download_article_from_pmcid

dag = DAG(
    dag_id='article_downloader',
    description='DAG que irá efetuar o download dos artigos baseado em uma lista de pubmeds',
    schedule=None,start_date=days_ago(1),
    dag_display_name='Baixar artigos',default_view='graph',tags=['rpa']
)

start = EmptyOperator(task_id='start',task_display_name='Inicio',dag=dag)
end = EmptyOperator(task_id='end',task_display_name='Fim',dag=dag)

def extract_list_of_pubmed():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_arbovirus')
    sqlalchemy_engine = mysql_hook.get_sqlalchemy_engine()
    dataframe = pd.read_sql_query(sql='SELECT DISTINCT pubmed FROM arbovirus.processed_sequences where pubmed is not null',con=sqlalchemy_engine)
    os.makedirs(DATA_FOLDER,exist_ok=True)
    dataframe.to_csv(os.path.join(DATA_FOLDER,'pubmed_list.csv'),index=False)

extract_list_of_pubmed_tsk = PythonOperator(task_id='extract_list_of_pubmed',task_display_name='Extrair a lista de pubmed',python_callable=extract_list_of_pubmed,dag=dag)

def extract_pmcid():
    pubmed_df = pd.read_csv(os.path.join(DATA_FOLDER,'pubmed_list.csv'))
    pmcid_dict = {}
    for pubmed in pubmed_df['pubmed']:
        response = extract_pcmid_from_pubmed(pubmed=pubmed)
        pmcid_dict[pubmed] = response.get('pmcid')
    pubmed_df['pmcid'] = pubmed_df['pubmed'].apply(lambda x:pmcid_dict.get(x,None))
    pubmed_df.to_csv(os.path.join(DATA_FOLDER,'pubmed_pmcid_list.csv'),index=False)

extract_pmcid_tsk = PythonOperator(task_id=f'extract_pmcid',task_display_name=f'Extrair a lista de PMCID',python_callable=extract_pmcid,dag=dag)

def download_articles():
    dataframe = pd.read_csv(os.path.join(DATA_FOLDER,'pubmed_pmcid_list.csv'))
    pmcid_list = set(dataframe['pmcid'].dropna().to_list())
    for pmcid in pmcid_list:
        download_article_from_pmcid(pmcid=pmcid,save_in=os.path.join(DATA_FOLDER,'artigos/'))

download_tsk = PythonOperator(task_id=f'download',task_display_name=f'Efetuar download dos artigos',python_callable=download_articles,dag=dag)

start >> extract_list_of_pubmed_tsk >> extract_pmcid_tsk >> download_tsk >> end