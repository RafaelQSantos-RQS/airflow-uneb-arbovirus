import os
import sys
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Custom modules
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DAG_FOLDER)
from modules.etl import Landing,Bronze,Silver
from modules.utils import prepare_data_filesystem

dag = DAG(
    dag_id='chikungunya_etl',
    description='Pipeline no qual será feita a extração dos sequenciamentos genéticos do genbank.',
    schedule=None, start_date=datetime(year=2024,month=7,day=20),
    dag_display_name='ETL Sequências (Chikungunya)',catchup=False, default_view='graph'
)

start = EmptyOperator(task_id='start',task_display_name='Inicio',dag=dag)
landing_to_bronze = EmptyOperator(task_id='landing_to_bronze',task_display_name='Landing para bronze',dag=dag)
bronze_to_silver = EmptyOperator(task_id='bronze_to_silver',task_display_name='Bronze para Silver',dag=dag)
end = EmptyOperator(task_id='end',task_display_name='Fim',dag=dag)

with TaskGroup(group_id='landing_stg',tooltip='Landing stage',dag=dag) as landing_stg:
    prepare_filesystem = PythonOperator(task_id='prepare_filesystem', task_display_name='Preparação das pastas de dados',python_callable=prepare_data_filesystem,op_kwargs={'root_path':DAG_FOLDER},dag=dag)

    op_kwargs = {
        'raw_folder_path':os.path.join(DAG_FOLDER,'data/raw'),
        'database':'nucleotide',
        'term':'Chikungunya',
        'retmax':1000,
        'datetype':"pdat",
        'reldate':120
    }
    extract_uid = PythonOperator(task_id='extract_uid',task_display_name='Extração dos UID',python_callable=Landing.extract_uids,op_kwargs=op_kwargs,dag=dag)

    prepare_filesystem >> extract_uid

with TaskGroup(group_id='bronze_stg',tooltip='Bronze stage',dag=dag) as bronze_stg:
    op_kwargs = {
        'raw_folder_path':os.path.join(DAG_FOLDER,'data/raw'),
        'save_in':os.path.join(DAG_FOLDER,'data/processed'),
        'database':'nucleotide',
        'rettype':'gbc',
        'retmode':'xml'
    }
    extract_sequences = PythonOperator(task_id='extract_sequences',task_display_name='Extração das sequências',python_callable=Bronze.process_landing_response,op_kwargs=op_kwargs,dag=dag)
    load_proccessed_data = PythonOperator(task_id='load_proccess_data',task_display_name='Carregamento do xml pré tratado',python_callable=Bronze.load_processed_data,op_kwargs={'processed_folder':os.path.join(DAG_FOLDER,'data/processed')},dag=dag)
    
    extract_sequences >> load_proccessed_data

with TaskGroup(group_id='silver_stg',tooltip='Silver Stage',dag=dag) as silver_stg:
    extract_bronze_data = PythonOperator(task_id='extract_bronze_data',task_display_name='Extração dos dados para limpeza',python_callable=Silver.extract_bronze_data,op_kwargs={'save_in':os.path.join(DAG_FOLDER,'data/processed')},dag=dag)
    clean_bronze_data = PythonOperator(task_id='clean_bronze_data',task_display_name='Limpeza dos dados',python_callable=Silver.clean_data,op_kwargs={'parquet_file_path':os.path.join(DAG_FOLDER,'data/processed/','bronze.parquet')},dag=dag)
    load_clean_data = PythonOperator(task_id='load_silver_data',task_display_name='Carregamento dos dados limpos',python_callable=Silver.load_data,op_kwargs={'parquet_file':os.path.join(DAG_FOLDER,'data/processed/','cleaned_silver.parquet')},dag=dag)

    extract_bronze_data >> clean_bronze_data >> load_clean_data


start >> landing_stg >> landing_to_bronze >> bronze_stg >> bronze_to_silver >> silver_stg >>end