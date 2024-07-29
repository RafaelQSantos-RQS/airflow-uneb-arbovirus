import os
import ast
import pytz
import xmltodict
import pandas as pd
from datetime import datetime
from modules.entrez import esearch,efetch
from modules.utils import current_date,filter_kwargs
from airflow.providers.mysql.hooks.mysql import MySqlHook
from modules.utils import extract_pubmed_list, extract_country, extract_collection_date

class Landing:
    @staticmethod
    def extract_uids(raw_folder_path: str, database: str, term: str, **kwargs):
        """
        Descrição
        ---------
        Realiza a etapa de busca na NCBI e salva a resposta em um arquivo XML.

        Argumentos
        ----------
            database (str): Banco de dados da NCBI a ser utilizado (ex: "pubmed").
            term (str): Termo de busca na NCBI.
            **kwargs: Argumentos adicionais para a função entrez.esearch.

        Retorno
        -------
            None: Se a etapa for executada com sucesso.

        Exceções
        --------
            Exception: Se ocorrer algum erro durante a execução da etapa.
        """

        # Filtrar o kwargs para manter apenas os parâmetros aceitos por esearch
        filtered_kwargs = filter_kwargs(esearch,**kwargs)

        print(filtered_kwargs)
        
        # Chamar a função esearch com os parâmetros filtrados
        response_esearch = esearch(database=database, term=term, **filtered_kwargs)

        # Salvar a resposta em um arquivo XML
        filename_full_path = os.path.join(raw_folder_path, f"{current_date()}.xml")
        with open(filename_full_path, 'wb') as xml_file:
            xml_file.write(response_esearch.content)

class Bronze:
    @staticmethod
    def process_landing_response(raw_folder_path: str, save_in: str, database: str, **kwargs):
        '''
        Processa a resposta de landing, lê o XML mais recente, transforma em um dicionário Python,
        extrai e processa uma lista de UIDs e salva as respostas das requisições.
        '''
        
        def get_latest_xml_path(folder_path: str) -> str:
            """Retorna o caminho do XML mais recente no diretório fornecido."""
            files = [os.path.join(folder_path, file) for file in os.listdir(folder_path)]
            return max(files, key=os.path.getmtime)

        def read_xml_file(file_path: str) -> str:
            """Lê o conteúdo de um arquivo XML."""
            with open(file_path, 'r') as xml_file:
                return xml_file.read()

        def split_list_into_chunks(data_list: list, chunk_size: int) -> list:
            """Divide uma lista em várias listas menores de tamanho chunk_size."""
            return [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

        def save_response(content: bytes, save_path: str):
            """Salva o conteúdo da resposta em um arquivo."""
            with open(save_path, 'wb') as file:
                file.write(content)

        print("Selecionando o XML mais novo para efetuar a transformação")
        xml_path = get_latest_xml_path(raw_folder_path)

        print(f"Lendo o xml {os.path.basename(xml_path)}.")
        xml_data = read_xml_file(xml_path)

        print("Transformando o xml em um dicionário Python.")
        xml_dict = xmltodict.parse(xml_data)

        print("Extraindo a lista de UID.")
        id_list = xml_dict.get('eSearchResult').get('IdList').get('Id')
        if not id_list:
            print("Nenhum UID encontrado no XML.")
            return

        print("Quebrando a lista de UID em listas menores de até 200 IDs.")
        id_chunks = split_list_into_chunks(id_list, 200)

        print("Efetuando as requisições.")
        for i, id_chunk in enumerate(id_chunks, start=1):
            print(f"Efetuando a requisição {i}.")
            # Filtrar o kwargs para manter apenas os parâmetros aceitos por esearch
            filtered_kwargs = filter_kwargs(esearch,**kwargs)
            efetch_response = efetch(database=database, id=id_chunk, **filtered_kwargs)
            efetch_response.raise_for_status()

            print(f"Salvando a requisição {i}.")
            save_path = os.path.join(save_in, f'{os.path.basename(xml_path).replace(".", f" ({i}).")}')
            save_response(efetch_response.content, save_path)

        print("Processo de landing concluído com sucesso.")

    @staticmethod
    def load_processed_data(processed_folder:str):
        xml_list = [os.path.join(processed_folder,xml_file) for xml_file in os.listdir(processed_folder) if xml_file.startswith(current_date()) and xml_file.endswith('.xml')]
        dataframe = pd.DataFrame()
        for xml in xml_list:
            with open(xml,'r') as xml_file:
                xml_data = xml_file.read()

            dataframe = pd.concat([dataframe,pd.DataFrame(xmltodict.parse(xml_data).get('INSDSet').get('INSDSeq'))],ignore_index=True)
        
        utc_minus_3 = pytz.timezone('America/Bahia')
        timestamp = datetime.now(tz=utc_minus_3)
        dataframe['extract_datetime'] = timestamp

        mysql_hook = MySqlHook(mysql_conn_id='mysql_arbovirus')
        sqlalchemy_engine = mysql_hook.get_sqlalchemy_engine()
        dataframe.to_sql(name='raw_sequences',con=sqlalchemy_engine,if_exists='replace')

class Silver:
    @staticmethod
    def extract_bronze_data(save_in:str):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_arbovirus')
        sqlalchemy_engine = mysql_hook.get_sqlalchemy_engine()
        bronze_df = pd.read_sql_table(table_name='raw_sequences',con=sqlalchemy_engine,index_col='index')
        full_parquet_path = os.path.join(save_in,'bronze.parquet')
        bronze_df.to_parquet(full_parquet_path,index=False)

    @staticmethod
    def clean_data(parquet_file_path:str):
        dataframe = pd.read_parquet(parquet_file_path)

        full_df = dataframe[['INSDSeq_locus','INSDSeq_length','INSDSeq_update-date','INSDSeq_create-date','INSDSeq_references','INSDSeq_feature-table','INSDSeq_sequence']]

        # Renomeando colunas
        colunas_a_serem_renomeadas = {
            'INSDSeq_locus': 'locus',
            'INSDSeq_length':'length',
            'INSDSeq_update-date': 'update_date',
            'INSDSeq_create-date': 'create_date',
            'INSDSeq_sequence':'sequence'
            }
        full_df = full_df.rename(columns=colunas_a_serem_renomeadas) 

        # Alterar tipo das colunas
        colunas_a_serem_convertidas = {
            'length': 'int64',
            'update_date':'datetime64[ns]',
            'create_date':'datetime64[ns]'
            }
        full_df = full_df.astype(colunas_a_serem_convertidas)

        # Extraindo os pubmeds
        column = 'INSDSeq_references'
        full_df['pubmed'] = full_df[column].apply(lambda x: extract_pubmed_list(cell=ast.literal_eval(x)))
        full_df.drop(columns=column,inplace=True)

        # Extraindo os countrys e data de coleta
        column = 'INSDSeq_feature-table'
        full_df['country'] = full_df[column].apply(lambda x: extract_country(cell=ast.literal_eval(x))) 
        full_df['collection_date'] = full_df['INSDSeq_feature-table'].apply(lambda x: extract_collection_date(cell=ast.literal_eval(x)))
        full_df.drop(columns=column, inplace=True)

        # Explodindo a coluna pubmed
        full_df = full_df.explode(column='pubmed')

        # Salvando o dataframe em um parquet
        cleaned_data_path = os.path.join(os.path.dirname(parquet_file_path),'cleaned_silver.parquet')
        full_df.to_parquet(cleaned_data_path,index=False)

    def load_data(parquet_file:str):
        data = pd.read_parquet(parquet_file)
        mysql_hook = MySqlHook(mysql_conn_id='mysql_arbovirus')
        sqlalchemy_engine = mysql_hook.get_sqlalchemy_engine()
        data.to_sql(name='processed_sequences',con=sqlalchemy_engine,if_exists='replace')
