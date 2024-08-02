import os
import requests
from ftplib import FTP
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, unquote

def download_article_from_pmcid_ftp(pmcid: str, save_in: str = '.') -> bool:
    BASE_URL = 'https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi'
    params = {"id": pmcid}

    try:
        print(f"Efetuando requisição para o pmcid {pmcid}.")
        response = requests.get(url=BASE_URL, params=params)
        response.raise_for_status()
    except requests.RequestException as err:
        print(f"Erro ao executar a requisição do pmcid {pmcid} -> {err}")
        return False

    print("Verificando se há arquivo para baixar")
    root = ET.fromstring(response.content)
    if 'error' in [child.tag for child in root]:
        print("Baixar via requisição não funcionou, será usado webscraping.")
        return False

    record = root.find("records").find("record")
    lastest_record = record.findall("link")[0].attrib
    ftp_url = lastest_record['href']
    ftp_format_file = lastest_record['format']

    parsed_url = urlparse(ftp_url)
    ftp_host = parsed_url.hostname
    ftp_path = parsed_url.path
    filename = unquote(ftp_path.split("/")[-1])
    filename_path = f'{save_in}/{ftp_format_file}/{filename}'

    print("Conectando ao servidor FTP")
    with FTP(ftp_host) as ftp:
        ftp.login()

        print("Baixando o arquivo")
        os.makedirs(f'{save_in}/{ftp_format_file}', exist_ok=True)
        try:
            with open(filename_path, 'wb') as file:
                ftp.retrbinary("RETR " + ftp_path, file.write)
            print(f'Successfully downloaded {filename}')
            return True
        except Exception as err:
            print(f"Erro ao baixar ou salvar o arquivo -> {err}")
            return False

def download_article_from_pmcid_data_scraping(pmcid: str, output_dir: str = 'data/articles/pdf/') -> None:
    '''
    Baixa um artigo do PMC usando um PMC ID e salva-o como um arquivo PDF.

    Parâmetros:
        - pmcid (str): PMC ID do artigo.
        - output_dir (str): Diretório para salvar o arquivo PDF (padrão: 'data/articles/pdf/').

    Retorna:
        - None
    '''
    os.makedirs(output_dir, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        url = f'https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/'
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        pdf_element = soup.find(class_='pdf-link other_item').find(class_='int-view')

        download_url = 'https://www.ncbi.nlm.nih.gov' + pdf_element['href']
        pdf_response = requests.get(download_url, headers=headers)
        pdf_response.raise_for_status()

        with open(f'{output_dir}/{pmcid}.pdf', 'wb') as pdf_file:
            pdf_file.write(pdf_response.content)
        print(f'Successfully downloaded {pmcid}.pdf')

    except requests.exceptions.RequestException as e:
        print(f'Failed to download {pmcid}: {e}')
        raise e

    except Exception as e:
        print(f'An error occurred for {pmcid}: {e}')
        raise e

def download_article_from_pmcid(pmcid: str, save_in: str = '.') -> None:
    """
    Tenta baixar um artigo do PMC usando dois métodos diferentes: web scraping e requisições HTTP diretas.

    O método primeiro tenta baixar o artigo via web scraping. Se falhar, tenta baixar via requisição HTTP direta.
    Se ambos os métodos falharem, o erro é registrado e o PMC ID é salvo em um arquivo de log.

    Args:
        pmcid (str): O ID do artigo no PubMed Central (PMC) a ser baixado.
        save_in (str): Diretório para salvar o arquivo (padrão: diretório atual).

    Raises:
        Exception: Propaga a exceção se ambos os métodos falharem.
    """
    try:
        # Primeiro método: web scraping
        try:
            download_article_from_pmcid_data_scraping(pmcid=pmcid, output_dir=save_in)
        except Exception as e:
            print(f"Erro ao baixar o artigo via web scraping {pmcid} -> {e}")
            # Segundo método: requisição
            try:
                download_article_from_pmcid_ftp(pmcid=pmcid, save_in=save_in)
            except Exception as e:
                print(f"Erro ao baixar o artigo via requisição {pmcid} -> {e}")
                raise e
    except Exception as err:
        print(f"Erro inesperado ao baixar o artigo (usando ambos os métodos) {pmcid} -> {err}")
        with open(os.path.join('data/', 'Artigos_nao_baixados.txt'), 'a') as fail_request:
            fail_request.write(f'(Erro inesperado) {pmcid} - Erro: {err}\n')