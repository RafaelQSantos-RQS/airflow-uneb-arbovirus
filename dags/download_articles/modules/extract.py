import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

BASE_URL = 'https://pubmed.ncbi.nlm.nih.gov'
def extract_pcmid_from_pubmed(pubmed:str) -> dict:
    """
    Extrai o PMCID (PubMed Central ID) associado a um número de acesso do PubMed.

    Parâmetros:
    - pubmed (str): O número de acesso do PubMed.

    Retorna:
    - dict: Um dicionário contendo as informações, incluindo o número de acesso do PubMed e o PMCID.

    Exceções:
    - requests.HTTPError: Caso ocorra um erro HTTP ao fazer a requisição à URL.
    - Exception: Para outros erros inesperados durante a execução.

    Exemplo:
    ```python
    result = extract_pcmid_from_pubmed('12345678')
    # Saída esperada: {'pubmed_accession_number': '12345678', 'pmcid': 'PMC1234567'}
    ```

    OBSERVAÇÃO:
    - A função acessa a página do PubMed correspondente ao número de acesso fornecido.
    - Extrai o PMCID da página, se disponível.
    - Retorna um dicionário com informações sobre o número de acesso do PubMed e o PMCID.
    - Se o PMCID não estiver disponível, será retornado como `None`.
    - Caso ocorram erros durante a requisição ou análise da página, exceções são levantadas.
    """
    url = f'{BASE_URL}/{pubmed}'
    try:
        response = requests.get(url=url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content,"html.parser")
        tag = soup.find(attrs={"data-ga-action": "PMCID"})
        if tag:
            pmcid = tag.text.strip()
        else:
            pmcid = None
        result = {'pubmed_accession_number': str(pubmed), 'pmcid': pmcid}
        return result
    except requests.RequestException as err:
        print(f"Erro ao fazer requisição a url ({url}) -> {err}")
        raise err
    except Exception as err:
        print(f"Erro inesperado -> {err}")
        raise err