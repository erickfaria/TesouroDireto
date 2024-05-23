import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

class TesouroDiretoDatasets:
    """
    A class for downloading and saving datasets from the Brazilian Treasury Direct (Tesouro Direto).

    This class provides methods to download data from the Tesouro Direto directly from URLs
    provided by Tesouro Transparente and save them locally in Parquet format with Snappy compression.

    Methods:
        download_all_data(base_filepath): Downloads and saves all datasets in Parquet format.
    """
    def __init__(self):
        self.urls = {
            "estoque": "https://www.tesourotransparente.gov.br/ckan/dataset/4d4dac3b-96d2-4011-92c9-ddf7d8392622/resource/650cdc18-0513-4bb1-9222-003ad1c11ac7/download/EstoqueTesouroDireto.csv",
            "vendas": "https://www.tesourotransparente.gov.br/ckan/dataset/f0468ecc-ae97-4287-89c2-6d8139fb4343/resource/e5f90e3a-8f8d-4895-9c56-4bb2f7877920/download/VendasTesouroDireto.csv",
            "taxas": "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv",
            "operacoes": "https://www.tesourotransparente.gov.br/ckan/dataset/78739a33-4d2f-4e35-88fd-65f1ccbe81c4/resource/4100d614-d1ad-4b62-9435-84f7943e46f3/download/OperacoesTesouroDireto.csv",
            "investidores": "https://www.tesourotransparente.gov.br/ckan/dataset/48a7fd9d-78e5-43cb-bcba-6e7dcaf2d741/resource/0fd2ac86-4673-46c0-a889-b46224ade563/download/InvestidoresTesouroDireto.csv"
        }

    def download_csv(self, url, filepath):
        """
        Downloads a CSV file from the given URL and saves it locally.

        Args:
            url (str): The URL to download the CSV file from.
            filepath (str): The local path to save the downloaded CSV file.
        """
        response = requests.get(url)
        with open(filepath, 'wb') as file:
            file.write(response.content)

    def convert_to_parquet(self, csv_filepath, parquet_filepath, date_columns):
        """
        Converts a CSV file to Parquet format with Snappy compression.

        Args:
            csv_filepath (str): The path to the CSV file.
            parquet_filepath (str): The path to save the Parquet file.
            date_columns (list): List of columns to parse as dates.
        """
        df = pd.read_csv(csv_filepath, delimiter=';', decimal=',', dayfirst=True, parse_dates=date_columns, date_parser=lambda x: pd.to_datetime(x, format='%d/%m/%Y'))
        df.to_parquet(parquet_filepath, compression='snappy')

    def download_and_convert(self, key, url, base_filepath, date_columns):
        """
        Downloads a CSV file and converts it to Parquet format.

        Args:
            key (str): The dataset key.
            url (str): The URL to download the CSV file from.
            base_filepath (str): The base file path to save the Parquet file.
            date_columns (list): List of columns to parse as dates.
        """
        csv_filepath = f"{base_filepath}_{key}.csv"
        parquet_filepath = f"{base_filepath}_{key}.parquet"
        self.download_csv(url, csv_filepath)
        self.convert_to_parquet(csv_filepath, parquet_filepath, date_columns)
        os.remove(csv_filepath)

    def download_all_data(self, base_filepath):
        """
        Downloads and saves all datasets in Parquet format in parallel.

        Args:
            base_filepath (str): The base file path to save the Parquet files.
        """
        date_columns = {
            "estoque": ['Vencimento do Titulo', 'Mes Estoque'],
            "vendas": ['Vencimento do Titulo', 'Data Venda'],
            "taxas": ['Data Vencimento', 'Data Base'],
            "operacoes": [],
            "investidores": []
        }

        tasks = []
        with ThreadPoolExecutor() as executor:
            for key, url in self.urls.items():
                tasks.append(executor.submit(self.download_and_convert, key, url, base_filepath, date_columns[key]))
            
            for _ in tqdm(tasks, desc="Downloading and converting datasets"):
                _ = [task.result() for task in tasks]