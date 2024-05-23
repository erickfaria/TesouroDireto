import pandas as pd
import requests
import zipfile
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, Future
from tqdm import tqdm
from typing import List, Optional

class TesouroDiretoDatasets:
    """
    A class for downloading and saving datasets from the Brazilian Treasury Direct (Tesouro Direto).

    This class provides methods to download data from Tesouro Transparente URLs and save them
    locally in Parquet format with Snappy compression.

    Attributes:
        urls (dict): A dictionary containing URLs for different datasets.

    Methods:
        download_file(url: str, filepath: str) -> None:
            Downloads a file from the given URL and saves it locally.

        convert_csv_to_parquet(csv_filepath: str, parquet_filepath: str, date_columns: List[str], encoding: str = 'latin1', compression: Optional[str] = None) -> None:
            Converts a CSV or compressed CSV file to Parquet format with Snappy compression.

        clean_up(path: str) -> None:
            Removes a file or directory.

        process_estoque(base_filepath: str) -> None:
            Processes the 'Estoque' dataset.

        process_vendas(base_filepath: str) -> None:
            Processes the 'Vendas' dataset.

        process_taxas(base_filepath: str) -> None:
            Processes the 'Taxas' dataset.

        process_operacoes(base_filepath: str) -> None:
            Processes the 'Operações' dataset.

        process_investidores(base_filepath: str) -> None:
            Processes the 'Investidores' dataset.

        find_csv_file(directory: str) -> Optional[str]:
            Finds the first CSV file in a directory structure.

        download_all_data(base_filepath: str) -> None:
            Downloads and saves all datasets in Parquet format in parallel.
    """
    
    def __init__(self) -> None:
        """Initializes the URLs for each dataset."""
        self.urls = {
            "estoque": "https://www.tesourotransparente.gov.br/ckan/dataset/4d4dac3b-96d2-4011-92c9-ddf7d8392622/resource/650cdc18-0513-4bb1-9222-003ad1c11ac7/download/EstoqueTesouroDireto.csv",
            "vendas": "https://www.tesourotransparente.gov.br/ckan/dataset/f0468ecc-ae97-4287-89c2-6d8139fb4343/resource/e5f90e3a-8f8d-4895-9c56-4bb2f7877920/download/VendasTesouroDireto.csv",
            "taxas": "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv",
            "operacoes": "https://www.tesourotransparente.gov.br/ckan/dataset/78739a33-4d2f-4e35-88fd-65f1ccbe81c4/resource/745a5e1f-d07a-45bb-b0cb-4d4e7fc685bd/download/OperacoesTesouroDireto.zip",
            "investidores": "https://www.tesourotransparente.gov.br/ckan/dataset/48a7fd9d-78e5-43cb-bcba-6e7dcaf2d741/resource/bc99d7cc-e658-4b65-950e-cbf4bb5ed6d2/download/InvestidoresTesouroDireto.gz"
        }

    def download_file(self, url: str, filepath: str) -> None:
        """
        Downloads a file from the given URL and saves it locally.

        Args:
            url (str): The URL to download the file from.
            filepath (str): The local path to save the downloaded file.
        """
        response = requests.get(url)
        response.raise_for_status()
        with open(filepath, 'wb') as file:
            file.write(response.content)

    def convert_csv_to_parquet(self, csv_filepath: str, parquet_filepath: str, date_columns: List[str], encoding: str = 'latin1', compression: Optional[str] = None) -> None:
        """
        Converts a CSV or compressed CSV file to Parquet format with Snappy compression.

        Args:
            csv_filepath (str): The path to the CSV or compressed CSV file.
            parquet_filepath (str): The path to save the Parquet file.
            date_columns (List[str]): List of columns to parse as dates.
            encoding (str): Encoding for the CSV file (default is 'latin1').
            compression (Optional[str]): Compression type for the CSV file (default is None).
        """
        df = pd.read_csv(csv_filepath, delimiter=';', decimal=',', dayfirst=True, parse_dates=date_columns, date_format='%d/%m/%Y', encoding=encoding, compression=compression)
        df.to_parquet(parquet_filepath, compression='snappy')

    def clean_up(self, path: str) -> None:
        """
        Removes a file or directory.

        Args:
            path (str): The path to the file or directory to remove.
        """
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)

    def process_estoque(self, base_filepath: str) -> None:
        """
        Processes the 'Estoque' dataset: downloads, converts to Parquet, and cleans up temporary files.

        Args:
            base_filepath (str): The base file path to save the Parquet file.
        """
        csv_filepath = f"{base_filepath}_estoque.csv"
        parquet_filepath = f"{base_filepath}_estoque.parquet"
        self.download_file(self.urls["estoque"], csv_filepath)
        self.convert_csv_to_parquet(csv_filepath, parquet_filepath, ['Vencimento do Titulo', 'Mes Estoque'])
        self.clean_up(csv_filepath)

    def process_vendas(self, base_filepath: str) -> None:
        """
        Processes the 'Vendas' dataset: downloads, converts to Parquet, and cleans up temporary files.

        Args:
            base_filepath (str): The base file path to save the Parquet file.
        """
        csv_filepath = f"{base_filepath}_vendas.csv"
        parquet_filepath = f"{base_filepath}_vendas.parquet"
        self.download_file(self.urls["vendas"], csv_filepath)
        self.convert_csv_to_parquet(csv_filepath, parquet_filepath, ['Vencimento do Titulo', 'Data Venda'])
        self.clean_up(csv_filepath)

    def process_taxas(self, base_filepath: str) -> None:
        """
        Processes the 'Taxas' dataset: downloads, converts to Parquet, and cleans up temporary files.

        Args:
            base_filepath (str): The base file path to save the Parquet file.
        """
        csv_filepath = f"{base_filepath}_taxas.csv"
        parquet_filepath = f"{base_filepath}_taxas.parquet"
        self.download_file(self.urls["taxas"], csv_filepath)
        self.convert_csv_to_parquet(csv_filepath, parquet_filepath, ['Data Vencimento', 'Data Base'])
        self.clean_up(csv_filepath)

    def process_operacoes(self, base_filepath: str) -> None:
        """
        Processes the 'Operações' dataset: downloads, extracts, converts to Parquet, and cleans up temporary files.

        Args:
            base_filepath (str): The base file path to save the Parquet file.
        """
        zip_filepath = f"{base_filepath}_operacoes.zip"
        extract_dir = f"{base_filepath}_operacoes"
        parquet_filepath = f"{base_filepath}_operacoes.parquet"
        self.download_file(self.urls["operacoes"], zip_filepath)
        
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        csv_filepath = self.find_csv_file(extract_dir)
        if csv_filepath:
            self.convert_csv_to_parquet(csv_filepath, parquet_filepath, ['Data da Operacao', 'Vencimento do Titulo'])
        self.clean_up(zip_filepath)
        self.clean_up(extract_dir)

    def process_investidores(self, base_filepath: str) -> None:
        """
        Processes the 'Investidores' dataset: downloads, converts to Parquet, and cleans up temporary files.

        Args:
            base_filepath (str): The base file path to save the Parquet file.
        """
        gz_filepath = f"{base_filepath}_investidores.gz"
        parquet_filepath = f"{base_filepath}_investidores.parquet"
        self.download_file(self.urls["investidores"], gz_filepath)
        self.convert_csv_to_parquet(gz_filepath, parquet_filepath, ['Data de Adesao'], compression='gzip')
        self.clean_up(gz_filepath)

    def find_csv_file(self, directory: str) -> Optional[str]:
        """
        Finds the first CSV file in a directory structure.

        Args:
            directory (str): The directory to search in.

        Returns:
            Optional[str]: The path to the CSV file or None if not found.
        """
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.csv'):
                    return os.path.join(root, file)
        return None

    def download_all_data(self, base_filepath: str) -> None:
        """
        Downloads and saves all datasets in Parquet format in parallel.

        Args:
            base_filepath (str): The base file path to save the Parquet files.
        """
        tasks = [
            ("estoque", self.process_estoque),
            ("vendas", self.process_vendas),
            ("taxas", self.process_taxas),
            ("operacoes", self.process_operacoes),
            ("investidores", self.process_investidores)
        ]

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(task[1], base_filepath) for task in tasks]
            for _ in tqdm(futures, desc="Downloading and converting datasets"):
                for future in futures:
                    future.result()