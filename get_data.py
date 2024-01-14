import pandas as pd

class TesouroDiretoDatasets:
    """
    A class for accessing and reading datasets from the Brazilian Treasury Direct (Tesouro Direto).

    This class provides methods to load data from the Tesouro Direto directly from URLs
    provided by Tesouro Transparente, eliminating the need to manually download files.

    Methods:
        ler_estoque(): Returns data about the stock of Tesouro Direto.
        ler_vendas(): Returns data about the sales of Tesouro Direto.
        ler_taxas(): Returns data about the rates of Tesouro Direto.
        ler_operacoes(): Returns data about the operations of Tesouro Direto.
        ler_investidores(): Returns data about the investors of Tesouro Direto.
    """
    def __init__(self):
        self.url_estoque = "https://www.tesourotransparente.gov.br/ckan/dataset/4d4dac3b-96d2-4011-92c9-ddf7d8392622/resource/650cdc18-0513-4bb1-9222-003ad1c11ac7/download/EstoqueTesouroDireto.csv"
        self.url_vendas = "https://www.tesourotransparente.gov.br/ckan/dataset/f0468ecc-ae97-4287-89c2-6d8139fb4343/resource/e5f90e3a-8f8d-4895-9c56-4bb2f7877920/download/VendasTesouroDireto.csv"
        self.url_td_tax = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"
        self.url_td_op = "https://www.tesourotransparente.gov.br/ckan/dataset/78739a33-4d2f-4e35-88fd-65f1ccbe81c4/resource/4100d614-d1ad-4b62-9435-84f7943e46f3/download/OperacoesTesouroDireto.csv"
        self.url_inves_td = "https://www.tesourotransparente.gov.br/ckan/dataset/48a7fd9d-78e5-43cb-bcba-6e7dcaf2d741/resource/0fd2ac86-4673-46c0-a889-b46224ade563/download/InvestidoresTesouroDireto.csv"

    def read_estoque(self):
        """
        Reads and returns data about the stock of Tesouro Direto.

        Returns:
            DataFrame: A Pandas DataFrame containing the stock data of Tesouro Direto.
        """
        return pd.read_csv(self.url_estoque)

    def read_vendas(self):
        """
        Reads and returns data about the sales of Tesouro Direto.

        Returns:
            DataFrame: A Pandas DataFrame containing the sales data of Tesouro Direto.
        """
        return pd.read_csv(self.url_vendas)

    def read_taxas(self):
        """
        Reads and returns data about the rates (taxas) of Tesouro Direto.

        This method fetches the dataset from the specified URL and loads it into a Pandas DataFrame.
        The dataset contains information about the rates associated with different securities in the Tesouro Direto program.
        It handles custom delimiters, decimal formats, and date formats.

        Returns:
            DataFrame: A Pandas DataFrame containing the rates data of Tesouro Direto.
        """
        # Reading the CSV file with specific delimiter and decimal separator
        df = pd.read_csv(self.url_td_tax, delimiter=';', decimal=',')

        # Converting date columns to the correct date format
        df['Data Vencimento'] = pd.to_datetime(df['Data Vencimento'], format='%d/%m/%Y')
        df['Data Base'] = pd.to_datetime(df['Data Base'], format='%d/%m/%Y')

        return df



    def read_operacoes(self):
        """
        Reads and returns data about the operations (operacoes) of Tesouro Direto.

        This method accesses the operations dataset, detailing the various transactions
        and operations carried out in the Tesouro Direto program. The data is loaded
        into a Pandas DataFrame directly from the provided URL.

        Returns:
            DataFrame: A Pandas DataFrame containing the operations data of Tesouro Direto.
        """
        return pd.read_csv(self.url_td_op)

    def read_investidores(self):
        """
        Reads and returns data about the investors (investidores) of Tesouro Direto.

        This method retrieves data regarding the investors in the Tesouro Direto program.
        The dataset includes demographic and other relevant information about the investors,
        and is loaded into a Pandas DataFrame from the specified URL.

        Returns:
            DataFrame: A Pandas DataFrame containing the investors data of Tesouro Direto.
        """
        return pd.read_csv(self.url_inves_td)
