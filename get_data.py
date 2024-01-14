import pandas as pd

class TesouroDiretoDatasets:
    def __init__(self):
        self.url_estoque = "https://www.tesourotransparente.gov.br/ckan/dataset/4d4dac3b-96d2-4011-92c9-ddf7d8392622/resource/650cdc18-0513-4bb1-9222-003ad1c11ac7/download/EstoqueTesouroDireto.csv"
        self.url_vendas = "https://www.tesourotransparente.gov.br/ckan/dataset/f0468ecc-ae97-4287-89c2-6d8139fb4343/resource/e5f90e3a-8f8d-4895-9c56-4bb2f7877920/download/VendasTesouroDireto.csv"
        self.url_td_tax = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"
        self.url_td_op = "https://www.tesourotransparente.gov.br/ckan/dataset/78739a33-4d2f-4e35-88fd-65f1ccbe81c4/resource/4100d614-d1ad-4b62-9435-84f7943e46f3/download/OperacoesTesouroDireto.csv"
        self.url_inves_td = "https://www.tesourotransparente.gov.br/ckan/dataset/48a7fd9d-78e5-43cb-bcba-6e7dcaf2d741/resource/0fd2ac86-4673-46c0-a889-b46224ade563/download/InvestidoresTesouroDireto.csv"

    def ler_estoque(self):
        return pd.read_csv(self.url_estoque)

    def ler_vendas(self):
        return pd.read_csv(self.url_vendas)

    def ler_taxas(self):
        return pd.read_csv(self.url_td_tax)

    def ler_operacoes(self):
        return pd.read_csv(self.url_td_op)

    def ler_investidores(self):
        return pd.read_csv(self.url_inves_td)
