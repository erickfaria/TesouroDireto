import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from typing import Dict

class TreasuryRateClassifier:
    """
    A class to classify Treasury Direct IPCA+ bond rates into different cycles (high, medium, low).

    Attributes:
    ----------
    df : pd.DataFrame
        The dataframe containing Treasury Direct data.
    ts : pd.Series
        The time series of the bond rates.
    clusters : np.ndarray
        The cluster assignments for the bond rates.
    labels : Dict[int, str]
        The mapping of cluster indices to labels (high, medium, low).
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initializes the TreasuryRateClassifier with the given dataframe.

        Parameters:
        ----------
        df : pd.DataFrame
            The dataframe containing Treasury Direct data.
        """
        self.df = df
        self.ts = self._prepare_time_series()
        self.clusters = None
        self.labels = None

    def _prepare_time_series(self) -> pd.Series:
        """
        Prepares the time series for the bond rates.

        Returns:
        -------
        pd.Series
            The prepared time series of the bond rates.
        """
        self.df['Data Base'] = pd.to_datetime(self.df['Data Base'])
        ts = self.df[self.df['Tipo Titulo'] == 'Tesouro IPCA+']
        ts = ts.set_index('Data Base')['Taxa Compra Manha'].sort_index()
        ts = ts[~ts.index.duplicated(keep='last')]
        ts = ts.asfreq('D').ffill()
        return ts

    def normalize_data(self) -> pd.Series:
        """
        Normalizes the time series data.

        Returns:
        -------
        pd.Series
            The normalized time series.
        """
        scaler = StandardScaler()
        return scaler.fit_transform(self.ts.values.reshape(-1, 1))

    def apply_clustering(self, n_clusters: int = 3) -> None:
        """
        Applies K-means clustering to classify the bond rates into cycles.

        Parameters:
        ----------
        n_clusters : int, optional
            The number of clusters for K-means (default is 3).
        """
        ts_normalized = self.normalize_data()
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        self.clusters = kmeans.fit_predict(ts_normalized)

    def label_clusters(self) -> None:
        """
        Labels the clusters as 'high', 'medium', and 'low' based on their mean values.
        """
        ts_clusters = pd.DataFrame(self.ts)
        ts_clusters['Cluster'] = self.clusters
        cluster_means = ts_clusters.groupby('Cluster')['Taxa Compra Manha'].mean().sort_values()
        self.labels = {cluster_means.index[0]: 'Baixa', cluster_means.index[1]: 'Média', cluster_means.index[2]: 'Alta'}
        ts_clusters['Label'] = ts_clusters['Cluster'].map(self.labels)
        self.ts_clusters = ts_clusters

    def classify_current_cycle(self) -> str:
        """
        Classifies the current cycle of the bond rates.

        Returns:
        -------
        str
            The label of the current cycle (high, medium, low).
        """
        current_label = self.ts_clusters['Label'].iloc[-1]
        return current_label

    def plot_classification(self) -> None:
        """
        Plots the classified bond rates.
        """
        plt.figure(figsize=(12, 6))
        for label in self.ts_clusters['Label'].unique():
            subset = self.ts_clusters[self.ts_clusters['Label'] == label]
            plt.plot(subset.index, subset['Taxa Compra Manha'], label=label)

        plt.title('Classification of Treasury IPCA+ Bond Rates')
        plt.xlabel('Date')
        plt.ylabel('Purchase Rate')
        plt.legend()
        plt.show()