"""
Registry for clustering algos to use

Use config to call specific model
"""
from sklearn.cluster import KMeans, DBSCAN, OPTICS, AgglomerativeClustering


D_MODELS_CLUSTERING = {
    'AgglomerativeClustering': AgglomerativeClustering,
    'KMeans': KMeans,
    'DBSCAN': DBSCAN,
    'OPTICS': OPTICS,
}
