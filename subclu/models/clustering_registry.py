"""
Registry for clustering algos to use

Use config to call specific model
"""
from sklearn.cluster import KMeans, DBSCAN, OPTICS, AgglomerativeClustering
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import Normalizer
from sklearn.metrics import (
    adjusted_mutual_info_score, adjusted_rand_score,
    homogeneity_score,
)


D_CLUSTER_PIPELINE = {
    'normalize': {
        'Normalizer': Normalizer,
    },

    'reduce': {
        'TruncatedSVD': TruncatedSVD,
    }
}


D_CLUSTER_MODELS = {
    'AgglomerativeClustering': AgglomerativeClustering,
    'KMeans': KMeans,
    'DBSCAN': DBSCAN,
    'OPTICS': OPTICS,
}


D_CLUSTER_METRICS_WITH_KNOWN_LABELS = {
    'adjusted_mutual_info_score': adjusted_mutual_info_score,
    'adjusted_rand_score': adjusted_rand_score,
    'homogeneity_score': homogeneity_score,
}


#
# ~ fin
#
