"""
Registry for clustering algos to use

Use config to call specific model
"""
from sklearn.cluster import KMeans, DBSCAN, OPTICS, AgglomerativeClustering
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import Normalizer



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


#
# ~ fin
#
