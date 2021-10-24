"""
Utils for clustering.

sklearn doesn't have tools out of the box to introspect hierarchical clusters
and scipy's tools need a little tweaking (like these fxns).

reference:
- Describe different ways to use scipy's tools
    - https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
"""
from typing import Union

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from scipy.cluster.hierarchy import dendrogram



def create_linkage_for_dendrogram(model) -> pd.DataFrame:
    """
    Create linkage matrix from an Sklearn model (e.g., AgglomerativeCluster)
    We can use this matrix to plot a dendogram and create cluster labels using fcluster.
    """
    # create the counts of samples under each node
    counts = np.zeros(model.children_.shape[0])
    n_samples = len(model.labels_)
    for i, merge in enumerate(model.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = pd.DataFrame(
        np.column_stack(
            [model.children_,
             model.distances_,
             counts]
        ),
        columns=['children_0', 'children_1', 'distance', 'count'],
    ).astype({
        'children_0': int,
        'children_1': int,
        'distance': float,
        'count': int,
    })

    return linkage_matrix


def fancy_dendrogram(
        Z: Union[pd.DataFrame, np.ndarray],
        max_d: float = None,
        annotate_above: float = 0,
        plot_title: str = 'Hierarchical Clustering Dendrogram (truncated)',
        xlabel: str = 'item index OR (cluster size)',
        ylabel: str = 'distance',
        dist_fontsize: float = 13,
        **kwargs
):
    """Wrapper around dendogram diagram that adds distances & cut off"""
    if max_d and 'color_threshold' not in kwargs:
        kwargs['color_threshold'] = max_d

    ddata = dendrogram(Z, **kwargs)

    if not kwargs.get('no_plot', False):
        plt.title(plot_title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        for i, d, c in zip(ddata['icoord'], ddata['dcoord'], ddata['color_list']):
            x = 0.5 * sum(i[1:3])
            y = d[1]
            if y > annotate_above:
                plt.plot(x, y, 'o', c=c)
                # original format: "%.3g"
                plt.annotate("%.1f" % y, (x, y), xytext=(0, -5),
                             textcoords='offset points',
                             fontsize=dist_fontsize,
                             va='top', ha='center')
        if max_d:
            plt.axhline(y=max_d, c='k')
    return ddata




#
# ~ fin
#
