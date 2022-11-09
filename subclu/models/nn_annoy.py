"""
Get nearest neighbors with ANNOY
"""
import logging
from logging import info
from typing import Tuple

import dask
import annoy
import pandas as pd
from tqdm import tqdm


class AnnoyIndex():
    def __init__(
            self,
            df_vectors,
            index_cols: iter = 'default',
            metric: str = 'angular',
            n_trees: int = 900,
    ):
        """it assumes that df_vectors has:
        - index = labels to use
        - columns = numeric vectors

        If search_k_trees=-1 -> search all the trees.

        Assumes index is a single column. Might get unexpected results if index is multi-index (multiple cols).
        Ideally it's subreddit_id because subreddit_name can change over time.

        We might need to convert vectors to float 32, if they're not already float32
        """
        if index_cols == 'default':
            index_cols = ['subreddit_id', 'subreddit_name']

        rows_, cols_ = df_vectors.drop(index_cols, axis=1).shape
        self.n_dimension = cols_
        self.n_rows = rows_

        self.metric = metric
        self.n_trees = n_trees
        self.vectors = df_vectors.drop(index_cols, axis=1).to_numpy()  # vectors.astype('float32')

        self.index = None
        self.index_labels = df_vectors[index_cols[0]].to_list()
        self.index_labels_name = index_cols[0]
        self.index_labels_df = df_vectors[index_cols].copy()

    def build(
            self,
    ) -> None:
        self.index = annoy.AnnoyIndex(self.n_dimension, self.metric)

        for i, vec in enumerate(self.vectors):
            self.index.add_item(i, vec)  # tolist() seems like busy work?
        self.index.build(self.n_trees, n_jobs=-1)

    def get_top_n_by_item(
            self,
            item_i: int,
            k=100,
            search_k: int = -1,
            include_distances: bool = True,
            append_i: bool = True,
            col_distance: str = 'distance',
            col_distance_rank: str = 'distance_rank',
            cosine_similarity: bool = False,
            col_cosine_similarity: str = 'cosine_similarity',
    ) -> pd.DataFrame:
        """
        We'll use this method to get the top_n items for each item in index
        Query by top item because we don't want to have to remove the item from its own
        query when we're building the top N neighbors

        Best to compute cosine similarity on all dfs rather than one at a time.
        from [github](https://github.com/spotify/annoy/issues/112#issuecomment-686513356)

        cosine_similarity = 1 - cosine_distance^2/2
        """
        indices = self.index.get_nns_by_item(
            item_i,
            k,
            search_k=search_k,
            include_distances=include_distances
        )

        suffixes_ = ('_a', '_b')
        if include_distances:
            df_results_ = (
                self.index_labels_df
                .merge(
                    pd.DataFrame(
                        {
                            self.index_labels_name: [self.index_labels[i] for i in indices[0]],
                            col_distance: indices[1],
                        }
                    ),
                    how='right',
                    on=self.index_labels_name,
                )
            )
        else:
            df_results_ = (
                self.index_labels_df
                .merge(
                    pd.DataFrame(
                        {
                            self.index_labels_name: [self.index_labels[i] for i in indices]
                        }
                    ),
                    how='right',
                    on=self.index_labels_name,
                )
            )

        # add col for rank so it's eaiser to filter by rank instead of only distances
        df_results_ = (
            df_results_.reset_index()
            .rename(columns={'index': col_distance_rank})
        )

        if append_i:
            df_i = (
                pd.DataFrame(
                    {f"{self.index_labels_name}": [self.index_labels[item_i]] * k,}
                )
                .merge(
                    self.index_labels_df,
                    how='left',
                    on=self.index_labels_name,
                )
            )
            df_i = df_i.rename(columns={c: f"{c}{suffixes_[0]}" for c in df_i.columns})

            l_dist_cols = [col_distance, col_distance_rank]
            df_nn = pd.concat(
                [
                    df_i,
                    df_results_.rename(columns={c: f"{c}{suffixes_[1]}" for c in df_results_.columns if c not in l_dist_cols})
                ],
                axis=1,
            )
        else:
            df_nn = df_results_

        if cosine_similarity & (self.metric == 'angular'):
            df_nn[col_cosine_similarity] = (
                    1 -
                    (df_nn[col_distance] ** 2) / 2
            )

        return df_nn

    def get_top_n_by_item_all_fast(
            self,
            k: int = 100,
            search_k: int = -1,
            include_distances: bool = True,
            append_i: bool = True,
            col_distance: str = 'distance',
            col_distance_rank: str = 'distance_rank',
            cosine_similarity: bool = True,
            col_cosine_similarity: str = 'cosine_similarity',
            tqdm_mininterval: int = 2,
            n_sample: int = None,
    ) -> pd.DataFrame:
        """
        Re-write to get top_n items for ALL items in parallel.
        This one reduces pandas overhead from some dict lookups and tries to do as
        many vectorized functions as possible.

        NOTE: using dask.delayed actually took longer than this optimized method

        We'll use the output of this table to create SQL table that can be shared & used by others.

        Args:
            k: Number of nearest neighbors
            search_k:
            include_distances: include distance (instead of only ANN IDs)
            append_i: if True, append the index values (sub names & sub IDs)
            col_distance:
                Name of column with distance value
            col_distance_rank:
                Name of column with distance rank. Rank=1 most similar
            cosine_similarity:
                Calculate cosine similarity? Default distance is euclidean
            col_cosine_similarity:
                Name of cosine similarity score
            tqdm_mininterval:
                seconds to wait before displaying TQDM refresh
            n_sample: set a limited number of sample to return
                use it to test function & outputs before running a long job

        Returns:
            pd.DataFrame with scores and other info
        """
        l_topk_dfs = list()
        if n_sample is not None:
            itms_to_get = range(min(n_sample, self.n_rows))
        else:
            itms_to_get = range(self.n_rows)

        for i in tqdm(
                itms_to_get,
                ascii=True,
                mininterval=tqdm_mininterval,
        ):
            # First get the tuples from ANNOY. These are SUPER FAST!
            # Note k+1 because this method returns self as most similar
            i_nn: Tuple[int, float] = self.index.get_nns_by_item(
                i,
                k + 1,
                search_k=search_k,
                include_distances=include_distances
            )
            # We should skip the first item because the most similar one is always self
            l_topk_dfs.append(
                pd.DataFrame(
                    {
                        'seed_ix': [i] * k,
                        'nn_ix': i_nn[0][1:],
                        col_distance: i_nn[1][1:],
                        col_distance_rank: [v for v in range(1, k + 1)]
                    },
                )
            )
        info(f"Start combining all ANNs into a df...")
        df_nn_top = pd.concat(l_topk_dfs, ignore_index=True)
        info(f"{df_nn_top.shape} <- df_nn_top shape")

        # TODO(djb): add sub names & IDs
        if append_i:
            info(f"Adding index labels (subreddit ID & Name)")
            df_labels_reset_index = self.index_labels_df.copy().reset_index(drop=True)
            prefix_similar_sub = 'similar'

            # append IDs & names for seed & nn (nearest neighbors)
            # NOTE: using .merge() based on index_labels_df is 100x (or more) faster than
            #   pd.Series().replace() with a dictionary(!!)
            df_nn_top = (
                df_labels_reset_index
                .merge(
                    df_nn_top,
                    how='right',
                    left_index=True,
                    right_on='seed_ix'
                )
                .merge(
                    df_labels_reset_index.rename(
                        columns={c: f"{prefix_similar_sub}_{c}" for c in df_labels_reset_index.columns}),
                    how='left',
                    left_on='nn_ix',
                    right_index=True,
                )
            )
            info(f"Done adding index names")
            info(f"{df_nn_top.shape} <- df_nn_top shape")

        if cosine_similarity:
            # from: https://github.com/spotify/annoy/issues/112#issuecomment-686513356
            # ```
            # cosine_similarity = 1 - cosine_distance^2/2
            # ```
            if self.metric == 'angular':
                info(f"Calculating cosine similarity...")
                df_nn_top[col_cosine_similarity] = (
                        1 -
                        (df_nn_top['distance'] ** 2) / 2
                )
            else:
                logging.error(
                    f"Cannont calculate cosine similarity because metric is not `angular`"
                    f"\nInput metric: {self.metric}"
                )

        return df_nn_top

    def get_top_n_by_item_all(
            self,
            k=100,
            search_k: int = -1,
            include_distances: bool = True,
            append_i: bool = True,
            col_distance: str = 'distance',
            col_distance_rank: str = 'distance_rank',
            cosine_similarity: bool = True,
            col_cosine_similarity: str = 'cosine_similarity',
            tqdm_mininterval: int = 2,
    ):
        """Convenience method to get top_n items for ALL items

        We'll use the output of this table to create SQL table that can be shared & used by others.
        """
        l_nn_dfs = list()

        for i in tqdm(
                range(self.n_rows),
                ascii=True,
                mininterval=tqdm_mininterval,
        ):
            l_nn_dfs.append(
                self.get_top_n_by_item(
                    i,
                    k=k,
                    search_k=search_k,
                    include_distances=include_distances,
                    append_i=append_i,
                    col_distance=col_distance,
                    col_distance_rank=col_distance_rank,
                    cosine_similarity=False,
                )
            )

        df_full = pd.concat(l_nn_dfs, axis=0, ignore_index=True)
        df_full = df_full[df_full[col_distance_rank] != 0]

        if cosine_similarity & (self.metric == 'angular'):
            df_full[col_cosine_similarity] = (
                    1 -
                    (df_full[col_distance] ** 2) / 2
            )

        logging.info(f"{df_full.shape} <- df_top_items shape")
        return df_full


