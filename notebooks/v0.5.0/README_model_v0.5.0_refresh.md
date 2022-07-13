# Instructions to run v0.5.0 model

Running the model can be broken down into these general steps:
- Pull data from BigQuery
    - TODO(djb): Link to Queries

- Vectorize text (convert into embeddings)
    - Subreddit metadata
        - `djb_01.0-2022-06-29-vectorize_subreddit_metadata.ipynb`
    - Post + Comments text
        - `djb_02.0-2022-06-29-vectorize_combined_post_and_comment_text.ipynb`
    
- Aggregate post embeddings
    - 2 levels of aggregation:
        - **Post-level-agg**: Post + Comments + Subreddit metadata
        - **Subreddit-level-agg**: MEAN(Post-level-agg) for all posts in each subreddit
    - Notebook:
        - `djb_04.00-2022-06-29-aggregate_v050_posts_comments_and_subreddit_meta_pandas.ipynb`

- **Subreddit level**: Clustering 
    - Run hyperparam search:
        - `djb_05.01-2022-07-05-run_v0.5.0_subreddit_level_clustering_hydra_parallel.ipynb`
    
    - Select best model from hyper param search
        - `djb_06.01-2022-07-06-select_best_model_for_v0.5.0.ipynb`
    - Upload clustering outputs to BigQuery
        - `TBD`

- **Subreddit level**: Create top 100 ANN (approximate nearest neighbors)
    - Create index & topK file
        - `TBD`
    - Upload topk file to BigQuery
        - `TBD`
    




