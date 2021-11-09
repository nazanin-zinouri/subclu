"""
Try creating aggregate embeddings in BigQuery.
Given that there's no plans for Spark and kubeflow is planned for end of
Q4, try BigQuery... maybe that can work in the meantime?

Colab notebook:
https://colab.research.google.com/drive/1JwyZi1dkj5ejdRMgATuePfwAK9CpiKSc#scrollTo=Xz2tlyIyMMnA
"""
def create_embeddings_agg_query(
        embeddings_table: str,


) -> str:
    """Create query to run on BigQuery to create a new table with aggregated embeddings

    I didn't create parametrized fxn because the raw SQL queries failed at the comment-level
    See SQL queries here:
    - data/v0.4.0_add_more_geo_and_active_subs/_05_a_create_agg_embeddings_comments_unweighted.sql
    - data/v0.4.0_add_more_geo_and_active_subs/_05_b_create_agg_embeddings_comments_weighted.sql
    """
    sql_create_agg_table = fr"""
        {embeddings_table}
    """
    return sql_create_agg_table


def scratch_test():
    """A dump for importnat code/queries from colab notebook
    https://colab.research.google.com/drive/1JwyZi1dkj5ejdRMgATuePfwAK9CpiKSc#scrollTo=Xz2tlyIyMMnA
    """
    # ==================
    # Testing queries
    # ===

    sql_get_comment_counts_per_post = r"""
    -- Get sample post-IDs to test mean embeddings query
    -- 
    -- Turns out my filtering screwed up somewhere and I actually kept some posts that have hundreds of comments 
    SELECT 
        post_id
        , COUNT(comment_id) AS comment_id_count
        , COUNT(DISTINCT comment_id) AS comment_id_unique_count
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments`
    
    GROUP BY 1
    ORDER BY 2 DESC
    ;
    """

    l_selected_posts = [
        # 1 comment, these should always return the same mean
        't3_oykxg3',
        't3_p9gf9t',

        # 2 comments, these might return a different mean, depending on upvote counts
        't3_p4watx',
        't3_pk0n9k',

        # 3 comments, these might return a different mean, depending on upvote counts
        't3_po67az',
        't3_pkged5',

        # 9
        't3_psgtvt',
        't3_oxpv38',
    ]

    sql_mean_baseline = r"""
    -- Get simple mean for test post_IDs
    SELECT 
        post_id
        , COUNT(comment_id)  AS comment_id_count
        , AVG(embeddings_0) AS embeddings_0
        , AVG(embeddings_1) AS embeddings_1
        , AVG(embeddings_2) AS embeddings_2
        , AVG(embeddings_3) AS embeddings_3
        , AVG(embeddings_4) AS embeddings_4
    
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments`
    
    WHERE post_id IN (
        # 1 comment, these should always return the same mean
        't3_oykxg3',
        't3_p9gf9t',
    
        # 2 comments, these might return a different mean, depending on upvote counts
        't3_p4watx',
        't3_pk0n9k',
    
        # 3 comments, these might return a different mean, depending on upvote counts
        't3_po67az',
        't3_pkged5',
    
        # 9
        't3_psgtvt',
        't3_oxpv38'
    )
    
    GROUP BY 1
    ORDER BY 2, 1
    ;
    """

    # ===
    # Get weighted average
    # ===

    # ---
    # let's make it easy to create SQL query:
    l_embedding_cols = ['embeddings_0', 'embeddings_1', 'embeddings_2', 'embeddings_3', 'embeddings_4']
    t_weighted_mean_cols = ""

    t_weighted_mean_template = """
        , (SUM({col_embedding} * {col_weights}) / 
           SUM({col_weights}))  AS {col_embedding}"""

    for c_ in l_embedding_cols:
        t_weighted_mean_cols += t_weighted_mean_template.format(
            col_embedding=c_,
            col_weights='ln_upvotes',
        )
    print(t_weighted_mean_cols)

    # ---
    # need to use `raw` string b/c I have a regex that breaks a regular SQL query
    sql_mean_weighted = r"""
    -- Get weighted mean, use CTE to split up logic for creating weights
    --  and prevent errors from (get aggregation of aggregations)
    WITH weights_for_mean AS (
        SELECT 
            cem.post_id
            , cem.comment_id
            , LN(3 + upvotes) AS ln_upvotes
    
        FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments` AS cem
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211004` AS cm
            ON cem.comment_id = cm.comment_id
    
        WHERE 1=1
            AND cem.post_id IN (
            # 1 comment, these should always return the same mean
            't3_oykxg3',
            't3_p9gf9t',
    
            # 2 comments, these might return a different mean, depending on upvote counts
            't3_p4watx',
            't3_pk0n9k',
    
            # 3 comments, these might return a different mean, depending on upvote counts
            't3_po67az',
            't3_pkged5',
    
            # 9
            't3_psgtvt',
            't3_oxpv38'
        )
    )
    
    SELECT 
        cem.post_id
        , COUNT(cem.comment_id)  AS comment_id_count
        , SUM(ln_upvotes) AS ln_upvotes_sum
    
        , (SUM(embeddings_0 * ln_upvotes) / 
           SUM(ln_upvotes))  AS embeddings_0
        , (SUM(embeddings_1 * ln_upvotes) / 
           SUM(ln_upvotes))  AS embeddings_1
        , (SUM(embeddings_2 * ln_upvotes) / 
           SUM(ln_upvotes))  AS embeddings_2
        , (SUM(embeddings_3 * ln_upvotes) / 
           SUM(ln_upvotes))  AS embeddings_3
        , (SUM(embeddings_4 * ln_upvotes) / 
           SUM(ln_upvotes))  AS embeddings_4
    
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments` AS cem
    LEFT JOIN weights_for_mean AS w
        ON cem.comment_id = w.comment_id
    
    WHERE 1=1
        AND cem.post_id IN (
        # 1 comment, these should always return the same mean
        't3_oykxg3',
        't3_p9gf9t',
    
        # 2 comments, these might return a different mean, depending on upvote counts
        't3_p4watx',
        't3_pk0n9k',
    
        # 3 comments, these might return a different mean, depending on upvote counts
        't3_po67az',
        't3_pkged5',
    
        # 9
        't3_psgtvt',
        't3_oxpv38'
    )
    
    GROUP BY 1
    ORDER BY 2, 1
    ;
    """

    # df_mean_weighted = (
    #     client.query(sql_mean_weighted)
    #         .to_dataframe()
    # )
    # print(df_mean_weighted.shape)


#
# ~ fin
#
