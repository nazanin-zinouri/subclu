"""
Create schemas for FPR QA.
This includes:
- BigQuery table schemas
- parquet schema/metadata

We'll define it once in BigQuery format and create a fxn to translate
BQ format to pyarrow (parquet) format.
"""
from typing import Union, List
from google.cloud import bigquery
import pyarrow as pa


def fpr_full_schema(
        schema_format: str = 'bigquery'
) -> Union[List[bigquery.SchemaField], pa.Schema]:
    """
    Return the schema for the `subclu_v0050_fpr_outputs` table in BQ
    Args:
        schema_format: Which format we want for the output

    Returns:
        a list with either BQ schema or pyarrow schema
    """
    l_bq_schema = [
        bigquery.SchemaField(
            name="run_id",
            field_type="STRING",
            description="Run ID (timestamp) for this FPR. Multiple countries could have the same run_id",
        ),
        bigquery.SchemaField(
            name="geo_country_code",
            field_type="STRING",
            description="Geo country code for relevance & FPR seed & recommendations",
        ),
        bigquery.SchemaField(
            name="country_name",
            field_type="STRING",
            description="Geo country name for relevance & FPR seed & recommendations",
        ),

        bigquery.SchemaField(
            name="subreddit_id_seed",
            field_type="STRING",
            description="Subreddit ID that a user is subscribed to",
        ),
        bigquery.SchemaField(
            name="subreddit_name_seed",
            field_type="STRING",
            description="Subreddit name that a user is subscribed to",
        ),

        bigquery.SchemaField(
            name="subreddits_to_rec_count",
            field_type="INTEGER",
            description="Count of subreddits to recommend for this seed",
        ),
        bigquery.SchemaField(
            name="cluster_subreddit_names_list",
            field_type="STRING",
            mode="REPEATED",
            description="List of subreddit names to recommend if user subscribes to seed subreddit",
        ),
        bigquery.SchemaField(
            name="cluster_subreddit_ids_list",
            field_type="STRING",
            mode="REPEATED",
            description="List of subreddit IDs to recommend if user subscribes to seed subreddit",
        ),

        bigquery.SchemaField(
            name="cluster_label",
            field_type="STRING",
            description="Nested cluster label the subreddit belongs to",
        ),
        bigquery.SchemaField(
            name="cluster_label_k",
            field_type="STRING",
            description=(
                "The depth (column) that this subreddit & recommendations belong to."
                " Example: k_0060_label -> subreddits are in the same cluster when we split "
                "all subs into 60 clusters."
            ),
        ),
        bigquery.SchemaField(
            name="pt",
            field_type="DATE",
            description="Partition date for subreddit_lookup + crowdsourced topic & ratings",
        ),
        bigquery.SchemaField(
            name="qa_pt",
            field_type="DATE",
            description="PT for table with QA rules & decisions",
        ),
        bigquery.SchemaField(
            name="cluster_label_int",
            field_type="INTEGER",
            description="Cluster label (ID) as integer. Same value as `cluster_label_k`",
        ),

        bigquery.SchemaField(
            name="qa_table",
            field_type="STRING",
            description="Name of table used for QA rules",
        ),
        bigquery.SchemaField(
            name="geo_relevance_table",
            field_type="STRING",
            description="Name of table used for geo-relevance scores",
        ),
    ]
    if schema_format == 'bigquery':
        return l_bq_schema
    elif schema_format == 'pyarrow':
        d_pa_dtype_list_cols = {
            'cluster_subreddit_names_list': pa.list_(pa.string()),
            'cluster_subreddit_ids_list': pa.list_(pa.string()),
        }
        l_pa_schema = list()
        for sf_ in l_bq_schema:
            l_pa_schema.append(
                pa.field(
                    sf_.name,
                    d_pa_dtype_list_cols.get(sf_.name, bq_to_pa_dtype(sf_.field_type)),
                    metadata={'description': sf_.description}
                )
            )
        return pa.schema(l_pa_schema)
    else:
        raise NotImplementedError(f"Schema not implemented {schema_format}")


def fpr_qa_summary_schema(
        schema_format: str = 'bigquery'
) -> Union[List[bigquery.SchemaField], pa.Schema]:
    """
    Return the schema for the `subclu_v0050_fpr_cluster_summary` table in BQ
    Args:
        schema_format: Which format we want for the output

    Returns:
        a list with either BQ schema or pyarrow schema
    """
    l_bq_schema = [
        bigquery.SchemaField(
            name="pt",
            field_type="DATE",
            description="Partition date for subreddit_lookup & crowdsourced topic & ratings",
        ),
        bigquery.SchemaField(
            name="geo_relevance_table",
            field_type="STRING",
            description="Name of table used for geo-relevance scores",
        ),
        bigquery.SchemaField(
            name="qa_pt",
            field_type="DATE",
            description="PT for table with QA decisions",
        ),
        bigquery.SchemaField(
            name="run_id",
            field_type="STRING",
            description="Run ID (timestamp) for this FPR. Multiple countries could have the same run_id",
        ),
        bigquery.SchemaField(
            name="geo_country_code",
            field_type="STRING",
            description="Geo country code for relevance & FPR",
        ),
        bigquery.SchemaField(
            name="relevant_subreddit_id_count",
            field_type="INTEGER",
            description="Num of subreddits relevant to country",
        ),
        bigquery.SchemaField(
            name="bucket",
            field_type="STRING",
            description="Bucket with FPR output",
        ),
        bigquery.SchemaField(
            name="path_fpr_json",
            field_type="STRING",
            description="Path for FPR JSON output. Multiple countries are allowed (1 file per country)",
        ),
        bigquery.SchemaField(
            name="path_df_fpr",
            field_type="STRING",
            description="Path for dataframe with FPR output",
        ),
        bigquery.SchemaField(
            name="path_df_fpr_qa_summary",
            field_type="STRING",
            description="Path for dataframe with FPR summary (this table!)",
        ),
        bigquery.SchemaField(
            name="path_df_fpr_cluster_summary",
            field_type="STRING",
            description="Path for df with cluster-level summary",
        ),

        bigquery.SchemaField(
            name="seed_subreddit_ids",
            field_type="STRING",
            mode="REPEATED",
            description="List of seed IDs. If user subscribes to a SEED we will rec posts from 'recommend list'",
        ),
        bigquery.SchemaField(
            name="seed_subreddit_ids_count",
            field_type="INTEGER",
            description="Count of seed subreddit IDs (Excludes orphans)",
        ),

        bigquery.SchemaField(
            name="recommend_subreddit_ids",
            field_type="STRING",
            mode="REPEATED",
            description="List of subreddit IDs to recommend in country FPR",
        ),
        bigquery.SchemaField(
            name="recommend_subreddit_ids_count",
            field_type="INTEGER",
            description="Count of subreddits to recommend in country FPR (Excludes orphans & do not recommend)",
        ),

        bigquery.SchemaField(
            name="orphan_or_exclude_seed_subreddit_ids_list",
            field_type="STRING",
            mode="REPEATED",
            description="List of orphan or excluded SEED subreddit IDs",
        ),
        bigquery.SchemaField(
            name="orphan_or_exclude_seed_subreddit_ids_count",
            field_type="INTEGER",
            description="Count of oprhan or exclude SEED subredit IDs",
        ),

        bigquery.SchemaField(
            name="orphan_seed_subreddit_ids_list",
            field_type="STRING",
            mode="REPEATED",
            description="List of subreddits that are by themselves or can't be recommended to something else",
        ),
        bigquery.SchemaField(
            name="orphan_seed_subreddit_ids_count",
            field_type="INTEGER",
            description="Count of subreddits that can't be recommended",
        ),

        bigquery.SchemaField(
            name="orphan_recommend_subreddit_ids_list",
            field_type="STRING",
            mode="REPEATED",
            description="List of subreddits that can be recommended, but are orphaned",
        ),
        bigquery.SchemaField(
            name="orphan_recommend_subreddit_ids_count",
            field_type="INTEGER",
            description="Count of subreddits that can be recommended but are by themselves",
        ),

        bigquery.SchemaField(
            name="clusters_total",
            field_type="INTEGER",
            description="Total num of clusters in country",
        ),
        bigquery.SchemaField(
            name="clusters_with_recommendations",
            field_type="INTEGER",
            description="Num of clusters that have 1+ subreddits to recommend",
        ),
    ]
    if schema_format == 'bigquery':
        return l_bq_schema
    elif schema_format == 'pyarrow':
        d_pa_dtype_list_cols = {
            'seed_subreddit_ids': pa.list_(pa.string()),
            'recommend_subreddit_ids': pa.list_(pa.string()),
            'orphan_or_exclude_seed_subreddit_ids_list': pa.list_(pa.string()),
            'orphan_seed_subreddit_ids_list': pa.list_(pa.string()),
            'orphan_recommend_subreddit_ids_list': pa.list_(pa.string()),
        }
        l_pa_schema = list()
        for sf_ in l_bq_schema:
            l_pa_schema.append(
                pa.field(
                    sf_.name,
                    d_pa_dtype_list_cols.get(sf_.name, bq_to_pa_dtype(sf_.field_type)),
                    metadata={'description': sf_.description}
                )
            )
        return pa.schema(l_pa_schema)
    else:
        raise NotImplementedError(f"Schema not implemented {schema_format}")


def bq_to_pa_dtype(
        bq_type: str,
) -> pa.types:
    """Take a string input of a BQ dtype and return a pa.dtype"""
    d_bq_to_pa = {
        'STRING': pa.string(),
        'TIMESTAMP': pa.timestamp('ns'),
        'DATE': pa.date32(),
        'INTEGER': pa.int64(),
    }
    return d_bq_to_pa[bq_type]


#
# ~ fin
#
