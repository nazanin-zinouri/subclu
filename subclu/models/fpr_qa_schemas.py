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


def fpr_qa_schema(
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
    else:
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
                    d_pa_dtype_list_cols.get(sf_.field_type, bq_to_pa_dtype(sf_.field_type)),
                    metadata={'description': sf_.description}
                )
            )
        fpr_qa_schema = pa.schema(
            [
                ('pt', pa.date32()),
                ('geo_relevance_table', pa.string()),
                ('qa_pt', pa.string()),
                ('qa_table', pa.string()),
                ('run_id', pa.string()),
                ('geo_country_code', pa.string()),
                ('country_name', pa.string()),
                ('relevant_subreddit_id_count', pa.int64()),
                ('bucket', pa.string()),
                ('path_fpr_json', pa.string()),
                ('path_df_fpr', pa.string()),
                ('path_df_fpr_qa_summary', pa.string()),
                ('path_df_fpr_cluster_summary', pa.string()),
                ('seed_subreddit_ids', pa.list_(pa.string())),
                ('seed_subreddit_ids_count', pa.int64()),
                ('recommend_subreddit_ids', pa.list_(pa.string())),
                ('recommend_subreddit_ids_count', pa.int64()),
                ('orphan_or_exclude_seed_subreddit_ids_list', pa.list_(pa.string())),
                ('orphan_or_exclude_seed_subreddit_ids_count', pa.int64()),
                ('orphan_seed_subreddit_ids_list', pa.list_(pa.string())),
                ('orphan_seed_subreddit_ids_count', pa.int64()),
                ('orphan_recommend_subreddit_ids_list', pa.list_(pa.string())),
                ('orphan_recommend_subreddit_ids_count', pa.int64()),
                ('clusters_total', pa.int64()),
                ('clusters_with_recommendations', pa.int64()),
            ],
            metadata={
                'pt': 'Partition time for subreddit_lookup & crowdsourced topic & ratings',
                'geo_relevance_table': 'Name of table used for geo-relevance scores',
                'qa_pt': 'PT for QA decisions',
                'qa_table': 'Table with QA logic',
                'run_id': 'Run ID (timestamp) for this FPR',
                'geo_country_code': 'Geo country code for relevance & FPR',
                'country_name': 'Country name (based on geo_country_code)',
                'relevant_subreddit_id_count': 'Num of subreddits relevant to country',
                'bucket': 'Bucket with FPR output',
                'path_fpr_json': 'Path for FPR JSON output',
                'path_df_fpr': 'Path for dataframe with FPR output',
                'path_df_fpr_qa_summary': 'Path for dataframe with FPR summary (this table!)',
                'path_df_fpr_cluster_summary': 'Path for df with cluster-level summary',
                'seed_subreddit_ids':
                    "List of seed IDs. If user subscribes to a SEED we will rec posts from 'recommend list'",
                'seed_subreddit_ids_count': 'Count of seed subreddit IDs. Note: EXCLUDES orphans.',
                'recommend_subreddit_ids': 'List of subreddit IDs to recommend in country FPR',
                'recommend_subreddit_ids_count': 'Count of subreddits to recommend in country FPR. NOTE: excludes orphans & do not recommend',
                'orphan_or_exclude_seed_subreddit_ids_list': 'List of orphan or exclude SEED subreddit IDs',
                'orphan_or_exclude_seed_subreddit_ids_count': 'Count of oprhan or exclude SEED subredit IDs',
                'orphan_seed_subreddit_ids_list': "List of subreddits that are by themselves or can't be recommended to something else",
                'orphan_seed_subreddit_ids_count': "Count of subreddits that can't be recommended",
                'orphan_recommend_subreddit_ids_list': "List of subreddits that can be recommended, but are orphaned",
                'orphan_recommend_subreddit_ids_count': "Count of subreddits that can be recommended but are by themselves",
                'clusters_total': "Total num of clusters in country",
                'clusters_with_recommendations': "Num of clusters that have 1+ subreddits to recommend",
            },
        )
        pa_schema = pa.schema(l_pa_schema)
        return pa_schema


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
