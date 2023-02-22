"""
Schemas for embeddings
"""
from typing import List
from google.cloud import bigquery


def embeddings_schema(
) -> List[bigquery.SchemaField]:
    """
    Return the schema for the `subclu_v0050_fpr_outputs` table in BQ
    Args:
        schema_format: Which format we want for the output

    Returns:
        a list with either BQ schema or pyarrow schema
    """
    l_bq_schema = [
        bigquery.SchemaField(
            name="pt",
            field_type="DATE",
            description="Date when posts were pulled",
        ),
        bigquery.SchemaField(
            name="mlflow_run_id",
            field_type="STRING",
            description="mlflow UUID for job that created the aggregated embeddings",
        ),
        bigquery.SchemaField(
            name="model_name",
            field_type="STRING",
            description="Short description of the model used to create the embeddings",
        ),
        bigquery.SchemaField(
            name="model_version",
            field_type="STRING",
            description="Model version",
        ),

        bigquery.SchemaField(
            name="subreddit_id",
            field_type="STRING",
            description="Subreddit ID",
            mode="REQUIRED"
        ),
        bigquery.SchemaField(
            name="subreddit_name",
            field_type="STRING",
            description="Subreddit Name (lowercase)",
        ),
        bigquery.SchemaField(
            name="posts_for_embeddings_count",
            field_type="INTEGER",
            description=(
                "How many posts were used to create this embedding." 
                " If zero, the embeddings are only based on text from the subreddit description."
            ),
        ),

        bigquery.SchemaField(
            name="embeddings",
            field_type="FLOAT",
            mode="REPEATED",
            description="Embeddings for this subreddit",
        ),
    ]
    return l_bq_schema


def similar_sub_schema() -> List[bigquery.SchemaField]:
    """
    Return the schema for the `cau_similar_subreddit` table in BQ
    Args:
        schema_format: Which format we want for the output

    Returns:
        a list with either BQ schema or pyarrow schema
    """
    l_bq_schema = [
        bigquery.SchemaField(
            name="pt",
            field_type="DATE",
            description="Date when Nearest Neighors were computed",
        ),
        bigquery.SchemaField(
            name="mlflow_run_id",
            field_type="STRING",
            description="mlflow UUID for job that created the aggregated embeddings",
        ),
        bigquery.SchemaField(
            name="model_name",
            field_type="STRING",
            description="Short description of the model used to create the embeddings",
        ),
        bigquery.SchemaField(
            name="model_version",
            field_type="STRING",
            description="Model version",
        ),

        bigquery.SchemaField(
            name="subreddit_id",
            field_type="STRING",
            description="Subreddit ID",
            mode="REQUIRED"
        ),
        bigquery.SchemaField(
            name="subreddit_name",
            field_type="STRING",
            description="Subreddit Name (lowercase)",
        ),

        bigquery.SchemaField(
            name="similar_subreddit",
            field_type="RECORD",
            mode="REPEATED",
            description="Most similar subreddits by distance",
            # The nested fields should be a list of dictionaries,
            #  so the mode should NOT be 'repeated'
            fields=(
                bigquery.SchemaField(
                    name="subreddit_id",
                    field_type="STRING",
                    description="Subreddit id",
                ),
                bigquery.SchemaField(
                    name="subreddit_name",
                    field_type="STRING",
                    description="Lower case subreddit name",
                ),
                bigquery.SchemaField(
                    name="cosine_similarity",
                    field_type="FLOAT",
                    description=(
                        "Cosine similarity between subreddits. Range from 1 to -1."
                        "  1: the same content, 0: unrelated, -1: opposite"
                    ),
                ),
                bigquery.SchemaField(
                    name="distance_rank",
                    field_type="INTEGER",
                    description="Rank for most similar subreddits where 1=closest",
                ),
            ),
        ),
    ]
    return l_bq_schema
