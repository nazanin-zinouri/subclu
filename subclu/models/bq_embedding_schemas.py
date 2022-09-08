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
