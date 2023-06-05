"""
Schema to upload PN model outputs
"""
from typing import List
from google.cloud import bigquery


def pn_model_schema() -> List[bigquery.SchemaField]:
    """
    Return the schema for the `pn_click_subreddit_user` table in BQ

    Returns:
        a list with either BQ schema or pyarrow schema
    """
    l_bq_schema = [
        bigquery.SchemaField(
            name="pt",
            field_type="DATE",
            mode="REQUIRED",
            description="Date when inputs for PN model were pulled",
        ),
        bigquery.SchemaField(
            name="model_name",
            field_type="STRING",
            description="Short description of the model used to create the PN model",
        ),
        bigquery.SchemaField(
            name="model_version",
            field_type="STRING",
            description="Model version",
        ),

        bigquery.SchemaField(
            name="target_subreddit_id",
            field_type="STRING",
            description="Subreddit ID",
        ),

        bigquery.SchemaField(
            name="target_subreddit",
            field_type="STRING",
            description="Subreddit Name (lowercase)",
            mode="REQUIRED"
        ),
        bigquery.SchemaField(
            name="subscribed",
            field_type="INTEGER",
            description="1 = users subscribed to target subreddit, 0 = users viewed sub in L30 days but not subscribed",
            mode="REQUIRED"
        ),
        bigquery.SchemaField(
            name="user_geo_country_code",
            field_type="STRING",
            description=(
                "Geo-country-code for target users. "
                "Note that this table only include users from target countries"
            ),
        ),

        bigquery.SchemaField(
            name="top_users",
            field_type="RECORD",
            mode="REPEATED",
            description="Users most likely to click on a PN",
            # The nested fields should be a list of dictionaries,
            #  so the mode should NOT be 'repeated'
            fields=(
                bigquery.SchemaField(
                    name="user_id",
                    field_type="STRING",
                    description="User ID",
                ),
                bigquery.SchemaField(
                    name="click_proba",
                    field_type="FLOAT",
                    description="Proba(bility) output from model. Range 0 to 1 where 1 is most likely to click",
                ),
                bigquery.SchemaField(
                    name="user_rank_by_sub_and_geo",
                    field_type="INTEGER",
                    description=(
                        "Rank for most likely users to click on a PN where 1=most likely"
                    ),
                ),
            ),
        ),
    ]
    return l_bq_schema
