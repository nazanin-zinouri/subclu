"""
Utilities to load & upload data from/to bigQuery
"""
import logging
from logging import info
from typing import List

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict


def table_exists(
    bq_table: str,
    bq_client: bigquery.Client,
) -> bool:
    try:
        bq_client.get_table(bq_table)
        logging.debug("Table %s already exists", bq_table)
        return True
    except NotFound:
        logging.debug("Table %s is not found", bq_table)
        return False


def create_partitioned_table_if_not_exist(
        bq_project: str,
        bq_dataset: str,
        bq_table_name: str,
        schema: List[bigquery.SchemaField],
        partition_column: str,
        bq_client: bigquery.Client,
        table_description: str = None,
        partition_expiration_days: int = 90,
) -> None:
    bq_table = ".".join([bq_project, bq_dataset, bq_table_name])

    if table_exists(bq_table=bq_table, bq_client=bq_client):
        info("Table %s already exist", bq_table)
    else:
        try:
            dataset_ref = bigquery.DatasetReference(bq_project, bq_dataset)
            table_ref = dataset_ref.table(bq_table_name)
            table = bigquery.Table(table_ref, schema=schema)

            partition_expiration_ms = partition_expiration_days * 24 * 60 * 60 * 1000
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_column,  # name of column to use for partitioning
                expiration_ms=partition_expiration_ms,
            )
            if table_description is not None:
                table.description = table_description

            table = bq_client.create_table(
                table,
                exists_ok=False,
            )
            info(
                "Created table %s.%s.%s",
                table.project,
                table.dataset_id,
                table.table_id,
            )
        except Conflict as err:
            logging.error("Conflict when creating table, %s", err)


def load_data_to_bq_table(
        uri: str,
        bq_project: str,
        bq_dataset: str,
        bq_table_name: str,
        schema: List[bigquery.SchemaField],
        partition_column: str,
        source_format: bigquery.SourceFormat = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
        table_description: str = None,
        update_table_description: bool = True,
        location: str = 'US',
        partition_expiration_days: int = 90,
        bq_client: bigquery.Client = None,
        verbose: bool = True,
) -> None:
    """Add JSON outputs of this run to target BQ table.
    It assumes that a table is partitions by a date field & will create the table
    if it doesn't exist.

    Tips:
    - WRITE_TRUNCATE -> replace existing (unless daily partition)
    - WRITE_APPEND -> append data
    """
    if bq_client is None:
        bq_client = bigquery.Client()

    bq_table = ".".join([bq_project, bq_dataset, bq_table_name])
    info(
        f"Loading this URI:\n  {uri}"
        f"\nInto this table:\n  {bq_table}"
    )

    create_partitioned_table_if_not_exist(
        bq_project=bq_project,
        bq_dataset=bq_dataset,
        bq_table_name=bq_table_name,
        schema=schema,
        partition_column=partition_column,
        bq_client=bq_client,
        partition_expiration_days=partition_expiration_days,
        table_description=table_description,
    )

    if verbose:
        if table_exists(bq_table=bq_table, bq_client=bq_client):
            destination_table = bq_client.get_table(bq_table)
            info(
                f"  {destination_table.num_rows:,.0f} rows in table BEFORE adding data"
            )

    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        write_disposition=write_disposition,
        schema=schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_column,
        )
    )

    # Make API request to load new
    load_job = bq_client.load_table_from_uri(
        uri,
        bq_table,
        location=location,  # Must match the destination dataset location.
        job_config=job_config,
    )

    load_job.result()  # Wait for the job to complete

    destination_table = bq_client.get_table(bq_table)
    if update_table_description & (table_description is not None):
        info(
            f"Updating subreddit description from:\n  {destination_table.description}"
            f"\nto:\n  {table_description}"
        )
        destination_table.description = table_description

        destination_table = bq_client.update_table(destination_table, ['description'])

    info(
        f"  {destination_table.num_rows:,.0f} rows in table AFTER adding data"
    )
