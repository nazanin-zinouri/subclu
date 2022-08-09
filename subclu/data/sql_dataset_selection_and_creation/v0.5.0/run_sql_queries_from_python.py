"""
Use this script to run a sql query from the command line.
It's helpful to run pseudo DAGs before porting to an ETL
"""
import argparse
from datetime import datetime
import logging
import os
import string
from typing import Literal, Optional

from google.cloud import bigquery


logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    # Use "%Y%m%d" (no spaces) so that BigQuery groups runs from multiple days into a single
    #  table object
    parser.add_argument("--query-name", type=str)

    # For local testing it's cleaner to skip logging the SQL query:
    parser.add_argument("--log-query", dest='log_query', action='store_true')
    parser.add_argument("--no-log-query", dest='log_query', action='store_false')
    parser.set_defaults(log_query=True)

    args = parser.parse_args()

    bigquery_client = bigquery.Client()
    with open(
            os.path.join(os.path.dirname(__file__), args.query_name), "r"
    ) as query_file:
        #  replace escape character b/c we sometimes need to use it
        #   regex or in JSON_EXTRACT() function
        lines = (
            query_file.read()
            .replace("$.", "$$.")
            .replace("$|", "$$|")
            .replace('$"', '$$"')
        )
    template = string.Template(lines)
    # these queries shouldn't be parameterized
    sql = template.substitute({'run_id': ''})
    # sql = template.substitute(
    #     {
    #         "run_id": args.run_id,
    #         "dataset": args.dataset,
    #         "end_date": args.end_date,
    #         "output_bucket_name": args.output_bucket_name,
    #     }
    # )
    if args.log_query:
        logger.info(sql)

    logger.info(f"## Running query... {args.query_name} ##")
    query_start_time = datetime.utcnow()
    logger.info(f"  {query_start_time} | query START time")

    query_job = bigquery_client.query(sql)
    query_job.result()
    query_end_time = datetime.utcnow()
    logger.info(f"  {query_end_time} | query END time")
    logger.info(f"  {query_end_time - query_start_time} | query ELAPSED time | {args.query_name}")


if __name__ == "__main__":
    main()
