import argparse
from datetime import datetime
import logging
import os
import string

from google.cloud import bigquery


logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    # Use "%Y%m%d" (no spaces) so that BigQuery groups runs from multiple days into a single
    #  table object
    parser.add_argument("--query-name", type=str)
    default_current_date_ = 'CURRENT_DATE() - 2'  # "DATE(2022, 06, 27)"
    parser.add_argument("--end-date", type=str, default=default_current_date_)

    parser.add_argument(
        "--post-lookback-days", type=int, default=90,
        help="How many days to lookback to get posts. Subtract these days from `end-date`"
    )

    run_id_default_ = datetime.utcnow().strftime("%Y%m%d")  # '20220624'
    parser.add_argument("--run-id", type=str, default=run_id_default_)

    # TODO(djb) change default after testing. 'tmp' gets deleted after 2 weeks
    parser.add_argument("--dataset", type=str, default='tmp')
    parser.add_argument(
        "--output-bucket-name", type=str, default='gazette-models-temp',
        help="bucket to export text needed for embeddings"
    )

    # For local testing it's cleaner to skip logging the SQL query:
    parser.add_argument("--log-query", dest='log_query', action='store_true')
    parser.add_argument("--no-log-query", dest='log_query', action='store_false')
    parser.set_defaults(log_query=True)

    # parser.add_argument(
    #     "--subreddit-buckets",
    #     type=str,
    #     help="File containing subreddit ids to process",
    # )
    # parser.add_argument(
    #     "--output-path",
    #     type=str,
    # )
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
    sql = template.substitute(
        {
            "run_id": args.run_id,
            "dataset": args.dataset,
            "end_date": args.end_date,
            "post_lookback_days": args.post_lookback_days,
            "output_bucket_name": args.output_bucket_name,
        }
    )
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
