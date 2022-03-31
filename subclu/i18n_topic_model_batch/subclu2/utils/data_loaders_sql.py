"""
Utils to query & get data from SQL as a dataframe
"""
from datetime import datetime
import logging

import pandas as pd
from google.cloud import bigquery


logger = logging.getLogger(__name__)


class LoadSubredditsSQL:
    """
    Class to load subreddits from SQL, simplified to only get columns
    needed to get text to get embeddings and save them
    """
    def __init__(
            self,
            table: str,
            dataset: str = 'tmp',
            project_name: str = 'reddit-relevance',
            columns: str = 'default',
            col_unique_check: str = 'subreddit_id',
            concat_text_cols: str = "CONCAT(name, '. ', COALESCE(title, ''), '. ', COALESCE(description, ''))",
            col_concat_text: str = 'concat_text',
            unique_check: bool = True,
            verbose: bool = False,
            log_query: bool = False,
            sql_template: str = 'subreddit_lookup',
    ):
        self.project_name = project_name
        self.dataset = dataset
        self.table = table
        self.col_unique_check = col_unique_check
        self.concat_text_cols = concat_text_cols
        self.col_concat_text = col_concat_text
        self.unique_check = unique_check
        self.verbose = verbose
        self.log_query = log_query
        self.df = None
        self.str_sql = None

        self.sql_template = get_sql_template(sql_template)

        if columns == 'default':
            self.columns = """
                subreddit_id
                , name
                # , title
                # , description
            """
        else:
            self.columns = columns
        # add concat col:
        if (concat_text_cols is not None) & (col_concat_text is not None):
            self.columns = (
                f"{self.columns}"
                f"\n, {concat_text_cols} AS {col_concat_text}"
            )

    def get_as_dataframe(self) -> pd.DataFrame:
        """
        Run SQL query and return a dataframe
        Returns: pd.DataFrame
        """
        self.str_sql = self.sql_template.format(
            columns=self.columns,
            project_name=self.project_name,
            dataset=self.dataset,
            table=self.table
        )

        if self.df is None:
            logger.info(f"# Connecting to BigQuery... #")
            bigquery_client = bigquery.Client()

            logger.info(f"# Running query... #")
            if self.log_query:
                logger.info(self.str_sql)
            query_start_time = datetime.utcnow()
            logger.info(f"  {query_start_time} | query START time")
            self.df = bigquery_client.query(self.str_sql).to_dataframe()
            query_end_time = datetime.utcnow()
            logger.info(f"  {query_end_time} | query END time")
            logger.info(f"  {query_end_time - query_start_time} | query ELAPSED time")
            if self.unique_check:
                assert(
                    len(self.df == self.df[self.col_unique_check].nunique())
                ), f"ERROR: Col {self.col_unique_check} NOT UNIQUE"
                logger.info(f"Col {self.col_unique_check} is unique")

            logger.info(f"  {self.df.shape} <- df.shape")
            return self.df
        else:
            logger.info(f"  Query already cached")
            logger.info(f"  {self.df.shape} <- df.shape")
            return self.df


def get_sql_template(
        name
) -> str:
    """SQL templates to test quick iterations these are best used with str.format()"""

    d_sql_templates = {
        'subreddit_lookup': """
        SELECT
            {columns}
        FROM {project_name}.{dataset}.{table}
        WHERE 1=1
            AND dt = (CURRENT_DATE() - 2)  -- subreddit_lookup
            -- Exclude user-profiles + spam & sketchy subs
            AND COALESCE(verdict, 'f') <> 'admin_removed'
            AND COALESCE(is_spam, FALSE) = FALSE
            AND COALESCE(is_deleted, FALSE) = FALSE
            AND deleted IS NULL
            AND type IN ('public', 'private', 'restricted')
            AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
        LIMIT 10000
        """,

        'all_reddit_subreddits': """
         SELECT
            {columns}
        FROM {project_name}.{dataset}.{table}
        WHERE 1=1
            AND DATE(pt) = (CURRENT_DATE() - 2)  -- all_reddit_subreddits
            
        LIMIT 10000
        """
    }

    return d_sql_templates[name]


def convert_iter_to_sql_str(
        array: iter
) -> str:
    """
    Convert an interatble of IDs or names into a string
    that we can insert into a SQL statement. Example:
    IN ('item1, 'item2')
    Args:
        array:

    Returns: string
    """
    return "".join(["'", "', '".join([x for x in array]), "'"])


#
# ~ fin
#
