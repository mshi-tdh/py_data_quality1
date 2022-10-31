from __future__ import annotations
import json
from pathlib import Path
from string import Template
import redshift_connector as redshift
import logging
from logger import getlogger
import re

REQUIRED_COLUMN_TYPES = {
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}

logger = getlogger()


CHECK_QUERY = Template(
    """
SELECT
    *
FROM (
    $query_string
) q
"""
)

RE_EXTRACT_TABLE_NAME = ".*Not found: Table (.+?) was not found in.*"


class RedshiftClient:
    logger.info("Hi")
    _client: redshift = None

    def __init__(
        self,
        redshift_credentials=None,
    ) -> None:
        if redshift_credentials:
            pass
        else:
            self._redshift_credentials = dict(
                host="redshift.prod.livongo.com",
                database='prod',
                user='mramakrishnan',
                password='sn+kkoDuhTYb+p94Bftr2A=='
            )

    def __repr__(self):
        return json.dumps(self._redshift_credentials)

    def get_connection(
        self, new: bool = False
    ) -> redshift.connect:
        """Creates return new Singleton database connection"""
        if self._client is None or new:
            try:
                self._client = redshift.connect(**self._redshift_credentials)
            except redshift.error.Error as e:
                raise f"Error message - {e}"
            return self._client
        else:
            return self._client

    def close_connection(self) -> None:
        if self._client:
            self._client.close()

    def check_query_dry_run(self, query_string: str) -> None:
        """check whether query is valid."""
        try:
            client=self.get_connection()
            cur = client.cursor()
            logger.info(f"Query executed: {query_string.strip()}")
            query = CHECK_QUERY.safe_substitute(query_string=query_string.strip())
            logger.info(f"Query after substitution: {query}")
            cur.execute(query)
            logger.info(f"Query executed successfully: {cur.redshift_rowcount}")
        except Exception as e:
            logger.error(f"Error message = {e}")
            raise e

    def is_table_exists(self, table: str, schema: str) -> bool:
        select_stmt = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name ='{table}' AND table_schema='{schema}'"
        client=self.get_connection()
        cur = client.cursor()
        cur.execute(select_stmt)
        if cur.fetchone()[0] == 1:
            return True
        return False

    def assert_required_columns_exist_in_table(
        self, table: str
    ) -> dict:
        try:
            client = self.get_connection()
            table_ref = client.get_table(table)
            column_names = {column.name for column in table_ref.schema}
            failures = {}
            for column_name, column_type in REQUIRED_COLUMN_TYPES.items():
                if column_name not in column_names:
                    failures[
                        column_name
                    ] = f"ALTER TABLE `{table}` ADD COLUMN {column_name} {column_type};\n"
            if failures:
                logger.info(
                    f"Cannot find required column '{list(failures.keys())}' in BigQuery table '{table}'.\n"
                    f"These will created by running the following SQL script :\n"
                    "```\n" + "\n".join(failures.values()) + "```"
                )
                return failures
        except KeyError as error:
            logger.fatal(f"Input table `{table}` is not valid.", exc_info=True)
            raise KeyError(f"\n\nInput table `{table}` is not valid.\n{error}")

    def execute_query(
        self,
        query_string: str,
    ):
        """
        The method is used to execute the sql query
        Parameters:
        query_string (str) : sql query to be executed
        Returns:
            result of the sql execution is returned
        """

        client=self.get_connection()
        cur = client.cursor()
        logger.info(f"Query executed: {query_string}")
        cur.execute(query_string)
        result=cur.fetchall()
        return result
