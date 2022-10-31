from datetime import date
from logger import getlogger
from integration.redshift.redshiftclient import RedshiftClient


logger = getlogger()


def load_target_summary_table(
    redshift_client: RedshiftClient,
    query_string: str,
    partition_date: date,
    target_summary_table: str,
    dq_summary_table_name: str,
    summary_to_stdout: bool = False,
):
    print(f"Inside load_target_table_from_redshift ")
    print(f"redshift_client.is_table_exists(target_summary_table):{redshift_client.is_table_exists(target_summary_table)} ")
    if redshift_client.is_table_exists(target_summary_table):

        query_string_load = f"""SELECT * FROM data_sciences.{dq_summary_table_name}
         WHERE invocation_id='{invocation_id}'
         and DATE(execution_ts)='{partition_date}'"""

        print(f"query_string_load:{query_string_load}")

        result=redshift_client.execute_query(
            query_string=query_string_load
        )

        logger.info(
            f"Table {target_summary_table} already exists "
            f"and query results are appended to the table."
        )

    else:
        print(f"target_summary_table {target_summary_table} is not present and hence create ")
        query_create_table = f"""CREATE TABLE
        data_sciences.{target_summary_table}
        AS
        SELECT * from data_sciences.{dq_summary_table_name}
        WHERE invocation_id='{invocation_id}'
        AND DATE(execution_ts)='{partition_date}'"""
        print(f"query_create_table string: {query_create_table}")
        # Create the summary table
        result=redshift_client.execute_query(query_string=query_create_table)
        print(f"Table created : {result} ")

        logger.info(
            f"Table created and dq summary results loaded to the "
            f"table {target_summary_table}"
        )
    # getting loaded rows
    query_string_affected = f"""SELECT * FROM data_sciences.{target_summary_table}
        WHERE invocation_id='{invocation_id}'
        and DATE(execution_ts)='{partition_date}'"""

    summary_data = redshift_client.execute_query(
        query_string=query_string_affected
    )

    #if summary_to_stdout:
    #    log_summary(summary_data)
    logger.info(
        f"Loaded {len(summary_data)} rows to {target_summary_table}."
    )
    return len(summary_data)


class TargetTable:

    invocation_id: str = None
    redshift_client: RedshiftClient = None

    def __init__(self, invocation_id: str, redshift_client: RedshiftClient):
        self.invocation_id = invocation_id
        self.redshift_client = redshift_client

    def write_to_target_bq_table(
        self,
        partition_date: date,
        target_summary_table: str,
        dq_summary_table_name: str,
        summary_to_stdout: bool = False,
    ) -> int:
        try:

            num_rows = load_target_table_from_redshift(
                redshift_client=self.redshift_client,
                invocation_id=self.invocation_id,
                partition_date=partition_date,
                target_summary_table=target_summary_table,
                dq_summary_table_name=dq_summary_table_name,
                summary_to_stdout=summary_to_stdout,
            )
            return num_rows

        except Exception as error:

            raise error
