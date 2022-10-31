from pprint import pformat
from pathlib import Path
import sys
import logging
import logging.config
from string import Template
from datetime import datetime
from datetime import timezone
import coloredlogs
from logger import getlogger
import click
from typing import Optional
import lib
from integration.redshift.redshiftclient import RedshiftClient
from utils import assert_not_none_or_empty

logger = getlogger()
coloredlogs.install(logger=logger)

@click.command()
@click.argument("rule_binding_ids")
@click.argument(
    "rule_binding_config_path",
    type=click.Path(exists=True)
)
@click.option(
    "--target_summary_table",
    help="Target summary table for data quality output. ",
    type=str,
)
@click.option(
    "--environment_target",
    help="Execution environment target as defined in dbt profiles.yml, "
    "e.g. dev, test, prod.  "
    "Defaults to 'dev' if not set. "
    "Uses the environment variable ENV if present. "
    "Set this to the same value as 'environment' in "
    "entity 'environment_override' config to trigger "
    "field substitution.",
    envvar="ENV",
    default="dev",
)
@click.option(
    "--dry_run",
    help="If True, do everything except run dbt itself.",
    is_flag=True,
    default=False,
)
@click.option(
    "--debug",
    help="If True, print additional diagnostic information.",
    is_flag=True,
    default=False,
)
@click.option(
    "--skip_sql_validation",
    help="If True, skip validation step of generated SQL using BigQuery dry-run.",
    is_flag=True,
    default=False,
)
@click.option(
    "--progress_watermark",
    help="Whether to set 'progress_watermark' column value "
    "to True/False in dq_summary. Defaults to True.",
    type=bool,
    default=True,
)
@click.option(
    "--summary_to_stdout",
    help="If True, the summary of the validation results will be logged to stdout. "
    "This flag only takes effect if target_bigquery_summary_table is specified as well.",
    is_flag=True,
    default=False,
)
@click.option(
    "--num_threads",
    help="Number of concurrent bigquery operations that can be "
    "increased to reduce run-time. We advice setting "
    "this to number of cores of your run-environment machines",
    default=1,
    type=int,
)
@click.option(
    "--snapshot_date",
    help="Pass snapshot date if its daily table else path snapshot year month value",
    type=str,
)
def main(
        rule_binding_ids: str,
        rule_binding_config_path: str,
        target_summary_table: str,
        snapshot_date: str,
        environment_target: Optional[str],
        dry_run: bool,
        progress_watermark: bool,
        num_threads: int,
        print_sql_queries: bool = False,
        skip_sql_validation: bool = False,
        summary_to_stdout: bool = False,
        debug: bool = False,
):
    if debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")
    logger.info(f"Input parameter... {sys.argv[1:]}")

    redshift_client = None
    try:
        logger.info("Starting DQ run with configs:")
        redshift = RedshiftClient()
        logger.info(f"redshift-{redshift}")
        dq_summary_table_name = "test_dq_summary"
        dq_summary_table_exists = False
        dq_summary_table_exists = redshift.is_table_exists(
            table=dq_summary_table_name, schema="data_sciences"
        )
        if not dq_summary_table_exists:
            raise RuntimeError(f"Summary table '{dq_summary_table_name}' does not exist ")
        if summary_to_stdout and target_summary_table:
            logger.info(
                "--summary_to_stdout is True. Logging summary results as json to stdout."
            )
        elif summary_to_stdout and not target_summary_table:
            logger.warning(
                "--summary_to_stdout is True but --target_bigquery_summary_table is not set. "
                "No summary logs will be logged to stdout."
            )
        configs_path = Path(rule_binding_config_path)
        logger.debug(f"Loading rule bindings from: {configs_path.absolute()}")
        all_rule_bindings = lib.load_rule_bindings_config(Path(configs_path))
        logger.debug(f"all_rule_bindings: {all_rule_bindings}")
        target_rule_binding_ids = [
            r.strip().upper() for r in rule_binding_ids.split(",")
        ]
        if len(target_rule_binding_ids) == 1 and target_rule_binding_ids[0] == "ALL":
            target_rule_binding_ids = [
                rule_binding.upper() for rule_binding in all_rule_bindings.keys()
            ]
        logger.info(f"Preparing SQL for rule bindings: {target_rule_binding_ids}")
        configs_cache = lib.prepare_configs_cache(configs_path=Path(configs_path))
        target_entity_summary_configs: dict = (
            configs_cache.get_entities_configs_from_rule_bindings(
                target_rule_binding_ids=target_rule_binding_ids,
            )
        )
        logger.info(f"target_entity_summary_configs-{target_entity_summary_configs}")
        failed_queries_configs = dict()
        # Create Rule_binding views
        for rule_binding_id in target_rule_binding_ids:
            rule_binding_configs = all_rule_bindings.get(rule_binding_id, None)
            assert_not_none_or_empty(
                rule_binding_configs,
                f"Target Rule Binding Id: {rule_binding_id} not found "
                f"in config path {configs_path.absolute()}.",
            )
            if debug:
                logger.debug(
                    f"Creating sql string from configs for rule binding: "
                    f"{rule_binding_id}"
                )
                logger.debug(
                    f"Rule binding config json:\n{pformat(rule_binding_configs)}"
                )
            high_watermark_filter_exists = True
            logger.info(f"Calling create_rule_binding_view_model:{redshift}")
            configs = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name=dq_summary_table_name,
                configs_cache=configs_cache,
                environment=None,
                metadata=None,
                debug=print_sql_queries,
                progress_watermark=True,
                dq_summary_table_exists=dq_summary_table_exists,
                high_watermark_filter_exists=high_watermark_filter_exists,
                redshift_client=redshift,
            )
            if not skip_sql_validation:
                logger.debug(
                    f"Validating generated SQL code for rule binding "
                    f"{rule_binding_id} using BigQuery dry-run client.",
                )
                target_sql_str = configs.get('generated_sql_string')
                logger.debug(
                    f"Generated SQL: {target_sql_str}",
                )
                lib.update_config_sql_string(configs,'snapshot_date',snapshot_date)
#                logger.info(
#                    f"after input_param update:{Template(target_sql_str).safe_substitute({'tgt_snapshot_value': snapshot_date})}")
                logger.info(
                    f"after input_param update:{configs.get('generated_sql_string')}")
                #logger.debug(
                #    f"Generated SQL: {configs.get('failed_records_sql_string')}",
                #)
                #redshift_client.check_query_dry_run(
                #    query_string=configs.get("generated_sql_string")
                #)
            #lib.write_sql_string_as_dbt_model(
            #    model_id=rule_binding_id,
            #    sql_string=configs.get("generated_sql_string"),
            #    dbt_model_path=dbt_rule_binding_views_path,
            #)
            failed_queries_configs[
                f"{rule_binding_id}_failed_records_sql_string"
            ] = configs.get("failed_records_sql_string")

    except Exception as error:
        logger.error(error, exc_info=True)
        raise SystemExit(f"\n\n{error}")
    finally:
        if redshift_client:
            redshift.close_connection()

#if __name__=="__main__":
main()



