from __future__ import annotations

from pathlib import Path
from pprint import pformat
from string import Template
import itertools
import json
import typing
from classes.dq_config_type import DqConfigType
from classes.dq_configs_cache import DqConfigsCache
from classes.dq_rule import DqRule
from classes.dq_rule_binding import DqRuleBinding
from logger import getlogger
from utils import assert_not_none_or_empty
from utils import load_jinja_template
from utils import load_yaml
from utils import sha256_digest
from integration.redshift.redshiftclient import RedshiftClient


logger = getlogger()


def load_configs(configs_path,configs_type: DqConfigType):
    logger.info(f"configs_path-{configs_path},configs_type-{configs_type}")
    if configs_path.is_file():
        yaml_files = [configs_path]
    else:
        other_configs = (configs_path/"..").resolve()
        #rules = (configs_path / ".." / "rules").resolve()
        #rules = (configs_path / ".." / "rules").resolve()
        #rules = (configs_path / ".." / "rules").resolve()
        #logger.info(f"configs_path is not file-{entities},{rules}")
        logger.info(f"configs_path is not file-{other_configs}")
        yaml_files = itertools.chain(
            #configs_path.glob("**/*.yaml"), configs_path.glob("**/*.yml"), entities.glob("**/*.yml"), rules.glob("**/*.yml")
            configs_path.glob("**/*.yaml"), configs_path.glob("**/*.yml"), other_configs.glob("**/*.yml")
        )
    #logger.info(f"yaml_files-{list(yaml_files)}")
    all_configs = {}
    for file in yaml_files:
        logger.info("Looping thru config files")
        config = load_yaml(file, configs_type.value)
        logger.info(f"file:{file}\nconfig:{config}")
        if not config:
            continue
        print(f"config={config}")
        all_configs = DqConfigsCache.update_config(configs_type, all_configs, config)
    #
    if configs_type.is_required():
        assert_not_none_or_empty(
            all_configs,
            f"Failed to load {configs_type.value} from file path: {configs_path}",
        )
    logger.info(f"all_configs={all_configs}")
    return all_configs


def load_rule_bindings_config(configs_path: Path) -> dict:
    logger.info("Inside load_rule_bindings_config")
    return load_configs(configs_path, DqConfigType.RULE_BINDINGS)


def load_rule_dimensions_config(configs_path: Path) -> list:
    return load_configs(configs_path, DqConfigType.RULE_DIMENSIONS)


def load_entities_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.ENTITIES)


def load_rules_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.RULES)


def load_row_filters_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.ROW_FILTERS)


def load_reference_columns_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.REFERENCE_COLUMNS)


def prepare_configs_cache(configs_path: Path) -> DqConfigsCache:
    configs_cache = DqConfigsCache()
    entities_collection = load_entities_config(configs_path)
    configs_cache.load_all_entities_collection(entities_collection)
    row_filters_collection = load_row_filters_config(configs_path)
    configs_cache.load_all_row_filters_collection(row_filters_collection)
    reference_columns_collection = load_reference_columns_config(configs_path)
    configs_cache.load_all_reference_columns_collection(reference_columns_collection)
    rule_dimensions_collection = load_rule_dimensions_config(configs_path)
    configs_cache.load_all_rule_dimensions_collection(rule_dimensions_collection)
    rules_collection = load_rules_config(configs_path)

    # validate rules against dimensions
    for rule_id, rule in rules_collection.items():
        DqRule.validate(rule_id, rule, rule_dimensions_collection)

    configs_cache.load_all_rules_collection(rules_collection)
    rule_binding_collection = load_rule_bindings_config(configs_path)
    configs_cache.load_all_rule_bindings_collection(rule_binding_collection)
    return configs_cache


def create_rule_binding_view_model(
    rule_binding_id: str,
    rule_binding_configs: dict,
    dq_summary_table_name: str,
    environment: str,
    configs_cache: DqConfigsCache,
    redshift_client: RedshiftClient,
    dq_summary_table_exists: bool = False,
    metadata: dict | None = None,
    debug: bool = False,
    progress_watermark: bool = True,
    high_watermark_filter_exists: bool = False,
) -> dict:
    template = load_jinja_template(
        template_path=Path("macros", "create_rule_binding_view.sql")
    )
    logger.info(f"template:{template}\nredshift_client:{redshift_client}")
    failed_records_template = load_jinja_template(
        template_path=Path("macros", "failed_records_query.sql")
    )
    logger.info(f"failed_records_template:{failed_records_template}")
    configs = prepare_configs_from_rule_binding_id(
        rule_binding_id=rule_binding_id,
        rule_binding_configs=rule_binding_configs,
        dq_summary_table_name=dq_summary_table_name,
        environment=environment,
        configs_cache=configs_cache,
        metadata=metadata,
        progress_watermark=progress_watermark,
        dq_summary_table_exists=dq_summary_table_exists,
        high_watermark_filter_exists=high_watermark_filter_exists,
        redshift_client=redshift_client
    )
    logger.info(f"configs:\n{pformat(configs)}")
    logger.info(f"config_keys:\n{pformat(configs.get('configs').keys())}")
    rule_ids=configs.get('configs').get('rule_ids')
    logger.info(f"rule_ids:\n{pformat(rule_ids)}")
    for rule_id in rule_ids:
        logger.info(f"columns:\n{pformat(rule_id)}")

    sql_string = template.render(configs)
    failed_records_sql_string = failed_records_template.render(configs)
    configs.update({"generated_sql_string": sql_string})
    #logger.info(f"after input_param update:{Template(sql_string).safe_substitute({'tgt_snapshot_value':'202207'})}")
    configs.update({"failed_records_sql_string": failed_records_sql_string})
    print(f"failed_records_sql_string:{failed_records_sql_string}")
    if debug:
        logger.info(pformat(configs))
    return configs


def update_configs_from_input_params(configs: dict):
    pass


def prepare_configs_from_rule_binding_id(
    rule_binding_id: str,
    rule_binding_configs: dict,
    dq_summary_table_name: str,
    environment: str | None,
    configs_cache: DqConfigsCache,
    redshift_client: RedshiftClient,
    dq_summary_table_exists: bool = False,
    metadata: dict | None = None,
    progress_watermark: bool = True,
    high_watermark_filter_exists: bool = False,
) -> dict:
    rule_binding = DqRuleBinding.from_dict(
        rule_binding_id, rule_binding_configs
    )
    resolved_rule_binding_configs = rule_binding.resolve_all_configs_to_dict(
        configs_cache=configs_cache,
    )
    configs: dict[typing.Any, typing.Any] = {
        "configs": dict(resolved_rule_binding_configs)
    }
    logger.info(f"configs:{configs}\nredshift_client:{redshift_client}")
    if environment:
        configs.update({"environment": environment})
    if not metadata:
        metadata = dict()
    if "metadata" in rule_binding_configs:
        metadata.update(rule_binding_configs["metadata"])
    configs.update({"dq_summary_table_name": dq_summary_table_name})
    configs.update({"metadata": metadata})
    configs.update({"dq_summary_table_exists": dq_summary_table_exists})
    logger.info(f"resolved_rule_binding_configs:{resolved_rule_binding_configs}")
    configs.update(
        {"configs_hashsum": sha256_digest(json.dumps(resolved_rule_binding_configs))}
    )
    configs.update({"progress_watermark": progress_watermark})
    incremental_time_filter_column = configs["configs"][
        "incremental_time_filter_column"
    ]
    logger.debug(f"Incremental time filter column {incremental_time_filter_column}")
    if incremental_time_filter_column:
        high_watermark_filter_exists = True
        fully_qualified_table_name = (
            f"{configs['configs']['entity_configs']['schema_name']}."
            f"{configs['configs']['entity_configs']['table_name']}"
        )
        high_watermark_dict = get_high_watermark_value(
            fully_qualified_table_name=fully_qualified_table_name,
            rule_binding_id=rule_binding_id,
            dq_summary_table_name=dq_summary_table_name,
            redshift_client=redshift_client,
        )
        configs.update(high_watermark_dict)
    configs.update({"high_watermark_filter_exists": high_watermark_filter_exists})
    logger.debug(f"Prepared json configs for {rule_binding_id}:\n{pformat(configs)}")
    return configs


def get_high_watermark_value(
    fully_qualified_table_name: str,
    rule_binding_id: str,
    dq_summary_table_name: str,
    redshift_client: RedshiftClient,
) -> dict:
    query = f"""SELECT
        COALESCE(MAX(execution_ts), cast('2022-01-01 00:00:00' as TIMESTAMP)) as high_watermark,
        CURRENT_TIMESTAMP as current_timestamp_value
        FROM data_sciences.{dq_summary_table_name}
        WHERE table_id = '{fully_qualified_table_name}'
        AND rule_binding_id = '{rule_binding_id}'"""
    logger.info(f"High watermark query is \n {query}")
    result = redshift_client.execute_query(query_string=query)
    logger.info(f"High watermark query result:{pformat(result)}")
    high_watermark_value = ""
    current_timestamp_value = ""
    for row in result:
        # Row values can be accessed by field name or index.
        logger.info(f"High watermark value is {row}")
        high_watermark_value = row[0]
        current_timestamp_value = row[1]
    out_dict = {
        "high_watermark_value": high_watermark_value,
        "current_timestamp_value": current_timestamp_value,
    }
    return out_dict


def create_entity_summary_model(
    entity_table_id: str,
    entity_target_rule_binding_configs: dict,
    failed_queries_configs: dict,
    debug: bool = False,
) -> str:
    if debug:
        logger.info(
            f"Generating Entity-level DQ Summary aggregate for entity "
            f"{entity_table_id} with entity_target_rule_binding_configs:\n"
            f"{pformat(entity_target_rule_binding_configs)}"
        )
    template = load_jinja_template(
        template_path=Path("macros", "create_entity_aggregate_dq_summary.sql")
    )
    configs = {
        "entity_target_rule_binding_configs": entity_target_rule_binding_configs,
        "failed_queries_configs": failed_queries_configs,
    }
    sql_string = template.render(configs)
    if debug:
        logger.debug(
            f"Generated sql for entity_table_id: {entity_table_id}:\n{sql_string}"
        )
    return sql_string


def write_sql_string_as_dbt_model(
    model_id: str, sql_string: str, dbt_model_path: Path
) -> None:
    print(f"Lib module - dbt_model_path:{dbt_model_path}")
    with open(dbt_model_path / f"{model_id}.sql", "w") as f:
        f.write(sql_string.strip())


def update_config_sql_string(configs: dict,
                             config_key: str,
                             config_value: str):
    generated_sql_string = configs.get("generated_sql_string")
    if config_key == 'snapshot_date':
        generated_sql_string = Template(generated_sql_string).safe_substitute({'tgt_tbl_snapshot_value':config_value})
        configs.update(dict(generated_sql_string=generated_sql_string))

    return configs
