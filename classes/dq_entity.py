from __future__ import annotations

from dataclasses import dataclass
from pprint import pformat

import json
from logger import getlogger

from classes.dq_entity_column import DqEntityColumn
from utils import assert_not_none_or_empty
from utils import get_format_string_arguments
from utils import get_from_dict_and_assert

logger = getlogger()

ENTITY_CUSTOM_CONFIG_MAPPING = {
    "MYSQL": {
        "table_name": "{table_name}",
        "schema_name": "{schema_name}",
        "instance_name": "{project_name}",
        "resource_type": "{resource_type}",
    },
    "REDSHIFT": {
        "table_name": "{table_name}",
        "schema_name": "{schema_name}",
        "instance_name": "{project_name}",
        "resource_type": "{resource_type}",
    }
}

logger = getlogger()


def get_custom_entity_configs(
    entity_id: str, configs_map: dict, source_database: str, config_key: str
) -> str:
    logger.info(f"entity_id={entity_id},configs_map={configs_map},source_database={source_database},"
                 f"config_key={config_key}")
    entity_configs = ENTITY_CUSTOM_CONFIG_MAPPING.get(source_database)
    logger.info(f"entity_configs={entity_configs}")
    if not entity_configs:
        raise NotImplementedError(
            f"Entity Config ID '{entity_id}' has unsupported source_database "
            f"'{source_database}'."
        )
    entity_config_template = entity_configs.get(config_key)
    logger.info(f"entity_config_template={entity_config_template}")
    if not entity_config_template:
        raise NotImplementedError(
            f"Entity Config ID '{entity_id}' with source_database "
            f"'{source_database}' has unsupported config value '{config_key}'."
        )
    entity_config_template_arguments = get_format_string_arguments(
        entity_config_template
    )
    entity_config_arguments = dict()
    for argument in entity_config_template_arguments:
        argument_value = configs_map.get(argument)
        if argument_value:
            entity_config_arguments[argument] = argument_value
    try:
        config_value = entity_config_template.format(**entity_config_arguments)
    except KeyError:
        if config_key in configs_map:
            config_value = configs_map.get(config_key)
        else:
            raise ValueError(
                f"Entity Config ID '{entity_id}' with source_database "
                f"'{source_database}' has incomplete config values.\n"
                f"Configs required: '{entity_config_template_arguments}'.\n"
                f"Configs supplied: '{configs_map}'."
            )
    return config_value


@dataclass
class DqEntity:
    """ """

    entity_id: str
    source_database: str
    table_name: str
    schema_name: str
    columns: dict[str, DqEntityColumn]
    environment_override: dict | None
    partition_fields: None


    def resolve_column_config(self: DqEntity, column_id: str) -> DqEntityColumn:
        """

        Args:
          self: DqRuleBinding:
          column_id: str:

        Returns:

        """
        logger.info(f"Inside resolve_column_config")
        dq_column_config = self.columns.get(column_id.upper(), None)
        logger.info(f"dq_column_config:{dq_column_config},type(dq_column_config):{type(dq_column_config)}")
        assert_not_none_or_empty(
            dq_column_config,
            f"Column ID '{column_id.upper()}' not found in Entity Config ID '{self.entity_id}'\n"
            f"Available column_ids:\n{pformat(list(self.columns.keys()))}\n"
            f"Complete entity configs:\n{pformat(self.dict_values())}.",
        )
        return dq_column_config

    @classmethod
    def from_dict(cls: DqEntity, entity_id: str, kwargs: dict) -> DqEntity:
        """

        Args:
          cls: DqEntity:
          entity_id: str:
          kwargs: typing.Dict:

        Returns:

        """
        logger.info(f"from_dict - entity_id:{entity_id},kwargs:{kwargs}")
        source_database = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="source_database"
        )
        source_database = source_database.upper()
        table_name = get_custom_entity_configs(
            entity_id=entity_id,
            configs_map=kwargs,
            source_database=source_database,
            config_key="table_name",
        )
        schema_name = get_custom_entity_configs(
            entity_id=entity_id,
            configs_map=kwargs,
            source_database=source_database,
            config_key="schema_name",
        )


        partition_fields = kwargs.get("partition_fields")

        logger.info(f"kwargs from columns_dict:{kwargs},entity_id:{entity_id}")

        columns_dict = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="columns"
        )
        logger.info(f"columns_dict:{columns_dict}")
        columns: dict[str, DqEntityColumn] = dict()
        for column_id, column_config in columns_dict.items():
            column = DqEntityColumn.from_dict(
                entity_column_id=column_id.upper(),
                kwargs=column_config,
                entity_source_database=source_database,
            )
            columns[column_id.upper()] = column
        # validate environment override
        environment_override = dict()
        input_environment_override = kwargs.get("environment_override", None)
        if input_environment_override and type(input_environment_override) == dict:
            for key, value in input_environment_override.items():
                target_env = get_from_dict_and_assert(
                    config_id=entity_id,
                    kwargs=value,
                    key="environment",
                    assertion=lambda v: v.lower() == key.lower(),
                    error_msg=f"Environment target key {key} must match value.",
                )
                override_configs = value["override"]
                schema_name_override = get_custom_entity_configs(
                    entity_id=entity_id,
                    configs_map=override_configs,
                    source_database=source_database,
                    config_key="schema_name",
                )
                try:
                    table_name_override = get_custom_entity_configs(
                        entity_id=entity_id,
                        configs_map=override_configs,
                        source_database=source_database,
                        config_key="table_name",
                    )
                except ValueError:
                    table_name_override = table_name
                environment_override[target_env] = {
                    "schema_name": schema_name_override,
                    "table_name": table_name_override,
                }
                environment_override[target_env].update(value)
        return DqEntity(
            entity_id=str(entity_id),
            source_database=source_database,
            table_name=table_name,
            schema_name=schema_name,
            columns=columns,
            environment_override=environment_override,
            partition_fields=partition_fields
        )

    def to_dict(self: DqEntity) -> dict:
        """

        Args:
          self: DqEntity:

        Returns:

        """
        logger.info(f"DQ Entity :{self.columns.items()}")
        columns = {
            column_id: column_config.dict_values()
            for column_id, column_config in self.columns.items()
        }
        logger.info(f"DQ Entity columns :{columns}")
        output = {
            "source_database": self.source_database,
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "columns": columns,
            "partition_fields": self.partition_fields,
        }
        if self.environment_override:
            output["environment_override"] = self.environment_override
        return dict({f"{self.entity_id}": output})

    def dict_values(self: DqEntity) -> dict:
        return dict(self.to_dict().get(self.entity_id))


    def get_table_name(self):
        return f"{self.schema_name}.{self.table_name}"
