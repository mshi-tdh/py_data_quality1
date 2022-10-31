from __future__ import annotations

from dataclasses import dataclass
from pprint import pformat

import re
import sqlite3

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError
from utils import unnest_object_to_list
from classes import dq_reference_columns
from logger import getlogger
import classes.dq_entity as dq_entity
import classes.dq_row_filter as dq_row_filter
import classes.dq_rule as dq_rule
import classes.dq_rule_binding as dq_rule_binding
import classes.dq_rule_dimensions as dq_rule_dimensions
from classes.dq_reference_columns import transform_dq_reference_columns_to_dict
from utils import convert_json_value_to_dict

logger = getlogger()

RE_NON_ALPHANUMERIC = re.compile(r"[^0-9a-zA-Z_]+")
NUM_RULES_PER_TABLE = 50
GET_ENTITY_SUMMARY_QUERY = """
select
    e.schema_name,
    e.table_name,
    group_concat(rb.id, ',') as rule_binding_ids_list,
    group_concat(json_array_length(rb.rule_ids), ',') as rules_per_rule_binding
from
    entities e
inner join
    rule_bindings rb
    on UPPER(e.id) = UPPER(rb.entity_id)
where
    rb.id in ({target_rule_binding_ids_list})
group by
    e.schema_name,
    e.table_name
"""

@dataclass
class DqConfigsCache:
    _cache_db: Database

    def __init__(self, sqlite3_db_name: str | None = None):
        if sqlite3_db_name:
            cache_db = Database(sqlite3.connect(sqlite3_db_name))
        else:
            cache_db = Database("dq_configs.db", recreate=True)
        self._cache_db = cache_db

    def get_table_entity_id(self, entity_id: str) -> dq_entity.DqEntity:
        entity_id = entity_id.upper()
        try:
            logger.debug(
                f"Attempting to get from configs cache table entity_id: {entity_id}"
            )
            logger.info(f"self._cache_db['entities']:{self._cache_db['entities']}")
            entity_record = self._cache_db["entities"].get(entity_id)
            logger.info(f"entity_record:{entity_record}")
        except NotFoundError:
            error_message = (
                f"Table Entity_ID: {entity_id} not found in 'entities' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from entities')))}"
            )
            raise NotFoundError(error_message)
        convert_json_value_to_dict(entity_record, "environment_override")
        convert_json_value_to_dict(entity_record, "columns")
        logger.info(f"entity_record_2:{entity_record}")
        entity = dq_entity.DqEntity.from_dict(entity_id, entity_record)
        #partition_fields = entity.get_partition_fields()
        #entity.partition_fields = partition_fields
        return entity

    def get_rule_id(self, rule_id: str) -> dq_rule.DqRule:
        rule_id = rule_id.upper()
        try:
            rule_record = self._cache_db["rules"].get(rule_id)
        except NotFoundError:
            error_message = (
                f"Rule_ID: {rule_id} not found in 'rules' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rules')))}"
            )
            logger.error(error_message, exc_info=True)
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_record, "params")
        rule = dq_rule.DqRule.from_dict(rule_id, rule_record)
        return rule

    def get_rule_dimensions(self) -> dq_rule.DqRuleDimensions:
        try:
            dims = self._cache_db["rule_dimensions"].get("rule_dimension")
        except NotFoundError:
            error_message = (
                f"Rule dimensions not found in config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rule_dimension')))}"
            )
            logger.error(error_message, exc_info=True)
            raise NotFoundError(error_message)
        return dq_rule_dimensions.DqRuleDimensions(dims)

    def get_row_filter_id(self, row_filter_id: str) -> dq_row_filter.DqRowFilter:
        row_filter_id = row_filter_id.upper()
        try:
            row_filter_record = self._cache_db["row_filters"].get(row_filter_id)
        except NotFoundError:
            error_message = (
                f"Row Filter ID: {row_filter_id} not found in 'row_filters' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from row_filters')))}"
            )
            raise NotFoundError(error_message)
        row_filter = dq_row_filter.DqRowFilter.from_dict(
            row_filter_id, row_filter_record
        )
        return row_filter

    def get_reference_columns_id(
        self, reference_columns_id: str
    ) -> dq_reference_columns.DqReferenceColumns:
        reference_columns_id = reference_columns_id.upper()
        try:
            reference_columns_record = self._cache_db["reference_columns"].get(
                reference_columns_id
            )
            reference_columns_record_obj = transform_dq_reference_columns_to_dict(
                reference_columns_record
            )
        except NotFoundError:
            error_message = (
                f"Reference Column ID: {reference_columns_id} not found in 'reference_columns' config cache:\n"
                f"List of available Reference Column ID's is \n "
                f"{pformat(list(self._cache_db.query('select id from reference_columns')))}"
            )
            raise NotFoundError(error_message)
        reference_columns = dq_reference_columns.DqReferenceColumns.from_dict(
            reference_columns_id, reference_columns_record_obj
        )
        return reference_columns

    def get_rule_binding_id(
        self, rule_binding_id: str
    ) -> dq_rule_binding.DqRuleBinding:
        rule_binding_id = rule_binding_id.upper()
        try:
            rule_binding_record = self._cache_db["rule_bindings"].get(rule_binding_id)
        except NotFoundError:
            error_message = (
                f"Rule_Binding_ID: {rule_binding_id} not found in 'rule_bindings' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rule_bindings')))}"
            )
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_binding_record, "rule_ids")
        convert_json_value_to_dict(rule_binding_record, "metadata")
        rule_binding = dq_rule_binding.DqRuleBinding.from_dict(
            rule_binding_id, rule_binding_record
        )
        return rule_binding

    def load_all_rule_bindings_collection(self, rule_binding_collection: dict) -> None:
        logger.debug(
            f"Loading 'rule_bindings' configs into cache:\n{pformat(rule_binding_collection.keys())}"
        )
        rule_bindings_rows = unnest_object_to_list(rule_binding_collection)
        for record in rule_bindings_rows:
            try:
                dq_rule_binding.DqRuleBinding.from_dict(
                    rule_binding_id=record["id"], kwargs=record, validate_uri=False
                )
            except Exception as e:
                raise ValueError(f"Failed to parse Rule Binding with error:\n{e}\n")
            if "entity_uri" not in record:
                record.update({"entity_uri": None})
        self._cache_db["rule_bindings"].upsert_all(
            rule_bindings_rows, pk="id", alter=True
        )

    def load_all_entities_collection(self, entities_collection: dict) -> None:
        logger.debug(
            f"Loading 'entities' configs into cache:\n{pformat(entities_collection.keys())}"
        )
        enriched_entities_configs = {}
        for entity_id, entity_configs in entities_collection.items():
            entity = dq_entity.DqEntity.from_dict(entity_id, entity_configs)
            logger.info(f"load_all_entities_collection:entity{entity}\nentity_configs:{pformat(entity_configs)}")
            enriched_entities_configs.update(entity.to_dict())
        logger.debug(
            f"entities_collection:\n{pformat(entities_collection)}\n"
            f"enriched_entities_configs:\n{pformat(enriched_entities_configs)}"
        )
        self._cache_db["entities"].upsert_all(
            unnest_object_to_list(enriched_entities_configs), pk="id", alter=True
        )

    def load_all_row_filters_collection(self, row_filters_collection: dict) -> None:
        logger.debug(
            f"Loading 'row_filters' configs into cache:\n{pformat(row_filters_collection.keys())}"
        )
        for row_filter_id, row_filter_record in row_filters_collection.items():
            try:
                dq_row_filter.DqRowFilter.from_dict(
                    row_filter_id=row_filter_id, kwargs=row_filter_record
                )
            except Exception as e:
                raise ValueError(f"Failed to parse Row Filter with error:\n{e}\n")
        self._cache_db["row_filters"].upsert_all(
            unnest_object_to_list(row_filters_collection), pk="id", alter=True
        )

    def load_all_reference_columns_collection(
        self, reference_columns_collection: dict
    ) -> None:
        logger.debug(
            f"Loading 'reference_columns' configs into cache:\n{pformat(reference_columns_collection.keys())}"
        )
        for (
            reference_columns_id,
            reference_columns_record,
        ) in reference_columns_collection.items():
            try:
                dq_reference_columns.DqReferenceColumns.from_dict(
                    reference_columns_id=reference_columns_id,
                    kwargs=reference_columns_record,
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to parse Reference Columns with error:\n{e}\n"
                )
        self._cache_db["reference_columns"].upsert_all(
            unnest_object_to_list(reference_columns_collection), pk="id", alter=True
        )

    def load_all_rules_collection(self, rules_collection: dict) -> None:
        logger.debug(
            f"Loading 'rules' configs into cache:\n{pformat(rules_collection.keys())}"
        )
        for rules_id, rules_record in rules_collection.items():
            try:
                dq_rule.DqRule.from_dict(rule_id=rules_id, kwargs=rules_record)
            except Exception as e:
                raise ValueError(f"Failed to parse Rule with error:\n{e}\n")
        self._cache_db["rules"].upsert_all(
            unnest_object_to_list(rules_collection), pk="id", alter=True
        )

    def load_all_rule_dimensions_collection(
        self, rule_dimensions_collection: list
    ) -> None:
        logger.debug(
            f"Loading 'rule_dimensions' configs into cache:\n{pformat(rule_dimensions_collection)}"
        )
        self._cache_db["rule_dimensions"].upsert_all(
            [{"rule_dimension": dim.upper()} for dim in rule_dimensions_collection],
            pk="rule_dimension",
            alter=True,
        )

    def update_config(
        configs_type: str, config_old: list | dict, config_new: list | dict
    ) -> list | dict:
        if configs_type == "rule_dimensions":
            return DqConfigsCache.update_config_lists(config_old, config_new)
        else:
            return DqConfigsCache.update_config_dicts(config_old, config_new)

    def update_config_dicts(config_old: dict, config_new: dict) -> dict:

        if not config_old and not config_new:
            return {}
        elif not config_old:
            return config_new.copy()
        elif not config_new:
            return config_old.copy()
        else:
            intersection = config_old.keys() & config_new.keys()

            # The new config defines keys that we have already loaded
            if intersection:
                # Verify that objects pointed to by duplicate keys are identical
                config_old_i = {}
                config_new_i = {}
                for k in intersection:
                    config_old_i[k] = config_old[k]
                    config_new_i[k] = config_new[k]

                # == on dicts performs deep compare:
                if not config_old_i == config_new_i:
                    raise ValueError(
                        f"Detected Duplicated Config ID(s): {intersection}. "
                        f"If a config ID is repeated, it must be for an identical "
                        f"configuration.\n"
                        f"Duplicated config contents:\n"
                        f"{pformat(config_old_i)}\n"
                        f"{pformat(config_new_i)}"
                    )

            updated = config_old.copy()
            updated.update(config_new)
            return updated

    def update_config_lists(config_old: list, config_new: list) -> list:

        if not config_old and not config_new:
            return []
        elif not config_old:
            return config_new.copy()
        elif not config_new:
            return config_old.copy()
        else:
            # Both lists contain data. This is only OK if they are identical.
            if not sorted(config_old) == sorted(config_new):
                raise ValueError(
                    f"Detected Duplicated Config: {config_new}."
                    f"If a config is repeated, it must be identical."
                )
            return config_old.copy()

    def get_entities_configs_from_rule_bindings(
        self, target_rule_binding_ids: list[str]
    ) -> dict[str, str]:
        query = GET_ENTITY_SUMMARY_QUERY.format(
            target_rule_binding_ids_list=",".join(
                [f"'{id.upper()}'" for id in target_rule_binding_ids]
            )
        )
        logger.info(f"dq_configs_cache.function_name=get_entities_configs_from_rule_bindings\nquery={query}")
        target_entity_summary_views_configs = {}
        entity_summary = self._cache_db.query(query)
        if entity_summary:
            #logger.info(f"entity_summary is not empty:{list(entity_summary)}")
            for record in entity_summary:
                logger.info(f"entity_summary_record={record}")
                num_rules_per_table_count = 0
                entity_table_id = "__".join(
                    [
                        record["schema_name"],
                        record["table_name"],
                        #record["column_id"],
                    ]
                )
                entity_table_id = re.sub(RE_NON_ALPHANUMERIC, "_", entity_table_id)
                if len(entity_table_id) > 1023:
                    entity_table_id = entity_table_id[-1022:]
                rule_binding_ids = record["rule_binding_ids_list"].split(",")
                rules_per_rule_binding = record["rules_per_rule_binding"].split(",")
                in_scope_rule_bindings = []
                table_increment = 0
                for index in range(0, len(rules_per_rule_binding)):
                    in_scope_rule_bindings.append(rule_binding_ids[index])
                    num_rules_per_table_count += int(rules_per_rule_binding[index])
                    if num_rules_per_table_count > NUM_RULES_PER_TABLE:
                        table_increment += 1
                        target_entity_summary_views_configs[
                            f"{entity_table_id}_{table_increment}"
                        ] = {
                            "rule_binding_ids_list": in_scope_rule_bindings.copy(),
                        }
                        in_scope_rule_bindings = []
                        num_rules_per_table_count = 0
                else:
                    if in_scope_rule_bindings:
                        table_increment += 1
                        target_entity_summary_views_configs[
                            f"{entity_table_id}_{table_increment}"
                        ] = {
                            "rule_binding_ids_list": in_scope_rule_bindings.copy(),
                        }
            logger.debug(
                f"target_entity_summary_views_configs:\n"
                f"{pformat(target_entity_summary_views_configs)}"
            )
            return target_entity_summary_views_configs

        else:
            raise ValueError(
                f"""Failed to retrieve entity and target rule binding id's \n
                    {target_rule_binding_ids} associated with it"""
            )