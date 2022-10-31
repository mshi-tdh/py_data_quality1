from __future__ import annotations
from enum import Enum
from classes.dq_entity import DqEntity
from classes.dq_reference_columns import DqReferenceColumns
from classes.dq_row_filter import DqRowFilter
from classes.dq_rule import DqRule
from classes.dq_rule_binding import DqRuleBinding
from classes.dq_rule_dimensions import DqRuleDimensions
from enum import unique


@unique
class DqConfigType(str, Enum):
    RULES = "rules"
    RULE_BINDINGS = "rule_bindings"
    RULE_DIMENSIONS = "rule_dimensions"
    ROW_FILTERS = "row_filters"
    REFERENCE_COLUMNS = "reference_columns"
    ENTITIES = "entities"
    METADATA_REGISTRY_DEFAULTS = "metadata_registry_defaults"

    def is_required(
        self: DqConfigType,
    ) -> bool:
        if self == DqConfigType.RULES:
            return True
        elif self == DqConfigType.RULE_BINDINGS:
            return True
        elif self == DqConfigType.RULE_DIMENSIONS:
            return False
        elif self == DqConfigType.ROW_FILTERS:
            return True
        elif self == DqConfigType.REFERENCE_COLUMNS:
            return False
        elif self == DqConfigType.ENTITIES:
            return False
        else:
            raise NotImplementedError(f"DQ Config Type: {self} not implemented.")

    def to_class(
        self,
    ) -> type[DqRule] | type[DqRuleBinding] | type[DqRuleDimensions] | type[
        DqRowFilter
    ] | type[DqEntity]:
        if self == DqConfigType.RULES:
            return DqRule
        elif self == DqConfigType.RULE_BINDINGS:
            return DqRuleBinding
        elif self == DqConfigType.RULE_DIMENSIONS:
            return DqRuleDimensions
        elif self == DqConfigType.ROW_FILTERS:
            return DqRowFilter
        elif self == DqConfigType.REFERENCE_COLUMNS:
            return DqReferenceColumns
        elif self == DqConfigType.ENTITIES:
            return DqEntity
        else:
            raise NotImplementedError(f"DQ Config Type: {self} not implemented.")
