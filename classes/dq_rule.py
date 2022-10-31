from __future__ import annotations

from dataclasses import dataclass

from classes.rule_type import RuleType
from logger import getlogger


logger = getlogger()

@dataclass
class DqRule:
    """ """

    rule_id: str
    rule_type: RuleType
    rule_sql_expr: str | None = None
    dimension: str | None = None
    params: dict | None = None

    @classmethod
    def validate(cls: DqRule, config_id: str, config: dict, rule_dims: list) -> None:
        print(f"config_id={config_id}\nconfig={config}\nrule_dims={rule_dims}")
        if "dimension" in config:
            if not rule_dims:
                raise ValueError(
                    f"Invalid rule dimension '{config['dimension']}' in "
                    f"rule configurations ID '{config_id}'. \n"
                    f"The list of allowed rule_dimensions is empty. You can add '{config['dimension']}' to"
                    f"the allowed list of dimensions in the 'rule_dimensions' YAML config node, e.g.\n"
                    f"```\nrule_dimensions:\n"
                    f"  - consistency\n"
                    f"  - correctness\n"
                    f"  - duplication\n"
                    f"  - completeness\n"
                    f"  - conformance\n"
                    f"  - integrity\n"
                    f"```"
                )
            if not config["dimension"].upper() in rule_dims:
                raise ValueError(
                    f"Invalid rule dimension '{config['dimension'].upper()}' in "
                    f"rule configurations ID '{config_id}'. \n"
                    f"Ensure it is one of the allowed rule_dimensions: {rule_dims} or add it to "
                    f"the allowed list of dimensions in the 'rule_dimensions' YAML config node, e.g.\n"
                    f"```\nrule_dimensions:\n"
                    f"  - consistency\n"
                    f"  - correctness\n"
                    f"  - duplication\n"
                    f"  - completeness\n"
                    f"  - conformance\n"
                    f"  - integrity\n"
                    f"```"
                )

    @classmethod
    def from_dict(cls: DqRule, rule_id: str, kwargs: dict) -> DqRule:
        """

        Args:
          cls: DqRule:
          rule_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        rule_type: RuleType = RuleType(kwargs.get("rule_type", ""))
        params: dict = kwargs.get("params", dict())
        dimension: str = kwargs.get("dimension", None)
        dimension = dimension.upper() if dimension else None
        return DqRule(
            rule_id=str(rule_id),
            rule_type=rule_type,
            dimension=dimension,
            params=params,
        )

    def to_dict(self: DqRule) -> dict:
        """

        Args:
          self: DqRule:

        Returns:

        """
        return {
            f"{self.rule_id}": {
                "rule_type": self.rule_type.name,
                "dimension": self.dimension,
                "params": self.params,
                "rule_sql_expr": self.resolve_sql_expr(),
            }
        }

    def dict_values(self: DqRule) -> dict:
        """

        Args:
          self: DqRule:

        Returns:

        """

        return dict(self.to_dict().get(self.rule_id))

    def resolve_sql_expr(self: DqRule) -> str:
        logger.info(f"self.params:{self.params}")
        self.rule_sql_expr = self.rule_type.to_sql(self.params).safe_substitute()
        return self.rule_sql_expr

    def update_configs_from_input_params(self, arguments: dict):
        pass

    def update_rule_binding_arguments(self, arguments: dict) -> None:
        params = {"rule_binding_arguments": arguments}
        if not self.params:
            self.params = params
        elif type(self.params) == dict:
            self.params.update(params)
        else:
            raise ValueError(
                f"DqRule ID: {self.rule_id} has invalid 'params' field:\n {self.params}"
            )
