from __future__ import annotations

from dataclasses import dataclass

from utils import assert_not_none_or_empty
from logger import getlogger


logger = getlogger()

@dataclass
class DqRowFilter:
    """ """

    row_filter_id: str
    filter_sql_expr: str

    @classmethod
    def from_dict(
        cls: DqRowFilter,
        row_filter_id: str,
        kwargs: dict,
    ) -> DqRowFilter:
        """

        Args:
          cls: DqRowFilter:
          row_filter_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        filter_sql_expr: str = kwargs.get("filter_sql_expr", "")
        assert_not_none_or_empty(
            filter_sql_expr,
            f"Row Filter ID: {row_filter_id} must define attribute "
            f"'filter_sql_expr'.",
        )
        return DqRowFilter(
            row_filter_id=str(row_filter_id),
            filter_sql_expr=filter_sql_expr,
        )

    def to_dict(self: DqRowFilter) -> dict:
        """

        Args:
          self: DqRowFilter:

        Returns:

        """

        return dict(
            {
                f"{self.row_filter_id}": {
                    "filter_sql_expr": self.filter_sql_expr,
                }
            }
        )

    def dict_values(self: DqRowFilter) -> dict:
        """

        Args:
          self: DqRowFilter:

        Returns:

        """

        return dict(self.to_dict().get(self.row_filter_id))
