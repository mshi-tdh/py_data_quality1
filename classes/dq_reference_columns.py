from __future__ import annotations

from dataclasses import dataclass

from utils import assert_list_type
from utils import assert_not_none_or_empty
from logger import getlogger


logger = getlogger()
def transform_dq_reference_columns_to_dict(reference_columns_record: dict) -> dict:
    return dict(
        {
            "id": reference_columns_record["id"],
            "include_reference_columns": reference_columns_record[
                "include_reference_columns"
            ]
            .strip("[]")
            .replace('"', "")
            .split(", "),
        }
    )


@dataclass
class DqReferenceColumns:
    """
    A class to represent a DqReferenceColumns.

    ...

    Attributes
    ----------
        reference_columns_id : str
            reference columns id
        include_reference_columns : list
            List of reference columns to be included.

    Methods
    -------
    from_dict(reference_columns_id: str, kwargs: dict):
        Returns the DqReferenceColumns object for
        the given reference_columns_id.

    to_dict(cls: DqReferenceColumns):
        Returns the DqReferenceColumns object for
        the given reference_columns_id.

    dict_values(cls: DqReferenceColumns):
        Returns the DqReferenceColumns dict.
    """

    reference_columns_id: str
    include_reference_columns: list

    @classmethod
    def from_dict(
        cls: DqReferenceColumns,
        reference_columns_id: str,
        kwargs: dict,
    ) -> DqReferenceColumns:
        """

        Args:
          cls: DqReferenceColumns:
          reference_columns_id: str:
          kwargs: dict:

        Returns:

        """

        include_reference_columns: list = kwargs.get("include_reference_columns", "")
        assert_not_none_or_empty(
            include_reference_columns,
            f"Reference Columns ID: {reference_columns_id} must define attribute "
            f"'include_reference_columns'.",
        )
        assert_list_type(
            include_reference_columns,
            f"Reference Columns ID: {reference_columns_id} must define attribute "
            f"'include_reference_columns' of type list.",
        )
        return DqReferenceColumns(
            reference_columns_id=str(reference_columns_id),
            include_reference_columns=include_reference_columns,
        )

    def to_dict(self: DqReferenceColumns) -> dict:
        """

        Args:
          self: DqReferenceColumns:

        Returns:

        """
        return dict(
            {
                f"{self.reference_columns_id}": {
                    "include_reference_columns": self.include_reference_columns
                }
            }
        )

    def dict_values(self: DqReferenceColumns) -> dict:
        """

        Args:
          self: DqReferenceColumns:

        Returns:

        """

        return dict(self.to_dict().get(self.reference_columns_id))
