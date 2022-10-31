from __future__ import annotations

from dataclasses import dataclass
from logger import getlogger

logger = getlogger()

@dataclass
class DqRuleDimensions:
    """ """

    dimensions: list | None = None
