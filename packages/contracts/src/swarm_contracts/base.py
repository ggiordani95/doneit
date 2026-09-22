"""Wire-format base model.

Every payload that crosses a process boundary serializes to ``camelCase`` and
accepts both spellings on the way in, so Python code stays idiomatic while the
wire format stays stable across language rewrites.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

__all__ = ["WireModel"]


class WireModel(BaseModel):
    """Base for anything published to a topic or an HTTP body."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        ser_json_timedelta="float",
        validate_assignment=True,
    )

    def to_wire(self) -> dict[str, object]:
        """Serialize with aliases, ready for the broker."""
        return self.model_dump(by_alias=True, mode="json", exclude_none=True)
