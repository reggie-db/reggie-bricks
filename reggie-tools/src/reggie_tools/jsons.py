"""JSON serialization helpers tailored for Databricks-friendly objects."""

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any


def dumps(obj: Any) -> Any:
    """Serialize ``obj`` while automatically handling dataclasses and ISO dates."""
    return json.dumps(obj, cls=DataclassJSONEncoder)


class DataclassJSONEncoder(json.JSONEncoder):
    """JSON encoder that understands dataclasses and ``datetime`` objects."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)
