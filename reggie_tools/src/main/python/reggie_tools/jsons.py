import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any


def dumps(obj: Any) -> Any:
    return json.dumps(obj, cls=DataclassJSONEncoder)


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)
