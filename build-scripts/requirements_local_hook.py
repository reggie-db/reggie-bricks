import os
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class RequirementsLocal(BuildHookInterface):
    def initialize(self, version, build_data):
        super().initialize(version, build_data)

        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return

        root = Path(self.root)
        req_file = root / "requirements-local.txt"
        if not req_file.exists():
            return

        # Read and clean requirements
        reqs = []
        for line in req_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                reqs.append(line)
        if not reqs:
            return

        # Inject dependencies dynamically
        deps = build_data.setdefault("dependencies", [])
        deps.extend(reqs)
