from hatchling.builders.hooks.plugin.interface import BuildHookInterface
import subprocess

class BuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        try:
            # Try to get the latest tag
            tag = subprocess.run(
                ["git", "describe", "--tags", "--exact-match"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            version = tag.lstrip("v")  # optional: strip "v" prefix
        except subprocess.CalledProcessError:
            # No tag found — fall back to short commit hash
            version = (
                subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
            )
        build_data["version"] = version