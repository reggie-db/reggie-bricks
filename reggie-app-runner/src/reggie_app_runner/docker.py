import shutil
from pathlib import Path


def path():
    for name in ["docker", "podman"]:
        if path := shutil.which(name):
            return Path(path)
    return None


if __name__ == "__main__":
    print(path())
