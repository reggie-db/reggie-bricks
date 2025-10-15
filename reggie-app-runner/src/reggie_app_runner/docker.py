import functools
import hashlib
import json
import platform
import shutil

from reggie_app_runner import conda

_CONDA_ENV_NAME = "_docker"


def path():
    for name in ["docker", "podman"]:
        if path := shutil.which(name):
            return path
    return None


def command():
    return conda.run(_conda_env_name()).bake(path() or "udocker")


def image_hash(image_name: str):
    data = _inspect(image_name)
    digest = data.get("Digest", None)
    if digest:
        return digest
    dumped_data = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(dumped_data).hexdigest()


def pull(image_name: str):
    command()("pull", image_name, _bg=True).wait()


@functools.cache
def _conda_env_name():
    dependencies = ["skopeo"]
    pip_dependencies = []
    linux = platform.system().casefold() == "linux"
    (dependencies if linux else pip_dependencies).append("udocker")
    conda.update(
        _CONDA_ENV_NAME,
        *dependencies,
        pip_dependencies=pip_dependencies,
    )
    return _CONDA_ENV_NAME


def _skopeo():
    return conda.run(_conda_env_name()).bake("skopeo")


def _inspect(image_name: str):
    image_os = "linux"
    image_arch = (
        "amd64" if platform.machine().lower() in ("arm64", "aarch64") else "amd64"
    )
    output = _skopeo()(
        "inspect",
        f"--override-os={image_os}",
        f"--override-arch={image_arch}",
        f"docker://{image_name}",
    )
    return json.loads(output)


if __name__ == "__main__":
    print(_conda_env_name())
    if True:
        print(image_hash("plexinc/pms-docker"))
        print(image_hash("plexinc/pms-docker:1.42.2.10156-f737b826c"))
        pull("plexinc/pms-docker")
    print(command()("version", _fg=True))
    command()("run", "hello-world", _fg=True)
