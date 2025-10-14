import json
import os
import re
from typing import Iterable, Union

import dynaconf
import giturlparse
import requirements
from reggie_core import logs, strs
from requirements.requirement import Requirement

_ENV_PREFIX = "DATABRICKS_APP_RUNNER"
_DEFAULT_NAME = "DEFAULT"


def read(**kwargs) -> Iterable["AppRunnerConfig"]:
    app_name_to_prefix: dict[str, str] = {}
    root_prefix_pattern = re.compile(rf"^{_ENV_PREFIX}_[^_]")
    app_prefix_pattern = re.compile(rf"^({_ENV_PREFIX}__(\S+)_)_[^_]")
    for name in os.environ:
        if match := re.match(root_prefix_pattern, name):
            app_name_to_prefix[""] = _ENV_PREFIX
        elif match := re.match(app_prefix_pattern, name):
            app_name = match.group(2)
            if not _DEFAULT_NAME == app_name:
                app_name_to_prefix[app_name] = match.group(1)
    for app_name, app_prefix in app_name_to_prefix.items():
        envar_prefixes = [f"{_ENV_PREFIX}__{_DEFAULT_NAME}_", app_prefix]
        settings = dynaconf.Dynaconf(envvar_prefix=",".join(envar_prefixes), **kwargs)
        yield AppRunnerConfig(settings, name=app_name)


def git_url(config: Union["AppRunnerConfig", str]) -> giturlparse.GitUrlParsed:
    source = config if isinstance(config, str) else config.source
    if not source:
        return None
    if requirement := _git_requirement(source):
        source = requirement.uri
    if giturlparse.validate(source):
        git_url = giturlparse.parse(source)
        print(git_url.url2ssh)
        return git_url

    return None


def _git_requirement(source: str) -> Requirement:
    try:
        for req in requirements.parse(source):
            if "git" == req.vcs and req.uri:
                return req
    except Exception:
        pass
    return None


def _git_revision(source: str) -> str:
    if requirement := _git_requirement(source):
        return requirement.revision
    return None


class AppRunnerConfig:
    ROOT_NAME = "ROOT"

    def __init__(self, settings: dynaconf.Dynaconf, name: str = None):
        self.settings = settings
        self._name = name

    @property
    def name(self) -> str:
        return self._name or self.ROOT_NAME

    @property
    def path(self) -> str:
        path = self.settings.get("path", None)
        if not path:
            path = "/".join(strs.tokenize(self._name)) if self._name else ""
        path = path.rstrip("/")
        if not path.startswith("/"):
            path = "/" + path
        return path

    @property
    def source(self) -> str:
        return self.settings.get("source", None)

    @property
    def github_token(self) -> str:
        return self.settings.get("github_token", None)

    @property
    def env(self, os_environ: bool = False) -> dict:
        env = {}
        if os_environ:
            for key, value in os.environ.items():
                if not key.startswith(self.ENV_PREFIX + "_"):
                    env[key] = value
        env.update(self.settings.get("env", {}))
        return env

    def __repr__(self) -> str:
        out = "AppRunnerConfig("
        for prop in self.properties():
            value = getattr(self, prop)
            out += f"{prop}={value}, "
        out += ")"
        return out

    @staticmethod
    def properties() -> Iterable[str]:
        for name in vars(AppRunnerConfig):
            attr = getattr(AppRunnerConfig, name)
            if isinstance(attr, property):
                yield name


if __name__ == "__main__":
    logs.auto_config()
    logs.logger().warning("info")
    print(AppRunnerConfig.properties())
    os.environ["DATABRICKS_APP_RUNNER__DEFAULT__GITHUB_TOKEN"] = "default_token"
    os.environ["DATABRICKS_APP_RUNNER_SOURCE"] = (
        "git+https://github.com/owner/repo.git@branch_name"
    )
    os.environ["DATABRICKS_APP_RUNNER_GITHUB_TOKEN"] = "abc123"
    os.environ["TEST"] = "root_replaced"
    os.environ["DATABRICKS_APP_RUNNER_TEST"] = "nested_replaced"
    os.environ["DATABRICKS_APP_RUNNER_ENV__PYTHONUNBUFFERED"] = "@format {this.test}"
    for name in ["APP_FUN", "APP_ABC_COOL0_1"]:
        tokenized_name = "_".join(strs.tokenize(name))
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__SOURCE"] = (
            f"https://github.com/reggie-db/reggie-bricks-py-{tokenized_name}"
        )
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__TEST"] = "app_replaced"
        env = {"test": "test", "test2": "@format {this.test}"}
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__ENV"] = f"@json {json.dumps(env)}"
    for config in read():
        print(config)
        url = git_url(config)
        print(_git_revision(config.source))
        print(url.branch if url else None)
