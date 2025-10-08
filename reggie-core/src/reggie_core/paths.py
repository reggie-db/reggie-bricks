import functools
import getpass
import hashlib
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from filelock import FileLock, Timeout

from reggie_core import logs

_CACHE_VERSION = 1


def path(input, resolve: bool = True, exists: bool = False) -> Optional[Path]:
    if input is not None:
        try:
            if not isinstance(input, Path):
                input = Path(input)
            if not resolve:
                input = input.resolve()
            if not exists or input.exists():
                return input
        except Exception:
            pass
    return None


@functools.cache
def temp_dir() -> Path:
    temp_dir = path(tempfile.gettempdir(), exists=True)
    if not temp_dir:
        temp_dir = path("/tmp", exists=True)
    if not temp_dir:
        temp_dir = path("./tmp")
        if not temp_dir.exists:
            temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@functools.cache
def home() -> Path:
    try:
        home_path = path(Path.home(), exists=True)
        if home_path:
            return home_path
    except Exception:
        pass
    try:
        username = getpass.getuser()
    except Exception:
        username = None
    if not username:
        username = "home"
    td = temp_dir()
    return td / username


def cache_store(name: str, loader: Callable[[Path], None]) -> Path:
    if name and re.search(r"[^A-Za-z0-9_]", name):
        name_parts = re.split(r"[^a-zA-Z0-9]", name)
        name_parts.append(hashlib.md5(name.encode("utf-8")).hexdigest())
        cache_dir_name = "_".join(name_parts) if name_parts else name
    else:
        cache_dir_name = name
    if not cache_dir_name:
        raise ValueError(f"invalid name:{name}")
    cache_dir = home() / ".path_cache" / (cache_dir_name + f"_v{_CACHE_VERSION}")

    done_path = cache_dir / ".done"
    store_path = cache_dir / "store"

    def _is_done() -> bool:
        return done_path.exists() and store_path.exists()

    if _is_done():
        return store_path
    log = logs.logger(__name__)
    summary = f"name:{name} store_path:{store_path}"
    lock_path = cache_dir / ".lock"
    lock = FileLock(str(lock_path))
    start = time.monotonic()
    while True:
        try:
            lock.acquire(timeout=5)
        except Timeout:
            elapsed = time.monotonic() - start
            log.warning("waiting for lock - elapsed:%s %s", f"{elapsed:.1f}", summary)
            continue
        try:
            if _is_done():
                return store_path
            if store_path.exists():
                if store_path.is_dir():
                    shutil.rmtree(store_path)
                else:
                    store_path.unlink()
            store_path.mkdir(parents=True, exist_ok=True)
            log.info("cache store load - %s", summary)
            loader(store_path)
            done_path.write_text(summary)
            return store_path
        finally:
            lock.release()


if "__main__" == __name__:
    print(home())
    store_path = cache_store(
        "test a", lambda store_path: (store_path / "test.txt").write_text("suh")
    )
    print(store_path)
