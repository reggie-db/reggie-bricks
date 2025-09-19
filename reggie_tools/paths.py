import hashlib
import re
import shutil
import time
from pathlib import Path
from typing import Callable

from filelock import FileLock, Timeout

from reggie_tools import logs

_VERSION = 1


def cache_store(name: str, loader: Callable[[Path], None]) -> Path:
    if name and re.search(r"[^A-Za-z0-9_]", name):
        name_parts = re.split(r"[^a-zA-Z0-9]", name)
        name_parts.append(hashlib.md5(name.encode("utf-8")).hexdigest())
        cache_dir_name = "_".join(name_parts) if name_parts else name
    else:
        cache_dir_name = name
    if not cache_dir_name:
        raise ValueError(f"invalid name:{name}")
    cache_dir = Path.home() / ".path_cache" / (cache_dir_name + f"_v{_VERSION}")

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
    store_path = cache_store(
        "test a", lambda store_path: (store_path / "test.txt").write_text("suh")
    )
    print(store_path)
