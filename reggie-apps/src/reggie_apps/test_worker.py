import asyncio
import logging

from aiolibs_executor import Executor
from reggie_core import logs

LOG = logs.logger()


async def _stream(
    pipe: asyncio.StreamReader, level: int, encoding: str = "utf-8"
) -> None:
    while True:
        line = await pipe.readline()
        if not line:
            break
        try:
            LOG.log(level, line.decode(encoding, errors="replace").rstrip())
        except Exception:
            LOG.log(level, line.rstrip())


async def worker(arg):
    try:
        print("sleeping")
        await asyncio.sleep(1)
        print("done")
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            "echo 'starting...'; sleep 3; echo 'done'; echo 'this is an error' 1>&2",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_task = asyncio.create_task(_stream(proc.stdout, logging.INFO))
        err_task = asyncio.create_task(_stream(proc.stderr, logging.ERROR))
        await asyncio.gather(proc.wait(), out_task, err_task)
        return "neat"

    except BaseException as e:
        print(f"error:{arg} {e}")
        raise e


async def main():
    executor = Executor(num_workers=3)
    try:
        futs = [await executor.submit(worker(i)) for i in range(5)]
        print(f"submitted:{len(futs)}")
        await asyncio.gather(*futs, return_exceptions=True)
    except asyncio.CancelledError:
        print("CANCELLED")
    finally:
        await asyncio.shield(executor.shutdown(cancel_futures=True))
        print("all done")


if __name__ == "__main__":
    asyncio.run(main())
