"""Async subprocess helpers with stdout/stderr streaming.

Provides tools for running subprocesses asynchronously or in a background
thread while streaming output line by line to optional callbacks.
"""

import asyncio
import concurrent
import inspect
import threading
import time
from subprocess import CalledProcessError
from typing import Any, Callable, List, Optional, Set

from reggie_core import funcs, logs

LOG = logs.logger(__name__)


class WorkerFuture(concurrent.futures.Future[int]):
    """A Future tracking an asyncio subprocess with safe result/exception handling."""

    def __init__(self):
        super().__init__()
        self.process: asyncio.subprocess.Process | None = None

    def add_done_callback(self, fn) -> None:
        """Register a callback; wrap zero-argument callables automatically."""
        if callable(fn) and len(inspect.signature(fn).parameters) == 0:
            super().add_done_callback(lambda _: fn())
        else:
            super().add_done_callback(fn)

    def set_result(self, result: int, skip_if_done: bool = False) -> bool:
        """Set result unless already completed."""
        if skip_if_done and self.done():
            return False
        super().set_result(result)
        return True

    def set_exception(self, exception, skip_if_done: bool = False) -> bool:
        """Set exception unless already done; log if suppressed."""
        if skip_if_done and self.done():
            cancel_exception = isinstance(
                exception, (asyncio.CancelledError, concurrent.futures.CancelledError)
            )
            if not cancel_exception:
                if self.cancelled():
                    current_exception = None
                else:
                    try:
                        current_exception = self.exception()
                    except BaseException as e:
                        current_exception = e
                if current_exception is None or current_exception != exception:
                    LOG.debug("Future done. Suppressing exception", exception)
            return False
        super().set_exception(exception)
        return True


class Worker:
    """Run a subprocess and stream its output to callbacks.

    Supports both async execution and threaded mode for use from sync code.
    When stdout/stderr writers are provided, each line is decoded and sent
    without the trailing newline.
    """

    _WORKER_THREAD_INDEXES: Set[int] = set()
    _WORKER_THREAD_INDEXES_LOCK = threading.Lock()

    def __init__(
        self,
        *args,
        stdout_writer: Optional[Callable[[str], None]] = None,
        stderr_writer: Optional[Callable[[str], None]] = None,
        check: bool = False,
        terminate_timeout: int = 10,
        **kwargs,
    ):
        """Initialize subprocess configuration."""
        self._args = args
        self._stdout_writer = stdout_writer
        self._stderr_writer = stderr_writer
        self._check = check
        self._terminate_timeout = terminate_timeout
        self._kwargs = kwargs

    async def run(self) -> int:
        """Run asynchronously and return exit code."""
        return await self._run()

    async def _run(self, workerFuture: WorkerFuture = None) -> int:
        """Execute subprocess, stream output, and handle completion."""
        if workerFuture is None:
            workerFuture = WorkerFuture()

        # Launch subprocess with appropriate pipes
        commands = self._run_args(workerFuture)
        kwargs = self._run_kwargs(workerFuture)
        workerFuture.process = await asyncio.subprocess.create_subprocess_exec(
            *commands, **kwargs
        )

        try:
            # Use a TaskGroup to manage process wait and readers
            async with asyncio.TaskGroup() as tg:
                wait_task = tg.create_task(workerFuture.process.wait())
                for error in [False, True]:
                    if self._output_writer(error):
                        tg.create_task(self._output_read(workerFuture.process, error))

            exit_code = await wait_task

            # Raise on non-zero exit if check=True
            if self._check and exit_code != 0:
                raise CalledProcessError(exit_code, commands)

            workerFuture.set_result(exit_code, skip_if_done=True)
            return exit_code
        except BaseException as e:
            workerFuture.set_exception(e, skip_if_done=True)
            raise
        finally:
            # Ensure process termination if still alive
            await self._terminate_process(workerFuture.process)

    def run_threaded(self, daemon: Optional[bool] = None) -> WorkerFuture:
        """Run subprocess in background thread and return a WorkerFuture."""

        def _thread_run(fut: WorkerFuture):
            event_loop = asyncio.new_event_loop()
            try:
                run_task = event_loop.create_task(self._run(fut))

                # Cancel if future cancelled
                def _done_callback():
                    if not run_task.done():
                        event_loop.call_soon_threadsafe(run_task.cancel)

                fut.add_done_callback(_done_callback)
                asyncio.set_event_loop(event_loop)
                event_loop.run_until_complete(asyncio.shield(run_task))
            except asyncio.CancelledError as e:
                fut.set_exception(e, skip_if_done=True)
            except BaseException as e:
                fut.set_exception(e, skip_if_done=True)
            finally:
                # Clean up event loop resources
                try:
                    event_loop.run_until_complete(event_loop.shutdown_asyncgens())
                    event_loop.run_until_complete(
                        event_loop.shutdown_default_executor()
                    )
                except Exception:
                    pass
                event_loop.close()

        # Acquire thread index for naming/debugging
        idx = self._worker_thread_index_acquire()
        future = WorkerFuture()
        future.add_done_callback(lambda: self._worker_thread_index_release(idx))
        thread = threading.Thread(
            target=_thread_run,
            args=(future,),
            name=f"worker-{idx}",
            daemon=daemon,
        )
        thread.start()
        return future

    def _run_args(self, workerFuture: WorkerFuture) -> List[str]:
        """Build argv list for subprocess."""
        return list(funcs.to_iter(self._args))

    def _run_kwargs(self, workerFuture: WorkerFuture) -> dict[str, Any]:
        """Combine user kwargs and stdout/stderr pipe settings."""
        output_writers_kwargs = {
            False: self._output_writer_kwargs(False),
            True: self._output_writer_kwargs(True),
        }
        return funcs.merge(self._kwargs, *output_writers_kwargs.values(), update=False)

    def _output_writer(self, error: bool):
        """Return writer for stdout (False) or stderr (True)."""
        return self._stderr_writer if error else self._stdout_writer

    def _output_writer_kwargs(self, error: bool) -> dict[str, Any]:
        """Return asyncio pipe config for given stream."""
        writer = self._output_writer(error)
        if not writer:
            return {}
        return {"stderr" if error else "stdout": asyncio.subprocess.PIPE}

    async def _output_read(self, process: asyncio.subprocess.Process, error: bool):
        """Read lines from process output and forward to writer."""
        reader = process.stderr if error else process.stdout
        while True:
            line_bytes = await reader.readline()
            if not line_bytes:
                break
            line = line_bytes.decode().rstrip()
            self._output_write(process, error, line)

    def _output_write(
        self, process: asyncio.subprocess.Process, error: bool, line: str
    ):
        """Send decoded line to the proper writer."""
        writer = self._output_writer(error)
        if writer:
            writer(line)

    async def _terminate_process(self, process: asyncio.subprocess.Process):
        """Terminate and kill process if still alive."""
        if process.returncode is not None:
            return

        async def wait_for(timeout: Optional[float] = None):
            await asyncio.shield(asyncio.wait_for(process.wait(), timeout=timeout))

        # Try terminate first
        process.terminate()
        try:
            if self._terminate_timeout:
                await wait_for(self._terminate_timeout)
                return
        except asyncio.TimeoutError:
            LOG.warning(
                "Terminate timeout (%s) for process %s",
                self._terminate_timeout,
                process.pid,
            )
        except BaseException:
            pass

        # Escalate to kill if still running
        if process.returncode is None:
            process.kill()
            LOG.warning("Killing process %s", process.pid)
            await wait_for(None)
            LOG.warning("Killed process %s", process.pid)

    @staticmethod
    def _worker_thread_index_acquire() -> int:
        """Reserve and return a unique worker thread index."""
        with Worker._WORKER_THREAD_INDEXES_LOCK:
            idx = 0
            while idx in Worker._WORKER_THREAD_INDEXES:
                idx += 1
            Worker._WORKER_THREAD_INDEXES.add(idx)
            return idx

    @staticmethod
    def _worker_thread_index_release(idx: int) -> bool:
        """Release reserved thread index."""
        with Worker._WORKER_THREAD_INDEXES_LOCK:
            Worker._WORKER_THREAD_INDEXES.discard(idx)
            return True


async def main(worker: Worker):
    """Manual entry point for testing."""
    return await worker.run()


if __name__ == "__main__":
    # Example manual test: run a long-lived bash loop
    worker = Worker(
        "bash",
        "-c",
        "for s in INT TERM HUP QUIT ABRT ALRM USR1 USR2 PIPE TSTP CHLD; "
        'do trap "echo Signal: $s" $s; done; while :; do echo tick; sleep 1; done',
        check=True,
        terminate_timeout=3,
    )

    fut = worker.run_threaded(daemon=False)
    time.sleep(3)
    fut.cancel()

    try:
        print(fut.result())
    except BaseException as e:
        print("Error:", e)

    # Demonstrate guarded set_exception
    fut = WorkerFuture()
    fut.set_exception(ValueError("test123"), skip_if_done=False)
    fut.set_exception(ValueError("test"), skip_if_done=True)
