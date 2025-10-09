"""Async subprocess helpers with stdout/stderr plumbing utilities."""

import asyncio
import concurrent
import signal
import threading
from typing import Any, Callable, List, Optional, Set

from reggie_core import funcs, logs


class Worker:
    """Launch a subprocess and stream its output to optional callbacks.

    Creates an asyncio subprocess using the provided args/kwargs, optionally wiring stdout/stderr
    lines to the given writer callables. Supports awaiting in the current loop or running in a
    background thread that manages its own event loop.
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
        """Initialize a subprocess configuration and optional output writers.

        Args:
            *args: Positional command/arguments for create_subprocess_exec.
            stdout_writer: Callback invoked per decoded stdout line.
            stderr_writer: Callback invoked per decoded stderr line.
            check: If True, raise ValueError on non-zero exit codes.
            terminate_timeout: Seconds to wait after terminate() before kill().
            **kwargs: Passed through to create_subprocess_exec.
        """
        self._args = args
        self._stdout_writer = stdout_writer
        self._stderr_writer = stderr_writer
        self._check = check
        self._terminate_timeout = terminate_timeout
        self._kwargs = kwargs

    def run(self) -> asyncio.Task[int]:
        """Schedule the subprocess for execution and return an awaitable task.

        Returns:
            asyncio.Task[int]: Completes with the process exit code (or raises if check=True).
        """
        done_callbacks: List[Callable[[asyncio.subprocess.Process], None]] = []
        task = asyncio.create_task(self._run_async(done_callbacks))

        def _done_callback(sub_process: asyncio.subprocess.Process):
            # Note: add_done_callback supplies the Task, not the subprocess.
            for done_callback in done_callbacks:
                done_callback(sub_process)

        task.add_done_callback(_done_callback)

        return task

    def run_threaded(self) -> concurrent.futures.Future[int]:
        """Run the subprocess in a dedicated thread and return a Future.

        A new event loop is created in the worker thread. The returned Future resolves
        to the process exit code (or exception).
        """

        async def _thread_run_async(fut: concurrent.futures.Future[int]):
            try:
                task = self.run()

                def _done_callback(_):
                    if not task.done():
                        task.cancel()

                fut.add_done_callback(_done_callback)

                fut.set_result(await task)
            except BaseException as e:
                fut.set_exception(e)

        def _thread_run(idx: int, fut: concurrent.futures.Future[int]):
            event_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(event_loop)
                event_loop.run_until_complete(_thread_run_async(fut))
            finally:
                event_loop.close()
                self._worker_thread_index_release(idx)

        idx = self._worker_thread_index_acquire()
        future = concurrent.futures.Future()
        thread_name = f"{Worker.__name__.lower()}-{idx}"
        thread = threading.Thread(
            target=_thread_run,
            args=(
                idx,
                future,
            ),
            name=thread_name,
        )
        thread.start()
        return future

    async def _run_async(
        self, done_callbacks: List[Callable[[asyncio.subprocess.Process], None]]
    ):
        """Execute the subprocess and propagate output through registered writers.

        Starts the process, launches reader tasks when writers are provided, and waits for
        completion. On non-zero exit and check=True, raises a ValueError with a message
        similar to subprocess.CalledProcessError formatting.
        """
        commands = self._run_args(done_callbacks)
        kwargs = self._run_kwargs(done_callbacks)
        sub_process = await asyncio.subprocess.create_subprocess_exec(
            *commands, **kwargs
        )
        # Lazy-start stream readers only when writers are provided.
        output_writer_tasks = {}

        for error in [False, True]:
            if self._output_writer(error) is None:
                continue
            output_writer_tasks[error] = asyncio.create_task(
                self._output_read(sub_process, error)
            )

        def _sub_process_done():
            return sub_process.returncode is not None

        try:
            results = await asyncio.gather(
                sub_process.wait(), *output_writer_tasks.values()
            )
        finally:
            for task in output_writer_tasks.values():
                task.cancel()
            if not _sub_process_done():
                sub_process.terminate()
                # Note: a timeout here will raise; kill() below will not run unless caught upstream.
                await asyncio.shield(
                    asyncio.wait_for(
                        sub_process.wait(), timeout=self._terminate_timeout
                    )
                )
                if not _sub_process_done():
                    sub_process.kill()
        exit_code = results[0]
        if not self._check or exit_code == 0:
            return exit_code
        if exit_code < 0:
            try:
                sig = signal.Signals(-exit_code)
                msg = f"Command {commands!r} died with {sig!r}."
            except ValueError:
                msg = f"Command {commands!r} died with unknown signal {-exit_code}."
        else:
            msg = f"Command {commands!r} returned non-zero exit status {exit_code}."
        raise ValueError(msg)

    def _run_args(
        self, done_callbacks: List[Callable[[asyncio.subprocess.Process], None]]
    ) -> List[str]:
        """Return the ordered list of command arguments for subprocess execution."""
        return list(funcs.to_iter(self._args))

    def _run_kwargs(
        self, done_callbacks: List[Callable[[asyncio.subprocess.Process], None]]
    ) -> dict[str, Any]:
        """Combine subprocess keyword arguments with stdout/stderr configuration."""
        output_writers_kwargs = {}
        for error in [False, True]:
            output_writers_kwargs[error] = self._output_writer_kwargs(error)

        return funcs.merge(
            self._kwargs,
            *output_writers_kwargs.values(),
            update=False,
        )

    def _output_writer(self, error: bool):
        """Return the appropriate writer for stdout or stderr."""
        return self._stderr_writer if error else self._stdout_writer

    def _output_writer_kwargs(self, error: bool) -> dict[str, Any]:
        """Generate subprocess pipe configuration for the requested output stream."""
        output_kwargs = {}
        writer = self._output_writer(error)
        if writer is not None:
            output_name = "stderr" if error else "stdout"
            output_kwargs[output_name] = asyncio.subprocess.PIPE
        return output_kwargs

    async def _output_read(self, process: asyncio.subprocess.Process, error: bool):
        """Read lines from the subprocess stream and forward them to the writer.

        Decodes bytes using the default encoding and strips trailing newlines.
        """
        reader = process.stderr if error else process.stdout
        while True:
            line_bytes = await reader.readline()
            if not line_bytes:
                break
            line_str = line_bytes.decode().rstrip()
            self._output_write(process, error, line_str)

    def _output_write(
        self, process: asyncio.subprocess.Process, error: bool, line_str: str
    ):
        """Invoke the writer callback, if configured, with the decoded line."""
        writer = self._output_writer(error)
        if writer is not None:
            writer(line_str)

    @staticmethod
    def _worker_thread_index_acquire() -> int:
        """Reserve and return a unique integer index for a worker thread."""
        with Worker._WORKER_THREAD_INDEXES_LOCK:
            idx = -1
            while True:
                idx += 1
                idx_len = len(Worker._WORKER_THREAD_INDEXES)
                Worker._WORKER_THREAD_INDEXES.add(idx)
                if idx_len < len(Worker._WORKER_THREAD_INDEXES):
                    return idx

    @staticmethod
    def _worker_thread_index_release(idx: int) -> bool:
        """Release a previously reserved worker thread index.

        Returns:
            True if the index was present and removed; False otherwise.
        """
        if idx in Worker._WORKER_THREAD_INDEXES:
            with Worker._WORKER_THREAD_INDEXES_LOCK:
                if idx in Worker._WORKER_THREAD_INDEXES:
                    Worker._WORKER_THREAD_INDEXES.remove(idx)
                    return True
        return False


async def main(worker: Worker):
    """Quick manual exercise when running the module stand-alone."""
    return await worker.run()


if __name__ == "__main__":
    commands = [
        "bash",
        "-c",
        "echo 'hello dude' 1>&2; echo 'suh';sleep 5; exit 1",
    ]
    log = logs.logger(__name__)
    worker = Worker(
        *commands,
        stdout_writer=log.info,
        stderr_writer=log.error,
        check=False,
    )
    print(asyncio.run(main(worker)))
    futures = []
    for i in range(10):
        futures.append(worker.run_threaded())
    for future in futures:
        print(future.result())
    print(worker.run_threaded().result())
