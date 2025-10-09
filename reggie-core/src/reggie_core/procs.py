"""Async subprocess helpers with stdout and stderr plumbing utilities.

This module provides helpers to run subprocesses with asyncio while streaming
stdout and stderr line by line to optional writer callbacks. It supports both
direct async execution in the current event loop and execution on a dedicated
thread with its own event loop for use from synchronous code.

Overview
    Worker
        Launches a subprocess with optional stdout and stderr writers. Writers
        receive decoded text lines without the trailing newline. You can await
        Worker.run from an async context or call Worker.run_threaded from sync
        code to receive a Future that resolves to the exit code.

    WorkerFuture
        A concurrent.futures.Future[int] subclass that also holds a reference
        to the asyncio.subprocess.Process. It adapts zero argument callbacks
        in add_done_callback and offers guarded setters via the skip_if_done
        parameter on set_result and set_exception.

Exit code validation
    When check is True and the process exits with a nonzero status, _run will
    raise subprocess.CalledProcessError. The error contains the argv and the
    exit status, aligned with the behavior of subprocess helpers.

Cancellation and shutdown
    When running on a thread, canceling the returned Future will signal the
    task to cancel. On completion, the Worker registers a callback that tries
    to stop the process by sending terminate first and kill if it does not
    exit within the configured terminate_timeout.
"""

import asyncio
import concurrent
import inspect
import os
import threading
import time
from subprocess import CalledProcessError
from typing import Any, Callable, List, Optional, Set

from reggie_core import funcs, logs

LOG = logs.logger(__name__)


class WorkerFuture(concurrent.futures.Future[int]):
    """Future that tracks the underlying asyncio subprocess and supports guarded completion.

    Attributes:
        process:
            The asyncio.subprocess.Process once created, otherwise None.

    Behavior:
        add_done_callback(fn):
            Accepts either a standard concurrent futures callback of the form
            fn(future) or a zero argument callable. Zero argument callables
            are adapted to the standard signature.

        set_result(result, skip_if_done=False) -> bool:
            Set a successful result. When skip_if_done is True and the future
            is already completed, the call is ignored and returns False.

        set_exception(exception, skip_if_done=False) -> bool:
            Set an error on the future. When skip_if_done is True and the
            future is already completed, the call is ignored and returns False.
            If the completed future is not cancelled and the new exception is
            different than the stored one, a debug log entry records that the
            exception was suppressed.
    """

    def __init__(self):
        super().__init__()
        self.process: asyncio.subprocess.Process | None = None

    def add_done_callback(self, fn) -> None:
        """Register a completion callback.

        If fn is callable and accepts zero parameters, it will be wrapped so that
        the Future argument provided by concurrent futures is ignored. Otherwise
        fn is registered as is.
        """
        if callable(fn) and len(inspect.signature(fn).parameters) == 0:
            super().add_done_callback(lambda _: fn())
        else:
            super().add_done_callback(fn)

    def set_result(self, result: int, skip_if_done: bool = False) -> bool:
        """Complete the future successfully.

        Args:
            result: The process exit code.
            skip_if_done: If True, do nothing when already completed.

        Returns:
            True if the result was set. False if the future was already done.
        """
        if skip_if_done and self.done():
            return False
        super().set_result(result)
        return True

    def set_exception(self, exception, skip_if_done: bool = False) -> bool:
        """Complete the future with an exception.

        Args:
            exception: The exception to set.
            skip_if_done: If True, do nothing when already completed. When an
                exception is suppressed, a debug message is logged if it differs
                from the existing exception and the future is not cancelled.

        Returns:
            True if the exception was set. False if the future was already done.
        """
        if skip_if_done and self.done():
            cancel_exception = isinstance(
                exception, asyncio.CancelledError
            ) or isinstance(exception, concurrent.futures.CancelledError)
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
    """Launch a subprocess and stream its output to optional callbacks.

    Creates an asyncio subprocess using the provided args and kwargs. When
    stdout_writer or stderr_writer is provided, lines from the respective
    stream are decoded using the default encoding and forwarded to the writer
    without the trailing newline.

    Usage
        worker = Worker("bash", "-c", "echo hello")
        code = await worker.run()

        fut = worker.run_threaded()
        code = fut.result()

    Threaded mode
        run_threaded creates a thread with a dedicated event loop, schedules
        the async run, and returns a WorkerFuture. The thread name includes a
        unique index to simplify debugging.

    Exit validation
        If check is True and the process exits non zero, _run raises
        subprocess.CalledProcessError with the argv and the exit code.
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
        """Configure command, output writers, and exit checking.

        Args:
            *args:
                Positional command and arguments for create_subprocess_exec.
            stdout_writer:
                Callable invoked for each decoded stdout line.
            stderr_writer:
                Callable invoked for each decoded stderr line.
            check:
                When True, a nonzero exit status raises CalledProcessError.
            terminate_timeout:
                Seconds to wait after terminate before sending kill.
            **kwargs:
                Extra keyword args forwarded to create_subprocess_exec.
        """
        self._args = args
        self._stdout_writer = stdout_writer
        self._stderr_writer = stderr_writer
        self._check = check
        self._terminate_timeout = terminate_timeout
        self._kwargs = kwargs

    async def run(self) -> int:
        """Run the configured subprocess in the current event loop and return its exit code."""
        return await self._run()

    async def _run(self, workerFuture: WorkerFuture = None) -> int:
        """Start the subprocess, stream output, and wait for completion.

        Creates the process, launches line readers when writers are present,
        and waits for the process to finish. When check is True and the exit
        code is non zero, raises CalledProcessError.

        Args:
            workerFuture:
                Optional WorkerFuture to complete on finish. A new one is
                created internally when not supplied.

        Returns:
            The process exit code.

        Raises:
            CalledProcessError when check is True and the process exits non zero.
            Any exception arising from process start, output reading, or waiting.
        """
        if workerFuture is None:
            workerFuture = WorkerFuture()
        commands = self._run_args(workerFuture)
        kwargs = self._run_kwargs(workerFuture)
        workerFuture.process = await asyncio.subprocess.create_subprocess_exec(
            *commands, **kwargs
        )
        workerFuture.add_done_callback(lambda: self._stop_process(workerFuture.process))

        try:
            async with asyncio.TaskGroup() as tg:
                wait_task = tg.create_task(workerFuture.process.wait())
                for error in [False, True]:
                    if self._output_writer(error) is None:
                        continue
                    tg.create_task(self._output_read(workerFuture.process, error))
            exit_code = await wait_task
            if self._check and exit_code != 0:
                raise CalledProcessError(exit_code, commands)
            workerFuture.set_result(exit_code, skip_if_done=True)
            return exit_code
        except BaseException as e:
            workerFuture.set_exception(e, skip_if_done=True)
            raise

    def run_threaded(self, daemon: Optional[bool] = None) -> WorkerFuture:
        """Run the subprocess in a dedicated thread and return a WorkerFuture.

        A new event loop is created in the worker thread. The returned future
        completes with the process exit code or with the first raised exception.

        Args:
            daemon:
                Optional thread daemon flag. When None, the default threading
                behavior is used.

        Returns:
            A WorkerFuture representing the async run.
        """

        def _thread_run(fut: WorkerFuture):
            event_loop = asyncio.new_event_loop()
            try:
                run_task = event_loop.create_task(self._run(fut))

                def _done_callback():
                    if not run_task.done():
                        event_loop.call_soon_threadsafe(run_task.cancel)

                fut.add_done_callback(_done_callback)

                asyncio.set_event_loop(event_loop)
                event_loop.run_until_complete(asyncio.shield(run_task))
            except asyncio.CancelledError as e:
                try:
                    event_loop.run_until_complete(asyncio.sleep(0))
                finally:
                    fut.set_exception(e, skip_if_done=True)
            except BaseException as e:
                fut.set_exception(e, skip_if_done=True)
            finally:
                try:
                    event_loop.run_until_complete(event_loop.shutdown_asyncgens())
                    event_loop.run_until_complete(
                        event_loop.shutdown_default_executor()
                    )
                except Exception:
                    pass
                event_loop.close()

        idx = self._worker_thread_index_acquire()

        thread_name = f"{Worker.__name__.lower()}-{idx}"
        future = WorkerFuture()
        future.add_done_callback(lambda: self._worker_thread_index_release(idx))
        thread = threading.Thread(
            target=_thread_run,
            args=(future,),
            name=thread_name,
            daemon=daemon,
        )
        thread.start()
        return future

    def _run_args(self, workerFuture: WorkerFuture) -> List[str]:
        """Return the ordered argv vector for subprocess execution."""
        return list(funcs.to_iter(self._args))

    def _run_kwargs(self, workerFuture: WorkerFuture) -> dict[str, Any]:
        """Combine user kwargs with pipe settings for stdout and stderr."""
        output_writers_kwargs = {}
        for error in [False, True]:
            output_writers_kwargs[error] = self._output_writer_kwargs(error)

        return funcs.merge(
            self._kwargs,
            *output_writers_kwargs.values(),
            update=False,
        )

    def _output_writer(self, error: bool):
        """Return the configured writer for stdout when error is False, else stderr."""
        return self._stderr_writer if error else self._stdout_writer

    def _output_writer_kwargs(self, error: bool) -> dict[str, Any]:
        """Return subprocess pipe kwargs for the requested stream."""
        output_kwargs = {}
        writer = self._output_writer(error)
        if writer is not None:
            output_name = "stderr" if error else "stdout"
            output_kwargs[output_name] = asyncio.subprocess.PIPE
        return output_kwargs

    async def _output_read(self, process: asyncio.subprocess.Process, error: bool):
        """Read lines from the process stream and forward them to the writer.

        Decodes bytes with the default encoding and strips the trailing newline.
        Reading stops when an empty read indicates end of stream.
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
        """Invoke the writer callback, if present, with the decoded line."""
        writer = self._output_writer(error)
        if writer is not None:
            writer(line_str)

    def _stop_process(self, process: asyncio.subprocess.Process):
        """Attempt graceful termination and escalate to kill if needed.

        Steps
            1. If the process is still running, send terminate.
            2. Wait up to terminate_timeout seconds for exit when configured.
            3. If still running, send kill and wait unconditionally.

        Notes
            Waiting uses event loop specific behavior to drive process.wait().
            This function is intended to be invoked from a done callback that
            runs in the context of the thread that created the loop.
        """
        process_loop = getattr(process, "_loop", None)

        if process.returncode is not None:
            return

        def _process_wait(t: Optional[int] = None):
            try:
                event_loop = asyncio.get_running_loop()
                wait_task = asyncio.shield(asyncio.wait_for(process.wait(), timeout=t))
                event_loop.run_until_complete(wait_task)
                return True
            except TimeoutError:
                return False
            except BaseException:
                pass
            start = time.monotonic()
            while True:
                try:
                    os.kill(process.pid, 0)
                except ProcessLookupError:
                    return True
                if t is not None and time.monotonic() - start > t:
                    return False
                time.sleep(0.1)

        try:
            process.terminate()
            if self._terminate_timeout and _process_wait(self._terminate_timeout):
                return
        except BaseException as e:
            LOG.warning("Error terminating process %s: %s", process.pid, e)
            pass
        finally:
            LOG.warning("Killing process %s", process.pid)
            process.kill()
            _process_wait()
            LOG.warning("Killed process %s", process.pid)

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
            True when the index was present and removed. False otherwise.
        """
        if idx in Worker._WORKER_THREAD_INDEXES:
            with Worker._WORKER_THREAD_INDEXES_LOCK:
                if idx in Worker._WORKER_THREAD_INDEXES:
                    Worker._WORKER_THREAD_INDEXES.remove(idx)
                    return True
        return False


async def main(worker: Worker):
    """Manual exercise entry point for running this module as a script."""
    return await worker.run()


if __name__ == "__main__":
    # if True:
    #     commands = [
    #         "bash",
    #         "-c",
    #         "echo 'hello dude' 1>&2; echo 'suh';sleep 3; exit 1",
    #     ]
    #     log = logs.logger(__name__)
    #     worker = Worker(
    #         *commands,
    #         stdout_writer=log.info,
    #         stderr_writer=log.error,
    #         check=False,
    #     )
    #     print(asyncio.run(main(worker)))
    #     futures = []
    #     for i in range(10):
    #         futures.append(worker.run_threaded())
    #     for future in futures:
    #         print(future.result())
    #     print(worker.run_threaded().result())
    worker = Worker(
        "bash",
        "-c",
        'for s in INT TERM HUP QUIT ABRT ALRM USR1 USR2 PIPE TSTP CHLD; do trap "echo Signal: $s" $s; done; while :; do echo tick; sleep 1; done',
        check=True,
        terminate_timeout=3,
    )
    fut = worker.run_threaded(daemon=True)
    time.sleep(3)
    fut.cancel()
    try:
        print(fut.result())
        print("done")
    except BaseException as e:
        print(e)
    fut = WorkerFuture()
    fut.set_exception(ValueError("test123"), skip_if_done=False)
    fut.set_exception(ValueError("test"), skip_if_done=True)
