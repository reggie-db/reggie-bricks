"""Async subprocess helpers with stdout/stderr streaming.

Provides tools for running subprocesses asynchronously or in a background
thread while streaming output line by line to optional callbacks.
"""

import asyncio
from dataclasses import dataclass
from subprocess import CalledProcessError
from typing import Callable, List, Type

from reggie_core import logs

LOG = logs.logger(__name__)


@dataclass
class Output:
    process: asyncio.subprocess.Process
    line: bytes

    def __str__(self):
        return self.line.decode(errors="replace").rstrip()


OutputWriter = Type[Callable[[Output], None]]


@dataclass
class CompletedProcess:
    process: asyncio.subprocess.Process
    stdout: List[Output]
    stderr: List[Output]

    @property
    def stdout_str(self) -> str:
        return self._output_str(self.stdout)

    @property
    def stderr_str(self) -> str:
        return self._output_str(self.stderr)

    def _output_str(self, output: List[Output]) -> str:
        return "\n".join((str(line) for line in output)) if output is not None else None


async def run(
    *args: str,
    **kwargs,
) -> CompletedProcess:
    output_writer_names = ["stdout_writer", "stderr_writer"]
    communicate = not any(name in kwargs for name in output_writer_names)
    output_sinks: dict[bool, asyncio.StreamReader()] = {}
    if not communicate:
        for error in [False, True]:
            output_writer_name = "stderr_writer" if error else "stdout_writer"
            if output_writer_name not in kwargs:
                output_sink = asyncio.StreamReader()
                output_sinks[error] = output_sink
                kwargs[output_writer_name] = lambda output: output_sink.feed_data(
                    output.line
                )

    proc = await start(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    if communicate:
        stdout, stderr = await proc.communicate()
        return stdout, stderr, proc
    else:
        await proc.wait()

        async def _output(error: bool) -> bytes:
            output_sink = output_sinks.get(error)
            if output_sink is None:
                return None
            else:
                output_sink.feed_eof()
                return await output_sink.read()

        stdout = await _output(False)
        stderr = await _output(True)
        return stdout, stderr, proc


async def start(
    *args: str,
    check: bool = False,
    stdout_writer: OutputWriter = None,
    stderr_writer: OutputWriter = None,
    **kwargs,
) -> asyncio.subprocess.Process:
    for error in [False, True]:
        output_name = "stdout" if not error else "stderr"
        kwargs.setdefault(output_name, asyncio.subprocess.PIPE)

    proc = await asyncio.create_subprocess_exec(*args, **kwargs)

    async def _output_write(error, output_writer):
        output_stream = proc.stdout if not error else proc.stderr
        while True:
            line = await output_stream.readline()
            if not line:
                break
            output_writer(Output(proc, line))

    output_writer_tasks = []

    for error in [False, True]:
        if output_writer := stderr_writer if error else stdout_writer:
            output_writer_task = asyncio.create_task(
                _output_write(error, output_writer)
            )
            output_writer_tasks.append(output_writer_task)

    if not output_writer_tasks:
        return proc

    proc_wait = proc.wait

    async def _proc_wait_wrapper():
        coros = [proc_wait(), *output_writer_tasks]
        try:
            results = await asyncio.gather(*coros)
            exit_code = results[0]
            if check and exit_code != 0:
                raise CalledProcessError(exit_code, args)
            return exit_code
        except Exception:
            for task in output_writer_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*coros, return_exceptions=True)
            raise

    proc.wait = _proc_wait_wrapper
    return proc


async def main():
    """Manual entry point for testing."""
    log = logs.logger("test")
    commands = ["bash", "-c", "echo wowy; echo cool;  echo tester >&2; exit 1"]
    stdout, stderr, proc = await run(
        *commands,
    )
    print(stdout.decode())

    proc = await start(
        *commands,
        check=False,
        stdout_writer=log.info,
        stderr_writer=log.error,
    )
    print(await proc.wait())
    completed_process = await run(
        "bash",
        "-c",
        "echo hello; echo dude; echo test >&2; exit 1",
        check=False,
        stderr_writer=log.error,
    )
    print(type(completed_process[0]))

    print(completed_process[0].decode())


if __name__ == "__main__":
    asyncio.run(main())
