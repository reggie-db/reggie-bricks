import io
import logging
import subprocess
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from io import IOBase
from typing import Callable, Iterable, List, Optional, Union

from reggie_core import logs

LOG = logs.logger(__name__)


class StreamType(IntEnum):
    STDIN = 1
    STDOUT = 2
    STDERR = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_input = self.name == "STD_IN"
        self.process_stream_name = self.name.lower()

    def process_stream(self, process) -> Optional[IOBase]:
        stream = getattr(process, self.process_stream_name)
        if stream is not None and not isinstance(stream, IOBase):
            stream = io.BytesIO(str(stream).encode())
        return stream

    @staticmethod
    def values(input: Optional[bool] = None) -> Iterable["StreamType"]:
        if input is None:
            return iter(StreamType)
        elif input:
            return [StreamType.STDIN]
        else:
            return [StreamType.STDOUT, StreamType.STDERR]

    @staticmethod
    def of(value) -> "StreamType":
        if value is not None:
            if isinstance(value, StreamType):
                return value
            elif not isinstance(value, int):
                value = str(value).strip()
            for target in StreamType:
                if isinstance(value, int):
                    if target.value == value:
                        return target
                    else:
                        continue
                else:
                    if target.name.casefold() == value.casefold():
                        return target
                    if target.process_stream_name.casefold() == value.casefold():
                        return target
        raise ValueError(f"Invalid {__class__.__name__} value: {value}")


class StreamTarget(IntEnum):
    PIPE = abs(subprocess.PIPE)
    STDOUT = abs(subprocess.STDOUT)
    DEVNULL = abs(subprocess.DEVNULL)

    @staticmethod
    def of(value) -> "StreamTarget":
        if value is not None:
            if isinstance(value, StreamTarget):
                return value
            elif isinstance(value, int):
                value = abs(value)
            else:
                value = str(value).strip()
            for target in StreamTarget:
                if isinstance(value, int):
                    if target.value == value:
                        return target
                    else:
                        continue
                else:
                    if target.name.casefold() == value.casefold():
                        return target
        raise ValueError(f"Invalid {__class__.__name__} value: {value}")


class Worker(subprocess.Popen):
    def __init__(
        self,
        *args,
        output_consumers: Union[
            Iterable["WorkerOutputConsumer"], "WorkerOutputConsumer"
        ] = None,
        **kwargs,
    ):
        if isinstance(output_consumers, WorkerOutputConsumer):
            output_consumers = [output_consumers]
        elif not output_consumers:
            output_consumers = []
        for output_consumer in output_consumers:
            for stream_type in StreamType.values(input=False):
                if stream_type not in output_consumer.stream_types:
                    continue
                stream_name = stream_type.process_stream_name
                stream_target_arg = kwargs.get(stream_name)
                if stream_target_arg is not None:
                    stream_target = StreamTarget.of(stream_target_arg)
                    if stream_target != StreamTarget.PIPE:
                        raise ValueError(
                            f"invalid stream target - name:{stream_name} target:{stream_target.name}"
                        )
                kwargs[stream_name] = subprocess.PIPE
                kwargs["text"] = True
        super().__init__(*args, **kwargs)
        self.threads = []
        for output_consumer in output_consumers:
            self.threads.extend(self._log_output(output_consumer))

    def stop(self, timeout: int = 10):
        if self.poll() is None:
            self.terminate()
            try:
                self.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.kill()
        for t in self.threads:
            t.join(timeout=timeout)

    def _log_output(self, output_consumer: "WorkerOutputConsumer"):
        threads = []
        for stream_type in StreamType.values(input=False):
            if stream_type not in output_consumer.stream_types:
                continue
            thread_name = f"{self.pid}_{stream_type.process_stream_name}"
            t = threading.Thread(
                target=self._pump_log_output,
                args=(output_consumer, stream_type),
                name=thread_name,
                daemon=True,
            )
            t.start()
            threads.append(t)
        return threads

    def _pump_log_output(
        self, output_consumer: "WorkerOutputConsumer", stream_type: StreamType
    ):
        stream = stream_type.process_stream(self)
        for line in iter(stream.readline, ""):
            output_consumer(self, stream_type == StreamType.STDERR, line.rstrip("\r\n"))


@dataclass
class WorkerOutputConsumer(Callable[[Worker, bool, str], None]):
    stdout: bool = field(default=False)
    stderr: bool = field(default=True)
    stream_types: List[StreamType] = field(default_factory=list, init=False)

    def __post_init__(self):
        self.stream_types.extend(
            [
                stream_type
                for stream_type in StreamType.values()
                if (self.stdout and stream_type == StreamType.STDOUT)
                or (self.stderr and stream_type == StreamType.STDERR)
            ]
        )

    @staticmethod
    def create(
        handler: Callable[[Worker, bool, str], None] = None,
        stdout: bool = True,
        stderr: bool = True,
    ) -> "WorkerOutputConsumer":
        class WorkerOutputConsumerImpl(WorkerOutputConsumer):
            def __init__(self):
                super().__init__(stdout, stderr)

            def __call__(self, worker: "Worker", error: bool, line: str):
                handler(worker, error, line)

        return WorkerOutputConsumerImpl()

    @staticmethod
    def log(
        name: Optional[str] = None,
        prefix: str = None,
        stdout_level: Optional[int] = logging.INFO,
        stderr_level: Optional[int] = logging.ERROR,
    ) -> "WorkerOutputConsumer":
        def _log(worker: "Worker", error: bool, line: str):
            level = stderr_level if error else stdout_level
            if level is None:
                return
            if prefix:
                line = f"{prefix} | {line}"
            output_logger = LOG if not name else logs.logger(name)
            output_logger.log(level, line)

        return WorkerOutputConsumer.create(
            _log,
            stdout=stdout_level is not None,
            stderr=stderr_level is not None,
        )
