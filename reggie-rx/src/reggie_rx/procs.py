import subprocess
import threading
import uuid
from typing import List

from reactivex.abc import ObserverBase
from reactivex.subject import AsyncSubject, Subject


class RxProcess(subprocess.Popen):
    PIPE_SUBJECT = uuid.uuid4()

    def __init__(
        self,
        *args,
        stdout_subscribers: List[ObserverBase] = [],
        stderr_subscribers: List[ObserverBase] = [],
        output_subscribers: List[ObserverBase] = [],
        **kwargs,
    ):
        self.stdout_subject, stdout_run = self._create_output_subject(
            "stdout", [*(stdout_subscribers or []), *(output_subscribers or [])], kwargs
        )
        self.stderr_subject, stderr_run = self._create_output_subject(
            "stderr", [*(stderr_subscribers or []), *(output_subscribers or [])], kwargs
        )
        self.exit_code_subject = AsyncSubject()
        super().__init__(*args, **kwargs)
        threading.Thread(target=self._wait_and_emit, daemon=False).start()
        for run in [stdout_run, stderr_run]:
            if run:
                run()

    def _create_output_subject(
        self,
        name: str,
        subscribers: List[ObserverBase],
        popen_kwargs,
    ):
        output_subject = Subject()
        output_arg = popen_kwargs.get(name)
        pump_output = output_arg is RxProcess.PIPE_SUBJECT
        if not pump_output and len(subscribers) > 0:
            if output_arg is not None:
                if subprocess.PIPE == output_arg:
                    output_arg_value = "PIPE"
                elif subprocess.STDOUT == output_arg:
                    output_arg_value = "STDOUT"
                elif subprocess.DEVNULL == output_arg:
                    output_arg_value = "DEVNULL"
                else:
                    output_arg_value = output_arg
                raise ValueError(
                    f"invalid output - name:{name} value:{output_arg_value}"
                )
            pump_output = True
        run = None
        if pump_output:
            popen_kwargs[name] = subprocess.PIPE
            popen_kwargs["text"] = True
            for subscriber in subscribers:
                output_subject.subscribe(subscriber)

            def _run():
                threading.Thread(
                    name=f"{self.pid}_{name}",
                    target=self._pump_output,
                    args=(
                        name,
                        output_subject,
                    ),
                    daemon=False,
                ).start()

            run = _run

        return output_subject, run

    def _pump_output(self, name: str, output_subject: Subject):
        try:
            output_stream = getattr(self, name)
            for line in output_stream:
                output_subject.on_next(line.rstrip("\n"))
        except Exception as e:
            output_subject.on_error(e)
        finally:
            output_subject.on_completed()

    def _wait_and_emit(self):
        code = self.wait()
        self.exit_code_subject.on_next(code)
        self.exit_code_subject.on_completed()
