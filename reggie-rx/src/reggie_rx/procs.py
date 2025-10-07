import subprocess
import threading
import uuid
from typing import Iterable, List, Optional, Tuple

from reactivex.abc import ObserverBase
from reactivex.subject import AsyncSubject, Subject


class RxProcess(subprocess.Popen):
    """
    A subprocess.Popen subclass that exposes reactive streams for stdout, stderr,
    and exit codes. It will spawn reader threads that push each output line to
    Subjects and complete them on EOF. It also ensures threads stop when the
    process ends and avoids common pitfalls like mutable defaults and partial
    configuration.

    Usage:
        proc = RxProcess(
            ["python", "-u", "app.py"],
            stdout_subscribers=[print],
            stderr_subscribers=[lambda s: print(f"ERR: {s}")],
            output_subscribers=[lambda s: print(f"ANY: {s}")],
            stdout=RxProcess.PIPE_SUBJECT,  # optional explicit signal to pipe
            stderr=RxProcess.PIPE_SUBJECT,
        )
        proc.exit_code_subject.subscribe(lambda code: print("exit", code))
        code = proc.wait()
    """

    # Unique sentinel to request piping for a given stream without using subprocess.PIPE directly
    PIPE_SUBJECT = uuid.uuid4()

    # Note: Do not use mutable default args
    def __init__(
        self,
        *args,
        stdout_subscribers: Optional[Iterable[ObserverBase]] = None,
        stderr_subscribers: Optional[Iterable[ObserverBase]] = None,
        output_subscribers: Optional[Iterable[ObserverBase]] = None,
        **kwargs,
    ):
        # Copy kwargs so we can adjust without mutating caller state
        _kwargs = dict(kwargs)

        # Normalize subscriber inputs and dedupe while preserving order
        def _subscribers(
            *subs_iterables: Optional[Iterable[ObserverBase]],
        ) -> List[ObserverBase]:
            out: List[ObserverBase] = []
            seen = set()
            for iterable in subs_iterables:
                if not iterable:
                    continue
                for s in iterable:
                    if id(s) in seen:
                        continue
                    seen.add(id(s))
                    out.append(s)
            return out

        stdout_subscribers = _subscribers(stdout_subscribers, output_subscribers)
        stderr_subscribers = _subscribers(stdout_subscribers, output_subscribers)

        # Create subjects and deferred stream-run callables that start after Popen init
        self.stdout_subject, _run_stdout = self._create_output_subject(
            name="stdout",
            subscribers=stdout_subscribers,
            popen_kwargs=_kwargs,
        )
        self.stderr_subject, _run_stderr = self._create_output_subject(
            name="stderr",
            subscribers=stderr_subscribers,
            popen_kwargs=_kwargs,
        )

        # Exit code is a single-value AsyncSubject that completes on process end
        self.exit_code_subject: AsyncSubject = AsyncSubject()

        # Start process
        super().__init__(*args, **_kwargs)

        # Launch a waiter thread that completes exit_code_subject and joins readers
        self._waiter_thread = threading.Thread(
            target=self._wait_and_emit, name=f"rxp-{self.pid}-wait", daemon=False
        )
        self._waiter_thread.start()

        # Launch reader threads after Popen so pid and pipes are available
        self._reader_threads: List[threading.Thread] = []
        for starter in (_run_stdout, _run_stderr):
            if starter:
                t = starter()
                if t:
                    self._reader_threads.append(t)

    def _create_output_subject(
        self,
        name: str,
        subscribers: List[ObserverBase],
        popen_kwargs: dict,
    ) -> Tuple[Subject, Optional[callable]]:
        """
        Creates a Subject for a given stream and returns a tuple:
        (subject, starter_fn). If output pumping is enabled, starter_fn starts
        the reader thread after Popen is initialized.
        """
        subject = Subject()

        # Determine if we should pump the stream
        requested = popen_kwargs.get(name)
        pump_output = requested is RxProcess.PIPE_SUBJECT or bool(subscribers)

        # Validate conflicting configurations
        if bool(subscribers) and requested not in (None, RxProcess.PIPE_SUBJECT):
            # User provided subscribers but also overrode stream target
            value_str = (
                "PIPE"
                if requested is subprocess.PIPE
                else "STDOUT"
                if requested is subprocess.STDOUT
                else "DEVNULL"
                if requested is subprocess.DEVNULL
                else repr(requested)
            )
            raise ValueError(
                f"Invalid {name} configuration. Subscribers present but {name}={value_str} provided."
            )

        # If pumping, force the stream to PIPE and subscribe observers
        if pump_output:
            popen_kwargs[name] = subprocess.PIPE
            popen_kwargs["text"] = True

            # Ensure we read text and line-buffered output for timely emission
            popen_kwargs.setdefault("bufsize", 1)
            popen_kwargs.setdefault("encoding", "utf-8")

            # Subscribe observers
            for sub in subscribers:
                subject.subscribe(sub)

            def _starter():
                # Start a daemon thread that reads lines and emits them to the subject
                t = threading.Thread(
                    name=f"{self.pid}-{name}",
                    target=self._pump_output,
                    args=(name, subject),
                    daemon=False,
                )
                t.start()
                return t

            return subject, _starter

        # Not pumping this stream
        return subject, None

    def _pump_output(self, name: str, subject: Subject):
        """
        Reads lines from the named stream until EOF and emits to the Subject.
        Ensures completion or error signaling for subscribers.
        """
        try:
            stream = getattr(self, name)
            if stream is None:
                # If stream is not piped, complete immediately
                subject.on_completed()
                return

            # Iterate line-by-line. readline returns '' on EOF in text mode.
            for line in iter(stream.readline, ""):
                # Emit without trailing newline
                subject.on_next(line.rstrip("\r\n"))
        except Exception as e:
            subject.on_error(e)
        finally:
            subject.on_completed()

    def _wait_and_emit(self):
        """
        Waits for process completion, emits the exit code, and then joins
        reader threads to ensure clean shutdown with no stray threads.
        """
        code = super().wait()
        try:
            self.exit_code_subject.on_next(code)
        finally:
            self.exit_code_subject.on_completed()

        # Join reader threads briefly to allow any last lines to flush
        for t in getattr(self, "_reader_threads", []):
            if t and t.is_alive():
                t.join(timeout=0.5)

    # Optional convenience methods

    def subscribe_stdout(self, observer: ObserverBase):
        """Subscribe an observer to stdout lines."""
        self.stdout_subject.subscribe(observer)

    def subscribe_stderr(self, observer: ObserverBase):
        """Subscribe an observer to stderr lines."""
        self.stderr_subject.subscribe(observer)
