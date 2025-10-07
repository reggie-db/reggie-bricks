import logging
import threading

from reactivex import Observer

from reggie_rx.procs import RxProcess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] | %(threadName)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("test_procs")

if __name__ == "__main__":
    subscriber = Observer(
        on_next=lambda x: print(f"{threading.current_thread().name} out: {x}"),
        on_error=lambda e: print(f"{threading.current_thread().name} err: {e}"),
        on_completed=lambda: print(f"{threading.current_thread().name} completed"),
    )

    def _subscriber(err: bool):
        level = logging.ERROR if err else logging.INFO

        def _on_next(v: str):
            LOG.log(level, v)

        return Observer(
            on_next=_on_next,
            on_error=lambda v: LOG.error(v),
            on_completed=lambda: LOG.log(level, "completed") if not err else None,
        )

    p = RxProcess(
        [
            "bash",
            "-c",
            "echo 'starting...'; sleep 1; echo 'done'; echo 'this is an error' 1>&2",
        ],
        stdout_subscribers=[_subscriber(False)],
        stderr_subscribers=[_subscriber(True)],
        output_subscribers=[subscriber],
        stdout=RxProcess.PIPE_SUBJECT,
        stderr=RxProcess.PIPE_SUBJECT,
        text=True,
    )
    p.stdout_subject.subscribe(
        on_next=lambda x: print(f"stdout: {x}"),
        on_error=lambda e: print(f"stdout error: {e}"),
        on_completed=lambda: print("stdout completed"),
    )
    p.stderr_subject.subscribe(
        on_next=lambda x: print(f"stderr: {x}"),
        on_error=lambda e: print(f"stderr error: {e}"),
        on_completed=lambda: print("stderr completed"),
    )
    p.exit_code_subject.subscribe(
        on_next=lambda x: print(f"Process completed with code: {x}"),
        on_error=lambda e: print(f"Process completed with error: {e}"),
        on_completed=lambda: print("Process completed"),
    )
    p.exit_code_subject.subscribe(
        on_next=lambda x: print(f"2 Process completed with code: {x}"),
        on_error=lambda e: print(f"2 Process completed with error: {e}"),
        on_completed=lambda: print("2Process completed"),
    )
