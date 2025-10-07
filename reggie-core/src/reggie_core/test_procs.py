import subprocess

from reggie_core.procs import Worker, WorkerOutputConsumer

if __name__ == "__main__":
    commands = [
        "bash",
        "-c",
        "echo 'starting...'; sleep 3; echo 'done'; echo 'this is an error' 1>&2",
    ]
    worker = Worker(
        commands,
        output_consumers=WorkerOutputConsumer.log(prefix="suh"),
        stdout=subprocess.DEVNULL,
    )
    worker.wait()
