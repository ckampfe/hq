import pytest
import shlex
import subprocess
import time


@pytest.fixture(scope="function")
def hq_server():
    command = "cargo run -- --database=foo --in-memory"
    command = shlex.split(command)

    proc = subprocess.Popen(
        command,
        cwd="../server",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    time.sleep(2)

    assert not proc.poll(), proc.stdout.read().decode("utf-8")

    yield proc

    proc.terminate()
