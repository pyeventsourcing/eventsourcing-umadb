import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator


@contextmanager
def temp_umadb_server() -> Generator[str, None, None]:
    """Starts UmaDB on an available port in a temp directory, yielding the connection URI."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])

    listen_addr = f"127.0.0.1:{port}"
    connection_uri = f"http://{listen_addr}"

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_data.db"

        # 1. Start process OUTSIDE the try block to avoid UnboundLocalError
        process = subprocess.Popen(
            ["umadb", "--db-path", str(db_path), "--listen", listen_addr],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        try:
            # 2. Wait for the server to be ready using a socket check
            _wait_for_port(port)

            # Check if it crashed immediately after binding
            if process.poll() is not None:
                msg = f"UmaDB crashed on startup (exit code {process.returncode})"
                raise RuntimeError()

            yield connection_uri

        finally:
            # 3. Teardown
            process.terminate()  # Standard way to ask a process to exit gracefully
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def _wait_for_port(port: int, host: str = "127.0.0.1", timeout: float = 5.0) -> None:
    """Polls the port until it accepts connections or times out."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.1)
            # connect_ex returns 0 if the connection succeeded
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError(f"UmaDB failed to bind to {host}:{port} within {timeout}s.")
