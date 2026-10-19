#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12,<3.14"
# dependencies = ["chalkcompute>=2.13.8"]
# ///
"""Deploy the sandbox suspend/resume demo (server.py + index.html) as a Chalk ScalingGroup.

The server creates, suspends, and resumes visitor sandboxes with its own workload identity
(`chalk_identity=True`), so no client secret is baked into the image. Visitor sandboxes get no
identity at all. Snapshots must be enabled for the environment, since a suspend is a checkpoint.

Credentials for the deploy itself come from your existing `chalk login` (or the
CHALK_* env vars):
    ./deploy.py
Then open the printed URL, press "Launch sandbox", start something, and press "Suspend".
"""

from __future__ import annotations

import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GHOSTTY_WEB_VERSION = "0.4.0"
STATIC_DIR = "/app/static"
APP_DIR = "/app/srv"

# ghostty-web ships its WASM inlined in the ESM bundle, so the page needs only dist/*.js.
FETCH_GHOSTTY = (
    'python -c "import io, tarfile, urllib.request; '
    + f"data = urllib.request.urlopen('https://registry.npmjs.org/ghostty-web/-/ghostty-web-{GHOSTTY_WEB_VERSION}.tgz').read(); "
    + "tar = tarfile.open(fileobj=io.BytesIO(data)); "
    + "members = [m for m in tar.getmembers() if m.name.startswith('package/dist/') and m.name.endswith('.js')]; "
    + "[setattr(m, 'name', m.name.split('/')[-1]) for m in members]; "
    + f"tar.extractall('{STATIC_DIR}', members=members)\""
)


def main() -> None:
    from chalkcompute import Image, ScalingGroup

    image = (
        Image.debian_slim(python_version="3.12")
        .pip_install(["chalkcompute>=2.13.8", "fastapi==0.116.1", "uvicorn[standard]==0.35.0", "httpx==0.28.1"])
        .run_commands(FETCH_GHOSTTY)
        # Together the two files exceed the 32 KiB inline-copy limit, so they ship as a mounted
        # volume. A volume shadows its whole directory, so it gets one of its own.
        .add_local_file(str(HERE / "server.py"), f"{APP_DIR}/server.py", strategy="volume")
        .add_local_file(str(HERE / "index.html"), f"{APP_DIR}/index.html", strategy="volume")
        .workdir("/app")
    )

    sg = ScalingGroup(
        image=image,
        name='sandbox-suspend',
        cpu="1",
        memory="2Gi",
        env={
            # Not a secret: which API the server's workload identity talks to.
            "PORT": "8080",
            "TERMINAL_STATIC_DIR": STATIC_DIR,
            "TERMINAL_MAX_RUNNING": '8',
            "TERMINAL_MAX_SUSPENDED": '24',
            "TERMINAL_WARM_POOL": '2',
        },
        entrypoint=["python", f"{APP_DIR}/server.py"],
        port=8080,
        chalk_identity=True,
        # Session state lives in the replica (the warm pool, suspended sandboxes awaiting a resume,
        # and per-IP limits), and each WebSocket must stay on the replica that holds its sandbox,
        # so this stays at one.
        min_replicas=1,
        max_replicas=1,
    )
    t0 = time.monotonic()
    try:
        sg.deploy(build_timeout=1800.0, ready_timeout=900.0)
    finally:
        print(f"deploy returned after {time.monotonic() - t0:.0f}s")
    print("web_url:", sg.web_url)


if __name__ == "__main__":
    main()
