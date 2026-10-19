"""Suspend a live browser terminal's Chalk sandbox to storage, then resume it with its state intact.

The page is one terminal on `/ws` with its own sandbox: gVisor kernel, no network egress, half a
CPU, and no credentials. "Suspend" checkpoints the running sandbox, memory included, and releases
its compute; "Resume" brings the same sandbox id back from the checkpoint on whichever host has
room. Running programs keep their PIDs and their in-memory state, and the shell keeps its
working directory, variables and history.

Wire protocol on `/ws?cols=<n>&rows=<n>[&resume=<token>]`:
  client -> server  text {"type": "input", "data": "<keystrokes>"}
  client -> server  text {"type": "resize", "cols": <n>, "rows": <n>}
  client -> server  text {"type": "suspend"} | {"type": "resume"}
  server -> client  binary frames: raw PTY output
  server -> client  text {"type": "status", "message"}: progress while a sandbox starts
  server -> client  text {"type": "ready", "expires_in", "sandbox", "resumed": <bool>}
  server -> client  text {"type": "suspended", "token", "suspend_ms", "memory_bytes", "keep_seconds"}
  server -> client  text {"type": "resumed", "resume_ms", "cold_ms", "expires_in"}
  server -> client  text {"type": "action_failed", "message"}
  server -> client  text {"type": "busy" | "limited", "message"}, then close
  server -> client  text {"type": "ended", "reason"}, then close

A `token` names a suspended sandbox. Closing the page while suspended keeps the sandbox for
`SUSPENDED_SECONDS`; reconnecting with `?resume=<token>` within that window resumes it.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import ipaddress
import json
import logging
import os
import secrets
import shlex
import threading
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from chalkcompute import (
    ComputeClass,
    ContainerSecurityPolicy,
    ContainerSession,
    Image,
    KernelPolicy,
    Sandbox,
)

# Interactive PTY sessions are exposed through the session client, which is not yet part of
# the public chalkcompute namespace.
from chalkcompute import ConnectClient
from chalkcompute._container import ContainerSessionClient
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

PORT = int(os.environ.get("PORT", "8080"))
HERE = Path(__file__).resolve().parent
STATIC_DIR = Path(os.environ.get("TERMINAL_STATIC_DIR", str(HERE / "static")))

# Running sandboxes across every visitor. Suspended ones hold no compute and have their own cap.
MAX_RUNNING = int(os.environ.get("TERMINAL_MAX_RUNNING", "8"))
MAX_SUSPENDED = int(os.environ.get("TERMINAL_MAX_SUSPENDED", "24"))
# Running time per visitor; time spent suspended does not count against it.
RUNNING_SECONDS = int(os.environ.get("TERMINAL_RUNNING_SECONDS", "300"))
# How long a suspended sandbox (and its checkpoint) is kept before it is deleted.
SUSPENDED_SECONDS = int(os.environ.get("TERMINAL_SUSPENDED_SECONDS", "900"))
IDLE_SECONDS = int(os.environ.get("TERMINAL_IDLE_SECONDS", "120"))
MAX_SUSPENDS = int(os.environ.get("TERMINAL_MAX_SUSPENDS", "10"))
PER_IP_PER_HOUR = int(os.environ.get("TERMINAL_PER_IP_PER_HOUR", "10"))
STARTS_PER_MINUTE = int(os.environ.get("TERMINAL_STARTS_PER_MINUTE", "20"))
WARM_POOL = int(os.environ.get("TERMINAL_WARM_POOL", "2"))
POOL_MAX_AGE_SECONDS = int(os.environ.get("TERMINAL_POOL_MAX_AGE_SECONDS", "600"))
SANDBOX_CPU = os.environ.get("TERMINAL_SANDBOX_CPU", "500m")
SANDBOX_MEMORY = os.environ.get("TERMINAL_SANDBOX_MEMORY", "1Gi")
STATUS_TIMEOUT_SECONDS = 90
RESUME_CONFLICT_RETRY_SECONDS = 45
SANDBOX_NAME_PREFIX = "web-suspend-"
SANDBOX_TAG = {"app": "web-suspend-demo"}
MAX_INPUT_BYTES = 16 * 1024

# Visitor sandboxes run this image: Python, tmux (which holds the shell), and a few tools.
SANDBOX_IMAGE = Image.debian_slim(python_version="3.12").apt_install(
    ["tmux", "procps", "less", "tree"]
)

# After a restore, gVisor kills every process that was started through exec, because the caller
# holding its stdio is gone; only the container's init and its descendants survive. So PID 1 is
# this spawner, which runs each line written to SPAWN_FIFO as its own child. The shell lives in a
# tmux server started that way, and the browser's terminal is only a tmux client, so a resume
# keeps the live shell and everything started from it.
SPAWN_FIFO = "/run/spawn"
SPAWNER = (
    f"rm -f {SPAWN_FIFO}; mkfifo {SPAWN_FIFO}; while true; do while read -r cmd; do "
    + f'setsid sh -c "$cmd" </dev/null >/dev/null 2>&1 & done < {SPAWN_FIFO}; done'
)
TMUX_SOCKET = "/run/tmux.sock"
TMUX_CONF = "/run/tmux.conf"
MOTD_FILE = "/run/motd"
# A bare terminal: no status line, no prefix key, and no bindings, so every key reaches the shell.
TMUX_CONFIG = (
    "set -g status off\n"
    + "set -g prefix None\n"
    + "unbind-key -a\n"
    + "set -g escape-time 0\n"
    + "set -g history-limit 5000\n"
    + "set -g default-terminal xterm-256color\n"
    + "set -g window-size latest\n"
)
SHELL_ENV = {
    "HOME": "/workspace",
    "PS1": r"\[\e[38;5;150m\]sandbox\[\e[0m\]:\[\e[38;5;114m\]\w\[\e[0m\]$ ",
}
# A counter that keeps its count only in memory, so a resume that restarted it would show 1.
COUNTER_SCRIPT = (
    "import time\n"
    + "n = 0\n"
    + "while True:\n"
    + "    n += 1\n"
    + "    print(f'\\rcount {n}', end='', flush=True)\n"
    + "    time.sleep(1)\n"
)

MOTD = """
\x1b[38;5;150m  chalk sandbox\x1b[0m  ·  gVisor kernel · no network · yours alone

  Start something, then press \x1b[38;5;150mSuspend\x1b[0m. The sandbox, memory included, is checkpointed
  and its compute released. \x1b[38;5;150mResume\x1b[0m brings it back: same PIDs, same variables, same files.

  Try:  \x1b[38;5;114mpython3 count.py\x1b[0m   (a counter that lives only in memory), or
        \x1b[38;5;114mpython3\x1b[0m, then  \x1b[38;5;114mx = 41\x1b[0m. Suspend, resume, and ask for  \x1b[38;5;114mx + 1\x1b[0m.

"""
MOTD_COMPACT = """
\x1b[38;5;150m  chalk sandbox\x1b[0m
  gVisor kernel · no network
  Run python3 count.py,
  then press \x1b[38;5;150mSuspend\x1b[0m.

"""
MOTD_MIN_COLS = 84

log = logging.getLogger("suspend-terminal")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def is_public(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_global
    except ValueError:
        return False


class RateLimiter:
    """Caps sandbox starts globally, and per client IP when the real client IP is visible.

    Some gateways forward only their own private address, which every visitor shares; limiting
    by it would lock everyone out at once, so private addresses get only the global caps.
    """

    def __init__(self, per_hour: int, global_per_minute: int) -> None:
        super().__init__()
        self.per_hour = per_hour
        self.global_per_minute = global_per_minute
        self.starts: dict[str, collections.deque[float]] = collections.defaultdict(
            collections.deque
        )
        self.global_starts: collections.deque[float] = collections.deque()
        self.live: set[str] = set()

    @staticmethod
    def _trim(window: collections.deque[float], now: float, horizon: float) -> None:
        while window and now - window[0] > horizon:
            window.popleft()

    def acquire_global(self) -> str | None:
        """Records one sandbox start or resume against the global window, or returns a refusal."""
        now = time.monotonic()
        self._trim(self.global_starts, now, 60)
        if len(self.global_starts) >= self.global_per_minute:
            return "Lots of people are using the demo right now. Try again in a minute."
        self.global_starts.append(now)
        return None

    def acquire(self, ip: str) -> str | None:
        """Admits a new visitor from `ip`, or returns a refusal message."""
        if is_public(ip):
            if ip in self.live:
                return "You already have a terminal open in another tab."
            window = self.starts[ip]
            self._trim(window, time.monotonic(), 3600)
            if len(window) >= self.per_hour:
                return "That's a lot of sandboxes for one hour. Try again later."
        refusal = self.acquire_global()
        if refusal is None and is_public(ip):
            self.starts[ip].append(time.monotonic())
            self.live.add(ip)
        return refusal

    def release(self, ip: str) -> None:
        self.live.discard(ip)


@dataclass
class Visit:
    """One visitor's sandbox, which outlives the WebSocket while it is suspended."""

    sandbox: Sandbox
    ip: str
    # Running time left, in seconds; it only counts down while the sandbox runs.
    budget: float
    suspends: int = 0
    token: str | None = None
    snapshot_id: str | None = None
    suspended_at: float | None = None
    connected: bool = False
    # Set when a suspend lost its marker: resume by restoring the checkpoint into a new sandbox.
    restore_into_new: bool = False
    # The suspend or resume in flight, which cleanup must wait for before touching the sandbox.
    action: asyncio.Task[None] | None = None


state: dict[str, Any] = {}


def make_sandbox() -> Sandbox:
    # No network_policy means no egress at all (not even DNS). No chalk_identity and no
    # secrets: nothing inside the sandbox can call back into Chalk.
    sandbox = Sandbox(
        image=SANDBOX_IMAGE,
        entrypoint=["/bin/sh", "-c", SPAWNER],
        name=f"{SANDBOX_NAME_PREFIX}{uuid.uuid4().hex[:10]}",
        cpu=SANDBOX_CPU,
        memory=SANDBOX_MEMORY,
        # A backstop only: the server deletes every sandbox well before this, suspended ones
        # included, and a resume does not extend the lifetime recorded at creation.
        lifetime=f"{POOL_MAX_AGE_SECONDS + RUNNING_SECONDS + SUSPENDED_SECONDS + 120}s",
        compute_class=ComputeClass.HOST,
        security_policy=ContainerSecurityPolicy(kernel_policy=KernelPolicy.RESTRICTED),
        tags=SANDBOX_TAG,
    )
    sandbox.run()
    sandbox.exec("mkdir", "-p", "/workspace")
    write_file(sandbox, TMUX_CONF, TMUX_CONFIG)
    write_file(sandbox, "/workspace/count.py", COUNTER_SCRIPT)
    return sandbox


def terminate_quietly(sandbox: Sandbox) -> None:
    try:
        sandbox.terminate()
    except Exception:
        log.exception("terminate failed for %s", sandbox.id)


def write_file(sandbox: Sandbox, path: str, content: str) -> None:
    encoded = base64.b64encode(content.encode()).decode()
    sandbox.exec("sh", "-c", f"printf %s {encoded} | base64 -d > {path}")


def start_shell(sandbox: Sandbox, cols: int, rows: int, motd: str) -> None:
    """Has PID 1 start the tmux server that holds the visitor's shell, and waits for it."""
    write_file(sandbox, MOTD_FILE, motd.replace("\n", "\r\n"))
    shell_env = shlex.join(f"{key}={value}" for key, value in SHELL_ENV.items())
    shell = f"cat {MOTD_FILE}; exec env {shell_env} bash --norc --noprofile -i"
    command = (
        f"tmux -f {TMUX_CONF} -S {TMUX_SOCKET} new-session -d -s main -x {cols} -y {rows} "
        + f"-c /workspace {shlex.quote(shell)}"
    )
    # The command reaches PID 1 as one line of the FIFO; it contains no newlines.
    result = sandbox.exec(
        "sh",
        "-c",
        # printf rather than echo: dash's echo would interpret the prompt's backslashes.
        f"printf '%s\\n' {shlex.quote(command)} > {SPAWN_FIFO}; "
        + f"for i in $(seq 100); do tmux -S {TMUX_SOCKET} has-session -t main 2>/dev/null && exit 0; sleep 0.05; done; exit 1",
    )
    if result.exit_code != 0:
        raise RuntimeError("the shell did not start")


def attach_tmux(sandbox: Sandbox, cols: int, rows: int) -> ContainerSession:
    """Opens a PTY session running a tmux client on the sandbox's shell."""
    return state["sessions"].new_session(
        sandbox.id,
        "tmux",
        "-S",
        TMUX_SOCKET,
        "attach-session",
        "-t",
        "main",
        env={"TERM": "xterm-256color"},
        pty=(cols, rows),
    )


def memory_in_use(sandbox: Sandbox) -> int | None:
    """Bytes of memory in use inside the sandbox, as its kernel reports it."""
    try:
        result = sandbox.exec(
            "sh",
            "-c",
            "awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} END{print (t-a)*1024}' /proc/meminfo",
        )
        out = (
            result.stdout.decode()
            if isinstance(result.stdout, (bytes, bytearray))
            else str(result.stdout)
        )
        value = int(out.strip())
        return value if value > 0 else None
    except Exception:
        log.exception("reading memory for %s failed", sandbox.id)
        return None


def reap_orphans(suspender: SuspendClient) -> None:
    """Deletes sandboxes a previous replica left behind, suspended ones included."""
    try:
        for info in Sandbox.list_all():
            if not (info.name or "").startswith(SANDBOX_NAME_PREFIX):
                continue
            if info.status in ("pending", "running"):
                terminate_quietly(Sandbox.from_id(info.id))
            elif info.status == "suspended":
                suspender.discard_suspended(info.id, None)
    except Exception:
        log.exception("orphan reap failed")


class SuspendClient:
    """Suspends and resumes sandboxes over Connect's JSON protocol.

    The released chalkcompute SDK does not wrap these RPCs yet, so they are called directly,
    authenticated with the same credentials the SDK uses.
    """

    def __init__(self) -> None:
        super().__init__()
        self.connect = ConnectClient()
        self.api = self.connect.api_server
        self.http = httpx.Client(timeout=120)

    def _call(self, method: str, body: dict[str, Any]) -> dict[str, Any]:
        headers = self.connect.request_headers()
        headers["content-type"] = "application/json"
        resp = self.http.post(f"{self.api}/{method}", json=body, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(
                f"{method} failed with {resp.status_code}: {resp.text[:300]}"
            )
        return resp.json()

    def status(self, sandbox_id: str) -> str:
        sandbox = self._call(
            "chalk.sandbox.v2.SandboxService/GetSandbox", {"id": sandbox_id}
        )["sandbox"]
        return sandbox.get("status", "SANDBOX_STATUS_UNSPECIFIED")

    def wait_for(self, sandbox_id: str, wanted: str) -> None:
        deadline = time.monotonic() + STATUS_TIMEOUT_SECONDS
        while True:
            status = self.status(sandbox_id)
            if status == wanted:
                return
            if (
                status in ("SANDBOX_STATUS_FAILED", "SANDBOX_STATUS_TERMINATED")
                or time.monotonic() > deadline
            ):
                raise RuntimeError(f"sandbox {sandbox_id} is {status}, not {wanted}")
            time.sleep(0.2)

    def suspend(self, sandbox_id: str) -> tuple[str, bool]:
        """Checkpoints and stops the sandbox.

        Returns the checkpoint's id, and whether the sandbox lost its suspension marker. The
        marker can be cleared by a status update that raced the suspend; the sandbox then reads
        as finished rather than suspended and cannot be resumed under its id, but the checkpoint
        itself is complete, so it can still be restored into a new sandbox.
        """
        self._call("chalk.sandbox.v2.SandboxService/SuspendSandbox", {"id": sandbox_id})
        deadline = time.monotonic() + STATUS_TIMEOUT_SECONDS
        while True:
            status = self.status(sandbox_id)
            if status in ("SANDBOX_STATUS_SUSPENDED", "SANDBOX_STATUS_SUCCEEDED"):
                break
            if (
                status in ("SANDBOX_STATUS_FAILED", "SANDBOX_STATUS_TERMINATED")
                or time.monotonic() > deadline
            ):
                raise RuntimeError(f"sandbox {sandbox_id} is {status} after a suspend")
            time.sleep(0.2)
        snapshots = self._call(
            "chalk.sandbox.v2.SandboxSnapshotService/ListSandboxSnapshots",
            {"filters": {"sandboxIds": [sandbox_id]}},
        ).get("snapshots", [])
        completed = [
            s
            for s in snapshots
            if s.get("status") == "SANDBOX_SNAPSHOT_STATUS_COMPLETED"
        ]
        newest = max(completed, key=lambda s: s.get("createdAt", ""), default=None)
        if newest is None:
            raise RuntimeError(f"no checkpoint recorded for {sandbox_id}")
        return newest["id"], status == "SANDBOX_STATUS_SUCCEEDED"

    def restore(self, snapshot_id: str) -> str:
        """Boots a new sandbox from a checkpoint and waits for it; returns the new sandbox's id."""
        created = self._call(
            "chalk.sandbox.v2.SandboxService/CreateSandbox", {"snapshotId": snapshot_id}
        )
        new_id = created["sandbox"]["id"]
        self.wait_for(new_id, "SANDBOX_STATUS_RUNNING")
        return new_id

    def resume(self, sandbox_id: str) -> None:
        # Right after a suspend, the old container's Kubernetes resource can still be deleting, and
        # resuming then fails with a conflict on creating the new one; it clears within seconds.
        deadline = time.monotonic() + RESUME_CONFLICT_RETRY_SECONDS
        while True:
            try:
                self._call(
                    "chalk.sandbox.v2.SandboxService/ResumeSandbox", {"id": sandbox_id}
                )
                break
            except RuntimeError as e:
                if "conflict" not in str(e) or time.monotonic() > deadline:
                    raise
                log.info("resume of %s hit the deletion race; retrying", sandbox_id)
                time.sleep(1)
        self.wait_for(sandbox_id, "SANDBOX_STATUS_RUNNING")

    def terminate(self, sandbox_id: str) -> None:
        self._call(
            "chalk.sandbox.v2.SandboxService/TerminateSandbox", {"id": sandbox_id}
        )

    def discard_suspended(self, sandbox_id: str, snapshot_id: str | None) -> None:
        """Deletes a suspended sandbox and its checkpoint.

        A suspended sandbox has no container, so TerminateSandbox reports it not found, and its
        checkpoint cannot be deleted while it is the one the sandbox would resume from. The only
        way to discard it is to resume it, terminate it, and then delete the checkpoint.
        """
        try:
            self.resume(sandbox_id)
            self.terminate(sandbox_id)
        except Exception:
            log.exception("discarding suspended sandbox %s failed", sandbox_id)
        if snapshot_id is not None:
            self.delete_snapshot(snapshot_id)

    def delete_snapshot(self, snapshot_id: str) -> None:
        try:
            self._call(
                "chalk.sandbox.v2.SandboxSnapshotService/DeleteSandboxSnapshot",
                {"snapshotId": snapshot_id},
            )
        except Exception:
            log.exception("deleting snapshot %s failed", snapshot_id)


def discard(visit: Visit) -> None:
    """Deletes a visitor's sandbox and, if it is suspended, the checkpoint it would resume from."""
    suspender: SuspendClient = state["suspender"]
    if visit.suspended_at is None:
        terminate_quietly(visit.sandbox)
    elif visit.restore_into_new:
        # The sandbox already reads as finished; only the checkpoint is left to delete.
        if visit.snapshot_id is not None:
            suspender.delete_snapshot(visit.snapshot_id)
    else:
        suspender.discard_suspended(visit.sandbox.id, visit.snapshot_id)


class WarmPool:
    """Keeps `size` started sandboxes ready, because a cold start takes anywhere from 2 to 60 s.

    Every sandbox handed out is fresh: a visitor never gets one another visitor has used.
    """

    def __init__(self, size: int) -> None:
        super().__init__()
        self.size = size
        self.ready: collections.deque[tuple[float, Sandbox]] = collections.deque()
        self.filling = 0

    def fill(self) -> None:
        while len(self.ready) + self.filling < self.size:
            self.filling += 1
            asyncio.create_task(self._create())

    async def prime(self) -> None:
        """Starts one sandbox and waits for it, then fills the rest in the background."""
        self.filling += 1
        await self._create()
        self.fill()

    async def _create(self) -> None:
        try:
            t0 = time.monotonic()
            sandbox = await asyncio.to_thread(make_sandbox)
            self.ready.append((time.monotonic(), sandbox))
            log.info(
                "pooled sandbox %s started in %.1fs", sandbox.id, time.monotonic() - t0
            )
        except Exception:
            log.exception("pooled sandbox start failed")
            await asyncio.sleep(10)
        finally:
            self.filling -= 1

    async def take(self) -> Sandbox:
        while self.ready:
            created, sandbox = self.ready.popleft()
            if time.monotonic() - created < POOL_MAX_AGE_SECONDS:
                self.fill()
                return sandbox
            asyncio.create_task(asyncio.to_thread(terminate_quietly, sandbox))
        self.fill()
        return await asyncio.to_thread(make_sandbox)

    async def recycle_forever(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            while self.ready and now - self.ready[0][0] >= POOL_MAX_AGE_SECONDS:
                _, stale = self.ready.popleft()
                await asyncio.to_thread(terminate_quietly, stale)
            self.fill()

    async def drain(self) -> None:
        while self.ready:
            _, sandbox = self.ready.popleft()
            await asyncio.to_thread(terminate_quietly, sandbox)


async def expire_suspended_forever() -> None:
    """Deletes suspended sandboxes, and their checkpoints, once they have been kept long enough."""
    parked: dict[str, Visit] = state["parked"]
    while True:
        await asyncio.sleep(10)
        now = time.monotonic()
        for token in [
            t
            for t, v in parked.items()
            if not v.connected and now - (v.suspended_at or now) > SUSPENDED_SECONDS
        ]:
            visit = parked.pop(token)
            log.info("suspended sandbox %s expired", visit.sandbox.id)
            await asyncio.to_thread(discard, visit)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    state["suspender"] = SuspendClient()
    await asyncio.to_thread(reap_orphans, state["suspender"])
    pool = WarmPool(WARM_POOL)
    # The first sandbox also builds the visitor image; wait for it so the replica only
    # reports ready once a visitor can get a terminal immediately.
    await pool.prime()
    state["pool"] = pool
    state["limiter"] = RateLimiter(PER_IP_PER_HOUR, STARTS_PER_MINUTE)
    state["running"] = 0
    state["parked"] = {}
    state["sessions"] = ContainerSessionClient.from_connect(ConnectClient())
    background = [
        asyncio.create_task(pool.recycle_forever()),
        asyncio.create_task(expire_suspended_forever()),
    ]
    yield
    for task in background:
        task.cancel()
    await pool.drain()
    for visit in list(state["parked"].values()):
        await asyncio.to_thread(discard, visit)


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse((HERE / "index.html").read_text())


# ghostty-web's bundle (its WASM is inlined) plus the empty chunk it imports.
STATIC_FILES = {"ghostty-web.js", "__vite-browser-external-2447137e.js"}


@app.get("/static/{name}")
async def static_file(name: str) -> FileResponse:
    if name not in STATIC_FILES:
        raise HTTPException(status_code=404)
    return FileResponse(
        STATIC_DIR / name,
        media_type="text/javascript",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/status")
async def status() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "running": state["running"],
            "running_capacity": MAX_RUNNING,
            "suspended": len(state["parked"]),
            "suspended_capacity": MAX_SUSPENDED,
        }
    )


def client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"


def clamp_dim(value: Any, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(hi, int(value)))
    except (TypeError, ValueError):
        return default


def short_id(sandbox: Sandbox) -> str:
    return (sandbox.id or "")[:8]


async def send(websocket: WebSocket, msg: dict[str, Any]) -> None:
    await websocket.send_text(json.dumps(msg))


async def refuse(websocket: WebSocket, kind: str, message: str) -> None:
    await send(websocket, {"type": kind, "message": message})
    await websocket.close()


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await websocket.accept()
    token = websocket.query_params.get("resume")
    if token:
        await reconnect(websocket, token)
    else:
        await launch(websocket)


async def launch(websocket: WebSocket) -> None:
    ip = client_ip(websocket)
    limiter: RateLimiter = state["limiter"]
    refusal = limiter.acquire(ip)
    if refusal is not None:
        await refuse(websocket, "limited", refusal)
        return
    if state["running"] >= MAX_RUNNING:
        limiter.release(ip)
        await refuse(
            websocket, "busy", "Every demo sandbox is in use. Try again in a minute."
        )
        return
    state["running"] += 1
    visit: Visit | None = None
    try:
        await send(
            websocket, {"type": "status", "message": "Starting a fresh sandbox…"}
        )
        sandbox = await state["pool"].take()
        visit = Visit(sandbox=sandbox, ip=ip, budget=RUNNING_SECONDS)
        cols = clamp_dim(websocket.query_params.get("cols"), 20, 300, 100)
        rows = clamp_dim(websocket.query_params.get("rows"), 5, 120, 30)
        motd = MOTD if cols >= MOTD_MIN_COLS else MOTD_COMPACT
        await asyncio.to_thread(start_shell, sandbox, cols, rows, motd)
    except Exception:
        state["running"] -= 1
        limiter.release(ip)
        log.exception("launch failed")
        if visit is not None:
            await asyncio.to_thread(discard, visit)
        await refuse(
            websocket, "busy", "That sandbox didn't start. Try again in a minute."
        )
        return
    await serve(websocket, visit, resumed=False)


async def reconnect(websocket: WebSocket, token: str) -> None:
    """Resumes a sandbox the visitor suspended before closing the page."""
    parked: dict[str, Visit] = state["parked"]
    visit = parked.get(token)
    if visit is None or visit.connected:
        await send(websocket, {"type": "ended", "reason": "resume_expired"})
        await websocket.close()
        return
    visit.connected = True
    if state["running"] >= MAX_RUNNING:
        visit.connected = False
        await refuse(
            websocket,
            "busy",
            "Every demo sandbox is in use. Your suspended one is kept; try again in a minute.",
        )
        return
    await send(
        websocket, {"type": "status", "message": "Resuming your suspended sandbox…"}
    )
    await serve(websocket, visit, resumed=True)


def take_running_slot(visit: Visit) -> str | None:
    """Reserves compute for a resume, or returns why it cannot run right now."""
    if state["running"] >= MAX_RUNNING:
        return "Every demo sandbox is running right now. Yours stays suspended; try again shortly."
    refusal = state["limiter"].acquire_global()
    if refusal is None:
        state["running"] += 1
    return refusal


async def suspend_visit(visit: Visit) -> dict[str, Any]:
    """Checkpoints the visitor's sandbox and releases its compute; returns the message to send."""
    suspender: SuspendClient = state["suspender"]
    memory = await asyncio.to_thread(memory_in_use, visit.sandbox)
    t0 = time.monotonic()
    snapshot_id, marker_lost = await asyncio.to_thread(
        suspender.suspend, visit.sandbox.id
    )
    suspend_ms = round((time.monotonic() - t0) * 1000)
    visit.snapshot_id = snapshot_id
    visit.restore_into_new = marker_lost
    visit.suspended_at = time.monotonic()
    visit.suspends += 1
    visit.token = visit.token or secrets.token_urlsafe(16)
    state["parked"][visit.token] = visit
    state["running"] -= 1
    log.info(
        "suspended %s in %d ms (%s bytes in use)%s",
        visit.sandbox.id,
        suspend_ms,
        memory,
        "; its suspension marker was lost, so it will resume into a new sandbox"
        if marker_lost
        else "",
    )
    return {
        "type": "suspended",
        "token": visit.token,
        "suspend_ms": suspend_ms,
        "memory_bytes": memory,
        "keep_seconds": SUSPENDED_SECONDS,
    }


async def resume_visit(visit: Visit) -> dict[str, Any]:
    """Brings a suspended sandbox back from its checkpoint; the caller holds a running slot.

    Normally the sandbox resumes under its original id. If its suspension marker was lost (see
    `SuspendClient.suspend`), the same checkpoint is restored into a new sandbox instead: the
    processes, memory and files are the same, and only the sandbox id changes.
    """
    suspender: SuspendClient = state["suspender"]
    t0 = time.monotonic()
    new_id = False
    if visit.restore_into_new and visit.snapshot_id is not None:
        restored = await asyncio.to_thread(suspender.restore, visit.snapshot_id)
        visit.sandbox = Sandbox.from_id(restored)
        visit.restore_into_new = False
        new_id = True
    else:
        await asyncio.to_thread(suspender.resume, visit.sandbox.id)
    resume_ms = round((time.monotonic() - t0) * 1000)
    cold_ms = round((t0 - (visit.suspended_at or t0)) * 1000)
    if visit.token is not None:
        state["parked"].pop(visit.token, None)
    snapshot_id, visit.snapshot_id, visit.suspended_at = visit.snapshot_id, None, None
    # A running sandbox no longer needs the checkpoint it resumed from.
    if snapshot_id is not None:
        asyncio.create_task(asyncio.to_thread(suspender.delete_snapshot, snapshot_id))
    log.info(
        "resumed %s in %d ms after %d ms suspended%s",
        visit.sandbox.id,
        resume_ms,
        cold_ms,
        " (restored into a new sandbox id)" if new_id else "",
    )
    return {
        "type": "resumed",
        "resume_ms": resume_ms,
        "cold_ms": cold_ms,
        "expires_in": max(1, int(visit.budget)),
        "sandbox": short_id(visit.sandbox),
        "new_id": new_id,
    }


async def serve(websocket: WebSocket, visit: Visit, resumed: bool) -> None:
    """Runs one connection to a visit, resuming it first when it arrives suspended."""
    visit.connected = True
    reason = "error"
    try:
        if resumed:
            refusal = take_running_slot(visit)
            if refusal is not None:
                visit.connected = False
                await refuse(websocket, "busy", refusal)
                return
            try:
                await resume_visit(visit)
            except Exception:
                state["running"] -= 1
                raise
        reason = await run_terminal(websocket, visit, resumed)
        await send(websocket, {"type": "ended", "reason": reason})
        await websocket.close()
    except WebSocketDisconnect:
        reason = "disconnected"
    except Exception:
        log.exception("sandbox %s failed", visit.sandbox.id)
        try:
            await send(websocket, {"type": "ended", "reason": "error"})
            await websocket.close(code=1011)
        except (RuntimeError, WebSocketDisconnect):
            log.info("the browser had already gone for %s", visit.sandbox.id)
    finally:
        # A suspend or resume still in flight decides whether the sandbox is running or parked.
        if visit.action is not None and not visit.action.done():
            await asyncio.wait([visit.action], timeout=STATUS_TIMEOUT_SECONDS + 30)
        visit.connected = False
        if visit.suspended_at is not None and reason in ("disconnected", "page_closed"):
            # Closing the page while suspended keeps the sandbox, so the visitor can come back.
            state["limiter"].release(visit.ip)
            log.info("kept suspended %s for a later resume", visit.sandbox.id)
        else:
            if visit.token is not None:
                state["parked"].pop(visit.token, None)
            if visit.suspended_at is None:
                state["running"] -= 1
            state["limiter"].release(visit.ip)
            await asyncio.to_thread(discard, visit)


async def run_terminal(websocket: WebSocket, visit: Visit, resumed: bool) -> str:
    """Pumps PTY output to the browser and keystrokes to the PTY; returns why it stopped.

    Suspending retires the tmux client and resuming attaches a fresh one, so the session in use
    changes. Every event is tagged with the generation of the session that produced it, and
    events from a retired session, including its exit, are ignored.
    """
    loop = asyncio.get_running_loop()
    events: asyncio.Queue[Any] = asyncio.Queue()
    size = [
        clamp_dim(websocket.query_params.get("cols"), 20, 300, 100),
        clamp_dim(websocket.query_params.get("rows"), 5, 120, 30),
    ]
    attached: dict[str, Any] = {"session": None, "gen": 0}
    busy = False
    last_input = time.monotonic()

    def read_events(sess: ContainerSession, gen: int) -> None:
        try:
            for event in sess.events():
                loop.call_soon_threadsafe(events.put_nowait, (gen, event))
        except Exception as e:  # noqa: BLE001 - any stream failure ends the pane the same way
            loop.call_soon_threadsafe(events.put_nowait, (gen, e))
        loop.call_soon_threadsafe(events.put_nowait, (gen, None))

    async def attach(note: str | None) -> None:
        attached["gen"] += 1
        fresh = await asyncio.to_thread(attach_tmux, visit.sandbox, size[0], size[1])
        attached["session"] = fresh
        threading.Thread(
            target=read_events, args=(fresh, attached["gen"]), daemon=True
        ).start()
        if note is not None:
            await asyncio.to_thread(
                visit.sandbox.exec,
                "tmux",
                "-S",
                TMUX_SOCKET,
                "display-message",
                "-d",
                "6000",
                "-t",
                "main",
                note,
            )

    async def detach() -> None:
        # A tmux client hands its terminal, a host-backed PTY, to the tmux server, and a surviving
        # process that holds a host fd cannot be restored. So the checkpoint is taken with no
        # client attached.
        attached["gen"] += 1
        attached["session"] = None
        await asyncio.to_thread(
            visit.sandbox.exec, "tmux", "-S", TMUX_SOCKET, "detach-client", "-s", "main"
        )

    note = None
    if resumed:
        note = "resumed · same processes, same memory"
    await attach(note)
    await send(
        websocket,
        {
            "type": "ready",
            "expires_in": max(1, int(visit.budget)),
            "sandbox": short_id(visit.sandbox),
            "resumed": resumed,
        },
    )

    async def pump_output() -> str:
        while True:
            gen, event = await events.get()
            if gen != attached["gen"]:
                continue
            if event is None or isinstance(event, Exception):
                return "exited"
            if isinstance(event, ContainerSession.OutputEvent):
                await websocket.send_bytes(event.data)
            elif isinstance(
                event,
                (
                    ContainerSession.ExitedEvent,
                    ContainerSession.FailedEvent,
                    ContainerSession.TimedOutEvent,
                    ContainerSession.ErrorEvent,
                ),
            ):
                return "exited"

    def suspend_refusal() -> str | None:
        if visit.suspends >= MAX_SUSPENDS:
            return f"Up to {MAX_SUSPENDS} suspends per visit."
        if len(state["parked"]) >= MAX_SUSPENDED:
            return "The demo is holding as many suspended sandboxes as it can. Try again shortly."
        return None

    async def suspend_and_report() -> dict[str, Any]:
        refusal = suspend_refusal()
        if refusal is not None:
            return {"type": "action_failed", "message": refusal}
        await detach()
        try:
            return await suspend_visit(visit)
        except Exception:
            log.exception("suspend of %s failed", visit.sandbox.id)
            await attach(None)
            return {
                "type": "action_failed",
                "message": "That suspend didn't work. Try again.",
            }

    async def resume_and_report() -> dict[str, Any]:
        nonlocal last_input
        refusal = take_running_slot(visit)
        if refusal is not None:
            return {"type": "action_failed", "message": refusal}
        try:
            msg = await resume_visit(visit)
        except Exception:
            state["running"] -= 1
            log.exception("resume of %s failed", visit.sandbox.id)
            return {
                "type": "action_failed",
                "message": "That resume didn't work. Try again.",
            }
        last_input = time.monotonic()
        await attach(
            f"resumed after {msg['cold_ms'] / 1000:.1f} s suspended · resume {msg['resume_ms']} ms"
        )
        return msg

    async def run_action(action: Any) -> None:
        """Runs a suspend or resume beside the pumps, then reports it; the state change has
        already happened by the time the report is sent, even if the browser has gone."""
        nonlocal busy
        try:
            msg = await action()
        finally:
            busy = False
        await send(websocket, msg)

    async def pump_input() -> str:
        nonlocal last_input, busy
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                raise WebSocketDisconnect
            text = msg.get("text")
            if not text or len(text) > MAX_INPUT_BYTES:
                continue
            ctrl = json.loads(text)
            kind = ctrl.get("type")
            session = attached["session"]
            if kind == "input" and isinstance(ctrl.get("data"), str):
                last_input = time.monotonic()
                # Keystrokes typed while the visit.sandbox is suspended or changing state are dropped.
                if session is not None and not busy:
                    await asyncio.to_thread(session.write, ctrl["data"].encode())
            elif kind == "resize":
                size[0] = clamp_dim(ctrl.get("cols"), 20, 300, 100)
                size[1] = clamp_dim(ctrl.get("rows"), 5, 120, 30)
                if session is not None and not busy:
                    await asyncio.to_thread(session.resize, size[0], size[1])
            elif kind == "suspend" and not busy and visit.suspended_at is None:
                busy = True
                visit.action = asyncio.create_task(run_action(suspend_and_report))
            elif kind == "resume" and not busy and visit.suspended_at is not None:
                busy = True
                visit.action = asyncio.create_task(run_action(resume_and_report))

    async def watchdog() -> str:
        last = time.monotonic()
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            if visit.suspended_at is None:
                # Running time is the budget; suspended time is free.
                visit.budget -= now - last
                if visit.budget <= 0:
                    return "time_limit"
                if now - last_input >= IDLE_SECONDS and not busy:
                    return "idle"
            elif now - visit.suspended_at >= SUSPENDED_SECONDS:
                return "suspend_expired"
            last = now

    tasks = [asyncio.create_task(c) for c in (pump_output(), pump_input(), watchdog())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        return next(iter(done)).result()
    finally:
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    import uvicorn

    # Pings well under the gateway's 60 s idle timeout keep a quiet terminal connected.
    uvicorn.run(app, host="0.0.0.0", port=PORT, ws_ping_interval=20, ws_ping_timeout=20)
