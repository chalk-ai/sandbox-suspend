# Sandbox suspend

Put a live terminal to sleep and wake it up later. The terminal is a shell in a short-lived,
isolated [Chalk](https://chalk.ai) sandbox. Press **Suspend** and the whole sandbox, memory
included, is checkpointed to storage and its compute is released. Press **Resume**, now or after
closing the tab, and the same sandbox id comes back with every process where it left off. The
terminal is drawn by [ghostty-web](https://github.com/coder/ghostty-web), Ghostty's VT parser
compiled to WebAssembly.

```
browser (ghostty-web) ──WebSocket──▶ server.py ──PTY session──▶ sandbox
                                         │
                                         ├─ SuspendSandbox(id) ─▶ checkpoint in storage, no compute
                                         └─ ResumeSandbox(id)  ─▶ same id, restored on any host
```

## What a suspend keeps

A suspended sandbox is a checkpoint of the whole sandbox, so a resume brings back:

- **Running processes:** with the same PIDs and the same in-memory state. A counter that only
  ever lived in memory picks up where it stopped, and a Python REPL keeps its variables.
- **The shell:** its working directory, its environment variables, and its history.
- **Files and `/tmp`:** the file system and the in-memory `/tmp`, exactly as they were.
- **The sandbox's identity:** the same sandbox id, spec, and isolation.

While suspended, nothing runs and no CPU or memory is reserved. Time stops for the processes
inside: a counter that was at 6 when suspended reads 7 a second after it resumes, however long it
was suspended.

Measured on CPU sandboxes (half a CPU, 1 GiB) with an interactive shell and a running counter:
a suspend takes about 1 to 1.4 seconds, a resume about 0.8 to 1 second, and the checkpoint of a
mostly idle sandbox (around 20 MiB of memory in use) is about 7 MB in storage.

## Which processes survive a resume

After a restore, gVisor kills every process that was started through `exec`, along with all of
its descendants, because the caller holding its stdin and stdout is gone. Only the container's
init process (PID 1) and its descendants survive.

So PID 1 is a tiny spawner (`SPAWNER` in `server.py`). It reads commands from a FIFO and runs each
one as its own child. The visitor's shell lives in a tmux server started that way, and the
browser's terminal is just a tmux client attached over a PTY session. A suspend detaches the
client first, because a surviving process must not hold a host file descriptor, and the client's
terminal is a host PTY. A resume attaches a fresh client to the same tmux server.

## Platform behaviours this demo works around

- **Resuming right after a suspend can conflict.** The suspended sandbox's old Kubernetes resource
  may still be deleting, and the resume fails with a conflict on creating the new one. The server
  retries for up to 45 seconds; the conflict usually clears within a few.
- **A suspended sandbox can't be terminated directly.** It has no container, so `TerminateSandbox`
  reports it not found, and its checkpoint can't be deleted while it is the one the sandbox would
  resume from. The server discards a suspended sandbox by resuming it, terminating it, and then
  deleting the checkpoint (`discard_suspended` in `server.py`).
- **A suspend can lose its marker.** Occasionally the sandbox ends up reading as finished
  (`SUCCEEDED`) rather than `SUSPENDED`, and `ResumeSandbox` refuses it, even though the checkpoint
  completed. The server then restores that checkpoint into a new sandbox: the same processes, memory
  and files, under a new sandbox id. The page says when that happens.

## Files

| File | Role |
| --- | --- |
| `deploy.py` | Builds the server image (with ghostty-web fetched from npm) and deploys it as a `chalkcompute.ScalingGroup`. |
| `server.py` | FastAPI app: `GET /` serves the page, `WS /ws` hands out a sandbox, relays its tmux session over a PTY, and suspends and resumes it on request. |
| `index.html` | The terminal page. Add `?embed=1` to drop the title when you put it in an iframe. |

## Run it

You need [`uv`](https://docs.astral.sh/uv/) and a Chalk environment with sandboxes and sandbox
snapshots enabled, since a suspend is a checkpoint. Checkpoints need host-backed sandboxes
(`ComputeClass.HOST`). Credentials come from `chalk config` in the current directory. You can also
set `CHALK_API_SERVER`, `CHALK_CLIENT_ID`, `CHALK_CLIENT_SECRET` and `CHALK_ENVIRONMENT_ID` yourself.

```bash
./deploy.py
```

Open the printed `web_url`, press **Launch sandbox**, start something, and press **Suspend**.

The deployed server creates, suspends, and resumes sandboxes with its own workload identity
(`chalk_identity=True`), so no client secret is baked into the image.

## Suspending from your own code

The released `chalkcompute` SDK does not wrap these calls yet, so `server.py` makes them directly
over Connect's JSON protocol, authenticated with the SDK's own credentials:

1. `chalk.sandbox.v2.SandboxService/SuspendSandbox` with `{"id": ...}` checkpoints the running
   sandbox and stops it. `GetSandbox` then reports `SANDBOX_STATUS_SUSPENDED`.
2. `chalk.sandbox.v2.SandboxService/ResumeSandbox` with `{"id": ...}` restores it under the same
   id, on whichever host has room. Wait for `SANDBOX_STATUS_RUNNING`.

See `SuspendClient` in `server.py`. After a resume, the server deletes the checkpoint it resumed
from.

## How a visitor's sandbox is locked down

- **Kernel:** `KernelPolicy.RESTRICTED` runs the sandbox under gVisor.
- **Network:** no `network_policy`, which means no egress at all, not even DNS.
- **Credentials:** none. The sandbox gets no workload identity and no secrets.
- **Size:** half a CPU and 1 GiB of memory by default.
- **Lifetime:** every sandbox has a hard `lifetime` as a backstop.
- **Single use:** a sandbox serves one visitor and is deleted when they are done.

## Abuse limits

All of these are environment variables on the ScalingGroup.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TERMINAL_MAX_RUNNING` | `8` | Running sandboxes across all visitors. |
| `TERMINAL_MAX_SUSPENDED` | `24` | Suspended sandboxes kept at once. |
| `TERMINAL_RUNNING_SECONDS` | `300` | Running time per visitor. Time spent suspended doesn't count. |
| `TERMINAL_SUSPENDED_SECONDS` | `900` | A sandbox suspended this long is deleted, along with its checkpoint. |
| `TERMINAL_MAX_SUSPENDS` | `10` | Suspends per visit. |
| `TERMINAL_IDLE_SECONDS` | `120` | A running terminal with no keystrokes for this long is closed. |
| `TERMINAL_STARTS_PER_MINUTE` | `20` | Sandbox starts and resumes across all visitors. |
| `TERMINAL_PER_IP_PER_HOUR` | `10` | New sessions per client IP, plus one live session per IP. |
| `TERMINAL_WARM_POOL` | `2` | Started sandboxes kept ready, so a visitor never waits for a cold start. |
| `TERMINAL_POOL_MAX_AGE_SECONDS` | `600` | A pooled sandbox nobody claimed is recycled after this long. |
| `TERMINAL_SANDBOX_CPU` / `TERMINAL_SANDBOX_MEMORY` | `500m` / `1Gi` | Per-sandbox resources. |

Closing the page while suspended keeps the sandbox for `TERMINAL_SUSPENDED_SECONDS`, and the page
offers to resume it when the visitor comes back. Closing it while running deletes the sandbox.
Per-IP limits apply only when the server can see the visitor's public address.

## WebSocket protocol

Open a terminal at `/ws?cols=<n>&rows=<n>`, or at `/ws?cols=<n>&rows=<n>&resume=<token>` to resume
a sandbox suspended in an earlier visit.

| Direction | Message |
| --- | --- |
| client → server | `{"type": "input", "data": "<keystrokes>"}` |
| client → server | `{"type": "resize", "cols": 120, "rows": 40}` |
| client → server | `{"type": "suspend"}` or `{"type": "resume"}` |
| server → client | binary frames of raw PTY output |
| server → client | `{"type": "status", "message": "..."}` while the sandbox starts |
| server → client | `{"type": "ready", "expires_in": 300, "sandbox": "1a2b3c4d", "resumed": false}` |
| server → client | `{"type": "suspended", "token": "...", "suspend_ms": 1100, "memory_bytes": 23801856, "keep_seconds": 900}` |
| server → client | `{"type": "resumed", "resume_ms": 900, "cold_ms": 15300, "expires_in": 240, "sandbox": "1a2b3c4d", "new_id": false}` |
| server → client | `{"type": "action_failed", "message": "..."}` |
| server → client | `{"type": "busy" \| "limited", "message": "..."}`, then close |
| server → client | `{"type": "ended", "reason": "exited" \| "idle" \| "time_limit" \| "suspend_expired" \| "resume_expired" \| "error"}`, then close |

When embedded in an iframe, the page posts
`{"type": "chalk-demo-interaction", "action", "label"}` to its parent for `launch`, `suspend`,
`resume`, `resume_saved`, `action_failed` and `session_ended`. Nothing typed into the terminal is
ever sent.

## Clean up

```bash
uv run --with chalkcompute python -c \
  "from chalkcompute import ScalingGroup; ScalingGroup.from_name('sandbox-suspend').delete()"
```

The server deletes suspended sandboxes when it starts and when they expire. If it is gone for
good, discard any that remain by resuming and then terminating them.

## License

Apache-2.0. ghostty-web is MIT.
