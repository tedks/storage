# Storage Performance Notes

## Issues

### 1. Directory listings always hit every backend
`StorageFS.readdir` still walks every backend on every call. Even if cache data are present, we re‑`stat` and `listdir` each backend, then merge. That means two network round‑trips per backend per GUI refresh, keeps the single FUSE thread occupied, and makes “cached” directories feel uncached. We also have no notion of remote directory freshness beyond the last listing; mtime on the backend isn’t checked first, and we don’t emit invalidate notifications to the kernel to wake up watchers.

#### Proposed Solution
- Fast path from cache: if we have cached entries and they are younger than a configurable TTL **and** no local invalidation (mkdir/rmdir/rename/unlink on that path) has occurred, return immediately.
- In parallel, fire async per‑backend probes that fetch only directory `mtime` (e.g., `sftp.stat` on the dir or a lightweight `ls -ld --time-style=+%s`); if any backend reports newer mtime than cached, trigger a full `listdir` refresh for that backend and update cache.
- Emit change notifications to userspace via FUSE `notify_inval_entry` / `notify_inval_inode` after refreshing so GUI file managers update automatically; use per‑backend refresh completion to drive these.
- Allow parallel backend refreshes but collapse multiple in‑flight refreshes for the same (dir, backend) pair to a single request to avoid stampedes.

### 2. Per‑entry checksums scheduled on every readdir
Every `readdir` schedules checksum work for every entry on every backend. On SSH backends `get_fast_checksum` shells out to `sha256sum`, reading whole files. We do this even when nothing has changed, consuming bandwidth, CPU, and SSH channels. We also ignore the timestamps already stored in checksum xattrs, so we can’t skip work when the backend file is older than the cached checksum.

#### Proposed Solution
- Make checksum scheduling conditional: if cached checksum xattr timestamp is newer than remote dir/file mtime (from the lightweight probe above), skip scheduling.
- Move checksum work off `readdir`: queue on first open/read or via a low‑rate background sweeper. Keep a per‑(path, backend) TTL to prevent rescheduling while still fresh.
- Prefer metadata‑only validation: if size/mtime match and checksum timestamp ≥ remote mtime, trust it; only run `sha256sum` (or faster `b2sum` if available) when metadata differs or on conflict detection.
- Reuse SSH multiplexed sessions for checksum execs to avoid repeated handshakes (see issue 4) and ensure channels are closed; keep a small bounded queue to avoid unbounded channel creation while still allowing multiplexing.

### 3. File open blocks on full download and full cache scan
`open` currently walks the entire cache to compute size and then downloads the entire file before returning. The first read blocks on a full fetch even if the caller asked for a small prefix. On slow/remote links this feels like a hang, and the cache walk is O(total files) per miss.

#### Proposed Solution
- Maintain cache size accounting incrementally (update counters on add/evict) to avoid `os.walk` on every fetch.
- On first open, fetch only the requested range into cache via SFTP/SSH and return promptly; immediately kick off a background full‑file download to fill the cache, updating file as data arrive.
- Use a faster bulk transfer tool for the background fetch when possible: rsync over SSH with `--partial --inplace`, tar|ssh stream, or HPN‑SSH/parallelized copy on high‑latency links; fall back to tuned sftp (`-R`, `-B`) when rsync isn’t available.
- Emit inotify/FUSE notifications when the background download completes so readers can revalidate size and GUI progress bars can refresh.
- Make eager full‑download optional/size‑thresholded (e.g., eager for <N MB; staged for larger files).

### 4. SSH channel/session leak from checksum/xattr tasks
Checksum/xattr tasks spawn `exec_command` per item, each opening a new SSH channel. Because we schedule many (issue 2), channel numbers climb until the server refuses new channels, producing the “Secsh channel … open FAILED” spam. We also pay full handshake cost per command.

#### Proposed Solution
- Enable OpenSSH ControlMaster/ControlPath to multiplex channels over a single TCP connection per backend, avoiding repeated handshakes while still allowing many concurrent channels.
- Implement a small channel‑use queue to ensure channels are closed in `finally` and to prevent leak from runaway task creation while still honoring multiplexing (light bound, not heavy throttling).
- Keep xattr reads/writes over the multiplexed session; ensure `set_xattr`/`get_xattr` use the shared control socket and are exception‑safe.
- Optionally allow disabling remote checksum/xattr over SSH for latency‑sensitive deployments.

### 5. Dead transports never reconnected
`SSHBackend._ensure_connected` only reconnects when `_ssh_client`/`_sftp_client` is `None`. If the underlying transport drops (idle timeout, server reset), the objects remain non‑None and subsequent operations reuse a dead session, yielding repeated channel‑open failures and “connect failed” spam instead of a clean reconnect. Availability flags are never cleared on such errors, so selection logic keeps sending work to the dead backend. (storage/backends/ssh.py:93‑107)

#### Proposed Solution
- Add liveness checks (e.g., `transport.is_active()` plus a lightweight `sftp.listdir('.')`) before use; if dead, close and reconnect.
- Mark backend unavailable on consecutive failures so selection can bypass it until a successful reconnect clears the flag.
- Record last‑error timestamps and throttle reconnect attempts to avoid churn.
- Optionally run a health thread to refresh dead sessions proactively.
