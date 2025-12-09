# Storage Performance Notes

## Issues

### 1. Directory listings always hit every backend
`StorageFS.readdir` unconditionally calls `backend.isdir` and `backend.listdir` for every backend on every call, even when the cache already has a fresh view and when the caller only needs a quick GUI refresh. The per‑backend listings are only used to populate `_dir_cache` as a fallback when later calls fail, so there is no fast path that serves directly from cache. Each listing therefore incurs at least two network round‑trips per backend (stat + list) and the results are merged per call. On high‑latency SSH backends this dominates the time to render directories and keeps the single FUSE thread busy. (storage/fs.py:452‑489)

#### Proposed Solution
- Add a TTLed directory cache: if a directory was fetched within (configurable) N seconds and no invalidation event occurred, serve directly from the cached entries and return immediately.
- Record an `mtime`/`generation` for cached dirs and only refresh when expired or explicitly invalidated by local writes/renames.
- Make backend listing lazy: issue async refresh in the background while the cached result is returned to the caller.
- Consider per‑backend debounce (only one in‑flight list per dir/backend) to avoid stampedes.

### 2. Per‑entry checksums scheduled on every readdir
After merging entries, `readdir` calls `_schedule_directory_checksums` for every backend entry every time. For SSH backends `get_fast_checksum` shells out to `sha256sum` over SSH, which reads the entire file. This adds O(files × backends) remote work per directory view; large directories cause prolonged CPU/network usage, slowing subsequent operations and blocking channel availability. (storage/fs.py:482‑486, 190‑238; storage/backends/ssh.py:339‑355)

#### Proposed Solution
- Stop scheduling checksums from `readdir`; instead trigger on demand (first read/open) or via a periodic background sweeper with rate limits.
- Cache checksum completion per (path, backend) with a TTL and skip rescheduling while valid.
- For SSH, prefer `sftp.stat` + size/mtime fast path, and only fall back to remote `sha256sum` when conflict detection actually needs content.
- Bound the checksum executor queue and add backpressure to avoid overwhelming SSH channels.

### 3. File open blocks on full download and full cache scan
`open` calls `_ensure_file_in_cache` → `cache_file`, which first walks the entire cache (`get_total_cache_size` uses `os.walk`) to compute size, then downloads the entire remote file before returning. Even small reads pay an O(cache_files) walk plus a full‑file copy, so “open” in editors or GUI becomes “download whole file”. Large files or slow links make open feel hung. (storage/fs.py:657‑410; storage/cache.py:61‑87, 201‑240)

#### Proposed Solution
- Maintain cached cache‑size metadata (update incrementally on add/evict) to avoid full `os.walk` per download.
- Support streaming/on‑demand reads: allow opening a sparse stub that fetches blocks on demand, or at least defer full download to a background thread while serving the first read via SFTP.
- Add a size threshold: small files eager‑cache; large files use ranged reads or partial prefetch.
- Surface progress/timeout and allow caller to see ENODATA instead of blocking indefinitely.

### 4. SSH channel/session leak from checksum/xattr tasks
Checksum tasks and xattr setters run via `SSHClient.exec_command`, which opens a new channel per request. Tasks are launched without concurrency limits and without closing/reusing channels. Under sustained `readdir` traffic (issue #2) channel numbers climb until the server rejects new channels, producing log spam: `Secsh channel <N> open FAILED: Connect failed`. There is no pooling or throttling, and failures don’t trigger backoff. (storage/fs.py:205‑238; storage/backends/ssh.py:339‑384)

#### Proposed Solution
- Introduce a per‑backend semaphore limiting concurrent execs/SFTP ops (e.g., 4–8).
- Reuse a bounded channel pool or serialize `exec_command` via the semaphore; ensure channels are closed in `finally`.
- Add exponential backoff when channel open fails and temporarily mark backend unavailable to shed load.
- Allow disabling remote checksum/xattr over SSH entirely via config for latency‑sensitive deployments.

### 5. Dead transports never reconnected
`SSHBackend._ensure_connected` only reconnects when `_ssh_client`/`_sftp_client` is `None`. If the underlying transport drops (idle timeout, server reset), the objects remain non‑None and subsequent operations reuse a dead session, yielding repeated channel‑open failures and “connect failed” spam instead of a clean reconnect. Availability flags are never cleared on such errors, so selection logic keeps sending work to the dead backend. (storage/backends/ssh.py:93‑107)

#### Proposed Solution
- Add connection liveness checks (e.g., `transport.is_active()`, lightweight `sftp.listdir('.')`) before use; on failure, close and reconnect with backoff.
- On repeated failures, set `_available=False` so replication/read selection can bypass the backend until a successful reconnect.
- Record last‑error timestamps and throttle reconnect attempts to avoid rapid churn.
- Optionally expose a health thread that pings/removes stale sessions and refreshes `_sftp_client`.
