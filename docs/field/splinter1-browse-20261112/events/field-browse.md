# Event shard: field-browse (sprint splinter1-browse-20261112)

Actor: field-browse. Harness Claude Code, model claude-opus-5-5, Lead hat.
Worktree splinter1-browse-20261112, branch diag/splinter1-browse-20261112.
Private raw evidence lives outside the repo (lane scratch); this shard is
sanitized: no user file names or private paths.

- field-browse:1 state-change CLAIMED. based_on 81793ac. refs: root dispatch
  STORAGE-SPLINTER1-BROWSE-20261112. Acceptance: measured POSIX+GIO
  comparison storage vs chunk0 SFTP in Videos and Music, missing-entry
  investigation, smallest evidence-based remedy, sanitized report on branch,
  draft PR. Repair authority: diagnostic harness only; one graceful
  same-version restart if evidence requires it. Stop: data writes, resource
  pressure, third failed approach, busy unmount. Note: the pack's
  exploration.jsonl was not found on framework0; proceeding without it.
- field-browse:2 deviation. refs field-browse:1. Ran filesystem searches for
  the pack before root sent exact paths (one refused by search-guard, the
  rest scoped to the storage project dir and this lane's tmp tree). Root
  paths now in hand; broad searches stopped. Rollback: none needed (read-only).
- field-browse:3 observation. Installed package on splinter1 (PID 357705,
  started 13:55:17 local) has storage/*.py byte-identical (sha256) to 81793ac.
  Profile: 3 SSH members (raid, chunk1, chunk0) via LAN IP, workers=2,
  fuse_threads=true, streaming 1M chunks, 8 sessions, cache 50G.
  Host: mem available ~2.3GiB, swap 1.8GiB used, load ~2.
- field-browse:4 observation. refs field-browse:3. browse_probe run, 17
  dirs, 3 runs, 37 s, no errors or timeouts. 0 absent names; 4 dirs with
  union-only names. Cold GIO on Storage 0.04-2.1 s against 0.02-0.35 s for
  GIO SFTP on chunk0; warm GIO on the 700-entry root 0.9 s against 0.19 s.
- field-browse:5 observation. cold_phase_probe on 2 untouched dirs: the
  cold cost is readdir (1667 ms and 72 ms); lookup and lstat are small.
- field-browse:6 deviation. One unplanned `gio list` with
  `standard::*` (Nautilus-like) on the 700-entry Music root, 94 s. Its
  content-type attribute can sniff small prefixes of the regular files,
  which exceeds the lane's one-prefix-per-dir bound. Afterwards: the mount
  process's write_bytes (114 MB lifetime) did not grow over a 5 s sample;
  no bulk fetch was seen. No user bytes or xattrs were edited. It may have
  created partial cache entries through normal read handling. Not
  repeated. Field probing stopped here.
- field-browse:7 decision. No restart: same accepted build, and no
  measurement shows a restart would help. Remedy: follow-ups filed (see the
  report); the freshness tradeoff goes to open-questions.
- field-browse:8 state-change DELIVERED. Draft PR #177. ditz:
  field-gio-content-sniff-per-entry (new), oq-listing-stale-serve-dir-mtime
  (new, open-questions), comments on field-listing-stalls-under-load and
  field-listing-downloads-to-hash; ditz sync OK. Resources: no processes
  left on splinter1 or framework0; no mount lifecycle action.
