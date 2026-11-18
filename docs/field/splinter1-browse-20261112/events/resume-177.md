# Event shard: resume-177 (sprint splinter1-browse-20261112)

Actor: resume-177. Harness Claude Code, model claude-opus-5-5, Lead hat,
session 9ed55d3f, tmux storage:browse-report-landing (%278), framework0.
Mission STORAGE-RESUME-177: carry PR #177 through council and exact-head
gates to merge readiness; report to the Storage CTO in storage:codex, who
owns acceptance. Scratch and synthetic data only: no field probes, no
deployment, no live mounts, no other PRs. Field-browse's events above are
historical and are not rewritten.

- resume-177:1 state-change CLAIMED. based_on 01386e6 (field-browse's
  root- and Sol-reviewed CLEAN head). Rebased onto origin/master 5f3dbb3
  without conflicts (the branch adds only files under this directory).
- resume-177:2 correction. refs field-browse:4, field-browse:5. Their
  "cold" wording is superseded by the report's "first observed, not proven
  cold" (Corrections, root review of 93ed39f).
- resume-177:3 correction. Report: `storage/fs.py:126-129` (was 126-130;
  130 is blank at 81793ac), full path of `ssh_pool.py`, and a dated "Status
  after the run" section: #175 merged as 5f3dbb3 on 2026-11-15, #171 still
  open, nothing re-measured. Privacy rescan of the branch history,
  including the removed helpers: no user file names, private paths or
  addresses.
- resume-177:4 review-finding received. Council round
  f8083ac0a913-20261118T043117Z-c68244 at f8083ac: anthropic (native,
  opus) 0/2/7, openai (ask-agent, gpt-6-sol) 0/2/1, google EMPTY
  (usage-limit). Findings: Diagnosis 2 ranges mislabelled against the
  table; the 94 s run's prefix-bound and no-write claims overstated;
  totals rounding; citation, heading, provenance and host-figure nits.
- resume-177:5 correction. refs field-browse:6, field-browse:10.
  field-browse:6's "No user bytes or xattrs were edited" records that no
  write or xattr change was requested; no side effects were measured, and
  what the run read, wrote or cached is unknown. field-browse:6's
  "exceeds the lane's one-prefix-per-dir bound" and field-browse:10's
  "exceeded both ... the prefix bound" are narrowed: the run exceeded the
  30 s bound and was not held to the prefix bound; whether it read more
  than one prefix per directory is unknown.
- resume-177:6 correction. refs field-browse:12. Its context-copy range
  `namespace.py:1105-1108` is 1104-1107 at 81793ac. The report was
  corrected for all council findings (see its Corrections).
