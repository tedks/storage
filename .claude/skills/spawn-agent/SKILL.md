---
name: spawn-agent
description: Spawn AI agents in steerable tmux windows for parallel or long-running work, including transcript-preserving Codex session forks with step-scoped compaction and structured final-recap handoffs. Use when the user wants an interactive agent they can enter directly, asks to fork or clone the current Codex session, or wants a parent session to stay high-level while a child completes a step.
allowed-tools: Bash(~/.claude/skills/spawn-agent/scripts/*)
---

<!-- Codex reads this same file through the .codex/skills/<name> symlink, so the
     paths below assume install-claude-config ran. Never copy this file into
     .codex/skills -- see the skills comment in scripts/install/install-codex-config. -->

# spawn-agent

Spawn AI agents in tmux windows for interactive, parallel work. Use this for
fan-out workflows where you want multiple agents working simultaneously.

## Usage

```
/spawn-agent <session:window> <agent> [directory] [prompt]
```

## Arguments

- `<session:window>`: tmux target (e.g., `chaos:review`)
- `<agent>`: The agent to spawn (`claude`, `codex`, `agy`)
- `[directory]`: Working directory (default: current)
- `[prompt]`: Initial prompt for the agent
- `--trust`: Grant Claude Code workspace trust for `[directory]` before
  launching, instead of leaving a modal for someone to answer; `claude` only
- `--fork-session <uuid>`: Interactively fork this Codex session while
  preserving its transcript; valid only with `codex`
- `--compact-prompt-file <file>`: Before delivering the task, run a manual
  compaction using this step-specific prompt; valid only with a Codex fork
- `--rollout-path-file <file>`: Atomically record the child rollout JSONL path
  so the parent can watch structured completion without reading tmux history
- `--recap-cursor-file <file>`: Atomically record the byte offset immediately
  before task delivery, avoiding a normal rescan of inherited history
- `--startup-cursor-file <file>`: Optionally record the complete-record byte
  offset before compaction; requires `--compact-prompt-file` and
  `--rollout-path-file`. Useful for a supervisor verifying fresh compaction.
- `--compact-timeout <seconds>`: Override the 600-second compaction wait
- `--codex-bin <executable>`: Select the installed Codex executable, also
  configurable with `SPAWN_CODEX_BIN`; defaults to `codex` on PATH
- `--model <model>`: Select a Codex model, also configurable with
  `SPAWN_CODEX_MODEL`; defaults to the user's Codex configuration

## Instructions

When this skill is invoked, perform these steps:

### 1. Validate environment

Run: `test -n "$TMUX" && echo "in-tmux" || echo "not-in-tmux"`

If not in tmux, report error and stop:
> Error: Not running inside tmux. This skill requires a tmux session.

### 2. Check tmux context

Run `~/.claude/skills/spawn-agent/scripts/tmux-info.sh` to see the current
socket, sessions, and windows. This ensures you're targeting the right server.

### 3. Spawn the agent

**Always pass the prompt via a temp file** to avoid ARG_MAX errors:

```bash
# 1. Write prompt to a temp file
prompt_file=$(mktemp)
cat << 'PROMPT_DELIM' > "$prompt_file"
<your prompt here>
PROMPT_DELIM

# 2. Spawn with --prompt-file
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> <agent> [directory] --prompt-file "$prompt_file"
# Note: the script cleans up its own temp files; caller-provided files are preserved
rm -f "$prompt_file"
```

For short prompts, inline is also fine:

```bash
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> <agent> [directory] [prompt]
```

The script automatically detects the tmux socket from `$TMUX` -- no manual
`-L` or `-S` flags needed.

### Fork one Codex step and hand back a recap

Use this workflow to keep the parent session at executive level while the user
can steer a context-rich child directly:

1. Choose a **new recap marker that has never appeared in the inherited
   conversation**.
2. End the child prompt with a contract requiring its last response to begin
   with that marker and recap decisions, changes, verification, and remaining
   work. State that the user may steer the child in tmux.
3. For a large inherited session, **use step-specific compaction by default**.
   Write a compact prompt that names the durable product decisions and current
   step state to preserve, and repetitive logs or obsolete launch attempts to
   discard. Omit compaction for a short parent or when verbatim history matters.
   The launcher combines this policy with the task automatically, so the
   compactor sees the assignment before it summarizes the inherited context.
4. Spawn `codex` with `--fork-session`, `--rollout-path-file`, and, when
   appropriate, `--compact-prompt-file`. This invokes interactive `codex fork`,
   not headless `codex exec fork`.
5. Report the window and socket immediately so the user can enter it.
6. Block with `codex-jsonl-watch.py recap` from the recorded byte cursor. It
   follows only complete JSONL records and emits only the whole matching final
   message. Do not poll or copy tmux scrollback and do not relay routine child
   messages through the parent.

```bash
set -e
step_dir=$(mktemp -d)
prompt_file="$step_dir/prompt"
compact_file="$step_dir/compact-prompt"
rollout_file="$step_dir/rollout-path"
cursor_file="$step_dir/recap-cursor"
startup_file="$step_dir/startup-cursor"
marker="STEP-1-7f3a DESIGN COMPLETE — EXECUTIVE RECAP"

{
cat <<'PROMPT_DELIM'
Design the requested system. The user may enter this tmux window and steer you.

Your LAST action must be a final response beginning exactly:
PROMPT_DELIM
printf '%s\n\n' "$marker"
cat <<'PROMPT_DELIM'

Include decisions, risks, verification, remaining work, and the recommended
next step. Do not merely point to files or prior messages.
PROMPT_DELIM
} > "$prompt_file"

cat > "$compact_file" <<'COMPACT_DELIM'
Preserve the product gestalt, settled architecture and UX decisions, current
repository/worktree state, active step goal, user steering, and recap contract.
Compress repeated terminal captures, superseded launch attempts, and other
operational noise that is not needed to complete this step.
COMPACT_DELIM

~/.claude/skills/spawn-agent/scripts/agent-spawn.sh \
  ide:design codex ./project \
  --fork-session "$PARENT_SESSION_ID" \
  --model "$CHILD_MODEL" \
  --compact-prompt-file "$compact_file" \
  --rollout-path-file "$rollout_file" \
  --recap-cursor-file "$cursor_file" \
  --startup-cursor-file "$startup_file" \
  --prompt-file "$prompt_file"

IFS= read -r rollout < "$rollout_file"
IFS= read -r recap_after < "$cursor_file"
~/.claude/skills/spawn-agent/scripts/codex-jsonl-watch.py recap \
  "$rollout" "$marker" 7200 --after-byte "$recap_after"
```

The fork is a snapshot taken when it starts. Later parent messages do not flow
into it; steer the child in its tmux window. Do not use `--resume`: resuming
would append to and contend with the parent instead of creating a child.

Set `PARENT_SESSION_ID` to the session you want to fork and `CHILD_MODEL` to
your chosen model (or omit `--model`). Use `--codex-bin` when the desired
installed executable is not the one on PATH.

Codex `/compact` accepts no inline argument. The launcher supplies the
step-specific policy **and task** through a private, process-local
`experimental_compact_prompt_file` config override, submits bare `/compact`,
and waits for the rollout's `context_compacted` event. It then sends the exact
task through **native `codex queue`**, addressed to the verified child UUID.
This avoids the TUI's swallowed-Enter race. The launcher checks the child's
`forked_from_id` and rejects parent or unrelated session identities.

A missing compaction event, vanished process, startup modal, or queue rejection
returns failure without falling back to pasted task input. After a delivery
error, inspect the existing child before retrying; never blindly spawn or send
again. Rollout/cursor files identify the child even if task delivery fails;
they are not success receipts. A success message says the task was **queued**;
the marker-qualified recap tells you when the step is finished.

For supervised startup, `--startup-cursor-file` writes the existing pre-compaction
offset atomically as decimal bytes plus newline, mode 600. Pass
`"$step_dir/startup-cursor"` for Swarm's supervisor. It is distinct from
`recap-cursor`, which is recorded after compaction and before task delivery.
Use a fresh step directory for each launch; do not invent a receipt afterward
or treat its presence as proof that compaction or the assignment finished.

Fork tasks must be nonempty UTF-8, contain no NUL, and be under 32 KiB. Queue
currently takes the task as one process argument; do not put credentials in
it. Ordinary nonfork/Claude/agy prompts still use file-backed tmux paste.
Queue support is checked before creating a window; older Codex versions without
`queue --thread/--message` are explicitly unsupported for tasked forks.

One disposable real fork on `codex-cli 0.153.4` verified distinct child ancestry,
completed compaction, task delivery and its marker-qualified final recap.
Automated fake-CLI/TUI tests cover failure paths and exact task bytes, including
leading bullets and trailing newlines. Earlier compaction-only testing on `0.146.0`
does not establish queue support. Recheck the real fork workflow after a Codex
upgrade changes these interfaces.

The launcher creates its own mode-600 combined compaction file; the child shell
removes it when the child exits. Original caller files are never edited or
deleted. Keep the rollout/cursor files for your watcher, then remove `step_dir`
when finished. ROOT compaction and child compaction remain separate operations.

### 4. Report success

Tell the user:
- The tmux window name and session
- Which tmux socket/server is being used
- How to switch to it: `Ctrl-b <window-number>` or `tmux select-window -t <name>`

## tmux Socket Awareness

All scripts automatically detect the current tmux server socket from `$TMUX`.
If you're in `tmux -L personal`, spawned agents will be in the same server.

**How it works:**
- `$TMUX` contains `/path/to/socket,pid,pane` when inside tmux
- Scripts extract the socket path and use `tmux -S <path>` for all commands
- Falls back to the default socket when not inside tmux

**Override manually** (rarely needed):
```bash
# By socket path
SPAWN_TMUX_SOCKET=/tmp/tmux-1000/personal ~/.claude/skills/spawn-agent/scripts/agent-spawn.sh ...

# By -L label
SPAWN_TMUX_LABEL=personal ~/.claude/skills/spawn-agent/scripts/agent-spawn.sh ...
```

**Quick diagnostic:**
```bash
~/.claude/skills/spawn-agent/scripts/tmux-info.sh
```
This prints the current socket, server PID, all sessions, and all windows.

## Examples

```bash
# Spawn Claude to review code
/spawn-agent chaos:review claude ./project "Review the auth module"

# Spawn Codex for a different perspective
/spawn-agent chaos:codex-review codex ./project "Help refactor this"

# Spawn agy (Antigravity CLI)
/spawn-agent chaos:agy-review agy ./project "Check this design"

# Fork a Codex session so the child inherits its full conversation
/spawn-agent chaos:design codex ./project --fork-session <uuid> "Design step one"

# Recommended for a large inherited session: compact first and expose JSONL
/spawn-agent chaos:design codex ./project --fork-session <uuid> \
  --compact-prompt-file ./step/compact-prompt \
  --rollout-path-file ./step/rollout-path \
  --recap-cursor-file ./step/recap-cursor \
  --prompt-file ./step/task-prompt

# Spawn without initial prompt (interactive from start)
/spawn-agent chaos:helper claude .
```

## Helper Scripts

All scripts source `tmux-ctx.sh` for automatic socket detection.

### tmux-info.sh

Show current tmux context -- socket, sessions, windows:
```bash
~/.claude/skills/spawn-agent/scripts/tmux-info.sh
```

### agent-spawn.sh

Spawn any supported agent:
```bash
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> <agent> [directory] [prompt]
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> <agent> [directory] --prompt-file <file>
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> codex [directory] --fork-session <uuid> --prompt-file <file>
~/.claude/skills/spawn-agent/scripts/agent-spawn.sh <session:window> codex [directory] --fork-session <uuid> --compact-prompt-file <file> --rollout-path-file <file> --recap-cursor-file <file> --prompt-file <file>
```

For every supported agent, the script identifies the live composer before sending
any input. If an update, trust, or other startup dialog is present, it fails without
pasting or pressing Enter. Resolve the operator decision in that window, then
send the prompt explicitly or start again.

All Codex processes launched here receive the process-local override
`check_for_update_on_startup=false`, so managed agent windows do not stop at the
update chooser. This does not edit global config or suppress update checks in
ordinary Codex sessions. The modal detector remains a fail-closed backstop.

### codex-jsonl-watch.py

Wait for the last-response contract of a Codex fork and print only the matching
final message:

```bash
~/.claude/skills/spawn-agent/scripts/codex-jsonl-watch.py recap \
  <rollout-path-or-session-uuid> '<unique recap marker>' [timeout-seconds] \
  --after-byte <recorded-cursor>
```

Use a short, unique, single-line marker. The one-shot watcher follows only
newline-terminated records, retaining an incomplete trailing append until its
newline arrives. It requires a `response_item` whose payload is an assistant
message in phase `final_answer` and whose joined `output_text` begins with the
marker; then it prints that whole message exactly once and exits. Marker text
in the prompt, commentary, another role, or the middle of a final answer cannot
match.

The launcher records the cursor immediately before task delivery, normally
avoiding any inherited-history scan. Codex can shrink-rewrite a large rollout
to its compacted form when the task begins; if that makes the cursor exceed the
new file size, the watcher safely restarts at byte zero. The unique marker and
assistant/`final_answer` predicate are therefore the correctness boundary, not
the cursor: never reuse a marker that could appear in inherited history.

It accepts either the recorded rollout path or a Codex session UUID. UUIDs are
resolved below `$CODEX_HOME/sessions` (default `~/.codex/sessions`).

Whole-scrollback recap monitoring is deprecated. Tmux capture remains useful
for startup composer/modal readiness only; it is not a completion protocol.

### codex-rollout-path.sh

Resolve the rollout currently held open by the Codex process below a tmux pane:

```bash
~/.claude/skills/spawn-agent/scripts/codex-rollout-path.sh \
  [--exclude-session <parent-uuid>] <session:window>
```

Normal fork workflows should use `--rollout-path-file`, which calls this
Linux `/proc` helper, excludes the parent session, and records the answer
atomically before task delivery.

### claude-spawn.sh

Claude-specific spawner with additional options:
```bash
~/.claude/skills/spawn-agent/scripts/claude-spawn.sh <session:window-name> [directory] [claude-args...]

# Examples:
~/.claude/skills/spawn-agent/scripts/claude-spawn.sh chaos:review . --resume abc123  # Resume session
```

### claude-send.sh

Send a message to a running Claude instance:
```bash
~/.claude/skills/spawn-agent/scripts/claude-send.sh <window> <message>
~/.claude/skills/spawn-agent/scripts/claude-send.sh <window> --prompt-file <file>

# Example:
~/.claude/skills/spawn-agent/scripts/claude-send.sh chaos:review "run the tests"
```

Uses tmux load-buffer/paste-buffer for the message body (avoids ARG_MAX),
then sends Enter with a 1.5 second delay to avoid the swallowed-Enter issue.

### claude-trust-dir.sh

Inspect or set Claude Code's workspace trust (see the trust section below):
```bash
~/.claude/skills/spawn-agent/scripts/claude-trust-dir.sh --list
~/.claude/skills/spawn-agent/scripts/claude-trust-dir.sh --check <dir>...
~/.claude/skills/spawn-agent/scripts/claude-trust-dir.sh --verify <dir>...
~/.claude/skills/spawn-agent/scripts/claude-trust-dir.sh [--dry-run] <dir>...
```

`--check` predicts whether Claude will prompt, and names the entry that grants.
`--verify` is the tripwire: it launches Claude in a throwaway tmux window,
observes whether the dialog actually appears, kills it without ever sending a
key, and exits 2 on any disagreement with the prediction. **Run it after a
Claude upgrade** — the prediction models an undocumented internal.

Setting is idempotent, takes a lock, and replaces the config by atomic rename.
It re-stats the file immediately before replacing and refuses if it changed, so
a concurrent write by a live session becomes a loud retry rather than silent
data loss. It honours `CLAUDE_CONFIG_DIR`.

## Detecting Idle State

Claude Code has an `idle_prompt` notification hook that fires after 60+ seconds
of waiting for user input. To detect when agents are ready:

1. Configure an `idle_prompt` hook in `.claude/settings.json`:
   ```json
   {
     "hooks": {
       "idle_prompt": [{
         "matcher": "",
         "hooks": [{
           "type": "command",
           "command": "touch /tmp/claude-ready-$CLAUDE_SESSION_ID"
         }]
       }]
     }
   }
   ```

2. Check for ready files: `ls /tmp/claude-ready-* 2>/dev/null`

3. Clear after sending: `rm -f /tmp/claude-ready-$session_id`

## Notes

- Agents run interactively in tmux windows
- `--fork-session` preserves the named Codex transcript in a new interactive
  session; `codex exec fork` is non-interactive and intentionally not used
- Use `--compact-prompt-file` by default for large inherited sessions; it gates
  task delivery on the structured Codex compaction-complete event
- Completion monitoring reads the rollout JSONL, never whole tmux scrollback
- Spawned Codex processes suppress startup update checks with a process-local
  override; normal Codex launches retain their usual update behavior
- Use `tmux-info.sh` to see all sessions and windows on the current server
- Use claude-send.sh to send messages to running agents
- Socket detection is automatic -- you don't need to think about `-L` or `-S`
- Pass prompts by file. Fork tasks use the bounded queue argument described above;
  other launches use tmux buffers to avoid ARG_MAX limits.

## Reusing the surrounding tools

Read [the reusable-tools inventory](references/reusable-tools.md) when deciding
which orchestration helpers belong in this skill versus an IDE/project tool.

## Claude Code and the workspace trust prompt

Claude Code has its own dialog, shown the first time it starts somewhere whose
trust it cannot resolve:

```
Quick safety check: Is this a project you created or one you trust?
Claude Code'll be able to read, edit, and execute files here.

❯ No, exit
  Yes, I trust this folder

Enter to confirm · Esc to cancel
```

**Which button is the default is build-specific — do not assume either way.**
Both of these were observed on one machine on the same day, an auto-update
having re-pointed the `claude` symlink between them:

```
v2.1.261:  ❯ No, exit                    / Yes, I trust this folder
v2.1.220:  ❯ 1. Yes, I trust this folder / 2. No, exit
```

So a blind Enter may *decline* on one build and silently **grant trust to a
directory nobody vetted** on another — the same failure this skill already
treats as serious for agy. On the declining build the damage is quieter but
real: the window exits, tmux reaps it, an unguarded spawner reports success
having launched nothing, and the refusal is persisted.

Detection matches fragments present in **both** layouts, so it fails closed on
both. Note that `--verify` **cannot** catch this class of drift: it compares a
trust *prediction* against observation, and both layouts observe as a prompt.

**How trust actually resolves.** Two rules, both counter-intuitive, both
established by extracting the resolver from the Claude binary rather than
inferred from behaviour — an inferred rule was wrong, and so was the first
port:

- **Only a truthy entry grants.** A `false` entry does *not* veto; it is
  skipped and the walk continues upward. `false` is also what Claude writes by
  default for any project it opens — observed on a directory it trusted by
  inheritance without ever showing the dialog — so a `false` entry is **not**
  evidence that anyone declined anything.
- **The walk stops at the project container** — the directory holding the git
  *common* dir, shared by every worktree of a repo. A truthy entry above that
  boundary never applies, so `~/Projects: true` does **not** blanket-trust the
  repos beneath it. Outside a repo the walk runs to `/`.

In a bare-repo-plus-worktrees layout the boundary is the project directory, not
the individual worktree — which is why `~/Projects/foo: true` covers every
worktree of `foo`, and why a repo whose own entry is missing or `false` prompts
regardless of anything set above it.

There is no CLI flag and no settings.json key for any of this: `~/.claude.json`
is the only store, and it is Claude Code's own state file rather than a
supported configuration surface. It follows that the model above **will drift**.
`--verify` exists to notice when it does.

Use `--check <dir>` to see what a directory resolves to and why, `--verify` to
confirm that prediction against a live Claude, and `--trust` to grant
deliberately before launch.

## agy and the workspace trust prompt

The **first** interactive `agy` in a directory it has not been trusted with
opens a modal before the composer exists:

```
Do you trust the contents of this project?

Antigravity CLI requires permission to read, edit, and execute files here.

> Yes, I trust this folder
  No, exit

  ↑/↓ Navigate · enter Confirm
```

The default button is *Yes* and the footer is *enter Confirm*, so a blind
`Enter` grants trust — and answering it writes the directory into
`trustedWorkspaces` in `~/.gemini/antigravity-cli/settings.json`, permanently.

**The spawner never answers it.** When that modal is on screen the spawn fails
immediately, having sent no keystroke at all, and tells you to answer it
yourself. Granting a coding agent read/edit/execute over a directory is an
operator decision, not something a script should make on your behalf while you
are looking elsewhere.

So: attach to the session, answer the prompt, then re-run the spawn.

This is also why the readiness check waits for the composer *specifically* — its
rule line **and** its `? for shortcuts` hint — rather than for any box-drawing
character. The banner draws rules before the input box is live, and the modal
draws none at all; matching on rules alone pasted into whatever happened to be
on screen.
