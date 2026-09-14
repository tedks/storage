#!/bin/bash
# Hermetic tests for interactive spawning, compaction, and JSONL recap watching.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPAWN="$SCRIPT_DIR/agent-spawn.sh"
RESOLVE="$SCRIPT_DIR/codex-rollout-path.sh"
WATCH="$SCRIPT_DIR/codex-jsonl-watch.py"
TEST_DIR=$(mktemp -d /tmp/test-agent-spawn.XXXXXX)
cleanup() {
    if [[ -f "$TEST_DIR/compact-paths" ]]; then
        while IFS= read -r path; do
            [[ "$path" == /tmp/spawn-agent-compact.* ]] && rm -f -- "$path"
        done < "$TEST_DIR/compact-paths"
    fi
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT
export FAKE_QUEUED_TASK="$TEST_DIR/queued-task"
export FAKE_QUEUE_THREAD="$TEST_DIR/queue-thread"
export FAKE_COMPACT_COPY="$TEST_DIR/compact-copy"
export FAKE_COMPACT_PATHS="$TEST_DIR/compact-paths"

mkdir -p "$TEST_DIR/bin" "$TEST_DIR/repo with spaces"
PROMPT="$TEST_DIR/prompt"
COMPACT_PROMPT="$TEST_DIR/repo with spaces/compact prompt.txt"
LOG="$TEST_DIR/tmux.log"
LAUNCH="$TEST_DIR/launch"
STDERR="$TEST_DIR/stderr"
LAST_BUFFER="$TEST_DIR/last-buffer"
COMPOSER="$TEST_DIR/composer"
ROLLOUT="$TEST_DIR/.codex/sessions/2026/09/05/rollout-2026-09-05T03-00-00-01a07050-1111-7222-8333-444444444444.jsonl"
PARENT_ROLLOUT="$TEST_DIR/.codex/sessions/2026/09/05/rollout-2026-09-05T02-00-00-01a0702a-7b5e-71e0-bb62-287d55a7f9ba.jsonl"
PROC_ROOT="$TEST_DIR/proc"

printf '%s\n' 'Design the step, then recap it.' > "$PROMPT"
printf '%s\n' 'Keep decisions and contracts; discard repetitive terminal output.' > "$COMPACT_PROMPT"
mkdir -p "$(dirname "$ROLLOUT")" \
    "$PROC_ROOT/4242/task/4242" "$PROC_ROOT/4242/fd" \
    "$PROC_ROOT/4243/task/4243" "$PROC_ROOT/4243/fd" \
    "$PROC_ROOT/4244/task/4244" "$PROC_ROOT/4244/fd"
printf '%s\n' '{"type":"session_meta","payload":{"id":"01a07050-1111-7222-8333-444444444444","forked_from_id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba"}}' > "$ROLLOUT"
printf '%s\n' '{"type":"session_meta","payload":{"id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba"}}' > "$PARENT_ROLLOUT"
printf '%s\n' '4243' > "$PROC_ROOT/4242/task/4242/children"
printf '%s\n' '4244' > "$PROC_ROOT/4243/task/4243/children"
: > "$PROC_ROOT/4244/task/4244/children"
ln -s "$PARENT_ROLLOUT" "$PROC_ROOT/4243/fd/6"
ln -s "$ROLLOUT" "$PROC_ROOT/4244/fd/7"

cat > "$TEST_DIR/bin/tmux" <<'STUB'
#!/bin/bash
set -eu
printf '%q ' "$@" >> "$FAKE_TMUX_LOG"
printf '\n' >> "$FAKE_TMUX_LOG"
case "${1:-}" in
    new-window)
        printf '%s\n' "${!#}" > "$FAKE_LAUNCH_FILE"
        python3 - "${!#}" <<'PY'
import json, os, shlex
from pathlib import Path
for arg in shlex.split(__import__('sys').argv[1]):
    if arg.startswith('experimental_compact_prompt_file='):
        path = json.loads(arg.split('=', 1)[1])
        Path(os.environ['FAKE_COMPACT_COPY']).write_bytes(Path(path).read_bytes())
        with open(os.environ['FAKE_COMPACT_PATHS'], 'a') as out:
            out.write(path + '\n')
PY
        if [[ -n "${FAKE_MUTATE_PROMPT:-}" ]]; then
            printf '%s' 'A changed caller task must not be delivered' > "$FAKE_MUTATE_PROMPT"
        fi
        ;;
    display-message)
        case " $* " in
            *' -t test:department '*|*' -t test:relative '*) ;;
            *) exit 64 ;;
        esac
        printf '%s\n' '4242'
        ;;
    load-buffer)
        printf '%s' "$(<"${!#}")" > "$FAKE_LAST_BUFFER"
        ;;
    paste-buffer)
        if [[ "${FAKE_PASTE_MODE:-normal}" != swallow ]]; then
            printf '%s' "$(<"$FAKE_LAST_BUFFER")" > "$FAKE_COMPOSER"
        fi
        ;;
    send-keys)
        if [[ "$(<"$FAKE_COMPOSER")" == '/compact' && \
              "${FAKE_COMPACTION_MODE:-complete}" == complete ]]; then
            if [[ -n "${FAKE_STARTUP_RECEIPT:-}" ]]; then
                [[ "$(<"$FAKE_STARTUP_RECEIPT")" -eq $(wc -c < "$FAKE_ROLLOUT") ]]
            fi
            printf '%s\n' '{"type":"event_msg","payload":{"type":"context_compacted"}}' >> "$FAKE_ROLLOUT"
        fi
        : > "$FAKE_COMPOSER"
        ;;
    has-session|new-session|delete-buffer)
        ;;
    capture-pane)
        composer='Ask Codex to do anything'
        if [[ -s "$FAKE_COMPOSER" ]]; then
            composer="$(<"$FAKE_COMPOSER")"
        fi
        case "${FAKE_PANE_MODE:-codex-ready}" in
            codex-ready)
                printf '%s\n' '╭ OpenAI Codex ╮' "› $composer" 'gpt-5.6-sol · Context 82% left'
                ;;
            codex-ready-with-old-dialog)
                printf '%s\n' '1. Update now' 'Press enter to continue' "› $composer" 'gpt-5.6-sol · Context 82% left'
                ;;
            codex-working-with-composer)
                printf '%s\n' "› $composer" 'Working (12s • esc to interrupt)' 'gpt-5.6-sol · Context 82% left'
                ;;
            update-over-old-composer)
                printf '%s\n' '› Ask Codex to do anything' 'gpt-5.6-sol · Context 82% left' '✨ Update available!' '› 1. Update now' 'Press enter to continue'
                ;;
            update)
                printf '%s\n' '✨ Update available!' '› 1. Update now' '  2. Skip' 'Press enter to continue'
                ;;
            trust)
                printf '%s\n' 'Do you trust the contents of this project?' '> Yes, I trust this folder' 'enter Confirm'
                ;;
            agy-ready)
                printf '%s\n' '────────────────' 'Type your message' '? for shortcuts'
                ;;
            claude-ready)
                printf '%s\n' '──────────────────────' '❯ ' '──────────────────────' '  bypass permissions on'
                ;;
            # Claude's trust dialog at a comfortable width.
            claude-trust)
                printf '%s\n' 'Quick safety check: Is this a project you created or one you trust?' \
                    "Claude Code'll be able to read, edit, and execute files here." \
                    '❯ No, exit' '  Yes, I trust this folder' 'Enter to confirm · Esc to cancel'
                ;;
            # The same dialog re-flowed to 33 columns, which is what tmux gives
            # when a small client attaches to a shared session. The footer wraps,
            # so "Enter to confirm" is no longer the last line and the long body
            # sentence is split -- the two conditions that together let a wrapped
            # dialog read as a ready composer and get an Enter sent into it.
            claude-trust-wrapped)
                printf '%s\n' 'Quick safety check: Is this a' 'project you created or one' 'you trust?' \
                    '❯ No, exit' '  Yes, I trust this folder' '' 'Enter to confirm · Esc to' 'cancel'
                ;;
            agy-trust-over-composer)
                printf '%s\n' '────────────────' '? for shortcuts' 'Do you trust the contents of this project?' '> Yes, I trust this folder' 'enter Confirm'
                ;;
            vanished)
                exit 1
                ;;
        esac
        ;;
esac
STUB

cat > "$TEST_DIR/bin/sleep" <<'STUB'
#!/bin/bash
exit 0
STUB
cat > "$TEST_DIR/bin/codex" <<'STUB'
#!/bin/bash
set -eu
if [[ "$1" == queue && "$2" == --help ]]; then
    [[ "${FAKE_QUEUE_MODE:-available}" != unavailable ]] || exit 2
    printf '%s\n' 'Usage: codex queue --thread <THREAD> --message <TEXT>'
elif [[ "$1" == queue ]]; then
    [[ "${FAKE_QUEUE_MODE:-available}" != rejected ]] || exit 1
    [[ "$2" == --thread && "$4" == --message=* && $# -eq 4 ]]
    printf '%s' "$3" > "$FAKE_QUEUE_THREAD"
    printf '%s' "${4#--message=}" > "$FAKE_QUEUED_TASK"
else
    printf '%s\n' "$@" > "$FAKE_CODEX_ARGS"
fi
STUB
chmod +x "$TEST_DIR/bin/tmux" "$TEST_DIR/bin/sleep" "$TEST_DIR/bin/codex"

run_spawn() {
    local mode="$1"
    shift
    : > "$COMPOSER"
    env -u TMUX \
        PATH="$TEST_DIR/bin:$PATH" \
        FAKE_TMUX_LOG="$LOG" \
        FAKE_LAUNCH_FILE="$LAUNCH" \
        FAKE_LAST_BUFFER="$LAST_BUFFER" \
        FAKE_COMPOSER="$COMPOSER" \
        FAKE_PANE_MODE="$mode" \
        FAKE_ROLLOUT="$ROLLOUT" \
        FAKE_COMPACTION_MODE="${FAKE_COMPACTION_MODE:-complete}" \
        FAKE_PASTE_MODE="${FAKE_PASTE_MODE:-normal}" \
        FAKE_QUEUE_MODE="${FAKE_QUEUE_MODE:-available}" \
        SPAWN_PROC_ROOT="$PROC_ROOT" \
        "$SPAWN" "test:department" codex "$TEST_DIR/repo with spaces" \
        --fork-session 01a0702a-7b5e-71e0-bb62-287d55a7f9ba \
        --prompt-file "$PROMPT" "$@"
}

run_agy() {
    : > "$COMPOSER"
    env -u TMUX \
        PATH="$TEST_DIR/bin:$PATH" \
        FAKE_TMUX_LOG="$LOG" \
        FAKE_LAUNCH_FILE="$LAUNCH" \
        FAKE_LAST_BUFFER="$LAST_BUFFER" \
        FAKE_COMPOSER="$COMPOSER" \
        FAKE_PANE_MODE="$1" \
        FAKE_ROLLOUT="$ROLLOUT" \
        "$SPAWN" test:department agy "$TEST_DIR/repo with spaces" \
        --prompt-file "$PROMPT"
}

run_claude() {
    : > "$COMPOSER"
    env -u TMUX \
        PATH="$TEST_DIR/bin:$PATH" \
        FAKE_TMUX_LOG="$LOG" \
        FAKE_LAUNCH_FILE="$LAUNCH" \
        FAKE_LAST_BUFFER="$LAST_BUFFER" \
        FAKE_COMPOSER="$COMPOSER" \
        FAKE_PANE_MODE="$1" \
        FAKE_ROLLOUT="$ROLLOUT" \
        "$SPAWN" test:department claude "$TEST_DIR/repo with spaces" \
        --prompt-file "$PROMPT"
}

assert_no_input() {
    if grep -Eq '^(load-buffer|paste-buffer|send-keys)' "$LOG"; then
        echo "FAIL: unsafe input reached tmux" >&2
        cat "$LOG" >&2
        exit 1
    fi
}

# A fork queues the exact task only after observing its live composer and identity.
: > "$LOG"
run_spawn codex-ready >/dev/null
grep -Fq 'codex fork ' "$LAUNCH"
grep -Fq -- '-C ' "$LAUNCH"
grep -Fq 'check_for_update_on_startup=false' "$LAUNCH"
grep -Fq '01a0702a-7b5e-71e0-bb62-287d55a7f9ba' "$LAUNCH"
grep -Fq 'repo\ with\ spaces' "$LAUNCH"
assert_no_input
cmp "$PROMPT" "$FAKE_QUEUED_TASK"
[[ "$(<"$FAKE_QUEUE_THREAD")" == 01a07050-1111-7222-8333-444444444444 ]]

# Dialog text in restored history does not mask a valid composer.
: > "$LOG"
run_spawn codex-ready-with-old-dialog >/dev/null
assert_no_input
cmp "$PROMPT" "$FAKE_QUEUED_TASK"

# The update suppression is scoped to every Codex spawned here, not only
# forks. The modal detector remains covered below as a backstop.
: > "$LOG"
env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" \
    FAKE_LAUNCH_FILE="$LAUNCH" FAKE_LAST_BUFFER="$LAST_BUFFER" \
    FAKE_COMPOSER="$COMPOSER" \
    FAKE_PANE_MODE=codex-ready FAKE_ROLLOUT="$ROLLOUT" \
    "$SPAWN" test:ordinary codex "$TEST_DIR/repo with spaces" \
    --prompt-file "$PROMPT" >/dev/null
grep -Fq 'check_for_update_on_startup=false' "$LAUNCH"

# A live update chooser and trust prompt fail closed: no paste, no Enter.
for mode in update update-over-old-composer trust; do
    : > "$LOG"
    if run_spawn "$mode" >/dev/null 2>"$STDERR"; then
        echo "FAIL: $mode modal was treated as a composer" >&2
        exit 1
    fi
    grep -Fq 'Nothing was pasted, and no key was sent' "$STDERR"
    assert_no_input
done

# A pinned composer/footer is not ready while Codex still reports an active
# turn. This prevents prompt delivery into the TUI before the turn settles.
: > "$LOG"
if run_spawn codex-working-with-composer >/dev/null 2>"$STDERR"; then
    echo 'FAIL: an actively working Codex pane was treated as ready' >&2
    exit 1
fi
grep -Fq 'input box did not appear' "$STDERR"
assert_no_input

# agy trust is checked before its live-frame composer hints, because the modal
# can be painted over an already-rendered frame.
: > "$LOG"
run_agy agy-ready >/dev/null
grep -q '^paste-buffer ' "$LOG"
: > "$LOG"
if run_agy agy-trust-over-composer >/dev/null 2>"$STDERR"; then
    echo 'FAIL: agy trust modal was treated as a composer' >&2
    exit 1
fi
grep -Fq 'permission decision' "$STDERR"
assert_no_input

# A disappearing process fails without input, and --fork-session cannot be
# silently applied to a different provider.
: > "$LOG"
if run_spawn vanished >/dev/null 2>"$STDERR"; then
    echo 'FAIL: vanished window reported success' >&2
    exit 1
fi
grep -Fq 'exited before the prompt could be sent' "$STDERR"
assert_no_input

: > "$LOG"
if env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" FAKE_LAUNCH_FILE="$LAUNCH" \
    "$SPAWN" test:department claude . \
    --fork-session 01a0702a-7b5e-71e0-bb62-287d55a7f9ba \
    --prompt-file "$PROMPT" >/dev/null 2>"$STDERR"; then
    echo 'FAIL: non-Codex fork reported success' >&2
    exit 1
fi
grep -Fq 'supported only for codex' "$STDERR"
[[ ! -s "$LOG" ]]

# The rollout resolver follows the pane's process tree instead of guessing
# which simultaneously-created fork file is newest.
resolved=$(env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" \
    FAKE_LAUNCH_FILE="$LAUNCH" FAKE_LAST_BUFFER="$LAST_BUFFER" \
    SPAWN_PROC_ROOT="$PROC_ROOT" "$RESOLVE" --exclude-session \
    01a0702a-7b5e-71e0-bb62-287d55a7f9ba test:department)
[[ "$resolved" == "$ROLLOUT" ]]

# Relative directories are canonicalized once, then reused for both tmux -c
# and Codex -C instead of being resolved a second time inside the new pane.
: > "$LOG"
(
    cd "$TEST_DIR"
    env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" \
        FAKE_LAUNCH_FILE="$LAUNCH" FAKE_LAST_BUFFER="$LAST_BUFFER" \
        FAKE_COMPOSER="$COMPOSER" \
        FAKE_PANE_MODE=codex-ready FAKE_ROLLOUT="$ROLLOUT" \
        SPAWN_PROC_ROOT="$PROC_ROOT" \
        "$SPAWN" test:relative codex 'repo with spaces' \
        --fork-session 01a0702a-7b5e-71e0-bb62-287d55a7f9ba \
        --prompt-file "$PROMPT" >/dev/null
)
grep -Fq "$TEST_DIR/repo\\ with\\ spaces" "$LAUNCH"
grep -Fq "$TEST_DIR/repo\\ with\\ spaces" "$LOG"

# Optional launch compaction passes only a prompt path to Codex, submits bare
# /compact, observes a structured completion event, and only then loads the
# task. It also records the exact child rollout atomically for the parent.
: > "$LOG"
ROLLOUT_OUT="$TEST_DIR/child-rollout"
CURSOR_OUT="$TEST_DIR/recap-cursor"
STARTUP_OUT="$TEST_DIR/startup-cursor"
startup_before=$("$WATCH" cursor "$ROLLOUT")
FAKE_STARTUP_RECEIPT="$STARTUP_OUT" run_spawn codex-ready \
    --compact-prompt-file "$COMPACT_PROMPT" \
    --rollout-path-file "$ROLLOUT_OUT" \
    --startup-cursor-file "$STARTUP_OUT" \
    --recap-cursor-file "$CURSOR_OUT" >/dev/null
grep -Fq 'experimental_compact_prompt_file' "$LAUNCH"
grep -Fq 'check_for_update_on_startup=false' "$LAUNCH"
if grep -Fq 'Keep decisions and contracts' "$LAUNCH"; then
    echo 'FAIL: compaction prompt body leaked into argv' >&2
    exit 1
fi
FAKE_CODEX_ARGS="$TEST_DIR/codex.args" PATH="$TEST_DIR/bin:$PATH" \
    bash -c "$(<"$LAUNCH")"
grep -Fq 'experimental_compact_prompt_file=' "$TEST_DIR/codex.args"
grep -Fq 'Keep decisions and contracts' "$FAKE_COMPACT_COPY"
grep -Fq 'Design the step, then recap it.' "$FAKE_COMPACT_COPY"
# Running the child command through its shell cleans its generated prompt.
[[ ! -e "$(tail -n 1 "$FAKE_COMPACT_PATHS")" ]]
mapfile -t codex_args < "$TEST_DIR/codex.args"
for index in "${!codex_args[@]}"; do
    if [[ "${codex_args[index]}" == experimental_compact_prompt_file=* ]]; then
        (( index > 0 )) && [[ "${codex_args[index - 1]}" == '-c' ]]
    fi
done
[[ "$(<"$ROLLOUT_OUT")" == "$ROLLOUT" ]]
[[ "$(<"$CURSOR_OUT")" -eq $(wc -c < "$ROLLOUT") ]]
[[ "$(<"$STARTUP_OUT")" -eq "$startup_before" ]]
[[ "$(<"$STARTUP_OUT")" -lt "$(<"$CURSOR_OUT")" ]]
[[ "$(stat -c %a "$STARTUP_OUT")" == 600 ]]
mapfile -t loaded_buffers < <(awk '$1 == "load-buffer" { print $3 }' "$LOG")
[[ ${#loaded_buffers[@]} -eq 1 ]]
[[ "${loaded_buffers[0]}" == spawn-agent-compact-* ]]
cmp "$PROMPT" "$FAKE_QUEUED_TASK"

# The optional startup receipt needs actual fork compaction and a distinct file.
for args in \
    '--startup-cursor-file only' \
    '--startup-cursor-file'; do
    : > "$LOG"
    read -r -a invalid_args <<< "$args"
    if run_spawn codex-ready "${invalid_args[@]}" >/dev/null 2>"$STDERR"; then exit 1; fi
    [[ ! -s "$LOG" ]]
done
: > "$LOG"
if run_spawn codex-ready --compact-prompt-file "$COMPACT_PROMPT" \
        --rollout-path-file "$ROLLOUT_OUT" --startup-cursor-file "$ROLLOUT_OUT" \
        >/dev/null 2>"$STDERR"; then exit 1; fi
[[ ! -s "$LOG" ]]

# If structured compaction completion never arrives, fail with the task still
# unsent. The only loaded input is the explicitly-authorized /compact command.
: > "$LOG"
printf '%s\n' '{"type":"session_meta","payload":{"id":"01a07050-1111-7222-8333-444444444444","forked_from_id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba"}}' > "$ROLLOUT"
: > "$FAKE_QUEUED_TASK"
if FAKE_COMPACTION_MODE=none run_spawn codex-ready \
    --compact-prompt-file "$COMPACT_PROMPT" --compact-timeout 1 \
    >/dev/null 2>"$STDERR"; then
    echo 'FAIL: missing compaction completion reported success' >&2
    exit 1
fi
grep -Fq 'task prompt not sent' "$STDERR"
[[ ! -s "$FAKE_QUEUED_TASK" ]]
[[ $(grep -c '^load-buffer ' "$LOG") -eq 1 ]]
if grep -Fq "$PROMPT" "$LOG"; then
    echo 'FAIL: task was loaded before compaction completed' >&2
    exit 1
fi

# Loading the tmux buffer is not proof that paste reached the composer. A
# swallowed paste must never cause the later Enter to submit stale text.
: > "$LOG"
printf '%s\n' '{"type":"session_meta","payload":{"id":"01a07050-1111-7222-8333-444444444444","forked_from_id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba"}}' > "$ROLLOUT"
if FAKE_PASTE_MODE=swallow run_spawn codex-ready \
    --compact-prompt-file "$COMPACT_PROMPT" \
    >/dev/null 2>"$STDERR"; then
    echo 'FAIL: swallowed /compact paste reported success' >&2
    exit 1
fi
grep -Fq 'does not contain exactly /compact' "$STDERR"
[[ $(grep -c '^load-buffer ' "$LOG") -eq 1 ]]
if grep -q '^send-keys ' "$LOG"; then
    echo 'FAIL: Enter was sent after /compact failed to reach the composer' >&2
    exit 1
fi

# Queue failures never become a pasted fallback or a success report.
for mode in unavailable rejected; do
    : > "$LOG"
    : > "$FAKE_QUEUED_TASK"
    if FAKE_QUEUE_MODE="$mode" run_spawn codex-ready >"$TEST_DIR/stdout" 2>"$STDERR"; then
        echo "FAIL: $mode queue reported success" >&2; exit 1
    fi
    assert_no_input
    [[ ! -s "$FAKE_QUEUED_TASK" ]]
    ! grep -q '^Spawned ' "$TEST_DIR/stdout"
    if [[ "$mode" == unavailable ]]; then [[ ! -s "$LOG" ]]; fi
done

: > "$LOG"
: > "$FAKE_QUEUED_TASK"
if FAKE_QUEUE_MODE=rejected run_spawn codex-ready --compact-prompt-file "$COMPACT_PROMPT" \
        >"$TEST_DIR/stdout" 2>"$STDERR"; then exit 1; fi
[[ ! -s "$FAKE_QUEUED_TASK" ]]
[[ $(grep -c '^load-buffer ' "$LOG") -eq 1 ]]
! grep -q '^Spawned ' "$TEST_DIR/stdout"

# Reject parent identity, unrelated forks, and partial metadata before sending.
cp "$ROLLOUT" "$TEST_DIR/saved-rollout"
for meta in \
    '{"id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba","forked_from_id":"01a0702a-7b5e-71e0-bb62-287d55a7f9ba"}' \
    '{"id":"01a07050-1111-7222-8333-444444444444","forked_from_id":"00000000-0000-0000-0000-000000000000"}'; do
    printf '{"type":"session_meta","payload":%s}\n' "$meta" > "$ROLLOUT"
    : > "$LOG"
    if run_spawn codex-ready >/dev/null 2>"$STDERR"; then
        echo 'FAIL: unrelated or parent session accepted' >&2; exit 1
    fi
    assert_no_input
done
printf '{"type":"session_meta"' > "$ROLLOUT"
if run_spawn codex-ready >/dev/null 2>"$STDERR"; then exit 1; fi
cp "$TEST_DIR/saved-rollout" "$ROLLOUT"

# Exact multiline UTF-8 bytes and trailing newlines survive argv delivery.
cp "$PROMPT" "$TEST_DIR/saved-prompt"
printf '%s\n\n' '- Review quotes " and shell $() `text`; café' 'second line' > "$PROMPT"
run_spawn codex-ready --codex-bin "$TEST_DIR/bin/codex" --model 'chosen-model' >/dev/null
cmp "$PROMPT" "$FAKE_QUEUED_TASK"
grep -Fq -- '-m chosen-model' "$LAUNCH"
python3 -c 'import sys; print("x" * 32768, end="")' > "$PROMPT"
: > "$LOG"
if run_spawn codex-ready >/dev/null 2>"$STDERR"; then exit 1; fi
[[ ! -s "$LOG" ]]
cp "$TEST_DIR/saved-prompt" "$PROMPT"

# Executable/model selection is explicit, shell-escaped, and overrideable.
cp "$TEST_DIR/bin/codex" "$TEST_DIR/bin/chosen codex"
SPAWN_CODEX_BIN="$TEST_DIR/bin/chosen codex" SPAWN_CODEX_MODEL=env-model \
    run_spawn codex-ready >/dev/null
grep -Fq 'chosen\ codex fork' "$LAUNCH"
grep -Fq -- '-m env-model' "$LAUNCH"
SPAWN_CODEX_MODEL=env-model run_spawn codex-ready --model cli-model >/dev/null
grep -Fq -- '-m cli-model' "$LAUNCH"
: > "$LOG"
if env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" \
        "$SPAWN" test:department claude . --model arbitrary >/dev/null 2>"$STDERR"; then exit 1; fi
[[ ! -s "$LOG" ]]
FAKE_MUTATE_PROMPT="$PROMPT" run_spawn codex-ready >/dev/null
cmp "$TEST_DIR/saved-prompt" "$FAKE_QUEUED_TASK"
! cmp -s "$PROMPT" "$FAKE_QUEUED_TASK"
cp "$TEST_DIR/saved-prompt" "$PROMPT"

# The JSONL watcher ignores prompt/user/commentary/non-prefix matches. A valid
# final assistant record is invisible until its terminating newline arrives,
# then its full multi-part output is printed exactly once.
JSONL="$TEST_DIR/watcher.jsonl"
WATCH_OUT="$TEST_DIR/watcher.out"
MARKER='STEP-JSONL-91ac COMPLETE — EXECUTIVE RECAP'
EXPECTED="$MARKER
All work is verified."
: > "$JSONL"
"$WATCH" recap "$JSONL" "$MARKER" 5 > "$WATCH_OUT" &
watch_pid=$!
printf '%s\n' \
    '{"type":"response_item","payload":{"type":"message","role":"user","phase":"final_answer","content":[{"type":"output_text","text":"STEP-JSONL-91ac COMPLETE — EXECUTIVE RECAP"}]}}' \
    '{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"STEP-JSONL-91ac COMPLETE — EXECUTIVE RECAP"}]}}' \
    '{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"prefix STEP-JSONL-91ac COMPLETE — EXECUTIVE RECAP"}]}}' \
    >> "$JSONL"
matching='{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"STEP-JSONL-91ac COMPLETE — EXECUTIVE RECAP\n"},{"type":"output_text","text":"All work is verified."}]}}'
split=$(( ${#matching} / 2 ))
printf '%s' "${matching:0:split}" >> "$JSONL"
/bin/sleep 0.4
kill -0 "$watch_pid"
[[ ! -s "$WATCH_OUT" ]]
printf '%s\n' "${matching:split}" >> "$JSONL"
wait "$watch_pid"
[[ "$(<"$WATCH_OUT")" == "$EXPECTED" ]]
[[ $(grep -oF "$MARKER" "$WATCH_OUT" | wc -l) -eq 1 ]]

# Session IDs resolve to rollout paths, and cursor stops after the final
# newline rather than skipping an incomplete trailing record.
session_result=$(CODEX_HOME="$TEST_DIR/.codex" "$WATCH" recap \
    01A07050-1111-7222-8333-444444444444 "$MARKER" 1 \
    --after-byte 0 2>"$STDERR" || true)
[[ -z "$session_result" ]]
grep -Fq 'timed out' "$STDERR"
printf '{}\n{"partial"' > "$JSONL"
[[ "$($WATCH cursor "$JSONL")" -eq 3 ]]

if "$WATCH" recap "$JSONL" "$MARKER" nan >/dev/null 2>"$STDERR"; then
    echo 'FAIL: non-finite timeout was accepted' >&2
    exit 1
fi
grep -Fq 'timeout must be a positive number' "$STDERR"

# Codex can rewrite a large rollout to its smaller compacted form when the
# task begins. The watcher restarts at byte zero, ignores inherited records by
# shape/marker, and still emits the later matching final response exactly once.
TRUNCATED_JSONL="$TEST_DIR/truncated-watcher.jsonl"
TRUNCATED_OUT="$TEST_DIR/truncated-watcher.out"
printf '%04096d\n' 0 > "$TRUNCATED_JSONL"
truncated_cursor=$(wc -c < "$TRUNCATED_JSONL")
"$WATCH" recap "$TRUNCATED_JSONL" "$MARKER" 5 \
    --after-byte "$truncated_cursor" > "$TRUNCATED_OUT" &
truncated_pid=$!
/bin/sleep 0.3
printf '%s\n' \
    '{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"an inherited final without the marker"}]}}' \
    > "$TRUNCATED_JSONL"
/bin/sleep 0.3
printf '%s\n' "$matching" >> "$TRUNCATED_JSONL"
wait "$truncated_pid"
[[ "$(<"$TRUNCATED_OUT")" == "$EXPECTED" ]]

# Compaction events have no unique marker, so a shrink must fail closed rather
# than rescan and accidentally accept an inherited context_compacted event.
printf '%04096d\n' 0 > "$TRUNCATED_JSONL"
truncated_cursor=$(wc -c < "$TRUNCATED_JSONL")
"$WATCH" compaction "$TRUNCATED_JSONL" "$truncated_cursor" 5 \
    > /dev/null 2> "$STDERR" &
truncated_pid=$!
/bin/sleep 0.3
printf '%s\n' '{"type":"event_msg","payload":{"type":"context_compacted"}}' \
    > "$TRUNCATED_JSONL"
if wait "$truncated_pid"; then
    echo 'FAIL: compaction watcher accepted an event after rollout truncation' >&2
    exit 1
fi
grep -Fq 'rollout was truncated' "$STDERR"

# Claude: a live composer accepts the prompt.
: > "$LOG"
run_claude claude-ready >/dev/null
grep -q '^paste-buffer ' "$LOG"

# Claude: the trust dialog is refused without sending anything. Its default
# button is BUILD-SPECIFIC -- 2.1.220 defaults to "Yes, I trust this folder" --
# so an Enter here may silently grant trust to an unvetted directory.
: > "$LOG"
if run_claude claude-trust >/dev/null 2>"$STDERR"; then
    echo 'FAIL: Claude trust dialog was treated as a composer' >&2
    exit 1
fi
grep -Fq 'asking whether to trust' "$STDERR"
assert_no_input

# The regression that shipped in the first cut of this feature: at 33 columns
# the dialog's footer wraps, so a detector anchored on the last line missed it
# while the composer check matched on the shared "❯" glyph alone. tmux sizes a
# session to its SMALLEST attached client, so a phone on the shared session is
# enough to reach this. Must behave identically to the unwrapped case.
: > "$LOG"
if run_claude claude-trust-wrapped >/dev/null 2>"$STDERR"; then
    echo 'FAIL: a wrapped Claude trust dialog was treated as a composer' >&2
    exit 1
fi
grep -Fq 'asking whether to trust' "$STDERR"
assert_no_input

# --trust is rejected for a non-claude agent BEFORE any session is created.
: > "$LOG"
if env -u TMUX PATH="$TEST_DIR/bin:$PATH" FAKE_TMUX_LOG="$LOG" FAKE_LAUNCH_FILE="$LAUNCH" \
    "$SPAWN" test:department codex "$TEST_DIR/repo with spaces" --trust \
    --prompt-file "$PROMPT" >/dev/null 2>"$STDERR"; then
    echo 'FAIL: --trust was accepted for codex' >&2
    exit 1
fi
grep -Fq 'applies to claude only' "$STDERR"
if grep -q '^new-session' "$LOG"; then
    echo 'FAIL: --trust rejection happened after the session was created' >&2
    exit 1
fi

echo 'PASS: Codex fork spawning, Claude trust-dialog refusal (incl. wrapped), and JSONL recap watching'
python3 "$SCRIPT_DIR/test-codex-fork-task.py"
