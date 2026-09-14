#!/bin/bash
# agent-spawn.sh - Spawn an AI agent in a tmux window (interactive mode)
#
# Usage: agent-spawn.sh <session:window> <agent> [directory] [prompt]
#        agent-spawn.sh <session:window> <agent> [directory] --prompt-file <file>
#
# Agents: claude, codex, agy (Antigravity CLI)
#
# Options:
#   -f, --prompt-file <file>          Read prompt from file (avoids ARG_MAX)
#   -F, --fork-session <uuid>         Fork this Codex session interactively
#       --compact-prompt-file <file>  Compact inherited history before the task
#       --compact-timeout <seconds>   Compaction timeout (default: 600)
#       --rollout-path-file <file>    Record the child Codex rollout path
#       --recap-cursor-file <file>    Record where recap watching should begin
#       --startup-cursor-file <file> Record the cursor before fork compaction
#       --codex-bin <executable>     Codex executable (default: codex on PATH)
#       --model <model>              Codex model (default: Codex configuration)
#
# Automatically targets the correct tmux server socket when run inside tmux.
# Override with SPAWN_TMUX_SOCKET or SPAWN_TMUX_LABEL env vars.
#
# Fork tasks use native codex queue after readiness/compaction. Other launches
# retain tmux load-buffer/paste-buffer delivery. Queue takes one bounded argv
# message; do not put credentials in task prompts.
#
# Examples:
#   agent-spawn.sh chaos:review claude ./project "Review auth module"
#   agent-spawn.sh chaos:codex-help codex . "Help me refactor this"
#   agent-spawn.sh chaos:review claude ./project --prompt-file /tmp/prompt.txt
#   agent-spawn.sh chaos:design codex ./project --fork-session UUID --prompt-file /tmp/prompt.txt

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/tmux-ctx.sh"

codex_composer_ready() {
    local active_tail last_nonempty
    active_tail=$(printf '%s\n' "$1" | tail -n 6)
    last_nonempty=$(printf '%s\n' "$active_tail" | awk 'NF { line = $0 } END { print line }')
    [[ "$active_tail" != *"Working ("* ]] && \
        [[ "$last_nonempty" =~ Context[[:space:]][0-9]+% ]] && \
        printf '%s\n' "$active_tail" | grep -q '^[[:space:]]*› '
}

codex_update_modal_active() {
    local active_tail last_nonempty
    active_tail=$(printf '%s\n' "$1" | tail -n 8)
    last_nonempty=$(printf '%s\n' "$active_tail" | awk 'NF { line = $0 } END { print line }')
    [[ "$active_tail" == *"Update available!"* && \
       "$last_nonempty" == *"Press enter to continue"* ]]
}

codex_trust_modal_active() {
    local active_tail last_nonempty
    active_tail=$(printf '%s\n' "$1" | tail -n 8)
    last_nonempty=$(printf '%s\n' "$active_tail" | awk 'NF { line = $0 } END { print line }')
    [[ "$active_tail" == *"Do you trust the contents of this project?"* && \
       "$last_nonempty" == *"enter Confirm"* ]]
}

# Claude Code pane classifiers live in their own file so that
# claude-trust-dir.sh --verify shares them verbatim rather than keeping a
# private copy that could drift from what the spawner actually enforces.
source "$SCRIPT_DIR/claude-pane-detect.sh"

usage() {
    echo "Usage: agent-spawn.sh <session:window> <agent> [directory] [prompt]" >&2
    echo "       agent-spawn.sh <session:window> <agent> [directory] --prompt-file <file>" >&2
    echo "  session:window: tmux target (e.g., 'chaos:review')" >&2
    echo "  agent: claude, codex, agy" >&2
    echo "  directory: working directory (default: current)" >&2
    echo "  prompt: initial prompt for the agent" >&2
    echo "  --prompt-file: read prompt from file (avoids ARG_MAX)" >&2
    echo "  --trust: pre-grant Claude Code workspace trust for <directory> (claude only)" >&2
    echo "  --fork-session: interactively fork a Codex session UUID" >&2
    echo "  --compact-prompt-file: compact inherited history with this prompt before the task" >&2
    echo "  --compact-timeout: seconds to wait for structured compaction completion" >&2
    echo "  --rollout-path-file: atomically write the child Codex rollout path" >&2
    echo "  --recap-cursor-file: atomically write the safe JSONL recap start offset" >&2
    echo "  --startup-cursor-file: atomically write the pre-compaction JSONL offset" >&2
    echo "  --codex-bin: executable path/name (or SPAWN_CODEX_BIN; default codex)" >&2
    echo "  --model: Codex model (or SPAWN_CODEX_MODEL; default Codex config)" >&2
    exit 1
}

if [[ $# -lt 2 ]]; then
    usage
fi

target="$1"
agent="$2"
shift 2

# Parse remaining args: [directory] [prompt] or --prompt-file <file>
# --prompt-file can appear anywhere in the remaining args.
prompt_file=""
prompt=""
directory="."
trust_directory=0
fork_session=""
compact_prompt_file=""
compact_timeout=600
compact_timeout_set=""
rollout_path_file=""
recap_cursor_file=""
startup_cursor_file=""
codex_bin="${SPAWN_CODEX_BIN:-codex}"
codex_model="${SPAWN_CODEX_MODEL:-}"
codex_options_set=""
_cleanup_prompt_file=""
_cleanup_command_file=""
_cleanup_rollout_output=""
_cleanup_cursor_output=""
_cleanup_startup_output=""
_cleanup_compact_file=""
_cleanup_fork_task=""

# Trap to clean up temp files on exit/interrupt (only files we created).
# Set early so failure paths during parsing don't leak temp files.
# An EXIT trap's status BECOMES the script's, so cleanup must be unable to
# fail. Without `return 0` every no-prompt spawn -- where the guard is simply
# false -- reported failure after succeeding; without `|| true` a failing rm
# would kill the trap under `set -e` before it got there.
cleanup() {
    if [[ -n "$_cleanup_prompt_file" ]]; then rm -f "$_cleanup_prompt_file" || true; fi
    if [[ -n "$_cleanup_command_file" ]]; then rm -f "$_cleanup_command_file" || true; fi
    if [[ -n "$_cleanup_rollout_output" ]]; then rm -f "$_cleanup_rollout_output" || true; fi
    if [[ -n "$_cleanup_cursor_output" ]]; then rm -f "$_cleanup_cursor_output" || true; fi
    if [[ -n "$_cleanup_startup_output" ]]; then rm -f "$_cleanup_startup_output" || true; fi
    if [[ -n "$_cleanup_compact_file" ]]; then rm -f "$_cleanup_compact_file" || true; fi
    if [[ -n "$_cleanup_fork_task" ]]; then rm -f "$_cleanup_fork_task" || true; fi
    return 0
}
trap cleanup EXIT

positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --codex-bin|--model)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: $1 requires a value" >&2
                exit 1
            fi
            if [[ "$1" == --codex-bin ]]; then codex_bin="$2"; else codex_model="$2"; fi
            codex_options_set=1
            shift 2
            ;;
        --trust)
            trust_directory=1
            shift
            ;;
        --prompt-file|-f)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --prompt-file requires a file path" >&2
                exit 1
            fi
            prompt_file="$2"
            if [[ ! -f "$prompt_file" ]]; then
                echo "Error: prompt file not found: $prompt_file" >&2
                exit 1
            fi
            if [[ ! -s "$prompt_file" ]]; then
                echo "Error: prompt file is empty: $prompt_file" >&2
                exit 1
            fi
            shift 2
            ;;
        --fork-session|-F)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --fork-session requires a Codex session UUID" >&2
                exit 1
            fi
            fork_session="$2"
            shift 2
            ;;
        --compact-prompt-file)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --compact-prompt-file requires a file path" >&2
                exit 1
            fi
            compact_prompt_file="$2"
            if [[ ! -f "$compact_prompt_file" ]]; then
                echo "Error: compaction prompt file not found: $compact_prompt_file" >&2
                exit 1
            fi
            shift 2
            ;;
        --compact-timeout)
            if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ || "$2" -eq 0 ]]; then
                echo "Error: --compact-timeout requires a positive integer" >&2
                exit 1
            fi
            compact_timeout="$2"
            compact_timeout_set=1
            shift 2
            ;;
        --rollout-path-file)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --rollout-path-file requires a file path" >&2
                exit 1
            fi
            rollout_path_file="$2"
            shift 2
            ;;
        --recap-cursor-file)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --recap-cursor-file requires a file path" >&2
                exit 1
            fi
            recap_cursor_file="$2"
            shift 2
            ;;
        --startup-cursor-file)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Error: --startup-cursor-file requires a file path" >&2
                exit 1
            fi
            startup_cursor_file="$2"
            shift 2
            ;;
        *)
            positional+=("$1")
            shift
            ;;
    esac
done

# Positional args: [directory] [prompt]
if [[ ${#positional[@]} -ge 1 ]]; then
    directory="${positional[0]}"
fi
if [[ ${#positional[@]} -ge 2 ]]; then
    prompt="${positional[1]}"
fi

requested_directory="$directory"
if ! directory=$(CDPATH='' cd -- "$requested_directory" 2>/dev/null && pwd); then
    echo "Error: working directory does not exist or is not accessible: $requested_directory" >&2
    exit 1
fi

# Parse session and window from target
session="${target%%:*}"
window="${target#*:}"

# Validate agent
case "$agent" in
    claude|codex|agy) ;;
    *)
        echo "Unknown agent: $agent" >&2
        echo "Supported agents: claude, codex, agy" >&2
        exit 1
        ;;
esac

if [[ -n "$codex_options_set" && "$agent" != codex ]]; then
    echo "Error: --codex-bin and --model apply to codex only" >&2
    exit 1
fi
if [[ "$agent" == codex ]]; then
    codex_bin=$(command -v -- "$codex_bin") || {
        echo "Error: Codex executable not found" >&2
        exit 1
    }
    if [[ ! -f "$codex_bin" || ! -x "$codex_bin" ]]; then
        echo "Error: --codex-bin must resolve to an executable file" >&2
        exit 1
    fi
    codex_bin="$(CDPATH='' cd -- "$(dirname -- "$codex_bin")" && pwd)/$(basename -- "$codex_bin")"
fi

if [[ "$trust_directory" -eq 1 && "$agent" != "claude" ]]; then
    echo "Error: --trust applies to claude only; agy and codex keep their own trust stores." >&2
    exit 1
fi

if [[ -n "$fork_session" ]]; then
    if [[ "$agent" != "codex" ]]; then
        echo "Error: --fork-session is currently supported only for codex" >&2
        exit 1
    fi
    if [[ ! "$fork_session" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]; then
        echo "Error: --fork-session requires a UUID, got: $fork_session" >&2
        exit 1
    fi
fi

if [[ -n "$compact_prompt_file" ]]; then
    if [[ "$agent" != "codex" || -z "$fork_session" ]]; then
        echo "Error: --compact-prompt-file requires a Codex --fork-session launch" >&2
        exit 1
    fi
    if [[ ! -s "$compact_prompt_file" ]]; then
        echo "Error: compaction prompt file is empty: $compact_prompt_file" >&2
        exit 1
    fi
    if [[ -z "$prompt_file" && -z "$prompt" ]]; then
        echo "Error: launch-time compaction requires a task prompt to deliver afterward" >&2
        exit 1
    fi
fi

if [[ -n "$rollout_path_file" && ( "$agent" != "codex" || -z "$fork_session" ) ]]; then
    echo "Error: --rollout-path-file requires a Codex --fork-session launch" >&2
    exit 1
fi
if [[ -n "$rollout_path_file" && -z "$prompt_file" && -z "$prompt" ]]; then
    echo "Error: --rollout-path-file requires a task prompt in the fork workflow" >&2
    exit 1
fi
if [[ -n "$recap_cursor_file" && -z "$rollout_path_file" ]]; then
    echo "Error: --recap-cursor-file requires --rollout-path-file" >&2
    exit 1
fi
if [[ -n "$compact_timeout_set" && -z "$compact_prompt_file" ]]; then
    echo "Error: --compact-timeout requires --compact-prompt-file" >&2
    exit 1
fi
if [[ -n "$startup_cursor_file" && ( -z "$compact_prompt_file" || -z "$rollout_path_file" ) ]]; then
    echo "Error: --startup-cursor-file requires --compact-prompt-file and --rollout-path-file" >&2
    exit 1
fi

# Normalize prompt to a file if provided as positional arg
if [[ -n "$prompt" && -z "$prompt_file" ]]; then
    prompt_file=$(mktemp /tmp/spawn-agent-prompt.XXXXXX)
    _cleanup_prompt_file="$prompt_file"
    printf '%s' "$prompt" > "$prompt_file"
fi

# Canonicalize prompt_file to absolute path
if [[ -n "$prompt_file" ]]; then
    prompt_file="$(CDPATH='' cd -- "$(dirname -- "$prompt_file")" && pwd)/$(basename -- "$prompt_file")"
fi

if [[ -n "$fork_session" && -n "$prompt_file" ]]; then
    # Keep compaction and later delivery on the same assignment even if the
    # caller edits their task file while this child is starting.
    _cleanup_fork_task=$(mktemp /tmp/spawn-agent-fork-task.XXXXXX)
    cp -- "$prompt_file" "$_cleanup_fork_task"
    prompt_file="$_cleanup_fork_task"
    python3 "$SCRIPT_DIR/codex-fork-task.py" preflight "$codex_bin" "$prompt_file"
fi

if [[ -n "$compact_prompt_file" ]]; then
    compact_dir=$(CDPATH='' cd -- "$(dirname -- "$compact_prompt_file")" && pwd) || {
        echo "Error: compaction prompt directory does not exist: $(dirname -- "$compact_prompt_file")" >&2
        exit 1
    }
    compact_prompt_file="$compact_dir/$(basename -- "$compact_prompt_file")"
    if [[ "$compact_prompt_file" =~ [[:cntrl:]] ]]; then
        echo "Error: compaction prompt path cannot contain control characters" >&2
        exit 1
    fi
    # Snapshot the policy + task before the child starts. Its shell owns this
    # private file for the child's entire lifetime, including later compactions.
    _cleanup_compact_file=$(mktemp /tmp/spawn-agent-compact.XXXXXX)
    python3 "$SCRIPT_DIR/codex-fork-task.py" compact \
        "$compact_prompt_file" "$prompt_file" "$_cleanup_compact_file"
    compact_prompt_file="$_cleanup_compact_file"
fi

if [[ -n "$rollout_path_file" ]]; then
    rollout_dir=$(CDPATH='' cd -- "$(dirname -- "$rollout_path_file")" && pwd) || {
        echo "Error: rollout path directory does not exist: $(dirname -- "$rollout_path_file")" >&2
        exit 1
    }
    rollout_path_file="$rollout_dir/$(basename -- "$rollout_path_file")"
fi

if [[ -n "$recap_cursor_file" ]]; then
    cursor_dir=$(CDPATH='' cd -- "$(dirname -- "$recap_cursor_file")" && pwd) || {
        echo "Error: recap cursor directory does not exist: $(dirname -- "$recap_cursor_file")" >&2
        exit 1
    }
    recap_cursor_file="$cursor_dir/$(basename -- "$recap_cursor_file")"
fi

if [[ -n "$startup_cursor_file" ]]; then
    startup_dir=$(CDPATH='' cd -- "$(dirname -- "$startup_cursor_file")" && pwd) || {
        echo "Error: startup cursor directory does not exist" >&2
        exit 1
    }
    startup_cursor_file="$startup_dir/$(basename -- "$startup_cursor_file")"
    if [[ -d "$startup_cursor_file" || "$startup_cursor_file" == "$rollout_path_file" || \
          "$startup_cursor_file" == "$recap_cursor_file" ]]; then
        echo "Error: startup cursor must be a separate receipt file" >&2
        exit 1
    fi
fi

# Build one shell-escaped command string for tmux. A bare `codex fork` is the
# interactive transcript-preserving command; `codex exec fork` is headless and
# therefore intentionally does not belong here. Explicit -C avoids Codex's
# current-vs-saved-directory chooser, which is another modal where an automated
# Enter would be unsafe.
agent_command=("$agent")
if [[ "$agent" == "codex" ]]; then
    # Managed agent windows must not stop at an update chooser. Keep this a
    # process-local override: ordinary Codex launches still perform their
    # normal update check, and no global configuration is changed.
    agent_command=("$codex_bin" -c check_for_update_on_startup=false)
    if [[ -n "$codex_model" ]]; then agent_command+=(-m "$codex_model"); fi
fi
if [[ -n "$fork_session" ]]; then
    agent_command=("$codex_bin" fork -c check_for_update_on_startup=false)
    if [[ -n "$codex_model" ]]; then agent_command+=(-m "$codex_model"); fi
    if [[ -n "$compact_prompt_file" ]]; then
        # Pass only the path in argv, never the prompt body. TOML basic-string
        # escaping handles spaces, quotes, and backslashes without touching
        # the user's global Codex configuration.
        toml_compact_path=${compact_prompt_file//\\/\\\\}
        toml_compact_path=${toml_compact_path//\"/\\\"}
        agent_command+=(-c "experimental_compact_prompt_file=\"$toml_compact_path\"")
    fi
    agent_command+=(-C "$directory" "$fork_session")
fi
printf -v agent_shell_command '%q ' "${agent_command[@]}"
if [[ -n "$_cleanup_compact_file" ]]; then
    printf -v compact_cleanup 'rm -f -- %q' "$_cleanup_compact_file"
    printf -v compact_trap 'trap %q EXIT; ' "$compact_cleanup"
    agent_shell_command="$compact_trap$agent_shell_command"
fi

# Granting workspace trust happens here, before launch, and only because the
# caller asked for it -- never by answering a modal after the fact.
if [[ "$trust_directory" -eq 1 ]]; then
    if ! "$SCRIPT_DIR/claude-trust-dir.sh" "$directory"; then
        echo "Error: could not grant workspace trust for $directory; not launching." >&2
        exit 1
    fi
fi

# Start the agent only after options, task size and queue support are checked.
if ! "${TMUX_CMD[@]}" has-session -t "$session" 2>/dev/null; then
    echo "Creating tmux session: $session" >&2
    "${TMUX_CMD[@]}" new-session -d -s "$session"
fi
"${TMUX_CMD[@]}" new-window -t "$session" -n "$window" -c "$directory" "$agent_shell_command"
_cleanup_compact_file="" # child shell now owns cleanup, including failed delivery

if [[ -n "$prompt_file" ]]; then
    # Wait for the actual composer before sending anything. In particular,
    # Codex may show an update chooser first; the old fixed sleep pasted the
    # task into that chooser and its Enter selected "Update now".
    # Every supported agent has a readiness probe below. The else branch
    # survives as a fallback so a future agent added to the validator above
    # degrades to the old fixed sleep instead of hanging here.
    if [[ "$agent" == "agy" || "$agent" == "codex" || "$agent" == "claude" ]]; then
        # agy paints a full-screen TUI and discards anything delivered before it
        # is listening, so a flat `sleep 2` races it -- on a cold start the
        # prompt vanishes and the spawn still reports success.
        #
        # Worse, agy's FIRST run in an untrusted directory opens a modal asking
        # whether to trust it, whose default button is "Yes, I trust this
        # folder" and whose footer is "enter Confirm". Pasting into that does
        # nothing and the Enter that follows GRANTS TRUST. That is how
        # /tmp/pr64-test ended up in agy's trustedWorkspaces during testing.
        # Never send keys until the composer specifically is on screen.
        agent_ready=""
        agent_gone=0
        for _ in $(seq 1 120); do
            # A window that disappeared is not a slow window: say so, rather
            # than burning 30s and then claiming the agent is waiting for input.
            if pane="$("${TMUX_CMD[@]}" capture-pane -p -t "$session:$window" 2>/dev/null)"; then
                agent_gone=0
            else
                agent_gone=$(( agent_gone + 1 ))
                if (( agent_gone >= 3 )); then
                    echo "Error: $session:$window is gone -- $agent exited before the prompt could be sent." >&2
                    echo "  Nothing was pasted." >&2
                    exit 1
                fi
                sleep 0.25
                continue
            fi

            # Only inspect the active bottom of the pane. A Codex fork can
            # contain old transcript text that quotes one of these dialogs.
            pane_tail=$(printf '%s\n' "$pane" | tail -n 30)

            if [[ "$agent" == "codex" ]]; then
                if codex_update_modal_active "$pane_tail"; then
                    echo "Error: Codex is showing its update chooser in $session:$window." >&2
                    echo "  Nothing was pasted, and no key was sent -- selecting an update is an" >&2
                    echo "  operator decision. Resolve the chooser in that window, then send the" >&2
                    echo "  prompt explicitly or start the fork again." >&2
                    exit 1
                fi
                if codex_trust_modal_active "$pane_tail"; then
                    echo "Error: Codex is asking whether to trust this directory before it will run:" >&2
                    echo "    $directory" >&2
                    echo "  Nothing was pasted, and no key was sent -- answering that prompt is a" >&2
                    echo "  permission decision this tooling will not make for you." >&2
                    echo "  Answer it yourself, then send the prompt explicitly." >&2
                    exit 1
                fi
                # A live composer's Context status is the bottommost non-empty
                # line. This distinguishes it from a stale composer in restored
                # history when a modal is actually on top.
                if codex_composer_ready "$pane_tail"; then
                    agent_ready=1
                    break
                fi
            elif [[ "$agent" == "claude" ]]; then
                if claude_trust_modal_active "$pane_tail"; then
                    echo "Error: Claude Code is asking whether to trust this directory before it will run:" >&2
                    echo "    $directory" >&2
                    echo "  Nothing was pasted, and no key was sent." >&2
                    echo "  Which button an Enter would hit is BUILD-SPECIFIC: 2.1.261 defaults" >&2
                    echo "  to \"No, exit\", 2.1.220 defaults to \"Yes, I trust this folder\". So an" >&2
                    echo "  automated Enter may silently GRANT trust to an unvetted directory." >&2
                    echo "  Answer it yourself, or re-run with --trust to grant it explicitly." >&2
                    exit 1
                fi
                if claude_composer_ready "$pane_tail"; then
                    agent_ready=1
                    break
                fi
            elif [[ "$pane_tail" == *"Do you trust the contents of this project?"* ]]; then
                echo "Error: agy is asking whether to trust this directory before it will run:" >&2
                echo "    $directory" >&2
                echo "  Nothing was pasted, and no key was sent -- answering that prompt is a" >&2
                echo "  permission decision this tooling will not make for you." >&2
                echo "  Answer it yourself, then send the prompt explicitly." >&2
                exit 1
            # agy's composer specifically: its rule AND shortcuts hint. Check
            # trust first because a modal may be painted over a live frame.
            elif [[ "$pane_tail" == *"? for shortcuts"* && "$pane_tail" == *"─"* ]]; then
                agent_ready=1
                break
            fi
            sleep 0.25
        done
        if [[ -z "$agent_ready" ]]; then
            echo "Error: $agent's input box did not appear within 30s; prompt not sent." >&2
            echo "  $agent is running in $session:$window -- attach and type it by hand." >&2
            exit 1
        fi
        # The box is painted a beat before input is accepted.
        if [[ "$agent" == "codex" ]]; then sleep 3; else sleep 1; fi
    else
        sleep 2
    fi
    if [[ "$agent" == "codex" && -n "$fork_session" ]]; then
        rollout_path=$("$SCRIPT_DIR/codex-rollout-path.sh" \
            --exclude-session "$fork_session" "$target") || {
            echo "Error: could not resolve the child rollout; task prompt not sent." >&2
            echo "  The child remains running and un-tasked in $target." >&2
            exit 1
        }
        python3 "$SCRIPT_DIR/codex-fork-task.py" child "$rollout_path" "$fork_session" >/dev/null
        if [[ -n "$rollout_path_file" ]]; then
            _cleanup_rollout_output=$(mktemp "$rollout_dir/.codex-rollout.XXXXXX")
            chmod 600 "$_cleanup_rollout_output"
            printf '%s\n' "$rollout_path" > "$_cleanup_rollout_output"
            mv -f -- "$_cleanup_rollout_output" "$rollout_path_file"
            _cleanup_rollout_output=""
        fi
    fi

    if [[ -n "$compact_prompt_file" ]]; then
        compact_after=$("$SCRIPT_DIR/codex-jsonl-watch.py" cursor "$rollout_path")
        if [[ -n "$startup_cursor_file" ]]; then
            _cleanup_startup_output=$(mktemp "$startup_dir/.codex-startup.XXXXXX")
            chmod 600 "$_cleanup_startup_output"
            printf '%s\n' "$compact_after" > "$_cleanup_startup_output"
            mv -f -- "$_cleanup_startup_output" "$startup_cursor_file"
            _cleanup_startup_output=""
        fi
        compact_before_paste=$("${TMUX_CMD[@]}" capture-pane -p -t "$session:$window")
        compact_before_count=$(printf '%s\n' "$compact_before_paste" | tail -n 20 | \
            grep -c '^[[:space:]]*› /compact[[:space:]]*$' || true)
        _cleanup_command_file=$(mktemp /tmp/spawn-agent-command.XXXXXX)
        printf '%s' '/compact' > "$_cleanup_command_file"
        compact_buf="spawn-agent-compact-$$"
        "${TMUX_CMD[@]}" load-buffer -b "$compact_buf" "$_cleanup_command_file"
        if ! "${TMUX_CMD[@]}" paste-buffer -d -t "$session:$window" -b "$compact_buf"; then
            "${TMUX_CMD[@]}" delete-buffer -b "$compact_buf" 2>/dev/null || true
            echo "Error: failed to paste /compact to $session:$window; task prompt not sent." >&2
            exit 1
        fi
        compact_loaded=""
        for _ in $(seq 1 20); do
            if ! pane=$("${TMUX_CMD[@]}" capture-pane -p -t "$session:$window" 2>/dev/null); then
                echo "Error: $session:$window exited before /compact could be submitted." >&2
                echo "  No Enter was sent and the task prompt was not sent." >&2
                exit 1
            fi
            compact_tail=$(printf '%s\n' "$pane" | tail -n 20)
            if codex_update_modal_active "$compact_tail" || \
               codex_trust_modal_active "$compact_tail"; then
                break
            fi
            compact_after_count=$(printf '%s\n' "$compact_tail" | \
                grep -c '^[[:space:]]*› /compact[[:space:]]*$' || true)
            if [[ "$pane" != "$compact_before_paste" ]] && \
               (( compact_after_count > compact_before_count )); then
                compact_loaded=1
                break
            fi
            sleep 0.25
        done
        if [[ -z "$compact_loaded" ]]; then
            echo "Error: the Codex composer does not contain exactly /compact." >&2
            echo "  No Enter was sent; the child remains running and un-tasked in $target." >&2
            exit 1
        fi
        "${TMUX_CMD[@]}" send-keys -t "$session:$window" Enter
        echo "Waiting for Codex to finish the requested history compaction..." >&2
        if ! "$SCRIPT_DIR/codex-jsonl-watch.py" compaction \
            "$rollout_path" "$compact_after" "$compact_timeout"; then
            echo "Error: Codex did not report completed compaction; task prompt not sent." >&2
            echo "  The child remains running and un-tasked in $target; inspect or close it." >&2
            echo "  If Codex was upgraded, verify its compaction config/event compatibility." >&2
            exit 1
        fi

        # The structured event is authoritative, but wait for the composer to
        # become interactive again before placing the task in it.
        compact_ready=""
        for _ in $(seq 1 120); do
            if ! pane=$("${TMUX_CMD[@]}" capture-pane -p -t "$session:$window" 2>/dev/null); then
                echo "Error: $session:$window exited after compaction; task prompt not sent." >&2
                exit 1
            fi
            pane_tail=$(printf '%s\n' "$pane" | tail -n 30)
            if codex_update_modal_active "$pane_tail" || \
               codex_trust_modal_active "$pane_tail"; then
                echo "Error: Codex opened a startup modal after compaction; task prompt not sent." >&2
                echo "  No key was sent; resolve it in $target or close the child." >&2
                exit 1
            fi
            if codex_composer_ready "$pane_tail"; then
                compact_ready=1
                break
            fi
            sleep 0.25
        done
        if [[ -z "$compact_ready" ]]; then
            echo "Error: Codex compacted but its composer did not return; task prompt not sent." >&2
            echo "  The child remains running and un-tasked in $target; inspect or close it." >&2
            exit 1
        fi
        sleep 1
    fi

    if [[ -n "$recap_cursor_file" ]]; then
        recap_after=$("$SCRIPT_DIR/codex-jsonl-watch.py" cursor "$rollout_path")
        _cleanup_cursor_output=$(mktemp "$cursor_dir/.codex-cursor.XXXXXX")
        chmod 600 "$_cleanup_cursor_output"
        printf '%s\n' "$recap_after" > "$_cleanup_cursor_output"
        mv -f -- "$_cleanup_cursor_output" "$recap_cursor_file"
        _cleanup_cursor_output=""
    fi

    if [[ "$agent" == codex && -n "$fork_session" ]]; then
        child_session=$(python3 "$SCRIPT_DIR/codex-fork-task.py" queue \
            "$codex_bin" "$rollout_path" "$fork_session" "$prompt_file") || {
            echo "Error: task delivery was not confirmed; inspect $target before retrying." >&2
            exit 1
        }
        echo "Spawned $agent in $target (dir: $directory); task queued to $child_session"
        exit 0
    fi

    # Codex needs time to finish paste-burst handling for large task files.
    buf_name="spawn-agent-task-$$"
    "${TMUX_CMD[@]}" load-buffer -b "$buf_name" "$prompt_file"
    if ! "${TMUX_CMD[@]}" paste-buffer -d -t "$session:$window" -b "$buf_name"; then
        "${TMUX_CMD[@]}" delete-buffer -b "$buf_name" 2>/dev/null || true
        echo "Error: failed to paste prompt to $session:$window" >&2
        exit 1
    fi
    if [[ "$agent" == "codex" ]]; then sleep 3; else sleep 1.5; fi
    "${TMUX_CMD[@]}" send-keys -t "$session:$window" Enter
fi

echo "Spawned $agent in $target (dir: $directory)"
