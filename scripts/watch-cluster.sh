#!/usr/bin/env bash
#
# Naranja DFS — live cluster tracker (for demos / presentations).
#
# Redraws every second:
#   * top    — each storage server's on-disk chunks under /data (id + size),
#              plus the naming server's metadata DB, so you can watch chunks
#              appear on all 3 replicas during a write and vanish on delete;
#   * bottom — a merged, time-sorted log stream from every service, so you can
#              narrate the allocate -> push -> commit -> register / read flow.
#
# Usage:
#   ./scripts/watch-cluster.sh            # refresh 1s, 16 log lines, hide healthz
#   REFRESH=2 LOG_LINES=24 ./scripts/watch-cluster.sh
#   SHOW_HEALTH=1 ./scripts/watch-cluster.sh   # also show /healthz probes
#   MAX_CHUNKS=20 ./scripts/watch-cluster.sh   # chunk lines per server (0 = all)
#
# Stop with Ctrl-C. Requires the stack to be running (`docker compose up`).

set -uo pipefail

REFRESH="${REFRESH:-1}"
LOG_LINES="${LOG_LINES:-16}"
MAX_CHUNKS="${MAX_CHUNKS:-12}"   # cap chunk lines per server so the frame stays readable
SHOW_HEALTH="${SHOW_HEALTH:-0}"  # hide noisy /healthz probes by default

NAMESERVER="naranja-nameserver"
CLIENT="naranja-client"
STORAGES=("naranja-storage-1" "naranja-storage-2" "naranja-storage-3")
ALL=("$CLIENT" "$NAMESERVER" "${STORAGES[@]}")

# Colors (fall back to empty strings if not a tty).
if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
  ORANGE=$'\033[38;5;208m'; GREEN=$'\033[32m'; RED=$'\033[31m'
  CYAN=$'\033[36m'; YELLOW=$'\033[33m'; BLUE=$'\033[34m'; MAGENTA=$'\033[35m'
else
  BOLD=""; DIM=""; RESET=""; ORANGE=""; GREEN=""; RED=""
  CYAN=""; YELLOW=""; BLUE=""; MAGENTA=""
fi

# Short label + color per container, for the merged log view.
label_of() {
  case "$1" in
    "$CLIENT")            printf '%sclient   %s' "$CYAN" "$RESET" ;;
    "$NAMESERVER")        printf '%sname     %s' "$ORANGE" "$RESET" ;;
    "naranja-storage-1")  printf '%sstore-1  %s' "$GREEN" "$RESET" ;;
    "naranja-storage-2")  printf '%sstore-2  %s' "$YELLOW" "$RESET" ;;
    "naranja-storage-3")  printf '%sstore-3  %s' "$MAGENTA" "$RESET" ;;
    *)                    printf '%-8s' "$1" ;;
  esac
}

is_running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }

human() { # bytes -> human-ish
  awk -v b="$1" 'BEGIN{
    if (b < 1024) printf "%d B", b;
    else if (b < 1048576) printf "%.1f KB", b/1024;
    else printf "%.1f MB", b/1048576;
  }'
}

# Per-storage chunk listing: emit "<size> <chunk_id>" per chunk file, skip .tmp.
storage_files() {
  docker exec "$1" sh -c '
    cd /data 2>/dev/null || exit 0
    for f in *; do
      [ -e "$f" ] || continue
      case "$f" in *.tmp) continue ;; esac
      printf "%s %s\n" "$(wc -c < "$f")" "$f"
    done
  ' 2>/dev/null
}

render_storage() {
  for c in "${STORAGES[@]}"; do
    if ! is_running "$c"; then
      printf "  %s%-18s%s %sDOWN%s\n" "$BOLD" "$c" "$RESET" "$RED" "$RESET"
      continue
    fi
    local listing count total
    listing="$(storage_files "$c")"
    count=$(printf '%s' "$listing" | grep -c . || true)
    total=$(printf '%s\n' "$listing" | awk '{s+=$1} END{print s+0}')
    printf "  %s%-18s%s %s%d chunk(s)%s, %s\n" \
      "$BOLD" "$c" "$RESET" "$GREEN" "$count" "$RESET" "$(human "$total")"
    if [ "$count" -gt 0 ]; then
      local shown=0
      printf '%s\n' "$listing" | while read -r sz id; do
        [ -n "$id" ] || continue
        if [ "$MAX_CHUNKS" -gt 0 ] && [ "$shown" -ge "$MAX_CHUNKS" ]; then
          printf "      %s… +%d more%s\n" "$DIM" "$((count - MAX_CHUNKS))" "$RESET"
          break
        fi
        printf "      %s%-12.12s%s  %6s B\n" "$DIM" "$id" "$RESET" "$sz"
        shown=$((shown + 1))
      done
    fi
  done
}

render_nameserver() {
  if ! is_running "$NAMESERVER"; then
    printf "  %s%-18s%s %sDOWN%s\n" "$BOLD" "$NAMESERVER" "$RESET" "$RED" "$RESET"
    return
  fi
  local dbsize
  dbsize="$(docker exec "$NAMESERVER" sh -c 'wc -c < /data/naranja.db 2>/dev/null' 2>/dev/null)"
  if [ -n "${dbsize:-}" ]; then
    printf "  %s%-18s%s metadata DB %s (chunk bytes are NOT here)\n" \
      "$BOLD" "$NAMESERVER" "$RESET" "$(human "$dbsize")"
  else
    printf "  %s%-18s%s %sno DB yet%s\n" "$BOLD" "$NAMESERVER" "$RESET" "$DIM" "$RESET"
  fi
}

# Merged, time-sorted log tail across all running services.
render_logs() {
  local merged=""
  for c in "${ALL[@]}"; do
    is_running "$c" || continue
    # docker logs -t prefixes each line with an RFC3339 timestamp; we prepend
    # the container name + a tab so we can colorize, then sort by timestamp.
    merged+="$(docker logs -t --tail="$LOG_LINES" "$c" 2>&1 \
                | sed "s/^/$c\t/")"$'\n'
  done
  # Hide the periodic /healthz probes unless asked — they bury the real flow.
  if [ "$SHOW_HEALTH" != "1" ]; then
    merged="$(printf '%s' "$merged" | grep -v '/healthz' || true)"
  fi
  # Sort by the timestamp (2nd field), keep the most recent LOG_LINES.
  printf '%s' "$merged" | grep -v '^[[:space:]]*$' \
    | sort -t$'\t' -k2 | tail -n "$LOG_LINES" \
    | while IFS=$'\t' read -r c rest; do
        local ts msg
        ts="${rest%% *}"; msg="${rest#* }"
        # Trim to a HH:MM:SS time for compactness.
        ts="${ts#*T}"; ts="${ts%.*}"
        printf "%s %s%s%s %s\n" "$(label_of "$c")" "$DIM" "$ts" "$RESET" "$msg"
      done
}

cleanup() { printf '\033[?25h\n'; } # restore cursor
trap cleanup EXIT INT TERM

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found on PATH." >&2; exit 1
fi

printf '\033[?25l' # hide cursor to reduce flicker
while true; do
  frame=""
  frame+="${ORANGE}${BOLD}  Naranja DFS — live cluster tracker${RESET}  ${DIM}(refresh ${REFRESH}s, Ctrl-C to quit)${RESET}"$'\n'
  frame+="${DIM}  $(date '+%Y-%m-%d %H:%M:%S')${RESET}"$'\n\n'
  frame+="${BOLD}  FILESYSTEM — chunks on disk (replication factor 3)${RESET}"$'\n'
  frame+="$(render_storage)"$'\n'
  frame+="$(render_nameserver)"$'\n\n'
  frame+="${BOLD}  LOGS — merged across services (oldest → newest)${RESET}"$'\n'
  frame+="$(render_logs)"

  # Home cursor, paint frame, clear anything left from a taller previous frame.
  printf '\033[H%s\033[J' "$frame"
  sleep "$REFRESH"
done
