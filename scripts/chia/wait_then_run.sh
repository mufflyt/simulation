#!/usr/bin/env bash
# Wait on the two currently-running jobs by PID (a pgrep pattern would match
# this watcher's own command line and never clear), then run the rest.
set -uo pipefail
for pid in "$@"; do
  while kill -0 "$pid" 2>/dev/null; do sleep 15; done
  echo "pid $pid finished"
done
exec /Volumes/MufflySamsung/chia_cadr_build/run_remaining.sh
