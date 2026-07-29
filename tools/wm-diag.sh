#!/usr/bin/env bash
# wm-diag.sh — read wrapper-manager's logs instead of guessing.
#
# Every section here exists because guessing it wrong cost real time:
#
#   deaths      The 2026-07-29 outage looked like four separate failures. It was
#               one 26-second window. Deaths are only meaningful with their exit
#               tail and whether the manager asked for it, so they print together.
#   cliff       The root cause was a concurrency cliff in the wrapper's key-setup
#               path: latency is flat to ~6 concurrent setups per instance and
#               collapses past ~10. Nothing else in the logs shows it; you have to
#               reconstruct the overlaps. This is the section that found it.
#   fallback    A silent AAC-LC downgrade is what the owner actually notices. It
#               is a backend event, not a manager one, so it is queried separately.
#   health      Condemnations and the gate. Distinguishes "the manager killed it"
#               from "it died on its own" — that distinction is what the 2026-07-29
#               investigation turned on and the logs could not answer at the time.
#
# Usage:  ./wm-diag.sh [since]        e.g. ./wm-diag.sh 6h   (default 1h)
#         ./wm-diag.sh 12h cliff      run one section only
#
# Reads production over ssh. Read-only: it never restarts or reconfigures
# anything. rog is a Windows host, so every remote call goes through
# `ssh rog bash -s` — an `ssh rog '...'` lands in PowerShell and misparses.
set -uo pipefail

SINCE="${1:-1h}"
ONLY="${2:-all}"
ROG_LOG() { ssh rog bash -s <<EOF 2>/dev/null
docker logs --since $SINCE wrapper-manager 2>&1
EOF
}
NAS_API() { ssh dsm bash -s <<EOF 2>/dev/null
export PATH=\$PATH:/usr/local/bin
docker run --rm --network amdl-portal_private curlimages/curl:latest -s --max-time 15 "http://amdl-backend:18080\$1"
EOF
}

LOG=$(mktemp); trap 'rm -f "$LOG"' EXIT
ROG_LOG > "$LOG"
[ -s "$LOG" ] || { echo "no logs — is wrapper-manager up on rog?"; exit 1; }
echo "wrapper-manager, last $SINCE — $(grep -c . "$LOG") lines"
echo

sec() { [ "$ONLY" = all ] || [ "$ONLY" = "$1" ]; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if sec instances; then
  # The verdict lives in wm-analyze.py. Regexes full of quotes and braces do not
  # survive `python3 -c '...'` inside a shell function inside an ssh heredoc.
  python3 "$HERE/wm-analyze.py" instances < "$LOG"
fi

if sec deaths; then
  echo "── deaths ─────────────────────────────────────────────"
  n=$(grep -c "wrapper exited:" "$LOG")
  echo "  $n death(s)"
  # The exit line plus its tail is the whole story; print them together.
  grep -E "wrapper exited:|exit tail |restarting in |giving up on" "$LOG" \
    | sed 's/time="\([^"]*\)".*msg="/\1  /; s/"$//' | tail -60 | sed 's/^/  /'
  echo
fi

if sec health; then
  echo "── health verdicts ────────────────────────────────────"
  echo "  condemned : $(grep -c "is unhealthy:" "$LOG")   (the manager killed it)"
  echo "  gate held : $(grep -c "keeping it until the pool refills" "$LOG")   (refused to condemn the last one)"
  echo "  failovers : $(grep -c "decrypt failover" "$LOG")"
  grep -E "is unhealthy|keeping it until|no longer waiting" "$LOG" \
    | sed 's/time="\([^"]*\)".*msg="/\1  /; s/"$//' | tail -12 | sed 's/^/  /'
  echo
fi

if sec cliff; then
  python3 "$HERE/wm-analyze.py" cliff < "$LOG"
fi

if sec fallback; then
  echo "── silent codec fallback (backend, not the manager) ───"
  NAS_API "/api/v1/downloads?limit=50" | python3 -c '
import sys, json
try: d = json.load(sys.stdin)
except Exception: print("   backend unreachable"); raise SystemExit
rows = d.get("downloads", [])
bad = [j for j in rows if j.get("failed_items")]
print(f"   {len(rows)} recent jobs, {len(bad)} with failed items")
for j in bad[:5]:
    print(f"     {j['"'"'status'"'"']:>10} {j['"'"'done_items'"'"']}/{j['"'"'total_items'"'"']} fail={j['"'"'failed_items'"'"']}  {j.get('"'"'title'"'"','"'"''"'"')[:44]}")
' 2>/dev/null || echo "   (backend unreachable)"
  echo "   codec_alternative=false means an ALAC failure is a hard job failure."
  echo "   If it is true, check the backend log for 'fallback_from=alac' instead —"
  echo "   the job will look successful and quietly contain AAC-LC."
  echo
fi
