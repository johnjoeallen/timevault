#!/bin/sh
# Run this ON a Timevault backup-source host (e.g. spitfire) to see what the
# cold-boot "was this host actually used?" check sees: the clock, the activity
# window, and every systemd-logind session inside it.
#
# Read-only. Run it as root (Timevault SSHes in as root), right after the host
# has been woken for a backup so /proc/uptime still reflects that boot.
#
#   sudo ./diagnose-cold-boot.sh              # 24h window, 300s minimum session
#   sudo ./diagnose-cold-boot.sh 172800       # arg 1: seconds of history to scan
#   sudo ./diagnose-cold-boot.sh 86400 30     # arg 2: minimum session seconds
#                                             #  (matches timevault --min-session-seconds)
set -eu

WINDOW_SECONDS="${1:-86400}"   # Timevault uses 24h
MIN_SESSION="${2:-300}"        # Timevault's remote.minimumSessionSeconds default
BUFFER_SECONDS=600             # Timevault trims 10 min at each end

now=$(date +%s)
uptime_s=$(cut -d. -f1 /proc/uptime)
boot=$((now - uptime_s))
win_start=$((boot - WINDOW_SECONDS + BUFFER_SECONDS))
win_end=$((boot - BUFFER_SECONDS))

sessions_raw=$(mktemp)
sessions_tsv=$(mktemp)
trap 'rm -f "$sessions_raw" "$sessions_tsv"' EXIT

hr() { printf '\n===== %s =====\n' "$1"; }
at() { date -d "@$1" '+%Y-%m-%d %H:%M:%S %z' 2>/dev/null || date -r "$1" 2>/dev/null || echo "@$1"; }
hms() { # seconds -> "Hh MMm SSs"
  s=${1:-0}; [ "$s" -lt 0 ] && s=0
  printf '%dh %02dm %02ds' $((s/3600)) $(((s%3600)/60)) $((s%60))
}

hr "host / clock"
echo "hostname:        $(hostname)"
echo "date now:        $(at "$now")  (epoch $now)"
echo "uptime:          ${uptime_s}s  -> booted $(at "$boot")"
if command -v timedatectl >/dev/null 2>&1; then
  timedatectl | sed 's/^/  /'
fi
echo
echo "Compare 'date now' above against the machine you run Timevault from."
echo "If they differ by more than a few seconds, the clock is drifting (NTP)."

hr "journal sanity"
me=$(id -un)
if [ "$me" != root ] && [ -z "$(journalctl -q -n1 -o cat _TRANSPORT=kernel 2>/dev/null)" ]; then
  echo "NOTE: running as '$me', which cannot read the system journal, so the"
  echo "sections below will be empty. Timevault SSHes in as root -- re-run:"
  echo "  sudo $0 $*"
fi

hr "activity window Timevault would inspect"
echo "window start:    $(at "$win_start")  (@$win_start)"
echo "window end:      $(at "$win_end")  (@$win_end)"
echo "boot:            $(at "$boot")  (@$boot)"
echo "minimum session: ${MIN_SESSION}s"
echo "a session counts only if it STARTS in [window start .. window end],"
echo "lasts >= ${MIN_SESSION}s, and ties to a non-Timevault SSH login."
echo
if [ -n "$(journalctl -q -n1 -o cat --since "@$win_start" --until "@$boot" 2>/dev/null)" ]; then
  echo "journal coverage: OK (has records inside the window)"
else
  echo "journal coverage: EMPTY -- the journal keeps nothing from before this"
  echo "boot (not persistent across reboots?). Timevault would back up anyway"
  echo "with a warning rather than power the host off. Set Storage=persistent"
  echo "or create /var/log/journal."
fi

hr "logind session lifecycle in the window (raw, for reference)"
journalctl --quiet --no-pager -o short-unix \
  --since "@$win_start" --until "@$boot" \
  SYSLOG_IDENTIFIER=systemd-logind --grep='session' || echo "(none)"

hr "reconstructed session durations (per boot)"
echo "how long each session was active. Session ids RESTART every boot, so each"
echo "row is one (boot id, session id) pair."
echo
# -o json: one entry per line (no NUL/binary-field headaches), with _BOOT_ID
# and the structured SESSION_ID / USER_ID that logind attaches to its
# session-create (8d45620c...) and session-remove (3354939424...) events.
journalctl --quiet --no-pager -o json \
  --since "@$win_start" --until "@$boot" \
  MESSAGE_ID=8d45620c1a4348dbb17410da57c60c66 + \
  MESSAGE_ID=3354939424b4456d9802ca8333ed424a > "$sessions_raw" || true

awk -v now="$now" -v boot="$boot" '
  function jval(line, key,   s, p) {
    s = "\"" key "\":\""; p = index(line, s)
    if (p == 0) return ""
    line = substr(line, p + length(s))
    return substr(line, 1, index(line, "\"") - 1)
  }
  {
    mid = jval($0, "MESSAGE_ID")
    ts  = int(jval($0, "__REALTIME_TIMESTAMP") / 1000000)
    bid = substr(jval($0, "_BOOT_ID"), 1, 8)
    id  = jval($0, "SESSION_ID")
    usr_ = jval($0, "USER_ID")
    if (id == "" || ts == 0) next
    key = bid "#" id
    if (mid == "8d45620c1a4348dbb17410da57c60c66") {
      if (!(key in start) || ts < start[key]) start[key] = ts
      bootof[key] = bid; idof[key] = id; if (usr_ != "") usr[key] = usr_
    } else if (mid == "3354939424b4456d9802ca8333ed424a") {
      if (!(key in end) || ts > end[key]) { end[key] = ts; closed[key] = 1 }
      if (!(key in start)) { bootof[key] = bid; idof[key] = id; if (usr_ != "") usr[key] = usr_ }
    }
  }
  END {
    n = 0; for (k in bootof) K[n++] = k
    for (a = 0; a < n; a++) {
      sk[K[a]] = (K[a] in start) ? start[K[a]] : end[K[a]]
    }
    for (a = 0; a < n; a++) for (b = a + 1; b < n; b++)
      if (sk[K[a]] > sk[K[b]]) { t = K[a]; K[a] = K[b]; K[b] = t }
    for (a = 0; a < n; a++) seen[idof[K[a]]]++
    for (a = 0; a < n; a++) {
      k = K[a]
      if (!(k in start)) { s = end[k]; e = end[k]; st = "closed(no New)"; act = 0 }
      else if (k in closed) { s = start[k]; e = end[k]; st = "closed"; act = e - s }
      else { s = start[k]; e = now; st = "open"; act = now - s }
      cred = (e < boot ? e : boot) - s; if (cred < 0) cred = 0
      flag = (st == "open") ? "no-Removed-line;credited-to-boot" : "-"
      printf "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\n", \
        idof[k], (k in usr ? usr[k] : "?"), bootof[k], st, s, e, act, cred, flag
    }
  }
' "$sessions_raw" > "$sessions_tsv" || true

if [ -s "$sessions_tsv" ]; then
  while IFS='	' read -r id user bid state s e active credited flag; do
    if   [ "$s" -ge "$win_start" ] && [ "$s" -le "$win_end" ]; then inwin="START in window"
    elif [ "$s" -lt "$win_start" ];                           then inwin="START before window"
    else                                                          inwin="START at/after boot"
    fi
    if [ "$state" = open ]; then endtxt="still open (as of now)"; else endtxt="ended   $(at "$e")"; fi
    if [ "$credited" -ge "$MIN_SESSION" ]; then longenough="lasts >= ${MIN_SESSION}s"
    else longenough="under ${MIN_SESSION}s"; fi
    printf '  session %-4s user %-13s boot %s   %s\n' "$id" "$user" "$bid" "$inwin"
    printf '      started %s\n' "$(at "$s")"
    printf '      %s\n' "$endtxt"
    printf '      active %s   |   Timevault credits %s   (%s)' \
      "$(hms "$active")" "$(hms "$credited")" "$longenough"
    [ "$flag" = "-" ] || printf '   [%s]' "$flag"
    printf '\n\n'
  done < "$sessions_tsv"
  if [ -n "$(cut -f1 "$sessions_tsv" | sort | uniq -d)" ]; then
    echo "  Note: a session number appears under more than one boot id above."
    echo "  Timevault keys sessions by (boot id, session id), so those stay"
    echo "  separate (older builds merged them into one long session)."
  fi
else
  echo "  (no New session records in range)"
fi

hr "session details: class / leader / seat / remote host"
# MESSAGE_ID for logind "New session ..." events.
journalctl --no-pager -o verbose \
  --since "@$win_start" --until "@$boot" \
  MESSAGE_ID=8d45620c1a4348dbb17410da57c60c66 2>/dev/null \
  | grep -E '^(SESSION_ID|USER_ID|LEADER|SEAT_ID|_HOSTNAME|__REALTIME_TIMESTAMP|MESSAGE)=' \
  || echo "(no structured session-start records in range)"

hr "SSH logins in the window (source addresses)"
journalctl --quiet --no-pager -o short-unix \
  --since "@$win_start" --until "@$boot" \
  _COMM=sshd _COMM=sshd-session \
  --grep='Accepted |Connection from |session opened' || true

hr "interactive logins from wtmp (independent of the journal)"
if command -v last >/dev/null 2>&1; then
  last -Faxi 2>/dev/null | head -30
else
  echo "(no 'last' command)"
fi

hr "sessions alive right now"
loginctl list-sessions 2>/dev/null || true
for u in $(loginctl list-users --no-legend 2>/dev/null | awk '{print $2}'); do
  printf '  linger %-12s ' "$u"
  loginctl show-user "$u" -p Linger --value 2>/dev/null || echo '?'
done

hr "recent boots (did it really power off?)"
journalctl --list-boots 2>/dev/null | tail -6 || true

hr "verdict hint"
echo "Timevault counts a session as 'a person used the host' only if ALL hold:"
echo "  - its START is between window start and window end"
echo "  - it lasts >= ${MIN_SESSION}s (remote.minimumSessionSeconds)"
echo "  - it lines up in time (same boot, same user) with an 'Accepted' SSH"
echo "    login above whose source address is NOT the Timevault host's"
echo "Sessions are keyed by (boot id, session id), so a number reused across"
echo "boots is never stitched into one long session. Greeter (Debian-gdm),"
echo "'systemd --user' manager, @reboot/cron and Timevault's own rsync/probe"
echo "sessions never count."
