#!/usr/bin/env bash
#
# aerostream-ctl.sh — start / stop / restart / status / tail the AeroStream
# (Aerospike + stream engine) dev server.
#
# Usage:
#   ./aerostream-ctl.sh start      # launch asd in the background
#   ./aerostream-ctl.sh stop       # graceful shutdown (SIGTERM)
#   ./aerostream-ctl.sh restart    # stop then start
#   ./aerostream-ctl.sh status     # is it running?
#   ./aerostream-ctl.sh tail       # follow the log (Ctrl-C to detach)
#   ./aerostream-ctl.sh logs       # alias for tail
#
set -euo pipefail

# Resolve paths relative to this script, so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="$SCRIPT_DIR/aerospike-server"
ASD="$SERVER_DIR/target/Linux-x86_64/bin/asd"
CONF="$SERVER_DIR/as/etc/aerospike_dev.conf"
RUN_DIR="$SERVER_DIR/run"
LOG_FILE="$RUN_DIR/asd.log"
PID_FILE="$RUN_DIR/asd.pid"

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

err()  { printf '\033[31m%s\033[0m\n' "$*" >&2; }
ok()   { printf '\033[32m%s\033[0m\n' "$*"; }
info() { printf '%s\n' "$*"; }

# Echo the running PID (from the pid file) if the process is alive, else nothing.
running_pid() {
	[[ -f "$PID_FILE" ]] || return 0
	local pid
	pid="$(cat "$PID_FILE" 2>/dev/null || true)"
	[[ -n "$pid" ]] || return 0
	if kill -0 "$pid" 2>/dev/null; then
		printf '%s' "$pid"
	fi
}

ensure_dirs() {
	mkdir -p "$RUN_DIR/work/usr/udf/lua" "$RUN_DIR/log" "$RUN_DIR/work/smd"
}

# ----------------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------------

cmd_start() {
	local pid
	pid="$(running_pid)"
	if [[ -n "$pid" ]]; then
		err "Already running (pid $pid). Use 'restart' to bounce it."
		return 1
	fi

	if [[ ! -x "$ASD" ]]; then
		err "asd binary not found at: $ASD"
		err "Build it first:  cd $SERVER_DIR && make"
		return 1
	fi
	if [[ ! -f "$CONF" ]]; then
		err "Config not found at: $CONF"
		return 1
	fi

	ensure_dirs

	# asd must run from SERVER_DIR — the dev conf uses relative paths
	# (work-directory run/work, log file run/log/...).
	info "Starting asd ..."
	(
		cd "$SERVER_DIR"
		exec "$ASD" --config-file "$CONF" --foreground
	) >> "$LOG_FILE" 2>&1 &

	local new_pid=$!
	echo "$new_pid" > "$PID_FILE"

	# Give it a moment, then confirm it survived startup.
	sleep 2
	if kill -0 "$new_pid" 2>/dev/null; then
		ok "Started (pid $new_pid). Logs: $LOG_FILE"
		info "Follow logs with:  $0 tail"
	else
		err "asd exited during startup. Last log lines:"
		tail -n 20 "$LOG_FILE" >&2 || true
		rm -f "$PID_FILE"
		return 1
	fi
}

cmd_stop() {
	local pid
	pid="$(running_pid)"
	if [[ -z "$pid" ]]; then
		info "Not running."
		rm -f "$PID_FILE"
		return 0
	fi

	info "Stopping asd (pid $pid) ..."
	kill -TERM "$pid" 2>/dev/null || true

	# Wait up to 30s for a clean shutdown.
	for _ in $(seq 1 60); do
		if ! kill -0 "$pid" 2>/dev/null; then
			rm -f "$PID_FILE"
			ok "Stopped."
			return 0
		fi
		sleep 0.5
	done

	err "Did not stop after 30s; sending SIGKILL."
	kill -KILL "$pid" 2>/dev/null || true
	rm -f "$PID_FILE"
	ok "Killed."
}

cmd_status() {
	local pid
	pid="$(running_pid)"
	if [[ -n "$pid" ]]; then
		ok "Running (pid $pid)."
		info "Config: $CONF"
		info "Log:    $LOG_FILE"
	else
		info "Not running."
		return 1
	fi
}

cmd_tail() {
	if [[ ! -f "$LOG_FILE" ]]; then
		err "No log file yet at: $LOG_FILE"
		err "Start the server first:  $0 start"
		return 1
	fi
	info "Tailing $LOG_FILE  (Ctrl-C to detach, server keeps running)"
	tail -n 40 -f "$LOG_FILE"
}

cmd_restart() {
	cmd_stop
	cmd_start
}

usage() {
	cat >&2 <<EOF
AeroStream dev server control.

Usage: $0 {start|stop|restart|status|tail}

  start     Launch asd in the background (logs to run/asd.log)
  stop      Graceful shutdown via SIGTERM (SIGKILL after 30s)
  restart   stop then start
  status    Report whether asd is running
  tail      Follow the log file
EOF
	exit 2
}

# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

case "${1:-}" in
	start)        cmd_start ;;
	stop)         cmd_stop ;;
	restart)      cmd_restart ;;
	status)       cmd_status ;;
	tail|logs)    cmd_tail ;;
	*)            usage ;;
esac
