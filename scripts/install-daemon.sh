#!/bin/bash
# research-radar daemon installer — sets up a launchd agent that polls
# for scan jobs every 300 seconds.
set -euo pipefail

BIN="${HOME}/.local/bin/research-radar"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST="${PLIST_DIR}/com.openclaw.research-radar.scan-worker.plist"
LOG_DIR="${HOME}/.research-radar/logs"

mkdir -p "${PLIST_DIR}" "${LOG_DIR}"

if [[ ! -x "${BIN}" ]]; then
  echo "error: ${BIN} not found or not executable" >&2
  echo "  run: cargo build --release && cp target/release/research-radar ${BIN}" >&2
  exit 1
fi

# Unload existing agent if present
launchctl unload "${PLIST}" 2>/dev/null || true

cat > "${PLIST}" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.openclaw.research-radar.scan-worker</string>
  <key>ProgramArguments</key>
  <array>
    <string>${BIN}</string>
    <string>scan-worker</string>
    <string>--poll-interval</string>
    <string>300</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/scan-worker.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/scan-worker.err</string>
</dict>
</plist>
EOF

launchctl load "${PLIST}"
echo "Daemon installed and started."
echo "  binary: ${BIN}"
echo "  plist:  ${PLIST}"
echo "  logs:   ${LOG_DIR}/"
echo "  status: launchctl list | grep research-radar"
