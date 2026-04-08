# research-radar Setup

## Quick Start

### 1. Build
```bash
cargo build --release
```

### 2. Install daemon
```bash
~/.research-radar/install-daemon.sh
```

### 3. Configure environment
```bash
export ANTHROPIC_API_KEY=sk-ant-...   # for LLM scoring (optional, uses keyword scoring if absent)
export DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...  # for notifications (optional)
```

### 4. Create a profile
```bash
# Via MCP JSON-RPC (preferred):
echo '{"jsonrpc":"2.0","id":null,"method":"profile_create","params":{"name":"AI Research","keywords":["AI","machine learning","safety","alignment","Rust","compilers","systems"],"score_threshold":0.4}}' | research-radar mcp
```

### 5. Run first scan
```bash
research-radar scan-once
```

### 6. Start daemon (auto-runs on login after install)
```bash
launchctl start com.openclaw.research-radar.scan-worker
```

## Daemon

The launchd plist is installed at:
```
~/Library/LaunchAgents/com.openclaw.research-radar.scan-worker.plist
```

It polls every 300 seconds for new scan jobs. Logs go to:
```
~/.research-radar/logs/scan-worker.log
```

Check status:
```bash
launchctl list | grep research-radar
```

## MCP Tools

The MCP JSON-RPC server (`research-radar mcp`) exposes these tools:

| Method | Description |
|--------|-------------|
| `profile_create` | Create a new scanning profile |
| `profile_update` | Update an existing profile |
| `scan_now` | Trigger a scan for a profile |
| `scan_poll` | Poll a scan job's status |
| `matches_list` | List actionable matches |
| `match_get` | Get a specific match by ID |
| `subscription_set` | Set a notification subscription |
| `source_health` | Get source health summary |

## Data

- **SQLite:** `~/.research-radar/data.db`
- **LanceDB:** `~/.research-radar/lance/`
- **Binary:** `~/.local/bin/research-radar`

## Uninstall daemon
```bash
launchctl unload ~/Library/LaunchAgents/com.openclaw.research-radar.scan-worker.plist
```
