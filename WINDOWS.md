# Flare on Windows

The easiest way to run Flare on Windows — one command to install, start, and browse.

---

## Download

1. Go to the [releases page](https://github.com/blaspat/flare/releases)
2. Download `flare_<version>_windows_amd64.exe` (or `_windows_arm64.exe` for ARM devices like Surface Pro X)
3. Rename it to `flare.exe` and place it in `C:\flare\` (or any folder you prefer)

---

## One-command setup

Open **Command Prompt as Administrator** (right-click → Run as administrator), then:

```cmd
cd C:\flare
flare.exe start
```

That's it. Flare will:

1. **Download NSSM** (Non-Sucking Service Manager) automatically if needed
2. **Install itself as a Windows service** — auto-starts on boot
3. **Start the service** and open the dashboard in your browser
4. Exit — Flare is running in the background

Your terminal is free. No need to leave it open.

### Setup wizard

Open **http://localhost:9722** in your browser:
- Set your **node name** (e.g. `my-laptop`)
- Add **peers** to connect to (e.g. `ws://your-vps.local:9721`)
- Add **watch directories** to sync
- Click Save

After saving, restart with `flare.exe stop && flare.exe start` to apply mesh settings.

---

## Commands

| Command | What it does |
|---------|-------------|
| `flare.exe start` | Install service + start if needed, then exit |
| `flare.exe stop` | Stop the Windows service |
| `flare.exe install` | Install/reinstall the service (downloads NSSM if needed) |
| `flare.exe uninstall` | Stop and remove the service |
| `flare.exe status` | Show node and mesh status |
| `flare.exe dashboard` | Open the web dashboard in browser |
| `flare.exe run <job>` | Run a cron job immediately |

All commands except `start` work whether or not the service is running.

---

## View the dashboard

After starting, open **http://localhost:9722** in any browser.

The first time you visit, the **setup wizard** walks you through config. After that, it shows the live dashboard with peers, sync status, and cron jobs.

To enable password protection, set credentials in the setup wizard or manually in `flare.toml`:

```toml
[node]
web_username = "admin"
web_password = "your-password"
```

---

## Configuration

A typical `C:\flare\flare.toml` connecting to a VPS node looks like:

```toml
[node]
name = "my-laptop"
listen = ":9721"
data_dir = "./data"
web_port = 9722

[mesh]
peers = ["ws://your-vps.local:9721"]
discovery = "static"

[sync]
watch_dirs = [
  { path = "C:\\Users\\You\\FlareSync", tag = "default" },
]
poll_interval = "5s"
```

### Path notes

- Use either `C:\Users\You\Path` or `C:/Users/You/Path` (Go accepts both)
- The `data_dir` is relative to your working directory (`C:\flare\`)
- Watch directory paths can be absolute or relative

---

## Updating

```cmd
flare.exe stop
:: Replace flare.exe with the new version
copy /Y new-flare.exe flare.exe
flare.exe start
```

Your config and data are preserved.

---

## Troubleshooting

### "Access is denied"

Run **Command Prompt as Administrator** before running `flare start` or `flare install`.

### "Flare" is not recognized

Make sure you're in the right folder (`cd C:\flare`) or add it to your PATH:

```cmd
setx PATH "%PATH%;C:\flare"
```

Then restart your terminal.

### Can't see the dashboard

Check **http://localhost:9722**. The dashboard is enabled by default (port 9722).

### Firewall blocking

Windows Defender Firewall may ask for permission when Flare starts. Click **Allow access**. If you miss the prompt, add a rule manually:

```cmd
netsh advfirewall firewall add rule name="Flare" dir=in action=allow program="C:\flare\flare.exe"
```
