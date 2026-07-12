# Flare on Windows

This guide covers setting up Flare on Windows — from download to running as a background service.

---

## Download

1. Go to the [releases page](https://github.com/blaspat/flare/releases)
2. Download `flare_<version>_windows_amd64.exe` (or `_windows_arm64.exe` for ARM devices like Surface Pro X)
3. Rename it to `flare.exe` and place it in `C:\flare\` (or any folder you prefer)

---

## Quick start

Open **Command Prompt** or **PowerShell** and navigate to your Flare folder:

```cmd
cd C:\flare
```

### Interactive setup

```cmd
flare.exe init
```

This walks you through:
- **Node name** — unique name for this machine (e.g. `my-laptop`)
- **Listen address** — default `:9721` is fine unless you have a port conflict
- **Data directory** — where Flare stores its state and synced files
- **Peers** — other Flare nodes to connect to (e.g. `ws://vpn-instance:9721` for your VPS, or `ws://192.168.1.50:9721` for a local machine)
- **Watch directories** — folders to keep in sync across the mesh
- **Cron jobs** — optional scheduled commands

Your config is written to `flare.toml`.

### Start in terminal

```cmd
flare.exe start
```

You'll see logs as Flare connects to peers and starts syncing. Leave the terminal open to keep it running.

### View the dashboard

Open **http://localhost:9722**) in your browser. Add credentials in `flare.toml` if you want password protection:

```toml
[node]
web_username = "admin"
web_password = "your-password"
```

---

## Run as a Windows service (recommended)

Running in a terminal is fine for testing. For always-on operation, use **NSSM** (Non-Sucking Service Manager) to run Flare as a proper Windows service that starts on boot.

### Install NSSM

Download from [nssm.cc/download](https://nssm.cc/download) — grab `nssm-2.24.zip` and extract `win64/nssm.exe` to `C:\Windows\system32\` (so it's available everywhere).

### Install the Flare service

```cmd
nssm install Flare "C:\flare\flare.exe" "start"
nssm set Flare AppDirectory "C:\flare"
nssm set Flare DisplayName "Flare Edge Mesh"
nssm set Flare Description "P2P edge mesh server for file sync and distributed cron"
nssm set Flare Start SERVICE_AUTO_START
nssm set Flare AppStdout "C:\flare\flare.log"
nssm set Flare AppStderr "C:\flare\flare.log"
nssm set Flare AppRotateFiles 1
nssm set Flare AppRotateSeconds 86400
```

### Start

```cmd
nssm start Flare
```

### Managing the service

| Action | Command |
|--------|---------|
| Stop | `nssm stop Flare` |
| Status | `nssm status Flare` |
| Restart | `nssm restart Flare` |
| Remove | `nssm remove Flare confirm` |
| Edit GUI | `nssm edit Flare` (opens a settings window) |

After this, Flare starts automatically on every boot. The dashboard is always available at **http://localhost:9722**.

---

## Alternative: Task Scheduler

If you prefer not to use NSSM, use Windows Task Scheduler:

```cmd
schtasks /create /tn "Flare" ^
  /tr "C:\flare\flare.exe start" ^
  /sc onstart ^
  /delay 0000:30 ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /f
```

This starts Flare 30 seconds after boot as the SYSTEM user.

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

1. Download the new `flare.exe`
2. Stop the service: `nssm stop Flare`
3. Replace `C:\flare\flare.exe`
4. Start the service: `nssm start Flare`

Your config and data are preserved.

---

## Troubleshooting

### "Access is denied" on service install

Run Command Prompt **as Administrator** before running `nssm install`.

### "Flare" is not recognized

Make sure you're in the right folder (`cd C:\flare`) or add the folder to your PATH:

```cmd
setx PATH "%PATH%;C:\flare"
```

Then restart your terminal.

### Can't see the dashboard

Make sure `web_port` is set in `flare.toml`. Default is 9722. Check `http://localhost:9722`.

### Firewall blocking

Windows Defender Firewall may ask for permission when Flare starts. Click **Allow access**. If you miss the prompt, add a rule manually:

```cmd
netsh advfirewall firewall add rule name="Flare" dir=in action=allow program="C:\flare\flare.exe"
```
