# Flare on Windows

Download, double-click, done.

---

## Download

1. Go to the [releases page](https://github.com/blaspat/flare/releases)
2. Download `flare_<version>_windows_amd64.exe` (or `_windows_arm64.exe` for ARM devices like Surface Pro X)
3. Place it anywhere — `C:\flare\`, your desktop, wherever

---

## Setup

**Double-click `flare.exe`.**

Windows will prompt "Do you want to allow this app to make changes to your device?" — click **Yes**. That's the UAC prompt asking for admin rights to install the service.

Flare will:

1. **Download NSSM** automatically if needed
2. **Install itself as a Windows service** — runs in the background, starts on boot
3. **Start the service**
4. **Open your browser** to the setup wizard at `http://localhost:9722`
5. Show "Press any key to exit" — close the window, Flare is running

That's it. No terminal, no commands, no config files to touch.

### If you prefer the terminal

```cmd
cd C:\flare
flare.exe start
```

Same result — the UAC prompt asks once, then it installs, starts, and opens the browser.

---

## Setup wizard

Open **http://localhost:9722** in your browser:
- Set your **node name** (e.g. `my-laptop`)
- Add **peers** to connect to (e.g. `ws://your-vps.local:9721`)
- Add **watch directories** to sync
- Click Save

After saving, restart with `flare.exe stop` then `flare.exe start` to apply mesh settings.

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

If double-clicking doesn't prompt for admin, right-click `flare.exe` → **Run as administrator**.

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
