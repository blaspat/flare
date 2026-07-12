# Flare on macOS

Covers both Intel and Apple Silicon Macs.

---

## Install

### Quick install

```bash
# Intel Mac
curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_darwin_amd64 \
  -o /usr/local/bin/flare && chmod +x /usr/local/bin/flare

# Apple Silicon (M1/M2/M3/M4)
curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_darwin_arm64 \
  -o /usr/local/bin/flare && chmod +x /usr/local/bin/flare
```

> **Note:** If `/usr/local/bin` doesn't exist, create it first: `sudo mkdir -p /usr/local/bin`

### Or download manually

Grab `flare_v0.2.0_darwin_amd64` (Intel) or `flare_v0.2.0_darwin_arm64` (Apple Silicon) from the [releases page](https://github.com/blaspat/flare/releases), then:

```bash
chmod +x ~/Downloads/flare_darwin_*
sudo mv ~/Downloads/flare_darwin_* /usr/local/bin/flare
```

### Build from source

```bash
git clone https://github.com/blaspat/flare.git
cd flare
go build -o /usr/local/bin/flare .
```

### Homebrew (if a tap is set up)

```bash
# Not yet available — PRs welcome!
```

---

## Gatekeeper

macOS may block the unsigned binary. The first time you run Flare, you might see:

> "flare" cannot be opened because the developer cannot be verified.

To fix:

```bash
sudo spctl --master-disable  # allow apps from anywhere (not recommended)
```

Or just right-click the binary in Finder → **Open** → click **Open** in the dialog. This registers an exception for that specific binary.

A cleaner alternative:

```bash
xattr -d com.apple.quarantine /usr/local/bin/flare
```

---

## Setup

```bash
mkdir -p ~/.flare && cd ~/.flare
flare init
```

This creates `~/.flare/flare.toml`.

---

## Run

### Foreground

```bash
flare start
```

### Background (daemon)

```bash
flare start -d
```

### LaunchAgent (login item — starts on every login)

```bash
mkdir -p ~/Library/LaunchAgents
cat > ~/Library/LaunchAgents/com.flare.daemon.plist <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.flare.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/flare</string>
        <string>start</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${HOME}/.flare</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${HOME}/.flare/flare.log</string>
    <key>StandardErrorPath</key>
    <string>${HOME}/.flare/flare.log</string>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.flare.daemon.plist
```

To stop: `launchctl unload ~/Library/LaunchAgents/com.flare.daemon.plist`  
To check: `launchctl list | grep flare`

---

## Dashboard

Open **http://localhost:9722** in your browser. Set `web_username` and `web_password` in `flare.toml` for auth.

---

## Updating

```bash
# Intel
sudo curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_darwin_amd64 \
  -o /usr/local/bin/flare && sudo chmod +x /usr/local/bin/flare

# Apple Silicon
sudo curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_darwin_arm64 \
  -o /usr/local/bin/flare && sudo chmod +x /usr/local/bin/flare

# Restart if running
launchctl unload ~/Library/LaunchAgents/com.flare.daemon.plist
launchctl load ~/Library/LaunchAgents/com.flare.daemon.plist
# Or: pkill flare && flare start -d
```

---

## Uninstall

```bash
launchctl unload ~/Library/LaunchAgents/com.flare.daemon.plist
rm ~/Library/LaunchAgents/com.flare.daemon.plist
sudo rm /usr/local/bin/flare
rm -rf ~/.flare
```
