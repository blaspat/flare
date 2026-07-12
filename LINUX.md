# Flare on Linux

Covers Debian/Ubuntu, Fedora/RHEL, Arch, and generic Linux.

---

## Install

### Quick install (Debian/Ubuntu)

```bash
curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_linux_amd64 \
  -o /usr/local/bin/flare && chmod +x /usr/local/bin/flare
```

For **ARM64** (Raspberry Pi, Oracle ARM) replace `_linux_amd64` with `_linux_arm64`.

### Or download manually

1. Grab the right binary from the [releases page](https://github.com/blaspat/flare/releases)
2. Move it to your PATH and make executable:

```bash
sudo cp flare_linux_amd64 /usr/local/bin/flare
sudo chmod +x /usr/local/bin/flare
```

### Build from source

```bash
git clone https://github.com/blaspat/flare.git
cd flare
go build -o /usr/local/bin/flare .
```

---

## Setup

```bash
mkdir ~/.flare && cd ~/.flare
flare init
```

This creates `~/.flare/flare.toml`. Edit it as needed.

---

## Run

### Foreground (testing)

```bash
flare start
```

### Daemon (background)

```bash
flare start -d
```

### Systemd service (permanent)

```bash
sudo tee /etc/systemd/system/flare.service > /dev/null <<'EOF'
[Unit]
Description=Flare Edge Mesh Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/flare start
WorkingDirectory=/etc/flare
Restart=on-failure
RestartSec=10
User=root

[Install]
WantedBy=multi-user.target
EOF

sudo mkdir -p /etc/flare
sudo cp ~/.flare/flare.toml /etc/flare/
sudo systemctl daemon-reload
sudo systemctl enable --now flare
```

Check status: `sudo systemctl status flare`  
View logs: `sudo journalctl -u flare -f`

---

## Dashboard

Open **http://localhost:9722** in a browser. Set `web_username` and `web_password` in `flare.toml` for auth.

If you want it behind nginx (like the VPS):

```nginx
server {
    listen 443 ssl;
    server_name flare.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:9722/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_buffering off;
    }
}
```

---

## Firewall

### UFW (Ubuntu/Debian)

```bash
sudo ufw allow 9721/tcp  # mesh port
sudo ufw allow 9722/tcp  # dashboard (optional)
```

### firewalld (Fedora/RHEL)

```bash
sudo firewall-cmd --permanent --add-port=9721/tcp
sudo firewall-cmd --permanent --add-port=9722/tcp
sudo firewall-cmd --reload
```

---

## Updating

```bash
# Download new binary
sudo curl -sL https://github.com/blaspat/flare/releases/latest/download/flare_v0.2.0_linux_amd64 \
  -o /usr/local/bin/flare && sudo chmod +x /usr/local/bin/flare

# Restart service
sudo systemctl restart flare
```

---

## Uninstall

```bash
sudo systemctl stop flare
sudo systemctl disable flare
sudo rm /etc/systemd/system/flare.service
sudo systemctl daemon-reload
sudo rm /usr/local/bin/flare
sudo rm -rf /etc/flare
```
