# Follow The Goat - Ubuntu/Linux Setup

## 🚀 Quick Start

The easiest way to start all services:

```bash
cd /root/follow_the_goat
./start_all.sh
```

To check status:
```bash
./check_status.sh
```

To stop all services:
```bash
./stop_all.sh
```

## 📋 Manual Start (Individual Services)

### Terminal 1 - Data Engine (NEVER restart)
```bash
source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py
```

### Terminal 2 - Website API (Can restart freely)
```bash
source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py
```

### Terminal 3 - Trading Logic (Can restart freely)
```bash
source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py
```

## 🌐 Access Points

- **Main Website**: http://195.201.84.5 (Nginx - Public)
- **Data Engine API**: http://127.0.0.1:5050/health (Internal)
- **Website API**: http://127.0.0.1:5051/health (Internal)
- **Webhook Server**: http://127.0.0.1:8001/webhook/health (Internal)

## 🔧 Nginx Management

The website files in `000website/` are automatically served by Nginx.

```bash
# Check Nginx status
systemctl status nginx

# Restart Nginx (after config changes)
systemctl restart nginx

# Reload Nginx (without downtime)
systemctl reload nginx

# Test configuration
nginx -t

# View error logs
tail -f /var/log/nginx/follow_the_goat_error.log
```

## 📺 Using Screen Sessions

View running sessions:
```bash
screen -ls
```

Attach to a session (to see live logs):
```bash
screen -r master        # View master.py logs
screen -r website_api   # View website_api.py logs
screen -r master2       # View master2.py logs
```

Detach from session (without stopping it):
- Press `Ctrl+A` then `D`

## 🔍 Health Checks

```bash
# Check Data Engine
curl http://127.0.0.1:5050/health | python3 -m json.tool

# Check Website API
curl http://127.0.0.1:5051/health | python3 -m json.tool

# Check table stats
curl http://127.0.0.1:5051/stats | python3 -m json.tool
```

## 🛠️ Troubleshooting

### Services won't start - "Address already in use"
```bash
# Kill processes on specific ports
fuser -k 5050/tcp 5051/tcp 8000/tcp 8001/tcp
```

### Website shows "API not available"
1. Check if website_api.py is running: `curl http://127.0.0.1:5051/health`
2. Check if master.py is running: `curl http://127.0.0.1:5050/health`
3. View logs: `screen -r website_api`

### Module not found error
```bash
source /root/follow_the_goat/venv/bin/activate
pip install <missing-module>
```

## 📁 Important Paths

- **Project Root**: `/root/follow_the_goat`
- **Virtual Env**: `/root/follow_the_goat/venv`
- **Website Files**: `/root/follow_the_goat/000website`
- **Nginx Config**: `/etc/nginx/sites-available/follow_the_goat`
- **Logs**: `/root/follow_the_goat/logs/`

## 🔐 Firewall (if needed)

To allow external access to APIs:
```bash
sudo ufw allow 80/tcp      # Nginx (already open)
sudo ufw allow 5051/tcp    # Website API
sudo ufw allow 8001/tcp    # Webhook
sudo ufw status
```

## 📖 Full Documentation

For complete details, see: `0000start_the_engine.txt`

