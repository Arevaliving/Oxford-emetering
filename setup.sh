#!/bin/bash
set -e

REPO="https://raw.githubusercontent.com/Arevaliving/Oxford-emetering/main"
INSTALL_DIR="/home/oxford/oxford-sync"
WWW_DIR="$INSTALL_DIR/www"
DATA_DIR="$WWW_DIR/data"

echo "=== Oxford Suites Water Dashboard Setup ==="
echo ""

# 1. System packages
echo "[1/9] Installing system packages..."
apt-get update -qq
apt-get install -y -qq nginx python3 python3-venv apache2-utils curl

# 2. Create user and directories
echo "[2/9] Creating oxford user and directories..."
id oxford &>/dev/null || useradd -m -s /bin/bash oxford
mkdir -p "$INSTALL_DIR" "$WWW_DIR" "$DATA_DIR" "$INSTALL_DIR/archive" "$INSTALL_DIR/logs"
chown -R oxford:oxford /home/oxford/oxford-sync

# 3. Download project files
echo "[3/9] Downloading project files from GitHub..."
curl -fsSL "$REPO/index.html"     -o "$WWW_DIR/index.html"
curl -fsSL "$REPO/sync.py"        -o "$INSTALL_DIR/sync.py"
curl -fsSL "$REPO/Suite-Meter_List_Oxford.xlsx" -o "$INSTALL_DIR/Suite-Meter_List_Oxford.xlsx"
chown oxford:oxford "$WWW_DIR/index.html" "$INSTALL_DIR/sync.py" "$INSTALL_DIR/Suite-Meter_List_Oxford.xlsx"

# 4. Python venv + dependencies
echo "[4/9] Setting up Python virtual environment..."
sudo -u oxford python3 -m venv "$INSTALL_DIR/venv"
sudo -u oxford "$INSTALL_DIR/venv/bin/pip" install -q --upgrade pip
sudo -u oxford "$INSTALL_DIR/venv/bin/pip" install -q paramiko pandas openpyxl

# 5. nginx config
echo "[5/9] Configuring nginx..."
cat > /etc/nginx/sites-available/oxford <<'NGINXCONF'
server {
    listen 80 default_server;
    server_name _;

    root /home/oxford/oxford-sync/www;
    index index.html;

    auth_basic "Oxford Suites — Areva Living";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /data/ {
        add_header Cache-Control "no-store, no-cache, must-revalidate";
        add_header Pragma "no-cache";
        expires 0;
    }
}
NGINXCONF

rm -f /etc/nginx/sites-enabled/default
ln -sf /etc/nginx/sites-available/oxford /etc/nginx/sites-enabled/oxford

# 6. HTTP Basic Auth password
echo "[6/9] Setting dashboard password..."
echo ""
echo ">>> Choose a password for the dashboard (username: areva)"
htpasswd -c /etc/nginx/.htpasswd areva
echo ""

# 7. Test nginx and reload
echo "[7/9] Testing and reloading nginx..."
nginx -t
systemctl enable nginx
systemctl reload nginx

# 8. Install crontab for oxford user
echo "[8/9] Installing cron jobs..."
cat > /tmp/oxford_cron <<'CRONTAB'
# Oxford water sync — every 2 hours at :30
30 0,2,4,6,8,10,14,16,18,20,22 * * * /home/oxford/oxford-sync/venv/bin/python3 /home/oxford/oxford-sync/sync.py >> /home/oxford/oxford-sync/logs/sync.log 2>&1
# Force sync at noon
30 12 * * * /home/oxford/oxford-sync/venv/bin/python3 /home/oxford/oxford-sync/sync.py --force >> /home/oxford/oxford-sync/logs/sync.log 2>&1
# Weekly log rotation (Sunday 03:00)
0 3 * * 0 cp /home/oxford/oxford-sync/logs/sync.log /home/oxford/oxford-sync/logs/sync.log.$(date +\%Y\%m\%d) && truncate -s 0 /home/oxford/oxford-sync/logs/sync.log
CRONTAB
crontab -u oxford /tmp/oxford_cron
rm /tmp/oxford_cron

# 9. Initial data sync
echo "[9/9] Running initial data sync..."
sudo -u oxford "$INSTALL_DIR/venv/bin/python3" "$INSTALL_DIR/sync.py" --force \
  && echo "✓ Initial sync complete" \
  || echo "⚠  Initial sync failed — check SFTP connectivity. Dashboard will show seed data until sync succeeds."

echo ""
echo "==================================================="
echo "✓ Setup complete!"
echo "   Dashboard: http://143.198.76.191"
echo "   Username:  areva"
echo "   Logs:      $INSTALL_DIR/logs/sync.log"
echo "==================================================="
