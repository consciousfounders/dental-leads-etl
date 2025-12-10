#!/bin/bash
# Deploy Dental Dashboard to VM with password protection
# Usage: ./scripts/deploy_dashboard.sh

set -e

# Configuration
VM_IP="${VM_IP:-YOUR_VM_IP}"  # Set this or pass as env var
VM_USER="${VM_USER:-zander}"
DOMAIN="data.consciousfounders.com"  # Subdomain for dashboard
DASHBOARD_PASSWORD="${DASHBOARD_PASSWORD:-onpharma2024}"  # Shared password
DASHBOARD_PORT=8502

echo "=============================================="
echo "🦷 DENTAL DASHBOARD DEPLOYMENT"
echo "=============================================="
echo ""

# Step 1: Check if we have VM IP
if [ "$VM_IP" == "YOUR_VM_IP" ]; then
    echo "❌ Please set VM_IP environment variable"
    echo "   Example: VM_IP=34.123.45.67 ./scripts/deploy_dashboard.sh"
    exit 1
fi

echo "📦 Step 1: Syncing dashboard files to VM..."
rsync -avz --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
    -e "ssh -o StrictHostKeyChecking=no" \
    dashboards/ utils/ requirements.txt \
    ${VM_USER}@${VM_IP}:~/dental-dashboard/

echo ""
echo "🔧 Step 2: Setting up VM environment..."
ssh ${VM_USER}@${VM_IP} << 'SETUP_EOF'
cd ~/dental-dashboard

# Install Python dependencies
pip3 install streamlit plotly pandas snowflake-connector-python --quiet

# Create systemd service for Streamlit
sudo tee /etc/systemd/system/dental-dashboard.service > /dev/null << 'SERVICE'
[Unit]
Description=Dental Market Intelligence Dashboard
After=network.target

[Service]
Type=simple
User=zander
WorkingDirectory=/home/zander/dental-dashboard
Environment="SKIP_SECRET_MANAGER=true"
Environment="SNOWFLAKE_ACCOUNT=xxx"
Environment="SNOWFLAKE_USER=xxx"
Environment="SNOWFLAKE_PASSWORD=xxx"
Environment="SNOWFLAKE_WAREHOUSE=COMPUTE_WH"
Environment="SNOWFLAKE_DATABASE=DENTAL_LEADS"
ExecStart=/usr/local/bin/streamlit run client_dashboard.py --server.port 8502 --server.headless true --server.address 127.0.0.1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

# Note: You'll need to edit the service file to add Snowflake credentials
echo "⚠️  Edit /etc/systemd/system/dental-dashboard.service to add Snowflake credentials"

sudo systemctl daemon-reload
sudo systemctl enable dental-dashboard
SETUP_EOF

echo ""
echo "🔐 Step 3: Setting up nginx with password protection..."
ssh ${VM_USER}@${VM_IP} << NGINX_EOF
# Install nginx if not present
sudo apt-get update -qq
sudo apt-get install -y nginx apache2-utils certbot python3-certbot-nginx -qq

# Create password file
echo "${DASHBOARD_PASSWORD}" | sudo htpasswd -ci /etc/nginx/.htpasswd onpharma

# Create nginx config
sudo tee /etc/nginx/sites-available/dental-dashboard > /dev/null << 'NGINX'
server {
    listen 80;
    server_name ${DOMAIN};

    location / {
        auth_basic "OnPharma Dashboard";
        auth_basic_user_file /etc/nginx/.htpasswd;
        
        proxy_pass http://127.0.0.1:8502;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 86400;
    }
}
NGINX

# Enable site
sudo ln -sf /etc/nginx/sites-available/dental-dashboard /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
NGINX_EOF

echo ""
echo "=============================================="
echo "✅ DEPLOYMENT COMPLETE"
echo "=============================================="
echo ""
echo "📋 NEXT STEPS:"
echo ""
echo "1. Add DNS record in Squarespace:"
echo "   Type: A"
echo "   Host: data"
echo "   Value: ${VM_IP}"
echo "   TTL: 1 hour"
echo ""
echo "2. SSH to VM and add Snowflake credentials:"
echo "   ssh ${VM_USER}@${VM_IP}"
echo "   sudo nano /etc/systemd/system/dental-dashboard.service"
echo "   # Add your SNOWFLAKE_* environment variables"
echo "   sudo systemctl daemon-reload"
echo "   sudo systemctl restart dental-dashboard"
echo ""
echo "3. Set up SSL (after DNS propagates):"
echo "   ssh ${VM_USER}@${VM_IP}"
echo "   sudo certbot --nginx -d ${DOMAIN}"
echo ""
echo "4. Access dashboard:"
echo "   URL: https://${DOMAIN}"
echo "   Username: onpharma"
echo "   Password: ${DASHBOARD_PASSWORD}"
echo ""

