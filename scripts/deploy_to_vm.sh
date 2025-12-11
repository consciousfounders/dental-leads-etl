#!/bin/bash
# Deploy Dental Dashboard to GCP VM
# Usage: ./scripts/deploy_to_vm.sh

set -e

VM_IP="34.168.135.142"
VM_USER="zander"
SSH_KEY="/Users/infinitespace/.ssh/id_ed25519"
PROJECT_DIR="/home/zander/healthcare-leads-data"
DOMAIN="onpharma.consciousfounders.com"

echo "🚀 Deploying Dental Dashboard to $VM_IP..."

# SSH function
ssh_cmd() {
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$VM_USER@$VM_IP" "$@"
}

# SCP function  
scp_cmd() {
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$@"
}

# Step 1: Create project directory
echo "📁 Setting up project directory..."
ssh_cmd "mkdir -p $PROJECT_DIR/.streamlit"

# Step 2: Copy necessary files
echo "📦 Copying files..."
scp_cmd -r \
    dashboards \
    requirements.txt \
    Dockerfile \
    docker-compose.prod.yml \
    "$VM_USER@$VM_IP:$PROJECT_DIR/"

# Step 3: Copy secrets
echo "🔐 Copying secrets..."
scp_cmd .streamlit/secrets.toml "$VM_USER@$VM_IP:$PROJECT_DIR/.streamlit/"

# Step 4: Build and run Docker container
echo "🐳 Building and starting Docker container..."
ssh_cmd "cd $PROJECT_DIR && sudo docker compose -f docker-compose.prod.yml up -d --build"

# Step 5: Set up nginx
echo "🌐 Configuring nginx..."
ssh_cmd "sudo tee /etc/nginx/sites-available/dental-dashboard > /dev/null << 'EOF'
server {
    listen 80;
    server_name $DOMAIN;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \"upgrade\";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 86400;
    }
}
EOF"

ssh_cmd "sudo ln -sf /etc/nginx/sites-available/dental-dashboard /etc/nginx/sites-enabled/"
ssh_cmd "sudo rm -f /etc/nginx/sites-enabled/default"
ssh_cmd "sudo nginx -t && sudo systemctl reload nginx"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Add DNS A record: $DOMAIN → $VM_IP"
echo "   2. After DNS propagates, run SSL setup:"
echo "      ssh -i $SSH_KEY $VM_USER@$VM_IP"
echo "      sudo certbot --nginx -d $DOMAIN"
echo ""
echo "🌐 Dashboard available at: http://$VM_IP:8501"
echo "   (After DNS + SSL: https://$DOMAIN)"

