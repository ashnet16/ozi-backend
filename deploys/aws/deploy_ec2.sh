#!/bin/bash

set -euo pipefail

OZI_REPO="https://github.com/ashnet16/ozi.git"
SNAPCHAIN_REPO="https://github.com/farcasterxyz/snapchain.git"
INSTALL_DIR="$HOME/ozi-deploy"
PROTO_DEST="$INSTALL_DIR/ozi/ozi/proto"
SERVICE_NAME="ozi"

echo "Installing Docker..."
sudo apt-get update
sudo apt-get install -y \
    docker.io \
    docker-compose \
    python3-pip \
    python3-dev \
    gcc \
    git

sudo systemctl enable docker
sudo usermod -aG docker "$USER"

echo "Cloning repos..."
rm -rf "$INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

git clone "$OZI_REPO" ozi
git clone "$SNAPCHAIN_REPO" snapchain

# 3. Copy and Compile Proto Files
echo "Copying .proto files..."
mkdir -p "$PROTO_DEST"
cp snapchain/proto/*.proto "$PROTO_DEST"

echo "Installing gRPC tools..."
pip3 install grpcio grpcio-tools

echo "Compiling .proto files..."
python3 -m grpc_tools.protoc \
  -I "$PROTO_DEST" \
  --python_out="$PROTO_DEST" \
  --grpc_python_out="$PROTO_DEST" \
  "$PROTO_DEST"/*.proto

# 4. Create Ozi user and group
echo "Creating 'ozi' user and group..."
sudo groupadd --system ozi || true
sudo useradd --system --gid ozi --home "$INSTALL_DIR" --shell /usr/sbin/nologin ozi || true

# Give the ozi user access to Docker
sudo usermod -aG docker ozi

# Set ownership of the whole directory
sudo chown -R ozi:ozi "$INSTALL_DIR"

# 5. Start Docker Compose
cd ozi
echo "Building and launching Docker containers..."
docker-compose up --build -d

# 6. Create systemd service
echo "Setting up systemd service..."
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=Ozi Farcaster Analytics Service
After=network.target docker.service
Requires=docker.service

[Service]
User=$USER
WorkingDirectory=$INSTALL_DIR/ozi
ExecStart=/usr/bin/docker-compose up
ExecStop=/usr/bin/docker-compose down
Restart=always
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

echo "Enabling and starting systemd service..."
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl start "$SERVICE_NAME"

echo "Ozi deployed and running via systemd!"
