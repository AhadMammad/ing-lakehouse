#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  setup-certs.sh — generate mkcert SSL certificates for RustFS
#
#  Prerequisites:
#    macOS:  brew install mkcert && mkcert -install
#    Ubuntu: sudo apt install libnss3-tools
#            curl -Lo /usr/local/bin/mkcert \
#              https://github.com/FiloSottile/mkcert/releases/latest/download/mkcert-v1.4.4-linux-amd64
#            chmod +x /usr/local/bin/mkcert && mkcert -install
#
#  Usage:  bash scripts/setup-certs.sh
#          make setup-certs
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
CERTS_DIR="$REPO_ROOT/certs"

# Read RUSTFS_DOMAIN from .env if present, fall back to default
DOMAIN="rustfs.lakehouse.local"
if [ -f "$ENV_FILE" ]; then
  env_domain=$(grep -E '^RUSTFS_DOMAIN=' "$ENV_FILE" | cut -d= -f2 | tr -d '[:space:]')
  [ -n "$env_domain" ] && DOMAIN="$env_domain"
fi

# Check mkcert is available
if ! command -v mkcert &>/dev/null; then
  echo "ERROR: mkcert not found."
  echo ""
  echo "  macOS:  brew install mkcert && mkcert -install"
  echo "  Ubuntu: sudo apt install libnss3-tools"
  echo "          curl -Lo /usr/local/bin/mkcert \\"
  echo "            https://github.com/FiloSottile/mkcert/releases/latest/download/mkcert-v1.4.4-linux-amd64"
  echo "          chmod +x /usr/local/bin/mkcert && mkcert -install"
  exit 1
fi

mkdir -p "$CERTS_DIR"

echo "Generating certificate for: $DOMAIN, localhost, 127.0.0.1"
mkcert \
  -cert-file "$CERTS_DIR/rustfs.pem" \
  -key-file  "$CERTS_DIR/rustfs-key.pem" \
  "$DOMAIN" localhost 127.0.0.1

echo ""
echo "✔  Certs written:"
echo "     $CERTS_DIR/rustfs.pem"
echo "     $CERTS_DIR/rustfs-key.pem"
echo ""
echo "──────────────────────────────────────────────"
echo "  Add to /etc/hosts (run once, requires sudo):"
echo ""
echo "    echo '127.0.0.1  $DOMAIN' | sudo tee -a /etc/hosts"
echo ""
echo "  Then access RustFS at:"
echo "    S3 API   → https://$DOMAIN:9000"
echo "    Console  → https://$DOMAIN:9001"
echo "──────────────────────────────────────────────"
