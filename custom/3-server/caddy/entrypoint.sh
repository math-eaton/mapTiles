#!/bin/sh
set -e

# Set defaults
export TILES_DOMAIN="${TILES_DOMAIN:-localhost:8080}"
export TILES_SERVE_DOMAIN="${TILES_SERVE_DOMAIN:-localhost}"
export TILES_EMAIL="${TILES_EMAIL:-admin@localhost}"
export PMTILES_PATH="${PMTILES_PATH:-/tiles}"
export MARTIN_UPSTREAM="${MARTIN_UPSTREAM:-http://martin:3000}"

echo "========================================="
echo "Caddy PMTiles Server Configuration"
echo "========================================="
echo "Domain: ${TILES_DOMAIN}"
echo "Serve Domain: ${TILES_SERVE_DOMAIN}"
echo "Email: ${TILES_EMAIL}"
echo "PMTiles Path: ${PMTILES_PATH}"
echo "Martin Upstream: ${MARTIN_UPSTREAM}"
echo "========================================="

# Process Caddyfile template with environment variables
envsubst '${TILES_DOMAIN} ${TILES_SERVE_DOMAIN} ${TILES_EMAIL} ${PMTILES_PATH} ${MARTIN_UPSTREAM}' \
    < /etc/caddy/Caddyfile.template > /etc/caddy/Caddyfile

echo "Generated Caddyfile:"
echo "-----------------------------------------"
cat /etc/caddy/Caddyfile
echo "-----------------------------------------"

# Execute the command
exec "$@"
