# Caddy server for serving PMTiles files
# PMTiles work with standard HTTP range requests, no plugin needed
FROM caddy:2.8.4-alpine

# Install envsubst for environment variable substitution
RUN apk add --no-cache gettext

# Copy Caddyfile template
COPY Caddyfile.template /etc/caddy/Caddyfile.template

# Copy entrypoint script and ensure Unix line endings
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    sed -i 's/\r$//' /entrypoint.sh

# Expose ports
EXPOSE 8080 8443

ENTRYPOINT ["/entrypoint.sh"]
CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile"]
