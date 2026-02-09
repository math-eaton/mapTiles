# Cloudflare Infrastructure for PMTiles

This directory contains the Cloudflare Worker implementation for serving PMTiles from R2 object storage.

## Overview

The Worker provides:
- ✅ **Efficient Range request handling** for PMTiles
- ✅ **Edge caching** for optimal performance
- ✅ **CORS support** for cross-origin requests
- ✅ **TileJSON generation** for MapLibre GL JS
- ✅ **Zero egress fees** with R2 storage

See [Protomaps Docs: Deploy on Cloudflare](https://docs.protomaps.com/deploy/cloudflare) for official documentation.

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure

Edit `wrangler.toml`:

```toml
name = "pmtiles-cloudflare"

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "your-bucket-name"        # Production
preview_bucket_name = "your-bucket-name" # Dev/Preview

[vars]
ALLOWED_ORIGINS = "*"  # Change to your domain in production
```

### 3. Create R2 Bucket

```bash
wrangler r2 bucket create your-bucket-name
```

### 4. Upload PMTiles

```bash
# Using the upload script (from project root)
cd ../scripts
.\upload-pmtiles.ps1 -BucketName your-bucket-name

# Or manually
wrangler r2 object put your-bucket-name/base.pmtiles --file ../../data/3-pmtiles/base.pmtiles
```

### 5. Deploy

```bash
npm run deploy
```

## Development

### Wrangler (Local Testing)

Run `npm run start` to serve your Worker at http://localhost:8787. The cache will not be active in development.

Test endpoints:
```bash
# TileJSON
curl http://localhost:8787/base.json

# Tile
curl -I http://localhost:8787/base/0/0/0.pbf
```

### Web Console (Basic)

Generate the Workers script using `npm run build` and copy `dist/index.js` to the Cloudflare Dashboard editor.

## Configuration

### Environment Variables

Set in `wrangler.toml` under `[vars]`:

- `ALLOWED_ORIGINS`: CORS allowed origins (comma-separated or `*`)
- `CACHE_CONTROL`: Cache-Control header (default: `public, max-age=86400`)
- `PMTILES_PATH`: Custom PMTiles path pattern (default: `{name}.pmtiles`)
- `PUBLIC_HOSTNAME`: Override hostname in TileJSON URLs

### Multiple Environments

Use Wrangler environments for staging/production:

```toml
[env.staging]
name = "pmtiles-staging"
[[env.staging.r2_buckets]]
binding = "BUCKET"
bucket_name = "tiles-staging"

[env.production]
name = "pmtiles-production"
[[env.production.r2_buckets]]
binding = "BUCKET"
bucket_name = "tiles-production"
```

Deploy to specific environment:
```bash
wrangler deploy --env production
```

## Documentation

For detailed setup and deployment instructions, see:
- [../../DEPLOYMENT.md](../../DEPLOYMENT.md) - Complete deployment guide
- [../../QUICKSTART.md](../../QUICKSTART.md) - Quick reference
- [Protomaps Cloudflare Docs](https://docs.protomaps.com/deploy/cloudflare)
- [Cloudflare Workers](https://developers.cloudflare.com/workers/)
- [Cloudflare R2](https://developers.cloudflare.com/r2/)

