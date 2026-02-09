# MapTiles - Web Mapping Application

A high-performance web mapping application using PMTiles, MapLibre GL JS, and Cloudflare infrastructure.

## Features

- 🗺️ **Vector tile basemap** with Overture Maps data
- 📦 **PMTiles** for efficient tile storage and delivery
- ⚡ **Cloudflare Workers + R2** for edge-cached tile serving
- 🌐 **Multi-environment support** (local dev, staging, production)
- 🎨 **Custom cartography** with MapLibre GL JS
- 📊 **Topographic contours** and hillshade
- 🏗️ **3D building extrusions**

## Quick Start

### For Production Deployment

See [QUICKSTART.md](QUICKSTART.md) for rapid deployment to Cloudflare.

### For Local Development

```bash
# Clone the repository
git clone <your-repo-url>
cd mapTiles

# Set up the viewer
cd 2-viewer
npm install

# Copy environment template
cp ../.env.example .env.development

# Start development server
npm run dev
```

Visit http://localhost:3000 to see your map!

## Project Structure

```
mapTiles/
├── 1-processing/          # PMTiles generation pipeline
│   ├── scripts/          # Data processing scripts
│   └── utilities/        # Analysis tools
├── 2-viewer/             # Web mapping application
│   ├── src/              # Source code (JS, CSS)
│   └── public/           # Static assets (fonts, sprites, styles)
├── 3-server/             # Local development server (Caddy)
├── 4-cloud/              # Cloud infrastructure
│   ├── cloudflare/       # Cloudflare Worker for PMTiles
│   └── scripts/          # Deployment scripts
└── data/                 # Map data
    ├── 1-input/          # Raw data sources
    ├── 2-scratch/        # Intermediate files
    └── 3-pmtiles/        # Generated PMTiles archives
```

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Quick reference for Cloudflare deployment
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Comprehensive deployment guide
- **[.env.example](.env.example)** - Environment configuration reference
- **[4-cloud/cloudflare/README.md](4-cloud/cloudflare/README.md)** - Worker documentation

## Environment Configuration

This project uses environment-based configuration for seamless development and production workflows.

### Development (.env.development)

```env
VITE_ENVIRONMENT=development
VITE_PMTILES_SOURCE=local
```

Uses local PMTiles files from `data/3-pmtiles/` or Caddy server.

### Production (.env.production)

```env
VITE_ENVIRONMENT=production
VITE_PMTILES_SOURCE=cloudflare
VITE_CLOUDFLARE_WORKER_URL=https://your-worker.workers.dev
```

Uses Cloudflare Worker + R2 for edge-cached tile delivery.

See [.env.example](.env.example) for all available options.

## Deployment Options

### Option 1: Cloudflare Pages + Workers (Recommended)

Best for production with global edge caching and no egress fees.

```bash
# Deploy Worker
cd 4-cloud/cloudflare
npm run deploy

# Build viewer
cd ../../2-viewer
npm run build

# Deploy to Cloudflare Pages
npx wrangler pages deploy dist --project-name=your-map
```

### Option 2: GitHub Pages

Good for simple hosting, but has limitations with PMTiles byte-serving.

```bash
cd 2-viewer
npm run build
# Push dist/ to gh-pages branch
```

### Option 3: Self-hosted

Use the included Caddy server for full control.

```bash
cd 3-server
./manage.sh start
```

## Architecture

### Development Mode

```
Browser
  ↓
Vite Dev Server (http://localhost:3000)
  ↓
Local PMTiles files or Caddy Server
```

### Production Mode (Cloudflare)

```
Browser
  ↓
Cloudflare Pages (Static Assets)
  ↓
Cloudflare Worker (Tile Server)
  ↓ Range Requests
Cloudflare R2 (PMTiles Storage)
```

## PMTiles Generation

Process raw data into PMTiles archives:

```bash
cd 1-processing

# Install Python dependencies
pip install -r requirements.txt

# Run processing pipeline
jupyter notebook processing_pipeline.ipynb
```

Generated PMTiles will be in `data/3-pmtiles/`.

## Technology Stack

- **Frontend**: MapLibre GL JS, Vite
- **Tiles**: PMTiles (v3), Overture Maps Foundation
- **Infrastructure**: Cloudflare Workers, R2, Pages
- **Processing**: Python, Tippecanoe, GDAL
- **Development**: Caddy, Docker

## Cost Estimation (Cloudflare)

For moderate traffic:
- **R2 Storage** (10GB): $0.15/month
- **Worker Requests** (1M/month): $0.50/month
- **Egress**: FREE ✨
- **Total**: ~$0.65/month

Compare to traditional hosting with egress fees: $90-900/TB!

## Development Workflow

1. **Process data** (1-processing/)
2. **Generate PMTiles** → data/3-pmtiles/
3. **Upload to R2** (4-cloud/scripts/)
4. **Deploy Worker** (4-cloud/cloudflare/)
5. **Build viewer** (2-viewer/)
6. **Deploy to Pages**

## Common Tasks

### Upload PMTiles to R2

```powershell
# Windows
cd 4-cloud/scripts
.\upload-pmtiles.ps1 -BucketName grid3-tiles
```

```bash
# Linux/Mac
cd 4-cloud/scripts
./upload-pmtiles.sh grid3-tiles
```

### Update Worker

```bash
cd 4-cloud/cloudflare
npm run deploy
```

### Build for Production

```bash
cd 2-viewer
npm run build
```

### Test Locally with Production Worker

Edit `2-viewer/.env.development`:
```env
VITE_PMTILES_SOURCE=cloudflare
VITE_CLOUDFLARE_WORKER_URL=https://your-worker.workers.dev
```

Then:
```bash
npm run dev
```

## Troubleshooting

### Tiles not loading

1. Check browser console for error messages
2. Verify environment configuration in `.env.*`
3. Test Worker directly: `curl https://your-worker.workers.dev/base.json`
4. Check R2 uploads: `wrangler r2 object list grid3-tiles`

### Worker deployment fails

1. Verify login: `wrangler whoami`
2. Check wrangler.toml configuration
3. Ensure R2 bucket exists: `wrangler r2 bucket list`

### Build errors

1. Clear node_modules: `rm -rf node_modules && npm install`
2. Clear Vite cache: `rm -rf .vite`
3. Check Node version: `node --version` (requires Node 18+)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## License

[Your License Here]

## Acknowledgments

- [Overture Maps Foundation](https://overturemaps.org/) for map data
- [Protomaps](https://protomaps.com/) for PMTiles specification
- [MapLibre GL JS](https://maplibre.org/) for rendering
- [Cloudflare](https://www.cloudflare.com/) for infrastructure

## Support

- Report issues: [GitHub Issues]
- Cloudflare setup: See [DEPLOYMENT.md](DEPLOYMENT.md)
- PMTiles questions: [PMTiles Docs](https://docs.pmtiles.io/)
