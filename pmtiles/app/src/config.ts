/**
 * Tile Configuration for PMTiles Sources
 * 
 * This configuration supports two modes:
 * 1. Direct PMTiles (via pmtiles:// protocol) - Good for local dev or simple hosting
 * 2. Cloudflare Workers (via tiles[] array) - Production-grade, fast edge delivery
 * 
 * CLOUDFLARE WORKER SETUP:
 * ========================
 * 
 * 1. Create a Cloudflare Worker with PMTiles support
 *    - Use the official protomaps/PMTiles cloudflare-worker template
 *    - Or use: https://github.com/protomaps/PMTiles/tree/main/serverless/cloudflare
 * 
 * 2. Bind your R2 bucket to the worker:
 *    - In Cloudflare Dashboard: Workers & Pages > Your Worker > Settings > Variables
 *    - Add R2 Bucket Binding: name = "BUCKET", bucket = "grid3-maptiles"
 * 
 * 3. Deploy the worker and note the URL (e.g., https://your-worker.yourname.workers.dev)
 * 
 * 4. Update CLOUDFLARE_WORKER_URL below with your worker URL
 * 
 * TILE SERVING PATTERNS:
 * =====================
 * - Cloudflare Worker: https://your-worker.workers.dev/{archive-name}/{z}/{x}/{y}.pbf
 * - Direct PMTiles: pmtiles://https://your-r2-url/planet.pmtiles
 * 
 * For production, Cloudflare Workers provide:
 * - Edge caching (faster global delivery)
 * - Automatic HTTP/2 and HTTP/3
 * - DDoS protection
 * - No egress fees from R2 (unlike direct R2 access)
 */

export interface AssetConfig {
  spriteBaseUrl: string;
}

export interface TileConfig {
  useCloudflare: boolean;
  cloudflareWorkerUrl: string;
  archiveName: string;
  directPMTilesUrl: string;
}

export interface AppConfig {
  tiles: TileConfig;
  assets: AssetConfig;
}

// Environment variable support for Cloudflare Pages
const getEnvVar = (key: string, defaultValue: string = ""): string => {
  // Vite exposes env vars prefixed with VITE_ at build time
  return import.meta.env[key] || defaultValue;
};

export const APP_CONFIG: AppConfig = {
  tiles: {
    useCloudflare: getEnvVar("VITE_USE_CLOUDFLARE", "true") === "true",
    cloudflareWorkerUrl: getEnvVar(
      "VITE_CLOUDFLARE_WORKER_URL",
      "https://pmtiles-cloudflare.mheaton-945.workers.dev"
    ),
    archiveName: getEnvVar("VITE_BASELAYER_NAME", "global"),
    directPMTilesUrl: getEnvVar(
      "VITE_BASELAYER_URL",
      "https://pub-927f42809d2e4b89b96d1e7efb091d1f.r2.dev/global.pmtiles"
    ),
  },
  assets: {
    // R2-hosted sprites (use Protomaps default if not set)
    spriteBaseUrl: getEnvVar(
      "VITE_SPRITE_BASE_URL",
      "https://protomaps.github.io/basemaps-assets/sprites/v4"
    ),
  },
};

// ============================================================================
// Helper Functions
// ============================================================================
/**
 * Get the tile source configuration for MapLibre
 * Returns either a 'url' (for pmtiles protocol) or 'tiles' array (for Cloudflare Worker)
 */
export function getTileSourceConfig(): { url?: string; tiles?: string[]; attribution: string } {
  const attribution = '<a href="https://github.com/protomaps/basemaps">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap</a>';
  const config = APP_CONFIG.tiles;
  
  if (config.useCloudflare) {
    if (!config.cloudflareWorkerUrl) {
      console.error("Cloudflare Worker URL not configured! Falling back to direct PMTiles.");
      return {
        url: `pmtiles://${config.directPMTilesUrl}`,
        attribution,
      };
    }
    
    // Cloudflare Worker pattern: {worker-url}/{archive-name}/{z}/{x}/{y}.mvt
    // The worker maps {name} to {name}.pmtiles in your R2 bucket
    return {
      tiles: [`${config.cloudflareWorkerUrl}/${config.archiveName}/{z}/{x}/{y}.mvt`],
      attribution,
    };
  } else {
    // Direct PMTiles via protocol
    return {
      url: `pmtiles://${config.directPMTilesUrl}`,
      attribution,
    };
  }
}

/**
 * Check if the configuration is valid
 */
export function validateConfig(): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  const config = APP_CONFIG.tiles;
  
  if (config.useCloudflare) {
    if (!config.cloudflareWorkerUrl) {
      errors.push("Cloudflare Worker URL is required when useCloudflare is true");
    }
    if (!config.archiveName) {
      errors.push("Archive name is required");
    }
  } else {
    if (!config.directPMTilesUrl) {
      errors.push("Direct PMTiles URL is required when useCloudflare is false");
    }
  }
  
  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Log the current configuration (for debugging)
 */
export function logConfig(): void {
  const tileSource = getTileSourceConfig();
  const config = APP_CONFIG.tiles;
  console.log("=== Tile Configuration ===");
  console.log("Mode:", config.useCloudflare ? "Cloudflare Worker" : "Direct PMTiles");
  console.log("Archive:", config.archiveName);
  console.log("Sprite Base URL:", APP_CONFIG.assets.spriteBaseUrl);
  
  if (tileSource.url) {
    console.log("PMTiles URL:", tileSource.url);
  }
  if (tileSource.tiles) {
    console.log("Tile Pattern:", tileSource.tiles[0]);
  }
  
  const validation = validateConfig();
  if (!validation.valid) {
    console.warn("Configuration errors:", validation.errors);
  }
  console.log("========================");
}
