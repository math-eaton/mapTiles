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

export interface TileConfig {
  // Set to true to use Cloudflare Worker, false for direct PMTiles
  useCloudflare: boolean;
  
  // Your Cloudflare Worker URL (only needed if useCloudflare = true)
  // Example: "https://tiles.yourname.workers.dev"
  cloudflareWorkerUrl: string;
  
  // The name of your PMTiles archive (without .pmtiles extension)
  // This should match the filename in your R2 bucket
  archiveName: string;
  
  // Direct R2 URL (only needed if useCloudflare = false)
  // Example: "https://pub-xyz.r2.dev/planet.pmtiles"
  directPMTilesUrl: string;
}

// ============================================================================
// CONFIGURATION - Update these values for your deployment
// ============================================================================

export const TILE_CONFIG: TileConfig = {
  // PRODUCTION: Set to true and configure cloudflareWorkerUrl
  // DEVELOPMENT: Set to false and use directPMTilesUrl
  useCloudflare: true,
  
  // Your Cloudflare Worker URL (update this after deploying your worker)
  // Leave empty if using direct PMTiles
  // NOTE: No trailing slash!
  cloudflareWorkerUrl: "https://pmtiles-cloudflare.mheaton-945.workers.dev",
  
  // Your PMTiles archive name (without .pmtiles extension)
  archiveName: "global",
  
  // Direct R2 URL - fallback for development or if not using Cloudflare Worker
  directPMTilesUrl: "https://pub-927f42809d2e4b89b96d1e7efb091d1f.r2.dev/global.pmtiles",
};

// ============================================================================
// Helper Functions - No need to modify below this line
// ============================================================================

/**
 * Get the tile source configuration for MapLibre
 * Returns either a 'url' (for pmtiles protocol) or 'tiles' array (for Cloudflare Worker)
 */
export function getTileSourceConfig(): { url?: string; tiles?: string[]; attribution: string } {
  const attribution = '<a href="https://github.com/protomaps/basemaps">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap</a>';
  
  if (TILE_CONFIG.useCloudflare) {
    if (!TILE_CONFIG.cloudflareWorkerUrl) {
      console.error("Cloudflare Worker URL not configured! Falling back to direct PMTiles.");
      return {
        url: `pmtiles://${TILE_CONFIG.directPMTilesUrl}`,
        attribution,
      };
    }
    
    // Cloudflare Worker pattern: {worker-url}/{archive-name}/{z}/{x}/{y}.mvt
    // The worker maps {name} to {name}.pmtiles in your R2 bucket
    return {
      tiles: [`${TILE_CONFIG.cloudflareWorkerUrl}/${TILE_CONFIG.archiveName}/{z}/{x}/{y}.mvt`],
      attribution,
    };
  } else {
    // Direct PMTiles via protocol
    return {
      url: `pmtiles://${TILE_CONFIG.directPMTilesUrl}`,
      attribution,
    };
  }
}

/**
 * Check if the configuration is valid
 */
export function validateConfig(): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  
  if (TILE_CONFIG.useCloudflare) {
    if (!TILE_CONFIG.cloudflareWorkerUrl) {
      errors.push("Cloudflare Worker URL is required when useCloudflare is true");
    }
    if (!TILE_CONFIG.archiveName) {
      errors.push("Archive name is required");
    }
  } else {
    if (!TILE_CONFIG.directPMTilesUrl) {
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
  const config = getTileSourceConfig();
  console.log("=== Tile Configuration ===");
  console.log("Mode:", TILE_CONFIG.useCloudflare ? "Cloudflare Worker" : "Direct PMTiles");
  console.log("Archive:", TILE_CONFIG.archiveName);
  
  if (config.url) {
    console.log("PMTiles URL:", config.url);
  }
  if (config.tiles) {
    console.log("Tile Pattern:", config.tiles[0]);
  }
  
  const validation = validateConfig();
  if (!validation.valid) {
    console.warn("Configuration errors:", validation.errors);
  }
  console.log("========================");
}
