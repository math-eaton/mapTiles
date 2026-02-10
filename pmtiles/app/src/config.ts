/**
 * Tile Configuration for PMTiles Sources via Cloudflare Workers + R2
 * 
 * This configuration exclusively uses Cloudflare Workers for serving PMTiles archives.
 * All archives are stored in R2 and served through a Cloudflare Worker for optimal
 * performance and cost-efficiency.
 * 
 * CLOUDFLARE WORKER SETUP:
 * ========================
 * 
 * 1. Deploy the PMTiles Cloudflare Worker:
 *    - Use: https://github.com/protomaps/PMTiles/tree/main/serverless/cloudflare
 *    - The worker handles tile requests from multiple .pmtiles archives
 * 
 * 2. Bind your R2 bucket to the worker:
 *    - In Cloudflare Dashboard: Workers & Pages > Your Worker > Settings > Variables
 *    - Add R2 Bucket Binding: name = "BUCKET", bucket = "grid3-maptiles"
 * 
 * 3. Upload your PMTiles archives to R2:
 *    - global.pmtiles (Protomaps basemap)
 *    - buildings.pmtiles (Overture buildings)
 *    - grid3.pmtiles (GRID3 data layer)
 * 
 * 4. Set VITE_CLOUDFLARE_WORKER_URL environment variable to your worker URL
 * 
 * TILE SERVING PATTERN:
 * ====================
 * https://your-worker.workers.dev/{archive-name}/{z}/{x}/{y}.mvt
 * 
 * The worker automatically maps {archive-name} to {archive-name}.pmtiles in R2.
 * 
 * BENEFITS:
 * =========
 * - Edge caching (faster global delivery)
 * - Automatic HTTP/2 and HTTP/3
 * - DDoS protection
 * - No R2 egress fees (Class A reads only)
 * - Multiple archive support without protocol complexity
 */

/**
 * Archive source definition
 */
export interface ArchiveSource {
  archiveName: string;
  attribution: string;
  maxzoom?: number;
}

/**
 * Asset configuration (sprites, fonts, etc.)
 */
export interface AssetConfig {
  spriteBaseUrl: string;
  glyphsUrl: string;
}

/**
 * Tile configuration
 */
export interface TileConfig {
  cloudflareWorkerUrl: string;
  sources: {
    protomaps: ArchiveSource;
    overture: ArchiveSource;
    grid3: ArchiveSource;
  };
}

/**
 * Application configuration
 */
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
    cloudflareWorkerUrl: getEnvVar(
      "VITE_CLOUDFLARE_WORKER_URL",
      "https://pmtiles-cloudflare.mheaton-945.workers.dev"
    ),
    sources: {
      protomaps: {
        archiveName: getEnvVar("VITE_PROTOMAPS_ARCHIVE", "global"),
        attribution: '<a href="https://github.com/protomaps/basemaps">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap</a>',
        maxzoom: 22,
      },
      overture: {
        archiveName: getEnvVar("VITE_OVERTURE_ARCHIVE", "buildings"),
        attribution: '<a href="https://overturemaps.org">Overture Maps Foundation</a>',
        maxzoom: 14,
      },
      grid3: {
        archiveName: getEnvVar("VITE_GRID3_ARCHIVE", "grid3"),
        attribution: '<a href="https://grid3.org">GRID3</a>',
        maxzoom: 15,
      },
    },
  },
  assets: {
    // R2-hosted sprites (use Protomaps default if not set)
    spriteBaseUrl: getEnvVar(
      "VITE_SPRITE_BASE_URL",
      "https://protomaps.github.io/basemaps-assets/sprites/v4"
    ),
    glyphsUrl: getEnvVar(
      "VITE_GLYPHS_URL",
      "https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf"
    ),
  },
};

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get the tile source configuration for MapLibre using Cloudflare Worker
 * 
 * @param sourceName - The source name ('protomaps', 'overture', or 'grid3')
 * @returns MapLibre source configuration with tiles array and attribution
 */
export function getTileSourceConfig(
  sourceName: keyof TileConfig["sources"]
): { tiles: string[]; attribution: string; maxzoom?: number } {
  const config = APP_CONFIG.tiles;
  const source = config.sources[sourceName];
  
  if (!config.cloudflareWorkerUrl) {
    console.error("Cloudflare Worker URL not configured!");
    throw new Error("VITE_CLOUDFLARE_WORKER_URL must be set");
  }
  
  if (!source) {
    console.error(`Unknown source: ${sourceName}`);
    throw new Error(`Source '${sourceName}' not found in configuration`);
  }
  
  // Cloudflare Worker pattern: {worker-url}/{archive-name}/{z}/{x}/{y}.mvt
  // The worker maps {archive-name} to {archive-name}.pmtiles in R2 bucket
  return {
    tiles: [`${config.cloudflareWorkerUrl}/${source.archiveName}/{z}/{x}/{y}.mvt`],
    attribution: source.attribution,
    ...(source.maxzoom && { maxzoom: source.maxzoom }),
  };
}

/**
 * Get tile source configuration by archive name (for backward compatibility)
 * Maps common archive names to source keys
 */
export function getTileSourceByArchive(archiveName: string): ReturnType<typeof getTileSourceConfig> {
  const archiveMap: Record<string, keyof TileConfig["sources"]> = {
    global: "protomaps",
    buildings: "overture",
    grid3: "grid3",
  };
  
  const sourceName = archiveMap[archiveName] || "protomaps";
  return getTileSourceConfig(sourceName);
}

/**
 * Check if the configuration is valid
 */
export function validateConfig(): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  const config = APP_CONFIG.tiles;
  
  if (!config.cloudflareWorkerUrl) {
    errors.push("Cloudflare Worker URL is required (VITE_CLOUDFLARE_WORKER_URL)");
  }
  
  // Validate each source
  (Object.entries(config.sources) as [keyof TileConfig["sources"], ArchiveSource][]).forEach(([name, source]) => {
    if (!source.archiveName) {
      errors.push(`Archive name is required for source '${name}'`);
    }
    if (!source.attribution) {
      errors.push(`Attribution is required for source '${name}'`);
    }
  });
  
  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Log the current configuration (for debugging)
 */
export function logConfig(): void {
  console.log("=== PMTiles Configuration (Cloudflare Workers + R2) ===");
  console.log("Worker URL:", APP_CONFIG.tiles.cloudflareWorkerUrl);
  console.log("\nSources:");
  
  (Object.keys(APP_CONFIG.tiles.sources) as (keyof TileConfig["sources"])[]).forEach((sourceName) => {
    const source = getTileSourceConfig(sourceName);
    console.log(`\n  ${sourceName}:`);
    console.log(`    Pattern: ${source.tiles[0]}`);
    if (source.maxzoom) {
      console.log(`    Max Zoom: ${source.maxzoom}`);
    }
  });
  
  console.log("\nAssets:");
  console.log("  Sprites:", APP_CONFIG.assets.spriteBaseUrl);
  console.log("  Glyphs:", APP_CONFIG.assets.glyphsUrl);
  
  const validation = validateConfig();
  if (!validation.valid) {
    console.warn("\nConfiguration errors:");
    validation.errors.forEach((error) => console.warn(`  - ${error}`));
  }
  console.log("===================================================");
}
