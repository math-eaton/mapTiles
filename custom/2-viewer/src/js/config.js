/**
 * Configuration for tile serving endpoints
 * Handles detection and fallback between self-hosted Caddy server, Cloudflare Workers, and local files
 */

export class TileConfig {
    constructor() {
        // Read configuration from environment
        const environment = import.meta.env.VITE_ENVIRONMENT || 'development';
        const pmtilesSource = import.meta.env.VITE_PMTILES_SOURCE || 'auto';
        const cloudflareWorkerUrl = import.meta.env.VITE_CLOUDFLARE_WORKER_URL || '';
        const caddyHost = import.meta.env.VITE_CADDY_HOST || '127.0.0.1';
        const caddyPort = import.meta.env.VITE_CADDY_PORT || '3002';
        
        this.environment = environment;
        this.pmtilesSource = pmtilesSource;
        
        this.endpoints = {
            // Cloudflare Worker endpoint (production)
            cloudflare: {
                enabled: !!cloudflareWorkerUrl,
                url: cloudflareWorkerUrl,
                // For Cloudflare Worker, we use direct HTTPS URLs, not the pmtiles:// protocol
                // The worker handles the tile serving with proper Range request support
            },
            // Self-hosted Caddy server endpoints (development)
            caddy: {
                pmtiles: `http://${caddyHost}:${caddyPort}/static`,
                mvt: `http://${caddyHost}:${caddyPort}/mvt`,
                health: `http://${caddyHost}:${caddyPort}/health`
            },
            // Local files fallback (development)
            local: {
                pmtiles: null // Will be set based on hosting environment
            }
        };

        this.currentEndpoint = null;
        this.isLocalhost = false;
        this.isGitHubPages = false;
        this.isCloudflarePages = false;
        this.repoName = '';
        this.basePath = '';
        
        this.detectEnvironment();
    }

    /**
     * Detect the current hosting environment
     */
    detectEnvironment() {
        this.isLocalhost = window.location.hostname === 'localhost' || 
                          window.location.hostname === '127.0.0.1';
        this.isGitHubPages = window.location.hostname.includes('github.io');
        this.isCloudflarePages = window.location.hostname.includes('.pages.dev');
        
        if (this.isGitHubPages) {
            this.repoName = window.location.pathname.split('/')[1] || '';
            this.basePath = this.repoName ? `/${this.repoName}` : '';
            this.endpoints.local.pmtiles = `${this.basePath}/tiles`;
        } else if (this.isLocalhost) {
            this.basePath = '';
            this.endpoints.local.pmtiles = './tiles';
        } else if (this.isCloudflarePages) {
            this.basePath = '';
            // On Cloudflare Pages, we can use relative paths or configure the worker URL
            this.endpoints.local.pmtiles = './tiles';
        }
    }

    /**
     * Check if Cloudflare Worker is accessible
     * Returns a promise that resolves to true if accessible, false otherwise
     */
    async checkCloudflareAvailability() {
        if (!this.endpoints.cloudflare.enabled || !this.endpoints.cloudflare.url) {
            return false;
        }
        
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000); // 3 second timeout
            
            // Try to fetch a small tile or health check endpoint
            // Cloudflare Workers should respond quickly with CORS headers
            const testUrl = `${this.endpoints.cloudflare.url}/base.json`;
            console.log(`🔍 Checking Cloudflare Worker at: ${testUrl}`);
            
            const response = await fetch(testUrl, {
                method: 'GET',
                mode: 'cors',
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            console.log(`✅ Cloudflare Worker responded: ${response.status} ${response.statusText}`);
            return response.ok || response.status === 404; // 404 is ok, means worker is running
        } catch (error) {
            console.warn(`❌ Cloudflare Worker check failed:`, error.name, error.message);
            return false;
        }
    }

    /**
     * Check if the Caddy server is accessible
     * Returns a promise that resolves to true if accessible, false otherwise
     */
    async checkCaddyAvailability() {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 2000); // 2 second timeout
            
            console.log(`🔍 Checking Caddy server at: ${this.endpoints.caddy.health}`);
            
            const response = await fetch(this.endpoints.caddy.health, {
                method: 'GET',
                mode: 'cors',
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            console.log(`✅ Caddy server responded: ${response.status} ${response.statusText}`);
            return response.ok;
        } catch (error) {
            // Server not accessible (CORS, timeout, network error, etc.)
            console.warn(`❌ Caddy server check failed:`, error.name, error.message);
            return false;
        }
    }

    /**
     * Determine the best endpoint to use for tiles
     * Priority: Cloudflare Worker (production) > Caddy (dev) > Local files (fallback)
     */
    async selectBestEndpoint() {
        // If pmtilesSource is explicitly set, respect it
        if (this.pmtilesSource === 'cloudflare' && this.endpoints.cloudflare.enabled) {
            const cloudflareAvailable = await this.checkCloudflareAvailability();
            if (cloudflareAvailable) {
                this.currentEndpoint = 'cloudflare';
                console.log('🌐 Using Cloudflare Worker for PMTiles (Production Mode)');
                console.log(`   Worker URL: ${this.endpoints.cloudflare.url}`);
                return 'cloudflare';
            }
            console.warn('⚠️  Cloudflare Worker configured but not accessible, falling back...');
        }

        if (this.pmtilesSource === 'caddy') {
            const caddyAvailable = await this.checkCaddyAvailability();
            if (caddyAvailable) {
                this.currentEndpoint = 'caddy';
                console.log('🏠 Using self-hosted Caddy server for tiles');
                return 'caddy';
            }
            console.warn('⚠️  Caddy server configured but not accessible, falling back...');
        }

        if (this.pmtilesSource === 'local') {
            this.currentEndpoint = 'local';
            console.log('📁 Using local PMTiles files');
            return 'local';
        }

        // Auto-detect mode
        console.log('🔍 Auto-detecting best tile endpoint...');

        // In production (non-localhost), prefer Cloudflare Worker
        if (!this.isLocalhost && this.endpoints.cloudflare.enabled) {
            const cloudflareAvailable = await this.checkCloudflareAvailability();
            if (cloudflareAvailable) {
                this.currentEndpoint = 'cloudflare';
                console.log('🌐 Using Cloudflare Worker for PMTiles (auto-detected)');
                return 'cloudflare';
            }
        }

        // On localhost during development, prefer Caddy if available
        if (this.isLocalhost) {
            const caddyAvailable = await this.checkCaddyAvailability();
            if (caddyAvailable) {
                this.currentEndpoint = 'caddy';
                console.log('🏠 Using self-hosted Caddy server for tiles');
                return 'caddy';
            }
            // Fall back to local files
            this.currentEndpoint = 'local';
            console.log('📁 Using local PMTiles files (Caddy server not available)');
            return 'local';
        }

        // On GitHub Pages or Cloudflare Pages, check if Caddy server is accessible
        if (this.isGitHubPages || this.isCloudflarePages) {
            const caddyAvailable = await this.checkCaddyAvailability();
            if (caddyAvailable) {
                this.currentEndpoint = 'caddy';
                console.log('🏠 Using self-hosted Caddy server for tiles (from hosted pages)');
                console.log('⚡ This provides better performance with proper HTTP range request support');
                return 'caddy';
            }
            // Fall back to hosted files
            this.currentEndpoint = 'local';
            console.log(`📁 Using ${this.isGitHubPages ? 'GitHub' : 'Cloudflare'} Pages hosted PMTiles files`);
            if (this.isGitHubPages) {
                console.warn('⚠️  GitHub Pages may have limitations with byte-serving for large PMTiles');
                console.warn('   Consider using Cloudflare Worker + R2 for production deployment');
            }
            return 'local';
        }

        // Default to local endpoint for other hosting scenarios
        this.currentEndpoint = 'local';
        console.log('📁 Using local PMTiles files (default fallback)');
        return 'local';
    }

    /**
     * Get the PMTiles URL for a given tile file
     * @param {string} tileName - Name of the tile file (e.g., 'buildings.pmtiles')
     * @returns {string} Full URL for the tile source
     */
    getPMTilesUrl(tileName) {
        if (!this.currentEndpoint) {
            throw new Error('Endpoint not selected. Call selectBestEndpoint() first.');
        }

        // For Cloudflare Worker, return direct HTTPS URL (not pmtiles:// protocol)
        // The worker serves tiles at: https://worker-url/{tileName}/{z}/{x}/{y}.{ext}
        if (this.currentEndpoint === 'cloudflare') {
            // Remove .pmtiles extension if present, as the worker expects just the name
            const name = tileName.replace('.pmtiles', '');
            return `${this.endpoints.cloudflare.url}/${name}`;
        }

        // For Caddy and local files, use the pmtiles:// protocol
        const endpoint = this.currentEndpoint === 'caddy' 
            ? this.endpoints.caddy.pmtiles 
            : this.endpoints.local.pmtiles;

        // Always use pmtiles:// protocol - it handles both HTTP and file URLs
        return `pmtiles://${endpoint}/${tileName}`;
    }

    /**
     * Get the base URL for PMTiles (without protocol prefix)
     * Useful for manual PMTiles instance creation
     */
    getPMTilesBaseUrl(tileName) {
        if (!this.currentEndpoint) {
            throw new Error('Endpoint not selected. Call selectBestEndpoint() first.');
        }

        // For Cloudflare Worker
        if (this.currentEndpoint === 'cloudflare') {
            const name = tileName.replace('.pmtiles', '');
            return `${this.endpoints.cloudflare.url}/${name}`;
        }

        // For Caddy and local
        const endpoint = this.currentEndpoint === 'caddy' 
            ? this.endpoints.caddy.pmtiles 
            : this.endpoints.local.pmtiles;

        return `${endpoint}/${tileName}`;
    }

    /**
     * Get the MVT endpoint URL (only available on Caddy server)
     * @param {string} tableName - Name of the PostGIS table
     * @returns {string|null} MVT endpoint URL or null if not available
     */
    getMVTUrl(tableName) {
        if (this.currentEndpoint === 'caddy') {
            return `${this.endpoints.caddy.mvt}/${tableName}/{z}/{x}/{y}`;
        }
        return null;
    }

    /**
     * Check if MVT tiles are available (requires Caddy server)
     */
    isMVTAvailable() {
        return this.currentEndpoint === 'caddy';
    }

    /**
     * Get configuration info for debugging
     */
    getInfo() {
        return {
            environment: this.environment,
            pmtilesSource: this.pmtilesSource,
            hostingPlatform: this.isLocalhost ? 'localhost' : 
                           (this.isGitHubPages ? 'github-pages' : 
                           (this.isCloudflarePages ? 'cloudflare-pages' : 'other')),
            currentEndpoint: this.currentEndpoint,
            basePath: this.basePath,
            repoName: this.repoName,
            cloudflareEnabled: this.endpoints.cloudflare.enabled,
            cloudflareWorkerUrl: this.endpoints.cloudflare.url,
            endpoints: this.endpoints
        };
    }

    /**
     * Check if we're using Cloudflare Worker for PMTiles
     */
    isUsingCloudflare() {
        return this.currentEndpoint === 'cloudflare';
    }

    /**
     * Check if we should use pmtiles:// protocol or direct HTTPS URLs
     */
    shouldUsePMTilesProtocol() {
        // Cloudflare Worker uses direct HTTPS URLs
        // All others use pmtiles:// protocol
        return this.currentEndpoint !== 'cloudflare';
    }
}

// Export a singleton instance
export const tileConfig = new TileConfig();
