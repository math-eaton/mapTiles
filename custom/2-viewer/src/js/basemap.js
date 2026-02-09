import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { PMTiles } from 'pmtiles';
import { tileConfig } from './config.js';

// Lazy load contour functionality
let mlcontour = null;
let demSource = null;

async function initContours() {
    if (!mlcontour) {
        // Import the module and get the default export
        const mlcontourModule = await import('maplibre-contour');
        mlcontour = mlcontourModule.default;
        
        // Now create DemSource using the imported module
        demSource = new mlcontour.DemSource({
            url: "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png",
            encoding: "terrarium", // "mapbox" or "terrarium" default="terrarium"
            maxzoom: 16,
            worker: true, // offload isoline computation to a web worker to reduce jank
            cacheSize: 100, // number of most-recent tiles to cache
            timeoutMs: 10_000, // timeout on fetch requests
        });
        demSource.setupMaplibre(maplibregl);
    }
    return { mlcontour, demSource };
}

class OvertureMap {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.options = {
            // Bounds and center will be auto-detected from tilejson.json
            bounds: null,
            center: null,
            zoom: 8,
            minZoom: 6,
            maxZoom: 22, // Allow zooming beyond tiles' native maxzoom (enables overzooming)
            showTileBoundaries: false,
            clampToBounds: false,
            useVectorTiles: false, // Set to true to use traditional vector tiles instead of PMTiles
            ...options
        };
        
        // Store tile metadata
        this.tileMetadata = null;
        
        this.map = null;
        this.protocol = null;
        
        // Layer draw order index - lower numbers draw first (bottom), higher numbers draw on top
        // This order also determines label priority: higher z-order = higher label placement priority
        this.layerDrawOrder = {
            // Base layers (0-9)
            'background': 1,
            'tile-grid': 999,  // Tile grid overlay (always on top)


            // Land use and land cover (20-39)
            'land': 2,           // Natural land features (forest, grass, etc.)
            'land-cover': 20,     // Land cover data (forest, crop, grass, etc.)
            
            // ESRI forest layers
            'esri-admin0-forest': 21,   // Large scale admin forest/parks
            'esri-admin1-forest': 22,   // Medium scale admin forest/parks  
            'esri-openspace-forest': 23, // Local scale forest/openspace

            'settlement-extents-fill': 26, // Settlement extent fills
            'settlement-extents-outlines': 31, // Settlement extent outlines

            'land-use': 30, // built env
            'land-residential': 15, // residential areas
                                    
            // Terrain and elevation 
            'hills': 35,

            // Water features (40-49)
            'water-lines-casing': 38,   // linear feature bg
            'land-cover-wetlands-fill': 37,
            'land-cover-wetlands-pattern': 37, // Wetlands pattern fill
            'water-lines': 39,           // Rivers, streams, canals
            'water-polygons': 40,        // Water body fills
            'water-texture': 49,         // Water texture overlay
            'water-polygon-outlines': 41, // Water body outlines
            
            // Contour lines (50-59)
            'contours': 32,       // Contour lines
            'contour-text': 51,   // Contour elevation labels

            
            // Transportation (60-79)
            'roads-solid': 60,    // Major road lines (solid)
            'roads-dashed': 61,   // Minor road lines (dashed)
            'roads-solid-casing': 59, // Background for solid roads (for better contrast)

            // Infrastructure (70-79)
            'infrastructure-polygons': 70,  // Infrastructure polygon fills
            'infrastructure-lines': 71,     // Infrastructure lines (power, communication, etc.)
            'infrastructure-points': 72,    // Infrastructure points (towers, utilities, etc.)
            
            // Buildings and structures (80-89)
            'buildings-flat': 80,           // Building fills
            'buildings-extrusion': 79,    // 3D building extrusions
            'building-outlines': 83,   // Building outlines

            // Administrative boundaries (85-89)
            'health-areas': 84,        // Health administrative areas (fill)
            'health-zones': 86,        // Health administrative zones (fill)
            'health-zones-outline': 85, // Health zones outline
            'health-zones-casing': 83,
            'health-areas-outline': 87, // Health areas outline
            'health-areas-casing': 84,

            // Points of interest (90-99)
            'places': 90,              // Place points/circles
            'health-facilities': 91,   // Health facilities points
            
            // Labels and text (100+)
            'health-areas-labels': 98, // Health area labels
            'health-zones-labels': 99, // Health zone labels
            'place-labels': 100,       // Place name labels
            'health-facilities-labels': 101, // Health facility labels
            'settlement-names': 102,   // Settlement name labels - highest priority
            'placenames': 103         // Other place names
        };
        
        // Label priority weightings for symbol-sort-key
        // In MapLibre, LOWER symbol-sort-key values = HIGHER priority (drawn first, gets preference)
        // We invert the layerDrawOrder so higher z-order = higher label priority
        this.labelPriority = {
            // Contour labels (lowest priority)
            'contour-text': 1000 - 51,
            
            // Administrative boundary labels
            'health-areas-labels-interior': 1000 - 80,
            'health-areas-labels-exterior': 1000 - 75,
            'health-zones-labels-interior': 1000 - 90,
            'health-zones-labels-exterior': 1000 - 85,
            
            // Place labels
            'place-labels': 1000 - 100,
            
            // Health facility labels
            'health-facilities-labels': 1000 - 101,
            
            // Settlement labels (highest priority)
            'settlement-names-labels': 1000 - 50,
        };
        
        this.init();
    }
    
    /**
     * Initialize the PMTiles protocol and create the map
     */
    async init() {
        // Initialize PMTiles protocol
        this.protocol = new Protocol();
        maplibregl.addProtocol("pmtiles", this.protocol.tile);
        
        try {
            // Detect and select the best tile endpoint (Caddy server or GitHub Pages)
            await tileConfig.selectBestEndpoint();
            console.log('Tile configuration:', tileConfig.getInfo());
            
            // Load tile metadata from tilejson
            await this.loadTileMetadata();
            
            // Load the style configuration
            const style = await this.loadStyle();
            this.createMap(style);
            this.setupEventHandlers();
            this.addControls();
        } catch (error) {
            console.error('Failed to load map style:', error);
            // Show user-friendly error message
            const mapContainer = document.getElementById(this.containerId);
            mapContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; font-family: sans-serif; color: #e74c3c; text-align: center; padding: 20px;"><div><h3>Map Loading Error</h3><p>Unable to load map tiles. This may be due to hosting limitations.<br>Please try refreshing the page or contact the administrator.</p></div></div>';
        }
    }
    
    /**
     * Load and parse tilejson.json to extract bounds and other metadata
     */
    async loadTileMetadata() {
        try {
            // Try to get tilejson from the configured tile endpoint
            let tilejsonUrl;
            
            if (tileConfig.currentEndpoint === 'cloudflare') {
                // Use Cloudflare Worker endpoint
                tilejsonUrl = `${tileConfig.endpoints.cloudflare.url}/tilejson.json`;
                console.log('Using Cloudflare Worker for tilejson:', tilejsonUrl);
            } else if (tileConfig.currentEndpoint === 'caddy') {
                // Use Caddy server endpoint
                tilejsonUrl = `${tileConfig.endpoints.caddy.pmtiles}/tilejson.json`;
                console.log('Using Caddy server for tilejson:', tilejsonUrl);
            } else {
                // Use local fallback
                const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
                const tileDir = isLocalhost ? './tiles' : './tiles';
                tilejsonUrl = `${tileDir}/tilejson.json`;
                console.log("Using local for tilejson:", tilejsonUrl);
            }
            
            console.log('Loading tile metadata from:', tilejsonUrl);
            
            const response = await fetch(tilejsonUrl);
            if (!response.ok) {
                throw new Error(`Failed to load tilejson: ${response.statusText}`);
            }
            
            this.tileMetadata = await response.json();
            console.log('Tile metadata loaded:', this.tileMetadata);
            
            // Extract bounds from tilejson
            if (this.tileMetadata.bounds && this.tileMetadata.bounds.length === 4) {
                const [west, south, east, north] = this.tileMetadata.bounds;
                this.options.bounds = [
                    [west, south],  // Southwest coordinates
                    [east, north]   // Northeast coordinates
                ];
                
                // Calculate center from bounds
                this.options.center = [
                    (west + east) / 2,
                    (south + north) / 2
                ];
                
                console.log('Bounds set from tilejson:', this.options.bounds);
                console.log('Center calculated:', this.options.center);
            }
            
            // Only update minZoom from tilejson if not explicitly set in options
            // DON'T override maxZoom from tilejson - we want to allow zooming beyond tiles' native maxzoom
            // This enables overzooming: map maxZoom (22) > tiles native maxzoom (13-14)
            const defaultMinZoom = 6;
            
            if (this.tileMetadata.minzoom !== undefined && this.options.minZoom === defaultMinZoom) {
                this.options.minZoom = this.tileMetadata.minzoom;
                console.log('MinZoom set from tilejson:', this.options.minZoom);
            }
            // Map maxZoom stays at 22 (or user override) to enable overzooming
            console.log('Map maxZoom:', this.options.maxZoom, '(allows overzooming beyond tiles native maxzoom)');
            if (this.tileMetadata.maxzoom !== undefined) {
                console.log('Tiles native maxzoom:', this.tileMetadata.maxzoom, '(will overzoom from this level)');
            }
            
        } catch (error) {
            console.error('Error loading tile metadata:', error);
            console.warn('Using default bounds and center');
            // Set fallback bounds and center
            this.options.bounds = [
                [20.5, -7.5],  // Southwest
                [23.5, -4.0]   // Northeast
            ];
            this.options.center = [22.0, -5.75];
        }
    }
    
    /**
     * Load the MapLibre style from JSON file (handles GH Pages base path and local dev)
     * Automatically filters out missing PMTiles sources
     */
    async loadStyle() {
        try {
            const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
            const isGitHubPages = window.location.hostname.includes('github.io');
            const repoName = isGitHubPages ? window.location.pathname.split('/')[1] : '';
            const basePath = isGitHubPages ? `/${repoName}` : '';
            // Use local relative path during dev, use basePath + /styles for production/GH Pages
            const styleDir = isLocalhost ? './styles' : `${basePath}/styles`;

            const styleFile = this.options.useVectorTiles ?
                `${styleDir}/cartography-vector.json` :
                `${styleDir}/cartography.json`;

            const response = await fetch(styleFile);
            if (!response.ok) {
                // Fallback if vector style not found
                if (this.options.useVectorTiles) {
                    console.warn('Vector tile style not found, falling back to PMTiles style');
                    const fallbackResponse = await fetch(`${styleDir}/cartography.json`);
                    if (!fallbackResponse.ok) {
                        throw new Error(`Failed to load style: ${fallbackResponse.statusText}`);
                    }
                    const style = await fallbackResponse.json();
                    this.updatePMTilesUrls(style);
                    await this.addContourToStyle(style);
                    this.sortLayersByDrawOrder(style);
                    return style;
                }
                throw new Error(`Failed to load style: ${response.statusText}`);
            }

            const style = await response.json();

            // Update URLs based on tile type
            if (this.options.useVectorTiles) {
                this.updateVectorTileUrls(style);
            } else {
                this.updatePMTilesUrls(style);
                // Filter to only include available PMTiles
                console.log('🔍 Checking which PMTiles files are available...');
                const filteredStyle = await this.filterAvailableSources(style);
                console.log(`✅ Loaded ${Object.keys(filteredStyle.sources).length} sources and ${filteredStyle.layers.length} layers`);
                // Update style reference
                Object.assign(style, filteredStyle);
            }

            // Add contour sources and layers to the style
            await this.addContourToStyle(style);

            // Sort layers according to draw order
            this.sortLayersByDrawOrder(style);

            return style;
        } catch (error) {
            console.error('Error loading style:', error);
            return this.getBasicStyle();
        }
    }

    /**
     * Check if a PMTiles file exists
     */
    async checkPMTilesExists(url) {
        try {
            // For Cloudflare Worker URLs, check the TileJSON endpoint
            if (tileConfig.isUsingCloudflare()) {
                // Cloudflare worker serves TileJSON at /{name}.json
                const response = await fetch(url, {
                    method: 'GET',
                    signal: AbortSignal.timeout(3000)
                });
                return response.ok;
            }
            
            // For local files and Caddy, use HEAD request
            const response = await fetch(url, {
                method: 'HEAD',
                signal: AbortSignal.timeout(3000)
            });
            return response.ok;
        } catch (error) {
            return false;
        }
    }

    /**
     * Filter style to only include sources/layers for available PMTiles
     */
    async filterAvailableSources(style) {
        const availableSources = new Set();
        
        // Determine check URL based on endpoint type
        let getCheckUrl;
        if (tileConfig.isUsingCloudflare()) {
            // For Cloudflare, check the TileJSON endpoint
            getCheckUrl = (fileName) => {
                const name = fileName.replace('.pmtiles', '');
                return `${tileConfig.endpoints.cloudflare.url}/${name}.json`;
            };
        } else {
            // For Caddy and local files, check the file directly
            const endpoint = tileConfig.currentEndpoint === 'caddy' 
                ? tileConfig.endpoints.caddy.pmtiles 
                : tileConfig.endpoints.local.pmtiles;
            getCheckUrl = (fileName) => `${endpoint}/${fileName}`;
        }

        // Check each PMTiles source
        const checkPromises = [];
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.url && source.url.includes('pmtiles://')) {
                const fileName = source.url.split('/').pop();
                const checkUrl = getCheckUrl(fileName);
                checkPromises.push(
                    this.checkPMTilesExists(checkUrl).then(exists => ({
                        sourceId,
                        exists
                    }))
                );
            } else {
                // Keep non-PMTiles sources (like esri)
                availableSources.add(sourceId);
            }
        }

        const results = await Promise.all(checkPromises);
        results.forEach(({ sourceId, exists }) => {
            if (exists) {
                availableSources.add(sourceId);
            } else {
                console.warn(`⚠️  Skipping missing source: ${sourceId}`);
            }
        });

        // Filter sources
        const filteredSources = {};
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (availableSources.has(sourceId)) {
                filteredSources[sourceId] = source;
            }
        }

        // Filter layers that reference missing sources
        const filteredLayers = style.layers.filter(layer => {
            if (layer.source && !availableSources.has(layer.source)) {
                console.warn(`⚠️  Skipping layer '${layer.id}' (missing source: ${layer.source})`);
                return false;
            }
            return true;
        });

        return {
            ...style,
            sources: filteredSources,
            layers: filteredLayers
        };
    }

    /**
     * Update PMTiles URLs using the configured endpoint
     * Handles Cloudflare Worker, Caddy server, or local files
     */
    updatePMTilesUrls(style) {
        console.log('Updating PMTiles URLs with endpoint:', tileConfig.currentEndpoint);
        
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.type === 'vector' && source.url && source.url.startsWith('pmtiles://tiles/')) {
                const tilePath = source.url.replace('pmtiles://tiles/', '');
                
                // Use the tile config to get the proper URL
                const newUrl = tileConfig.getPMTilesUrl(tilePath);
                
                console.log(`  ${sourceId}: ${source.url} → ${newUrl}`);
                
                // For Cloudflare Worker, use 'tiles' array instead of 'url'
                // because we're serving individual tiles, not a PMTiles archive via the protocol
                if (tileConfig.isUsingCloudflare()) {
                    delete source.url;
                    // Cloudflare worker pattern: https://worker-url/{name}/{z}/{x}/{y}.{ext}
                    const name = tilePath.replace('.pmtiles', '');
                    source.tiles = [`${tileConfig.endpoints.cloudflare.url}/${name}/{z}/{x}/{y}.pbf`];
                    // Add attribution if needed
                    if (!source.attribution) {
                        source.attribution = '© Overture Maps Foundation';
                    }
                } else {
                    // For Caddy and local files, use pmtiles:// protocol
                    source.url = newUrl;
                }
                
                // if no maxzoom is set in stylesheet, set to 22 to allow overzooming
                if (!source.maxzoom) {
                    source.maxzoom = 22; // Allow overzooming up to z22 (MapLibre default is 22)
                }
                
                // Mark as optional to suppress errors for missing tiles
                source.optional = true;
            }
        }
    }
    
    /**
     * Update vector tile URLs for traditional tile serving
     */
    updateVectorTileUrls(style) {
        const baseUrl = window.location.origin + window.location.pathname.replace(/\/[^\/]*$/, '');
        
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.type === 'vector' && source.tiles) {
                // Update relative URLs to absolute
                source.tiles = source.tiles.map(tileUrl => {
                    if (tileUrl.startsWith('./') || tileUrl.startsWith('/')) {
                        return `${baseUrl}${tileUrl.replace('./', '/')}`;
                    }
                    return tileUrl;
                });
            }
        }
    }
    
    /**
     * Create the MapLibre map instance
     */
    createMap(style) {
        // Detect if user is on a mobile device
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || 
                         ('ontouchstart' in window) || 
                         (navigator.maxTouchPoints > 0);
        
        // Configure interaction options for better mobile experience
        const interactionOptions = {
            // Only disable keyboard on mobile, let MapLibre handle desktop defaults
            ...(isMobile ? {
                keyboard: false, // Disable keyboard controls on mobile
                // Touch-specific optimizations
                touchZoomRotate: true,
                touchPitch: true,
                dragPan: {
                    deceleration: 2400,  // Faster deceleration for more responsive feel (default: 1400)
                },
                scrollZoom: {
                    around: 'center' // center point zoom
                },
                pitchWithRotate: false,  // Disable pitch on rotate for simpler interaction
                bearingSnap: 7          // Snap to cardinal directions more easily
            } : {
                // Desktop: Use MapLibre defaults by not overriding anything
                // This ensures the smoothest possible desktop experience
            })
        };

        this.map = new maplibregl.Map({
            container: this.containerId,
            style: style,
            // Initialize with center and zoom from options
            center: this.options.center,
            zoom: this.options.zoom,
            minZoom: this.options.minZoom,
            maxZoom: this.options.maxZoom,
            
            // Clamp camera to bounds if enabled
            ...(this.options.clampToBounds ? {
                maxBounds: this.options.bounds
            } : {}),
            
            // Disable default attribution control so we can add custom one
            attributionControl: false,
            
            // Apply interaction optimizations
            ...interactionOptions,
            
            // Performance optimizations for mobile
            ...(isMobile ? {
                antialias: false, // performance
                failIfMajorPerformanceCaveat: false,
                preserveDrawingBuffer: false,
                fadeDuration: 50, 
                crossSourceCollisions: false,  // performance
                optimizeForTerrain: false, // Disable terrain optimization for better touch performance
                renderWorldCopies: false, // performance
                refreshExpiredTiles: false // performance
            } : {})
        });
        
        // Additional mobile optimizations after map creation
        if (isMobile) {
            // Add touch-specific event listeners for better responsiveness
            this.setupMobileTouchOptimizations();
        }
        
        this.map.showTileBoundaries = this.options.showTileBoundaries;
    }
    
    /**
     * Setup map event handlers
     */
    setupEventHandlers() {
        // Add error handling with suppression for expected missing tile errors
        this.map.on('error', (e) => {
            if (e.error) {
                const errorMsg = e.error.message || '';
                // Suppress 404 errors for missing tiles (we already handled this)
                if (errorMsg.includes('404') || errorMsg.includes('Not Found')) {
                    return; // Silently ignore - we filtered these already
                }
                // PMTiles-specific errors (non-404)
                if (errorMsg.includes('pmtiles')) {
                    console.error('PMTiles Error:', errorMsg);
                    console.error('Check that:');
                    console.error('1. PMTiles files are properly served with HTTP range request support');
                    console.error('2. CORS headers are configured correctly');
                    console.error('3. File paths are correct');
                }
                // Tile loading errors (non-404)
                else if (errorMsg.includes('tile') || errorMsg.includes('source')) {
                    console.warn('Tile loading issue:', errorMsg);
                }
                // Other errors
                else {
                    console.error('Map error:', e.error);
                }
            }
        });

        // Map load event
        this.map.on('load', async () => {
            console.log('Map loaded successfully!');
            
            // console.log('Available sources:', Object.keys(this.map.getStyle().sources));
            
            // Check if layers exist and are visible
            const layers = this.map.getStyle().layers;
            console.log('All layers loaded:', layers.map(l => ({
                id: l.id,
                type: l.type,
                source: l.source,
                visibility: l.layout?.visibility || 'visible'
            })));
            
            // const contoursLayer = layers.find(layer => layer.id === 'contours');
            // const hillshadeLayer = layers.find(layer => layer.id === 'hills');
            
            // console.log('Contours layer found:', contoursLayer ? 'Yes' : 'No');
            // console.log('Hillshade layer found:', hillshadeLayer ? 'Yes' : 'No');
            
            // Check for PMTiles layers
            // const pmtilesLayers = layers.filter(layer => 
            //     layer.source && layer.source.includes('tiles') && 
            //     !layer.source.includes('contours') && 
            //     !layer.source.includes('dem')
            // );
            // console.log('PMTiles layers found:', pmtilesLayers.map(l => l.id));
            
            // debugging
            // this.printLayerOrder();
            
            // Update camera bounds if clampToBounds is enabled
            if (this.options.clampToBounds) {
                console.log('Camera bounds set to:', this.options.bounds);
                this.map.setMaxBounds(this.options.bounds);
            }
            
            // Apply label priorities to ensure proper text hierarchy
            this.applyLabelPriorities();
            
            // Debug: Force layer visibility after load
            // setTimeout(() => {
            //     const layers = ['land-use', 'land', 'water-polygons', 'water-lines', 'roads-solid', 'buildings'];
            //     layers.forEach(layerId => {
            //         if (this.map.getLayer(layerId)) {
            //             this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            //             console.log(`Forced ${layerId} to visible`);
            //         } else {
            //             console.warn(`Layer ${layerId} not found`);
            //         }
            //     });
            // }, 1000);
        });
        
        // Source loading feedback
        // this.map.on('sourcedataloading', (e) => {
        //     if (e.sourceId && e.sourceId.includes('tiles')) {
        //         console.log(`Loading ${e.sourceId}...`);
        //     }
        // });
        
        this.map.on('sourcedata', (e) => {
            if (e.sourceId && e.isSourceLoaded && e.sourceId.includes('tiles')) {
                // console.log(`✓ ${e.sourceId} loaded successfully`);
                
                // Debug: Check if source has data
                // const source = this.map.getSource(e.sourceId);
                // if (source) {
                //     console.log(`Source ${e.sourceId} details:`, {
                //         type: source.type,
                //         url: source._options?.url,
                //         loaded: e.isSourceLoaded
                //     });
                // }
                
                // Debug: Query features from this source after it loads
                // setTimeout(() => {
                //     try {
                //         const layersFromThisSource = this.map.getStyle().layers
                //             .filter(layer => layer.source === e.sourceId)
                //             .map(layer => layer.id);
                        
                //         // Log current map state
                //         console.log(`Current map center: [${this.map.getCenter().lng.toFixed(4)}, ${this.map.getCenter().lat.toFixed(4)}]`);
                //         console.log(`Current map zoom: ${this.map.getZoom().toFixed(2)}`);
                //         console.log(`Current map bounds:`, this.map.getBounds());
                        
                //         if (layersFromThisSource.length > 0) {
                //             const features = this.map.queryRenderedFeatures({
                //                 layers: layersFromThisSource
                //             });
                //             console.log(`Features visible from ${e.sourceId}:`, features.length);
                //             if (features.length > 0) {
                //                 console.log(`Sample feature from ${e.sourceId}:`, features[0]);
                //             } else {
                //                 console.warn(`No features visible from ${e.sourceId} - this might indicate an issue`);
                                
                //                 // Additional debugging: Check source-layer configuration
                //                 const sourceLayerInfo = layersFromThisSource.map(layerId => {
                //                     const layer = this.map.getLayer(layerId);
                //                     return {
                //                         layerId: layerId,
                //                         sourceLayer: layer['source-layer'],
                //                         type: layer.type,
                //                         visibility: layer.layout?.visibility || 'visible'
                //                     };
                //                 });
                //                 console.log(`Source layer config for ${e.sourceId}:`, sourceLayerInfo);
                                
                //                 // Try querying without specifying layers to see if any features exist
                //                 const allVisibleFeatures = this.map.queryRenderedFeatures();
                //                 const featuresFromThisSource = allVisibleFeatures.filter(f => f.source === e.sourceId);
                //                 console.log(`Total features from ${e.sourceId} (any layer):`, featuresFromThisSource.length);
                                
                //                 if (featuresFromThisSource.length > 0) {
                //                     console.log(`Sample feature (any layer) from ${e.sourceId}:`, featuresFromThisSource[0]);
                //                     console.log(`Source layer name found: "${featuresFromThisSource[0].sourceLayer}"`);
                //                 } else {
                //                     // The issue might be that we're not looking at the right geographic area
                //                     // Let's check if the PMTiles data covers the current view
                //                     console.log(`PMTiles file bounds from tippecanoe-decode suggest data around: 14.5-16.5°E, -5.0 to -3.0°N (DRC)`);
                //                     console.log(`Current view is centered at: [${this.map.getCenter().lng.toFixed(4)}, ${this.map.getCenter().lat.toFixed(4)}]`);
                //                     console.log(`Consider updating the map center to match the data bounds or regenerating tiles for your area of interest.`);
                //                 }
                //             }
                //         }
                //     } catch (error) {
                //         console.error(`Error querying features from ${e.sourceId}:`, error);
                //     }
                // }, 500);
            }
        });
        
        // Source data events
        // this.map.on('sourcedata', (e) => {
        //     if (e.sourceId === 'roads-tiles' && e.isSourceLoaded) {
        //         console.log('Roads tiles loaded successfully!');
        //         // Check if roads are visible at current zoom/extent
        //         setTimeout(() => {
        //             const features = this.map.queryRenderedFeatures({layers: ['roads']});
        //             console.log('Roads features visible:', features.length);
        //             if (features.length > 0) {
        //                 console.log('Sample road feature:', features[0]);
        //             }
        //         }, 1000);
        //     }
        // });
        
        // Click event for feature inspection
        // this.map.on('click', (e) => {
        //     const features = this.map.queryRenderedFeatures(e.point);
        //     if (features.length > 0) {
        //         const feature = features[0];
        //         console.log('Clicked feature:', feature);
                
        //         // Create popup with feature info
        //         new maplibregl.Popup()
        //             .setLngLat(e.lngLat)
        //             .setHTML(this.formatFeaturePopup(feature))
        //             .addTo(this.map);
        //     }
        // });
    }
    
    // controls
    addControls() {
        // Detect if user is on a mobile device (same logic as in createMap)
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || 
                         ('ontouchstart' in window) || 
                         (navigator.maxTouchPoints > 0);
        
        // Add navigation controls
        this.map.addControl(new maplibregl.NavigationControl(), 'top-right');
        
        // Add scale control
        this.map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
        
        // Add custom attribution control with responsive compact setting
        this.map.addControl(new maplibregl.AttributionControl({
            customAttribution: [
                '© <a href="https://overturemaps.org/" target="_blank">Overture Maps Foundation</a>',
                '© <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap contributors</a>',
                'Contours: <a href="https://www.usgs.gov/" target="_blank">USGS</a>'
            ],
            compact: isMobile // Compact on mobile, expanded on desktop
        }), 'bottom-right');
        
        // Add zoom level display
        this.addZoomDisplay();
        
        // Add tile grid control and layer
        this.addTileGrid();
        
        // Add layer legend control
        this.addLayerLegend();
    }
    
    /**
     * Add zoom level display control
     */
    addZoomDisplay() {
        const zoomDisplay = document.createElement('div');
        zoomDisplay.className = 'zoom-display';
        zoomDisplay.id = 'zoom-display';
        
        const updateZoom = () => {
            const zoom = this.map.getZoom();
            zoomDisplay.textContent = `Zoom: ${zoom.toFixed(1)}`;
        };
        
        this.map.on('zoom', updateZoom);
        updateZoom(); // Initial display
        
        document.getElementById(this.containerId).appendChild(zoomDisplay);
    }
    
    /**
     * Add tile grid overlay and toggle control
     */
    addTileGrid() {
        // Create tile grid control checkbox
        const tileGridControl = document.createElement('div');
        tileGridControl.className = 'tile-grid-control';
        tileGridControl.innerHTML = `
            <label>
                <input type="checkbox" id="tile-grid-toggle" />
                Tile Grid
            </label>
        `;
        
        document.getElementById(this.containerId).appendChild(tileGridControl);
        
        // Add tile grid source and layer after map loads
        this.map.on('load', () => {
            // Add tile grid source (using debug tiles style)
            if (!this.map.getSource('tile-grid-source')) {
                this.map.addSource('tile-grid-source', {
                    type: 'vector',
                    tiles: [
                        'data:application/x-protobuf;base64,'
                    ],
                    maxzoom: 22
                });
            }
            
            // Add tile grid layer using tile boundaries
            if (!this.map.getLayer('tile-grid')) {
                // Use maplibre's built-in tile boundary rendering
                this.map.showTileBoundaries = false; // We'll control this with our checkbox
            }
        });
        
        // Handle checkbox toggle
        const checkbox = tileGridControl.querySelector('#tile-grid-toggle');
        checkbox.addEventListener('change', (e) => {
            this.toggleTileGrid(e.target.checked);
        });
    }
    
    /**
     * Toggle tile grid visibility
     */
    toggleTileGrid(visible) {
        if (this.map) {
            this.map.showTileBoundaries = visible;
        }
    }
    
    
    // popups, later
    formatFeaturePopup(feature) {
        let content = `
            <div style="max-width: 200px;">
                <strong>Layer:</strong> ${feature.sourceLayer || feature.source}<br>
        `;
        
        // Special handling for contour features
        if (feature.sourceLayer === 'contours') {
            const elevation = feature.properties.ele;
            const level = feature.properties.level;
            content += `
                <strong>Elevation:</strong> ${elevation}' (${Math.round(elevation * 0.3048)}m)<br>
                <strong>Contour Type:</strong> ${level > 0 ? 'Major' : 'Minor'}<br>
            `;
        }
        
        content += `
                <strong>Properties:</strong><br>
                <pre style="font-size: 10px; white-space: pre-wrap;">${JSON.stringify(feature.properties, null, 2)}</pre>
            </div>
        `;
        
        return content;
    }
    
    // fallback
    getBasicStyle() {
        return {
            version: 8,
            glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf",
            sources: {},
            layers: [
                {
                    id: 'background',
                    type: 'background',
                    paint: {
                        'background-color': '#f0f0f0'
                    }
                }
            ]
        };
    }
    
    // append third party contours to cartographic style
    async addContourToStyle(style) {
        // Lazy load contour functionality
        const { demSource } = await initContours();
        
        // Add DEM source for hillshade
        style.sources.dem = {
            type: "raster-dem",
            encoding: "terrarium",
            tiles: [demSource.sharedDemProtocolUrl], // share cached DEM tiles with contour layer
            maxzoom: 16,
            tileSize: 256
        };
        
        // Add contour source
        style.sources.contours = {
            type: "vector",
            tiles: [
                demSource.contourProtocolUrl({
                    // meters to feet conversion for US data
                    multiplier: 3.28084,
                    thresholds: {
                        // zoom: [minor, major] contour intervals in feet
                        9: [300, 600],
                        10: [100, 400],
                        11: [50, 200],
                        12: [25, 100],
                        13: [12.5, 50]
                    },
                    elevationKey: "ele",
                    levelKey: "level",
                    contourLayer: "contours"
                })
            ],
            maxzoom: 16
        };
        
        // Insert hillshade layer after background but before other layers
        const hillshadeLayer = {
            id: "hills",
            type: "hillshade",
            source: "dem",
            paint: {
            "hillshade-exaggeration": [
                "interpolate",
                ["exponential", 1.5],
                ["zoom"],
                6, 0.65,
                9, 0.25,
                15, 0.15
            ],
            "hillshade-shadow-color": [
                "interpolate",
                ["linear"],
                ["zoom"],
                6, "rgba(0,0,0,0.2)",
                11, "rgba(0,0,0,0.05)",
                15, "rgba(123, 123, 123, 0)"
            ],
            "hillshade-highlight-color": [
                "interpolate",
                ["linear"],
                ["zoom"],
                11, "rgba(255,255,255,0.15)",
                13, "rgba(255,255,255,.1)",
                16, "rgba(239, 239, 239, 0.05)"
            ]
            }
        };
        
        // mix-blend-mode approximation
        // these opacity settings are the ones applied
        const contourLinesLayer = {
            id: "contours",
            type: "line",
            source: "contours",
            "source-layer": "contours",
            paint: {
            "line-color": "rgba(139, 69, 19, 0.4)",  // Neutral brown
            "line-width": [
                "interpolate",
                ["linear"],
                ["zoom"],
                6, [
                "case",
                ["==", ["get", "level"], 1], 0.15,  // Major contours
                0.1                                 // Minor contours
                ],
                9, [
                "case", 
                ["==", ["get", "level"], 1], 0.4,  // Major contours
                0.2                               // Minor contours
                ],
                13.5, [
                "case",
                ["==", ["get", "level"], 1], 0.5,  // Major contours
                0.25                              // Minor contours
                ]
            ],
            // Ramp opacity from X at zoom 11 to 1 at zoom 15
            "line-opacity": [
                "interpolate",
                ["exponential", 1.5],
                ["zoom"],
                10, 0.25,
                12, 1
            ]
            },
            layout: {
                "line-join": "round",
                "line-cap": "round"
            }
        };
        
        // contour labels, hidden rn
        const contourLabelsLayer = {
            id: "contour-text",
            type: "symbol",
            source: "contours",
            "source-layer": "contours",
            filter: [">", ["get", "level"], 0],
            paint: {
                "text-halo-color": "white",
                "text-halo-width": 2,
                "text-color": "rgba(139, 69, 19, 0.8)"
            },
            layout: {
                "visibility": "none",
                "symbol-placement": "line",
                "text-anchor": "center",
                "text-size": 10,
                "text-field": [
                    "concat",
                    ["number-format", ["get", "ele"], {}],
                    "'"
                ],
                "text-font": ["Noto Sans Bold"],
                "text-rotation-alignment": "map"
            }
        };
        
        // Add the contour and hillshade layers to the style
        // The sorting will be handled by sortLayersByDrawOrder() method
        style.layers.push(hillshadeLayer);
        style.layers.push(contourLinesLayer);
        style.layers.push(contourLabelsLayer);
    }

    /**
     * Sort layers according to the draw order index
     * @param {Object} style - The MapLibre style object
     * @returns {Object} - Style with sorted layers
     */
    sortLayersByDrawOrder(style) {
        if (!style.layers) return style;
        
        // Sort layers based on draw order index
        style.layers.sort((a, b) => {
            const orderA = this.layerDrawOrder[a.id] !== undefined ? this.layerDrawOrder[a.id] : 999;
            const orderB = this.layerDrawOrder[b.id] !== undefined ? this.layerDrawOrder[b.id] : 999;
            return orderA - orderB;
        });
        
        // console.log('Layer draw order applied:', style.layers.map(layer => ({
        //     id: layer.id,
        //     order: this.layerDrawOrder[layer.id] || 'unspecified'
        // })));
        
        return style;
    }
    
    /**
     * Add a new layer with specified draw order
     * @param {string} layerId - The layer ID
     * @param {number} drawOrder - The draw order index (0 = bottom, higher = top)
     * @param {Object} layerDefinition - The layer definition object
     */
    addLayerWithOrder(layerId, drawOrder, layerDefinition) {
        if (!this.map) return;
        
        // Update the draw order index
        this.layerDrawOrder[layerId] = drawOrder;
        
        // Find the correct position to insert the layer
        const sortedLayers = Object.entries(this.layerDrawOrder)
            .filter(([id, order]) => this.map.getLayer(id) && order <= drawOrder)
            .sort((a, b) => b[1] - a[1]); // Sort descending to find the layer just below
        
        const beforeLayerId = sortedLayers.length > 0 ? sortedLayers[0][0] : undefined;
        
        // Add the layer
        this.map.addLayer(layerDefinition, beforeLayerId);
        
        // console.log(`Added layer '${layerId}' with draw order ${drawOrder}`);
    }
    
    /**
     * Update layer draw order
     * @param {string} layerId - The layer ID
     * @param {number} newDrawOrder - The new draw order index
     */
    updateLayerOrder(layerId, newDrawOrder) {
        if (!this.map || !this.map.getLayer(layerId)) return;
        
        // Update the draw order index
        this.layerDrawOrder[layerId] = newDrawOrder;
        
        // Remove and re-add the layer to change its position
        const layerDefinition = this.map.getLayer(layerId);
        this.map.removeLayer(layerId);
        this.addLayerWithOrder(layerId, newDrawOrder, layerDefinition);
    }
    
    /**
     * Get the map instance
     */
    getMap() {
        return this.map;
    }
    
    /**
     * Apply label priority weightings to symbol layers
     * This ensures higher z-order labels get placement priority when overlapping
     * In MapLibre, lower symbol-sort-key values = higher priority (drawn first)
     */
    applyLabelPriorities() {
        if (!this.map) {
            console.warn('Map not initialized, cannot apply label priorities');
            return;
        }
        
        let appliedCount = 0;
        
        // Iterate through all label layers and apply symbol-sort-key
        for (const [layerId, priority] of Object.entries(this.labelPriority)) {
            const layer = this.map.getLayer(layerId);
            
            if (layer && layer.type === 'symbol') {
                try {
                    // Set symbol-sort-key in the layout properties
                    this.map.setLayoutProperty(layerId, 'symbol-sort-key', priority);
                    appliedCount++;
                    console.log(`Applied label priority ${priority} to layer: ${layerId}`);
                } catch (error) {
                    console.warn(`Failed to apply label priority to ${layerId}:`, error);
                }
            }
        }
        
        console.log(`Label priorities applied to ${appliedCount} layers`);
        
        // Log the priority hierarchy for verification
        if (appliedCount > 0) {
            const sortedPriorities = Object.entries(this.labelPriority)
                .sort((a, b) => a[1] - b[1]); // Sort by priority (low = high priority)
            
            console.log('Label priority hierarchy (highest to lowest):');
            console.table(sortedPriorities.map(([id, priority]) => ({
                'Layer ID': id,
                'Symbol Sort Key': priority,
                'Z-Order': 1000 - priority,
                'Priority': priority < 900 ? 'HIGH' : priority < 950 ? 'MEDIUM' : 'LOW'
            })));
        }
    }
    
    /**
     * Toggle layer visibility
     */
    toggleLayer(layerId, visible = null) {
        if (!this.map) return;
        
        const visibility = visible !== null ? 
            (visible ? 'visible' : 'none') : 
            (this.map.getLayoutProperty(layerId, 'visibility') === 'none' ? 'visible' : 'none');
        
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
    }
    
    /**
     * Toggle contour layers visibility
     */
    toggleContours(visible = null) {
        this.toggleLayer('contours', visible);
        this.toggleLayer('contour-text', visible);
    }
    
    /**
     * Toggle settlement extents layers visibility
     */
    toggleSettlementExtents(visible = null) {
        this.toggleLayer('settlement-extents-fill', visible);
        this.toggleLayer('settlement-extents-outlines', visible);
    }
    
    /**
     * Toggle hillshade visibility
     */
    toggleHillshade(visible = null) {
        this.toggleLayer('hills', visible);
    }
    
    /**
     * Set contour interval based on zoom level
     */
    setContourInterval(minorInterval, majorInterval) {
        if (!this.map) return;
        
        // Update the contour source with new thresholds
        const currentZoom = Math.floor(this.map.getZoom());
        const newThresholds = {};
        newThresholds[currentZoom] = [minorInterval, majorInterval];
        
        // Note: Changing contour intervals requires reloading the source
        console.log(`Contour intervals set to: minor=${minorInterval}ft, major=${majorInterval}ft`);
    }
    // /**
    //  * Get current contour paint properties (for debugging)
    //  */
    // getContourProperties() {
    //     if (!this.map || !this.map.getLayer('contours')) {
    //         return 'Contours layer not available';
    //     }
        
    //     return {
    //         color: this.map.getPaintProperty('contours', 'line-color'),
    //         opacity: this.map.getPaintProperty('contours', 'line-opacity'),
    //         width: this.map.getPaintProperty('contours', 'line-width'),
    //         zoom: this.map.getZoom(),
    //         sourceLoaded: this.map.isSourceLoaded('contours'),
    //         layerVisible: this.map.getLayoutProperty('contours', 'visibility') !== 'none'
    //     };
    // }

    /**
     * Cleanup resources
     */
    destroy() {
        if (!this.map) {
            this.map.remove();
        }
        if (this.protocol) {
            maplibregl.removeProtocol("pmtiles");
        }
    }
    
    /**
     * Get the current layer draw order configuration
     * @returns {Object} - The layer draw order index
     */
    getLayerDrawOrder() {
        return { ...this.layerDrawOrder };
    }
    
    /**
     * Get layers sorted by draw order
     * @returns {Array} - Array of layer IDs in draw order
     */
    getLayersByDrawOrder() {
        if (!this.map) return [];
        
        const layers = this.map.getStyle().layers;
        return layers
            .map(layer => ({
                id: layer.id,
                order: this.layerDrawOrder[layer.id] || 999
            }))
            .sort((a, b) => a.order - b.order)
            .map(item => item.id);
    }
    
    // /**
    //  * Print current layer order to console (for debugging)
    //  */
    // printLayerOrder() {
    //     if (!this.map) {
    //         console.log('Map not initialized');
    //         return;
    //     }
        
    //     const layers = this.map.getStyle().layers;
    //     console.log('Current layer stack (bottom to top):');
    //     console.table(layers.map((layer, index) => ({
    //         Position: index,
    //         'Layer ID': layer.id,
    //         'Draw Order': this.layerDrawOrder[layer.id] || 'unspecified',
    //         Type: layer.type
    //     })));
    // }
    
    /**
     * Get the current label priority configuration
     * @returns {Object} - The label priority index
     */
    getLabelPriorities() {
        return { ...this.labelPriority };
    }
    
    /**
     * Print current label priorities to console (for debugging)
     */
    printLabelPriorities() {
        if (!this.map) {
            console.log('Map not initialized');
            return;
        }
        
        const sortedPriorities = Object.entries(this.labelPriority)
            .sort((a, b) => a[1] - b[1]); // Sort by priority (low = high priority)
        
        console.log('Label priorities (lower value = higher priority):');
        console.table(sortedPriorities.map(([layerId, priority]) => {
            const layer = this.map.getLayer(layerId);
            const currentSortKey = layer ? this.map.getLayoutProperty(layerId, 'symbol-sort-key') : 'N/A';
            
            return {
                'Layer ID': layerId,
                'Priority Value': priority,
                'Z-Order': 1000 - priority,
                'Current Sort Key': currentSortKey,
                'Status': layer ? 'Active' : 'Not Found'
            };
        }));
    }
    
    /**
     * Setup mobile-specific touch optimizations
     */
    setupMobileTouchOptimizations() {
        if (!this.map) return;
        
        // Optimize canvas for touch interactions
        const canvas = this.map.getCanvas();
        canvas.style.touchAction = 'pan-x pan-y';
        
        // Improve rendering performance during touch interactions
        this.map.on('touchstart', () => {
            // Enable hardware acceleration during touch
            canvas.style.willChange = 'transform';
        });
        
        this.map.on('touchend', () => {
            // Restore normal rendering after touch interaction
            setTimeout(() => {
                canvas.style.willChange = 'auto';
            }, 200);
        });
        
        // Optimize rendering during zoom for better performance
        let isZooming = false;
        this.map.on('zoomstart', () => {
            isZooming = true;
            // Temporarily reduce rendering quality during zoom
            canvas.style.imageRendering = 'pixelated';
        });
        
        this.map.on('zoomend', () => {
            if (isZooming) {
                isZooming = false;
                // Restore high-quality rendering after zoom
                canvas.style.imageRendering = 'auto';
                // Force a repaint to ensure crisp rendering
                setTimeout(() => {
                    this.map.triggerRepaint();
                }, 50);
            }
        });
        
        // Optimize move events for better performance
        let isDragging = false;
        this.map.on('movestart', (e) => {
            if (e.originalEvent && e.originalEvent.type === 'touchstart') {
                isDragging = true;
                // Reduce quality during drag for better frame rate
                canvas.style.imageRendering = 'optimizeSpeed';
            }
        });
        
        this.map.on('moveend', () => {
            if (isDragging) {
                isDragging = false;
                // Restore quality after drag
                canvas.style.imageRendering = 'auto';
                setTimeout(() => {
                    this.map.triggerRepaint();
                }, 50);
            }
        });
    }
    
    /**
     * Toggle layer visibility
     */
    toggleLayerVisibility(layerId, visibility) {
        if (!this.map.getLayer(layerId)) {
            console.warn(`Layer with ID '${layerId}' does not exist.`);
            return;
        }
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
    }

    /**
     * Mute a layer (hide it)
     */
    muteLayer(layerId) {
        this.toggleLayerVisibility(layerId, 'none');
    }

    /**
     * Solo a layer (show only this layer)
     */
    soloLayer(layerId) {
        const layers = this.map.getStyle().layers;
        layers.forEach(layer => {
            const currentVisibility = this.map.getLayoutProperty(layer.id, 'visibility');
            if (currentVisibility !== 'none') {
                this.toggleLayerVisibility(layer.id, layer.id === layerId ? 'visible' : 'none');
            }
        });
    }

    /**
     * Add layer legend to the map
     */
    addLayerLegend() {
        document.addEventListener('DOMContentLoaded', () => {
            const legendContainer = document.createElement('div');
            legendContainer.id = 'map-legend';
            legendContainer.style.position = 'absolute';
            legendContainer.style.top = '10px';
            legendContainer.style.right = '10px';
            legendContainer.style.backgroundColor = 'rgba(255, 255, 255, 0.8)';
            legendContainer.style.padding = '10px';
            legendContainer.style.borderRadius = '5px';
            legendContainer.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.2)';

            const layers = [
                'background',
                'land-use',
                'land',
                'land-cover',
                'settlement-extents-fill',
                'settlement-extents-outlines',
                'water-polygons',
                'roads-solid',
                'buildings',
                // 'buildings-low-lod',
                // 'buildings-medium-lod',
                // 'buildings-high-lod',
                'places'
            ];

            layers.forEach(layerId => {
                const layerItem = document.createElement('div');
                layerItem.style.display = 'flex';
                layerItem.style.alignItems = 'center';
                layerItem.style.marginBottom = '5px';

                const soloButton = document.createElement('div');
                soloButton.style.width = '10px';
                soloButton.style.height = '10px';
                soloButton.style.borderRadius = '50%';
                soloButton.style.backgroundColor = '#333';
                soloButton.style.marginRight = '5px';
                soloButton.style.cursor = 'pointer';
                soloButton.title = `Solo ${layerId}`;
                soloButton.addEventListener('click', () => {
                    this.soloLayer(layerId);
                });

                const layerName = document.createElement('span');
                layerName.textContent = layerId;
                layerName.style.cursor = 'pointer';
                layerName.title = `Toggle ${layerId}`;
                layerName.addEventListener('click', () => {
                    const currentVisibility = this.getMap().getLayoutProperty(layerId, 'visibility');
                    this.toggleLayer(layerId, currentVisibility === 'none');
                });

                layerItem.appendChild(soloButton);
                layerItem.appendChild(layerName);
                legendContainer.appendChild(layerItem);
            });

            document.body.appendChild(legendContainer);
        });
    }
}

/**
 * Auto-cleanup on page unload
 */
window.addEventListener('beforeunload', () => {
    maplibregl.removeProtocol("pmtiles");
});

// Export the class as default
export default OvertureMap;
