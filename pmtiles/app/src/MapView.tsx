/* @refresh reload */
import { render } from "solid-js/web";
import "./index.css";
import MaplibreInspect from "@maplibre/maplibre-gl-inspect";
import "@maplibre/maplibre-gl-inspect/dist/maplibre-gl-inspect.css";
import * as maplibregl from "maplibre-gl";
import {
  AttributionControl,
  GeolocateControl,
  GlobeControl,
  Map as MaplibreMap,
  NavigationControl,
  Popup,
  addProtocol,
  getRTLTextPluginStatus,
  removeProtocol,
  setRTLTextPlugin,
} from "maplibre-gl";
import type {
  LngLatBoundsLike,
  MapGeoJSONFeature,
  MapTouchEvent,
  StyleSpecification,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import type { LayerSpecification } from "@maplibre/maplibre-gl-style-spec";
import { PMTiles, Protocol } from "pmtiles";
import {
  For,
  Show,
  createEffect,
  createMemo,
  createSignal,
  onMount,
} from "solid-js";
import { VERSION_COMPATIBILITY } from "./utils";
import { APP_CONFIG, getTileSourceConfig, logConfig } from "./config";
import baseStyle from "./cartography.json";

const STYLE_MAJOR_VERSION = 5;

function getSourceLayer(l: LayerSpecification): string {
  if ("source-layer" in l && l["source-layer"]) {
    return l["source-layer"];
  }
  return "";
}

const featureIdToOsmId = (raw: string | number) => {
  return Number(BigInt(raw) & ((BigInt(1) << BigInt(44)) - BigInt(1)));
};

const featureIdToOsmType = (i: string | number) => {
  const t = (BigInt(i) >> BigInt(44)) & BigInt(3);
  if (t === BigInt(1)) return "node";
  if (t === BigInt(2)) return "way";
  if (t === BigInt(3)) return "relation";
  return "not_osm";
};

const displayId = (featureId?: string | number) => {
  if (featureId) {
    const osmType = featureIdToOsmType(featureId);
    if (osmType !== "not_osm") {
      const osmId = featureIdToOsmId(featureId);
      return (
        <a
          class="underline text-purple"
          target="_blank"
          rel="noreferrer"
          href={`https://openstreetmap.org/${osmType}/${osmId}`}
        >
          {osmType} {osmId}
        </a>
      );
    }
  }
  return featureId;
};

const FeaturesProperties = (props: { features: MapGeoJSONFeature[] }) => {
  return (
    <div class="features-properties">
      <For each={props.features}>
        {(f) => (
          <div>
            <span>
              <strong>{getSourceLayer(f.layer)}</strong>
              <span> ({f.geometry.type})</span>
            </span>
            <table>
              <tbody>
                <tr>
                  <td>id</td>
                  <td>{displayId(f.id)}</td>
                </tr>
                <For each={Object.entries(f.properties)}>
                  {([key, value]) => (
                    <tr>
                      <td>{key}</td>
                      <td>{value}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        )}
      </For>
    </div>
  );
};

function getMaplibreStyle(demSource: any): StyleSpecification {
  // Start with base style from cartography.json
  const style = JSON.parse(JSON.stringify(baseStyle)) as StyleSpecification;

  // Get tile source configuration (either PMTiles URL or Cloudflare Worker tiles)
  const tileSourceConfig = getTileSourceConfig();
  const buildingsSourceConfig = getTileSourceConfig("buildings");

  // Override/add tile sources with our configuration
  style.sources = {
    ...style.sources,
    protomaps: {
      type: "vector",
      attribution: tileSourceConfig.attribution,
      // Use either 'url' (for pmtiles:// protocol) or 'tiles' (for Cloudflare Worker)
      ...(tileSourceConfig.url ? { url: tileSourceConfig.url } : {}),
      ...(tileSourceConfig.tiles ? { tiles: tileSourceConfig.tiles } : {}),
      maxzoom: 22, // Allow overzooming
    },
    buildings: {
      type: "vector",
      attribution: buildingsSourceConfig.attribution,
      ...(buildingsSourceConfig.url ? { url: buildingsSourceConfig.url } : {}),
      ...(buildingsSourceConfig.tiles ? { tiles: buildingsSourceConfig.tiles } : {}),
      maxzoom: 22,
    },
    dem: {
      type: "raster-dem",
      encoding: "terrarium",
      tiles: [demSource.sharedDemProtocolUrl],
      maxzoom: 16,
      tileSize: 256,
    },
    contours: {
      type: "vector",
      tiles: [
        demSource.contourProtocolUrl({
          multiplier: 1, // Keep meters
          thresholds: {
            9: [100, 200],
            10: [50, 100],
            11: [20, 100],
            12: [10, 50],
            13: [5, 25],
          },
          elevationKey: "ele",
          levelKey: "level",
          contourLayer: "contours",
        }),
      ],
      maxzoom: 16,
    },
  };

  // Add buildings layers
  style.layers = [
    ...style.layers,
    {
      id: "custom-buildings-fill",
      type: "fill",
      source: "buildings",
      "source-layer": "buildings",
      paint: {
        "fill-color": "#2bff00",
        "fill-opacity": 0.7,
      },
    },
    {
      id: "custom-buildings-outline",
      type: "line",
      source: "buildings",
      "source-layer": "buildings",
      paint: {
        "line-color": "#5050ff",
        "line-width": 0.5,
      },
    },
  ];

  return style;
}

function MapLibreView() {
  let mapContainer: HTMLDivElement | undefined;
  let mapRef: MaplibreMap | undefined;
  let hiddenRef: HTMLDivElement | undefined;
  let longPressTimeout: ReturnType<typeof setTimeout>;

  const [error, setError] = createSignal<string | undefined>();
  const [timelinessInfo, setTimelinessInfo] = createSignal<string>();
  const [protocolRef, setProtocolRef] = createSignal<Protocol | undefined>();
  const [zoom, setZoom] = createSignal<number>(0);
  const [mismatch, setMismatch] = createSignal<string>("");

  onMount(async () => {
    // Log tile configuration for debugging
    logConfig();

    if (getRTLTextPluginStatus() === "unavailable") {
      setRTLTextPlugin(
        "https://unpkg.com/@mapbox/mapbox-gl-rtl-text@0.2.3/mapbox-gl-rtl-text.min.js",
        true,
      );
    }

    if (!mapContainer) {
      console.error("Could not mount map element");
      return;
    }

    // Only register pmtiles protocol if not using Cloudflare Worker
    // (Cloudflare Worker serves tiles directly, doesn't need the protocol)
    if (!APP_CONFIG.tiles.useCloudflare) {
      const protocol = new Protocol({ metadata: true });
      setProtocolRef(protocol);
      addProtocol("pmtiles", protocol.tile);
    }

    // Setup maplibre-contour
    const mlcontourModule = await import("maplibre-contour");
    const mlcontour = mlcontourModule.default;
    
    // Create DEM source
    const demSource = new mlcontour.DemSource({
      url: "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png",
      encoding: "terrarium",
      maxzoom: 16,
      worker: true,
      cacheSize: 100,
      timeoutMs: 10_000,
    });
    
    // Setup maplibre with the DemSource
    demSource.setupMaplibre(maplibregl);

    // clamp to minimize tile calls
    const drcBounds: LngLatBoundsLike = [[8, -13], [35, 9]];

    // Get style with contours
    const style = getMaplibreStyle(demSource);

    const map = new MaplibreMap({
      hash: "map",
      container: mapContainer,
      style: style,
      center: [21.5, -4], // Center of DRC 
      zoom: 6, 
      minZoom: 3,
      maxZoom: 15.75,
      maxBounds: drcBounds, // viewport restriction
      attributionControl: false,
      refreshExpiredTiles: false,
      maxTileCacheSize: 500,
      cancelPendingTileRequestsWhileZooming: true,
      renderWorldCopies: false,
      fadeDuration: 200
    });

    map.addControl(new NavigationControl());
    map.addControl(new GlobeControl());
    map.addControl(
      new GeolocateControl({
        positionOptions: {
          enableHighAccuracy: true,
        },
        trackUserLocation: true,
        fitBoundsOptions: {
          animate: false,
        },
      }),
    );

    map.addControl(
      new AttributionControl({
        compact: false,
      }),
    );

    map.addControl(
      new MaplibreInspect({
        popup: new Popup({
          closeButton: false,
          closeOnClick: false,
        }),
      }),
    );

    const popup = new Popup({
      closeButton: true,
      closeOnClick: false,
      maxWidth: "none",
    });

    map.on("load", () => {
      map.resize();
    });

    map.on("error", (e) => {
      setError(e.error.message);
    });

    map.on("idle", () => {
      setZoom(map.getZoom());
      setError(undefined);
      archiveInfo().then((i) => {
        if (i?.metadata) {
          const m = i.metadata as {
            version?: string;
            "planetiler:osm:osmosisreplicationtime"?: string;
          };
          setTimelinessInfo(
            `tiles@${m.version} ${m["planetiler:osm:osmosisreplicationtime"]?.substr(0, 10)}`,
          );
        }
      });
    });

    const showContextMenu = (e: MapTouchEvent) => {
      const features = map.queryRenderedFeatures(e.point);
      if (hiddenRef && features.length) {
        hiddenRef.innerHTML = "";
        render(() => <FeaturesProperties features={features} />, hiddenRef);
        popup.setHTML(hiddenRef.innerHTML);
        popup.setLngLat(e.lngLat);
        popup.addTo(map);
      } else {
        popup.remove();
      }
    };

    map.on("contextmenu", (e: MapTouchEvent) => {
      showContextMenu(e);
    });

    map.on("touchstart", (e: MapTouchEvent) => {
      longPressTimeout = setTimeout(() => {
        showContextMenu(e);
      }, 500);
    });

    const clearLongPress = () => {
      clearTimeout(longPressTimeout);
    };

    map.on("zoom", (e) => {
      setZoom(e.target.getZoom());
    });

    map.on("touchend", clearLongPress);
    map.on("touchcancel", clearLongPress);
    map.on("touchmove", clearLongPress);
    map.on("pointerdrag", clearLongPress);
    map.on("pointermove", clearLongPress);
    map.on("moveend", clearLongPress);
    map.on("gesturestart", clearLongPress);
    map.on("gesturechange", clearLongPress);
    map.on("gestureend", clearLongPress);

    mapRef = map;

    return () => {
      // Only remove protocol if it was registered (not using Cloudflare Worker)
      if (!APP_CONFIG.tiles.useCloudflare) {
        setProtocolRef(undefined);
        removeProtocol("pmtiles");
      }
      map.remove();
    };
  });

  const archiveInfo = async (): Promise<
    { metadata: unknown; bounds: LngLatBoundsLike } | undefined
  > => {
    // When using Cloudflare Worker, we can't easily get metadata
    // In that case, return undefined (map will use default bounds/center)
    if (APP_CONFIG.tiles.useCloudflare) {
      // console.log("Using Cloudflare Worker - metadata not available");
      return undefined;
    }

    // For direct PMTiles, fetch metadata via protocol
    const p = protocolRef();
    if (p && APP_CONFIG.tiles.directPMTilesUrl) {
      let archive = p.tiles.get(APP_CONFIG.tiles.directPMTilesUrl);
      if (!archive) {
        archive = new PMTiles(APP_CONFIG.tiles.directPMTilesUrl);
        p.add(archive);
      }
      const metadata = await archive.getMetadata();
      const header = await archive.getHeader();
      return {
        metadata: metadata,
        bounds: [
          [header.minLon, header.minLat],
          [header.maxLon, header.maxLat],
        ],
      };
    }
  };

  const memoizedStyle = createMemo(async () => {
    // Lazy load contour module
    const mlcontourModule = await import("maplibre-contour");
    const mlcontour = mlcontourModule.default;
    
    // Create DEM source
    const demSource = new mlcontour.DemSource({
      url: "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png",
      encoding: "terrarium",
      maxzoom: 16,
      worker: true,
      cacheSize: 100,
      timeoutMs: 10_000,
    });
    
    demSource.setupMaplibre(maplibregl);
    
    return getMaplibreStyle(demSource);
  });

  createEffect(() => {
    archiveInfo().then((i) => {
      if (i && i.metadata instanceof Object && "version" in i.metadata) {
        const tilesetVersion = +(i.metadata.version as string).split(".")[0];
        if (
          VERSION_COMPATIBILITY[tilesetVersion].indexOf(STYLE_MAJOR_VERSION) < 0
        ) {
          setMismatch(
            `style v${STYLE_MAJOR_VERSION} may not be compatible with tileset v${tilesetVersion}. `,
          );
        } else {
          setMismatch("");
        }
      }
    });
  });

  createEffect(() => {
    if (mapRef) {
      const map = mapRef;
      memoizedStyle().then((style) => {
        map.setStyle(style);
      });
    }
  });

  return (
    <>
      <div class="hidden" ref={hiddenRef} />
      <div ref={mapContainer} class="h-full w-full flex" />
      <div class="absolute bottom-0 p-1 text-xs bg-white bg-opacity-50">
        {timelinessInfo()} z@{zoom().toFixed(2)}
        <Show when={mismatch()}>
          <div class="font-bold text-red">
            {mismatch()}
            <a
              class="underline"
              href="https://docs.protomaps.com/basemaps/downloads#current-version"
            >
              See Docs.
            </a>
          </div>
        </Show>
      </div>
      <Show when={error()}>
        <div class="absolute p-8 flex justify-center items-center bg-white bg-opacity-50 font-mono text-red">
          {error()}
        </div>
      </Show>
    </>
  );
}

function MapView() {
  return (
    <div class="flex flex-col h-dvh w-full">
      <div class="h-full flex grow-1">
        <MapLibreView />
      </div>
    </div>
  );
}

const root = document.getElementById("root");

if (root) {
  render(() => <MapView />, root);
}
