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
  ScaleControl,
  getRTLTextPluginStatus,
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
import {
  For,
  createSignal,
  onMount,
} from "solid-js";
import { getTileSourceConfig, logConfig } from "./config";
import baseStyle from "./cartography.json";

// Light configuration for 3D features
const LIGHT_CONFIG = {
  anchor: "map" as const, // 'viewport' or 'map'
  position: [240, 45, 45] as [number, number, number], // [radial, azimuthal, polar] in degrees
  color: "#bebebe",
  intensity: 0.9, // 0 to 1
};

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

  // Get tile source configurations from Cloudflare Worker
  const protomapsConfig = getTileSourceConfig("protomaps");
  const overtureConfig = getTileSourceConfig("overture");
  const grid3Config = getTileSourceConfig("grid3");

  // Update the existing sources with Cloudflare Worker tile endpoints
  if (style.sources.protomaps) {
    style.sources.protomaps = {
      type: "vector",
      attribution: protomapsConfig.attribution,
      tiles: protomapsConfig.tiles,
      maxzoom: protomapsConfig.maxzoom,
    };
  }

  if (style.sources.overture) {
    style.sources.overture = {
      type: "vector",
      attribution: overtureConfig.attribution,
      tiles: overtureConfig.tiles,
      maxzoom: overtureConfig.maxzoom,
    };
  }

  // Add GRID3 source when ready
  if (style.sources.grid3) {
    style.sources.grid3 = {
      type: "vector",
      attribution: grid3Config.attribution,
      tiles: grid3Config.tiles,
      maxzoom: grid3Config.maxzoom,
    };
  }


  // Add DEM and contours sources for terrain
  style.sources.dem = {
    type: "raster-dem",
    encoding: "terrarium",
    tiles: [demSource.sharedDemProtocolUrl],
    maxzoom: 15,
    tileSize: 256,
  };

  style.sources.contours = {
    type: "vector",
    tiles: [
      demSource.contourProtocolUrl({
        multiplier: 1, // Keep meters
        thresholds: {
          8: [240, 480],
          9: [120, 240],
          10: [60, 120],
          11: [30, 120],
          12: [15, 60],
          13: [5, 60],
        },
        elevationKey: "ele",
        levelKey: "level",
        contourLayer: "contours",
      }),
    ],
    maxzoom: 15,
  };

  // Add global light source for 3D features
  style.light = {
    anchor: LIGHT_CONFIG.anchor,
    position: LIGHT_CONFIG.position,
    color: LIGHT_CONFIG.color,
    intensity: LIGHT_CONFIG.intensity,
  };

  return style;
}

function MapLibreView() {
  let mapContainer: HTMLDivElement | undefined;
  let hiddenRef: HTMLDivElement | undefined;
  let longPressTimeout: ReturnType<typeof setTimeout>;

  const [zoom, setZoom] = createSignal<number>(0);

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

    // Using Cloudflare Worker for tile delivery - no PMTiles protocol needed

    // Setup maplibre-contour
    const mlcontourModule = await import("maplibre-contour");
    const mlcontour = mlcontourModule.default;
    
    // Create DEM source
    const demSource = new mlcontour.DemSource({
      url: "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png",
      encoding: "terrarium",
      maxzoom: 15,
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
      maxZoom: 15.99,
      maxBounds: drcBounds, // viewport restriction
      attributionControl: false,
      refreshExpiredTiles: false,
      maxTileCacheSize: 500,
      // cancelPendingTileRequestsWhileZooming: true,
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

    // Add scale control at bottom-left
    map.addControl(new ScaleControl(), 'bottom-left');

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
      closeOnClick: true,
      maxWidth: "none",
    });

    map.on("load", () => {
      map.resize();
    });

    map.on("error", (e) => {
      console.error("Map error:", e);
    });

    map.on("idle", () => {
      setZoom(map.getZoom());
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

    return () => {
      map.remove();
    };
  });

  return (
    <>
      <div class="hidden" ref={hiddenRef} />
      <div ref={mapContainer} class="h-full w-full flex" />
      <div class="absolute top-0 left-0 p-1 text-xs bg-white bg-opacity-50">
        z@{zoom().toFixed(2)}
      </div>
    </>
  );
}

function MapView() {
  return (
    <div class="flex flex-col h-dvh w-full">
      <div class="h-full flex grow">
        <MapLibreView />
      </div>
    </div>
  );
}

const root = document.getElementById("root");

if (root) {
  render(() => <MapView />, root);
}
