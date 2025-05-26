/**
 * MapManager - High-level wrapper for Leaflet map instance
 *
 * Provides a clean interface for map operations and integrates with
 * the StateManager and EventBus for coordinated state management.
 *
 * Features:
 * - Leaflet map lifecycle management
 * - Base map switching
 * - Layer management with event coordination
 * - Error handling and recovery
 * - Performance monitoring
 */

import { EventTypes } from './EventBus.js'

export class MapManager {
  constructor (stateManager, eventBus) {
    this.state = stateManager
    this.events = eventBus

    // Map instance and configuration
    this.map = null
    this.baseMaps = {}
    this.currentBasemap = 'streets'

    // Layer management
    this.layers = new Map()
    this.layerGroups = new Map()

    // Map initialization config
    this.defaultConfig = {
      center: [45.5152, -122.6784], // Portland, OR
      zoom: 11,
      zoomDelta: 0.25,
      zoomSnap: 0.25,
      wheelPxPerZoomLevel: 120,
      preferCanvas: false,
      maxZoom: 18,
      minZoom: 8
    }

    // Performance monitoring
    this.performanceMetrics = {
      initTime: null,
      layerRenderTimes: new Map(),
      lastFrameTime: performance.now()
    }

    // Set up event listeners
    this.setupEventListeners()

    console.log('🗺️ MapManager initialized')
  }

  /**
     * Initialize the Leaflet map
     * @param {string|HTMLElement} containerId - Map container ID or element
     * @param {Object} config - Optional map configuration
     * @returns {Promise<L.Map>} Initialized map instance
     */
  async initializeMap (containerId, config = {}) {
    const startTime = performance.now()

    try {
      this.events.emit(EventTypes.APP_INITIALIZING, { stage: 'map_init' })

      // Merge configuration
      const mapConfig = { ...this.defaultConfig, ...config }

      // Initialize Leaflet map with error handling
      this.map = this.createMapInstance(containerId, mapConfig)

      // Set up base maps
      this.initializeBaseMaps()

      // Set up map event handlers
      this.setupMapEventHandlers()

      // Set up error handling
      this.setupErrorHandling()

      // Add scale control
      this.addScaleControl()

      // Store map in state
      this.state.setState({ map: this.map }, { source: 'MapManager' })

      // Performance tracking
      this.performanceMetrics.initTime = performance.now() - startTime

      console.log(`🗺️ Map initialized in ${this.performanceMetrics.initTime.toFixed(2)}ms`)

      // Emit map ready event
      this.events.emit(EventTypes.MAP_READY, {
        map: this.map,
        config: mapConfig,
        initTime: this.performanceMetrics.initTime
      })

      return this.map
    } catch (error) {
      console.error('❌ Map initialization failed:', error)
      this.events.emit(EventTypes.ERROR_CRITICAL, {
        context: 'map_initialization',
        error: error.message
      })

      // Attempt fallback initialization
      return this.attemptFallbackInitialization(containerId)
    }
  }

  /**
     * Create Leaflet map instance with proper error handling
     * @param {string|HTMLElement} containerId - Map container
     * @param {Object} config - Map configuration
     * @returns {L.Map} Leaflet map instance
     */
  createMapInstance (containerId, config) {
    try {
      // Full feature initialization
      return L.map(containerId, {
        center: config.center,
        zoom: config.zoom,
        zoomDelta: config.zoomDelta,
        zoomSnap: config.zoomSnap,
        wheelPxPerZoomLevel: config.wheelPxPerZoomLevel,
        preferCanvas: config.preferCanvas,
        maxZoom: config.maxZoom,
        minZoom: config.minZoom,
        fullscreenControl: true,
        fullscreenControlOptions: {
          position: 'topleft'
        }
      })
    } catch (error) {
      console.warn('⚠️ Full map initialization failed, trying fallback:', error.message)

      // Fallback with minimal options
      return L.map(containerId, {
        center: config.center,
        zoom: config.zoom,
        zoomDelta: config.zoomDelta,
        zoomSnap: config.zoomSnap,
        wheelPxPerZoomLevel: config.wheelPxPerZoomLevel
      })
    }
  }

  /**
     * Initialize base map layers
     */
  initializeBaseMaps () {
    this.baseMaps = {
      streets: L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
        maxZoom: 19
      }),
      satellite: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: '© Esri, Digital Globe, GeoEye, Earthstar Geographics',
        maxZoom: 18
      }),
      topo: L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenTopoMap contributors',
        maxZoom: 17
      }),
      dark: L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors, © CARTO',
        maxZoom: 19
      }),
      'dark-nolabels': L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors, © CARTO',
        maxZoom: 19
      })
    }

    // Add default base map
    const defaultBasemap = this.state.getState('basemap') || 'streets'
    this.setBasemap(defaultBasemap)

    console.log('🗺️ Base maps initialized:', Object.keys(this.baseMaps))
  }

  /**
     * Set up map event handlers
     */
  setupMapEventHandlers () {
    if (!this.map) return

    // Zoom events
    this.map.on('zoomstart', () => {
      this.events.emit(EventTypes.MAP_ZOOM_CHANGED, {
        type: 'start',
        zoom: this.map.getZoom()
      })
    })

    this.map.on('zoomend', () => {
      const zoom = this.map.getZoom()
      this.events.emit(EventTypes.MAP_ZOOM_CHANGED, {
        type: 'end',
        zoom
      })
      this.state.setState({ currentZoom: zoom }, { source: 'MapManager' })
    })

    // Move events
    this.map.on('moveend', () => {
      const bounds = this.map.getBounds()
      const center = this.map.getCenter()

      this.events.emit(EventTypes.MAP_BOUNDS_CHANGED, {
        bounds,
        center
      })
    })

    // Click events
    this.map.on('click', (e) => {
      this.events.emit(EventTypes.MAP_FEATURE_CLICKED, {
        latlng: e.latlng,
        containerPoint: e.containerPoint,
        originalEvent: e.originalEvent
      })
    })

    // Error events
    this.map.on('tileerror', (e) => {
      console.warn('⚠️ Tile load error:', e)
      this.events.emit(EventTypes.ERROR_NETWORK, {
        context: 'tile_loading',
        url: e.tile.src,
        error: 'Tile failed to load'
      })
    })

    console.log('🗺️ Map event handlers set up')
  }

  /**
     * Set up global error handling for touch events
     */
  setupErrorHandling () {
    // Handle known Leaflet touch issues
    window.addEventListener('error', (e) => {
      if (e.message && e.message.includes('touchleave')) {
        console.warn('Ignoring touchleave error (known Leaflet issue):', e.message)
        e.preventDefault()
        return true
      }
    })

    console.log('🗺️ Error handling set up')
  }

  /**
     * Add scale control to map
     */
  addScaleControl () {
    if (!this.map) return

    L.control.scale({
      position: 'bottomleft',
      imperial: true,
      metric: true,
      maxWidth: 200
    }).addTo(this.map)

    console.log('🗺️ Scale control added')
  }

  /**
     * Switch base map
     * @param {string} basemapKey - Key of the base map to switch to
     */
  setBasemap (basemapKey) {
    if (!this.map || !this.baseMaps[basemapKey]) {
      console.warn(`⚠️ Invalid basemap key: ${basemapKey}`)
      return
    }

    // Remove current base map
    if (this.currentBasemap && this.baseMaps[this.currentBasemap]) {
      this.map.removeLayer(this.baseMaps[this.currentBasemap])
    }

    // Add new base map
    this.baseMaps[basemapKey].addTo(this.map)
    this.currentBasemap = basemapKey

    // Update state
    this.state.setState({ basemap: basemapKey }, { source: 'MapManager' })

    // Emit event
    this.events.emit(EventTypes.MAP_BASEMAP_CHANGED, {
      previousBasemap: this.currentBasemap,
      newBasemap: basemapKey
    })

    console.log(`🗺️ Basemap changed to: ${basemapKey}`)
  }

  /**
     * Add a layer to the map
     * @param {string} layerId - Unique identifier for the layer
     * @param {L.Layer} layer - Leaflet layer instance
     * @param {Object} options - Layer options
     */
  addLayer (layerId, layer, options = {}) {
    if (!this.map) {
      console.warn('⚠️ Cannot add layer: map not initialized')
      return
    }

    const startTime = performance.now()

    try {
      // Remove existing layer with same ID
      this.removeLayer(layerId)

      // Add layer to map
      layer.addTo(this.map)

      // Store layer reference
      this.layers.set(layerId, {
        layer,
        options,
        addedTime: Date.now()
      })

      // Performance tracking
      const renderTime = performance.now() - startTime
      this.performanceMetrics.layerRenderTimes.set(layerId, renderTime)

      // Update state if this is the current layer
      if (layerId === 'currentLayer') {
        this.state.setState({ currentLayer: layer }, { source: 'MapManager' })
      }

      // Emit event
      this.events.emit(EventTypes.MAP_LAYER_CHANGED, {
        action: 'added',
        layerId,
        renderTime
      })

      console.log(`🗺️ Layer '${layerId}' added in ${renderTime.toFixed(2)}ms`)
    } catch (error) {
      console.error(`❌ Failed to add layer '${layerId}':`, error)
      this.events.emit(EventTypes.ERROR_WARNING, {
        context: 'layer_addition',
        layerId,
        error: error.message
      })
    }
  }

  /**
     * Remove a layer from the map
     * @param {string} layerId - ID of the layer to remove
     */
  removeLayer (layerId) {
    const layerInfo = this.layers.get(layerId)

    if (layerInfo && this.map) {
      try {
        this.map.removeLayer(layerInfo.layer)
        this.layers.delete(layerId)

        // Clear performance metrics
        this.performanceMetrics.layerRenderTimes.delete(layerId)

        // Update state if this was the current layer
        if (layerId === 'currentLayer') {
          this.state.setState({ currentLayer: null }, { source: 'MapManager' })
        }

        // Emit event
        this.events.emit(EventTypes.MAP_LAYER_CHANGED, {
          action: 'removed',
          layerId
        })

        console.log(`🗺️ Layer '${layerId}' removed`)
      } catch (error) {
        console.error(`❌ Failed to remove layer '${layerId}':`, error)
      }
    }
  }

  /**
     * Get a layer by ID
     * @param {string} layerId - ID of the layer
     * @returns {L.Layer|null} Layer instance or null
     */
  getLayer (layerId) {
    const layerInfo = this.layers.get(layerId)
    return layerInfo ? layerInfo.layer : null
  }

  /**
     * Get all layer IDs
     * @returns {Array<string>} Array of layer IDs
     */
  getLayerIds () {
    return Array.from(this.layers.keys())
  }

  /**
     * Fit map to bounds
     * @param {L.LatLngBounds} bounds - Bounds to fit
     * @param {Object} options - Fit options
     */
  fitBounds (bounds, options = {}) {
    if (!this.map) return

    const defaultOptions = {
      padding: [20, 20],
      maxZoom: 16
    }

    this.map.fitBounds(bounds, { ...defaultOptions, ...options })

    console.log('🗺️ Map fitted to bounds')
  }

  /**
     * Set map view
     * @param {L.LatLng|Array} center - Center coordinates
     * @param {number} zoom - Zoom level
     * @param {Object} options - Pan options
     */
  setView (center, zoom, options = {}) {
    if (!this.map) return

    this.map.setView(center, zoom, options)

    console.log(`🗺️ Map view set to ${center}, zoom ${zoom}`)
  }

  /**
     * Attempt fallback initialization if main initialization fails
     * @param {string|HTMLElement} containerId - Map container
     * @returns {L.Map|null} Fallback map instance or null
     */
  attemptFallbackInitialization (containerId) {
    try {
      console.log('🔄 Attempting fallback map initialization...')

      // Minimal configuration fallback
      this.map = L.map(containerId).setView([45.5152, -122.6784], 11)

      // Add basic tile layer
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
      }).addTo(this.map)

      // Store in state
      this.state.setState({ map: this.map }, { source: 'MapManager:fallback' })

      console.log('✅ Fallback map initialization successful')

      this.events.emit(EventTypes.MAP_READY, {
        map: this.map,
        fallback: true
      })

      return this.map
    } catch (error) {
      console.error('❌ Fallback map initialization also failed:', error)
      this.events.emit(EventTypes.ERROR_CRITICAL, {
        context: 'fallback_map_initialization',
        error: error.message
      })
      return null
    }
  }

  /**
     * Get map performance metrics
     * @returns {Object} Performance metrics
     */
  getPerformanceMetrics () {
    return {
      ...this.performanceMetrics,
      totalLayers: this.layers.size,
      mapState: this.map ? 'initialized' : 'not_initialized',
      currentBasemap: this.currentBasemap
    }
  }

  /**
     * Cleanup and destroy map instance
     */
  destroy () {
    if (this.map) {
      try {
        // Remove all layers
        this.layers.forEach((layerInfo, layerId) => {
          this.removeLayer(layerId)
        })

        // Remove map
        this.map.remove()
        this.map = null

        // Clear state
        this.state.setState({
          map: null,
          currentLayer: null
        }, { source: 'MapManager:destroy' })

        // Clear performance metrics
        this.performanceMetrics.layerRenderTimes.clear()

        console.log('🗺️ MapManager destroyed')
      } catch (error) {
        console.error('❌ Error destroying map:', error)
      }
    }
  }

  /**
   * Set up event listeners for external events
   */
  setupEventListeners () {
    // Listen for basemap changes from UI
    this.events.on('ui:basemapChanged', (data) => {
      console.log(`🗺️ Received basemap change request: ${data.basemapKey}`)
      this.setBasemap(data.basemapKey)
    })
  }
}
