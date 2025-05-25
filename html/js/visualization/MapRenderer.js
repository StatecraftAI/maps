/**
 * MapRenderer - GeoJSON Styling and Map Visualization
 *
 * Handles:
 * - GeoJSON layer styling and rendering
 * - Layer management and updates
 * - Feature styling based on current field
 * - Hover and click interactions
 * - Performance optimization for large datasets
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'
import { ColorManager } from './ColorManager.js'
import { PopupManager } from './PopupManager.js'

export class MapRenderer {
  constructor (stateManager, eventBus, mapManager, colorManager, popupManager) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager

    // Use passed components instead of creating new ones
    this.colorManager = colorManager
    this.popupManager = popupManager

    // Layer management
    this.currentLayer = null
    this.layerCache = new Map()

    // Performance tracking
    this.renderMetrics = {
      totalRenders: 0,
      averageRenderTime: 0,
      lastRenderTime: 0
    }

    this.setupEventListeners()

    console.log('[MapRenderer] Initialized')
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    console.log('[MapRenderer] Setting up event listeners...')
    
    // Listen for data loading and render new data
    this.eventBus.on('data:ready', (data, eventName) => {
      console.log('[MapRenderer] ✅ data:ready event received:', {
        dataset: data?.dataset,
        features: data?.rawData?.features?.length || 0
      });
      
      try {
        this.renderElectionData(data?.rawData);
      } catch (error) {
        console.error('[MapRenderer] ❌ Error in renderElectionData:', error);
      }
    });

    // Listen for state changes that affect rendering
    this.stateManager.subscribe(['currentField', 'opacity', 'showPpsOnly'], 
      (newState) => this.handleStateChange(newState))

    // Listen for custom range updates
    this.eventBus.on('data:rangesUpdated', (data) => {
      console.log('[MapRenderer] Data ranges updated, re-rendering...')
      this.updateLayerStyles()
    })

    console.log('[MapRenderer] Event listeners set up')
  }

  /**
     * Render election data on the map
     */
  renderElectionData (electionData) {
    console.log('[MapRenderer] 🎨 Rendering data...', {
      hasData: !!electionData,
      features: electionData?.features?.length || 0
    });

    if (!electionData || !this.mapManager.map) {
      console.warn('[MapRenderer] ❌ Cannot render: missing data or map');
      return;
    }

    const startTime = performance.now()

    try {
      console.log(`[MapRenderer] 🔄 Rendering ${electionData.features.length} features`)

      // Remove existing layer
      this.clearCurrentLayer()

      // Create new layer
      this.currentLayer = this.createGeoJSONLayer(electionData)

      // Add to map
      this.mapManager.addLayer('currentLayer', this.currentLayer)

      // Auto-zoom to the layer bounds so user can see the data
      console.log('[MapRenderer] 🔍 Auto-zooming to layer bounds...')
      if (electionData.features.length > 0) {
        this.fitToLayer({ padding: [50, 50] })
        console.log('[MapRenderer] ✅ Auto-zoom completed')
      } else {
        console.log('[MapRenderer] ⚠️ Skipped auto-zoom (no features)')
      }

      // Update performance metrics
      const renderTime = performance.now() - startTime
      this.updateRenderMetrics(renderTime)

      console.log(`[MapRenderer] 🎉 Render completed in ${renderTime.toFixed(2)}ms`)

      this.eventBus.emit('map:layerRendered', {
        featureCount: electionData.features.length,
        renderTime
      })
    } catch (error) {
      console.error('[MapRenderer] ❌ Render failed:', error)
      this.eventBus.emit('map:renderError', {
        error: error.message
      })
    }
  }

  /**
     * Create GeoJSON layer with styling and interactions
     */
  createGeoJSONLayer (electionData) {
    return L.geoJSON(electionData, {
      style: (feature) => this.styleFeature(feature),
      onEachFeature: (feature, layer) => this.setupFeatureInteractions(feature, layer)
    })
  }

  /**
     * Style individual features based on current field
     */
  styleFeature (feature) {
    const props = feature.properties
    const currentField = this.stateManager.getState('currentField')
    const showPpsOnly = this.stateManager.getState('showPpsOnly')
    const mapOpacity = this.stateManager.getState('mapOpacity') || 0.7

    // Filter for PPS zones if enabled
    if (showPpsOnly && !props.is_pps_precinct) {
      return {
        fillColor: 'transparent',
        color: 'transparent',
        weight: 0,
        fillOpacity: 0
      }
    }

    // Handle "none" layer selection - show just outlines
    if (currentField === 'none') {
      return {
        fillColor: 'transparent',
        weight: 2,
        opacity: 0.8,
        color: '#666',
        fillOpacity: 0,
        dashArray: '3, 3'
      }
    }

    // Get color from ColorManager
    const fillColor = this.colorManager.getFeatureColor(props, currentField)

    return {
      fillColor,
      weight: 1,
      opacity: 0.8,
      color: '#666',
      fillOpacity: mapOpacity
    }
  }

  /**
     * Set up feature interactions (hover, click)
     */
  setupFeatureInteractions (feature, layer) {
    const props = feature.properties
    const showPpsOnly = this.stateManager.getState('showPpsOnly')

    // Skip interactions if filtering and not in zone
    if (showPpsOnly && !props.is_pps_precinct) return

    // Hover effects
    layer.on('mouseover', () => {
      this.handleFeatureHover(layer, props)
    })

    layer.on('mouseout', () => {
      this.handleFeatureMouseOut(layer)
    })

    // Click for detailed popup
    layer.on('click', () => {
      this.handleFeatureClick(layer, props)
    })
  }

  /**
     * Handle feature hover
     */
  handleFeatureHover (layer, properties) {
    const currentField = this.stateManager.getState('currentField')

    // Update layer style for hover
    layer.setStyle({
      weight: 3,
      color: '#fff',
      fillOpacity: currentField === 'none' ? 0 : 0.9
    })

    // Emit hover event for info panel update
    this.eventBus.emit('map:featureHover', {
      properties,
      currentField
    })
  }

  /**
     * Handle feature mouse out
     */
  handleFeatureMouseOut (layer) {
    // Reset layer style
    if (this.currentLayer) {
      this.currentLayer.resetStyle(layer)
    }

    // Emit mouse out event
    this.eventBus.emit('map:featureMouseOut')
  }

  /**
     * Handle feature click
     */
  handleFeatureClick (layer, properties) {
    // Create popup content using PopupManager
    const popupContent = this.popupManager.createPopupContent(properties)

    // Bind and open popup
    layer.bindPopup(popupContent, {
      maxWidth: 320,
      maxHeight: 500,
      className: 'election-popup'
    }).openPopup()

    // Emit click event
    this.eventBus.emit('map:featureClick', {
      properties
    })
  }

  /**
     * Update layer when field changes
     */
  updateLayer (layerKey) {
    if (!this.currentLayer) return

    console.log(`[MapRenderer] Updating layer to: ${layerKey}`)

    // Update state
    this.stateManager.setState({ currentField: layerKey })

    // Refresh layer styling
    this.refreshLayer()

    this.eventBus.emit('map:layerChanged', { layerKey })
  }

  /**
     * Update layer opacity
     */
  updateOpacity (opacity) {
    if (!this.currentLayer) return

    this.currentLayer.eachLayer((layer) => {
      const style = layer.options.style || {}
      layer.setStyle({
        ...style,
        fillOpacity: opacity
      })
    })

    console.log(`[MapRenderer] Updated opacity to: ${opacity}`)
  }

  /**
     * Refresh layer styling
     */
  refreshLayer () {
    if (!this.currentLayer) return

    const startTime = performance.now()

    // Re-style all features
    this.currentLayer.eachLayer((layer) => {
      if (layer.feature) {
        const style = this.styleFeature(layer.feature)
        layer.setStyle(style)
      }
    })

    const refreshTime = performance.now() - startTime
    console.log(`[MapRenderer] Layer refreshed in ${refreshTime.toFixed(2)}ms`)
  }

  /**
     * Update layer with custom range
     */
  updateLayerWithCustomRange (customRange) {
    console.log('[MapRenderer] Updating layer with custom range:', customRange)
    this.refreshLayer()
  }

  /**
     * Clear current layer from map
     */
  clearCurrentLayer () {
    if (this.currentLayer && this.mapManager.map) {
      this.mapManager.removeLayer('currentLayer');
      this.currentLayer = null;
    }
  }

  /**
     * Update render performance metrics
     */
  updateRenderMetrics (renderTime) {
    this.renderMetrics.totalRenders++
    this.renderMetrics.lastRenderTime = renderTime

    // Calculate running average
    const currentAvg = this.renderMetrics.averageRenderTime
    const count = this.renderMetrics.totalRenders
    this.renderMetrics.averageRenderTime =
            (currentAvg * (count - 1) + renderTime) / count
  }

  /**
     * Get current layer bounds
     */
  getLayerBounds () {
    if (!this.currentLayer) return null

    try {
      return this.currentLayer.getBounds()
    } catch (error) {
      console.warn('[MapRenderer] Could not get layer bounds:', error)
      return null
    }
  }

  /**
     * Fit map to current layer
     */
  fitToLayer (options = {}) {
    const bounds = this.getLayerBounds()
    if (bounds && this.mapManager.map) {
      this.mapManager.map.fitBounds(bounds, {
        padding: [20, 20],
        ...options
      })

      console.log('[MapRenderer] Fitted map to layer bounds')
    }
  }

  /**
     * Get render performance metrics
     */
  getRenderMetrics () {
    return { ...this.renderMetrics }
  }

  /**
     * Check if layer is currently rendered
     */
  hasLayer () {
    return this.currentLayer !== null
  }

  /**
     * Get current layer feature count
     */
  getFeatureCount () {
    if (!this.currentLayer) return 0

    let count = 0
    this.currentLayer.eachLayer(() => count++)
    return count
  }

  /**
     * Export layer as GeoJSON
     */
  exportLayerData () {
    if (!this.currentLayer) return null

    return this.currentLayer.toGeoJSON()
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.clearCurrentLayer()
    this.layerCache.clear()

    if (this.colorManager) {
      this.colorManager.destroy()
    }

    if (this.popupManager) {
      this.popupManager.destroy()
    }

    console.log('[MapRenderer] Destroyed')
  }

  /**
     * Handle state changes that affect rendering
     */
    handleStateChange(newState) {
        console.log('[MapRenderer] State changed:', Object.keys(newState));
        
        // Re-render if current field changed
        if (newState.currentField !== undefined) {
            this.updateLayerStyles();
        }
        
        // Update opacity if changed
        if (newState.opacity !== undefined) {
            this.updateOpacity(newState.opacity);
        }
        
        // Re-filter and render if PPS filter changed
        if (newState.showPpsOnly !== undefined) {
            this.updateLayerStyles();
        }
    }

    /**
     * Update layer styles based on current state
     */
    updateLayerStyles() {
        if (this.currentLayer) {
            console.log('[MapRenderer] Updating layer styles...');
            
            // Remove current layer
            this.mapManager.removeLayer('currentLayer');
            
            // Re-render with current data
            const electionData = this.stateManager.getState('electionData');
            if (electionData) {
                this.renderElectionData(electionData);
            }
        }
    }

    /**
     * Update layer opacity
     */
    updateOpacity(opacity) {
        if (this.currentLayer) {
            console.log(`[MapRenderer] Updating opacity to ${opacity}`);
            this.currentLayer.setStyle({ fillOpacity: opacity });
        }
    }
}
