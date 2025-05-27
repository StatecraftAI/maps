/**
 * Heatmap - Vote Density Heatmap Overlay
 *
 * Handles:
 * - Vote density heatmap visualization using leaflet-heat
 * - Coordinate extraction from GeoJSON polygons
 * - Intensity calculation based on vote totals
 * - Heatmap layer management and toggling
 * - Custom gradient and styling options
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Heatmap {
  constructor (stateManager, eventBus, mapManager) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager

    // Heatmap state
    this.heatmapLayer = null
    this.isActive = false

    // Heatmap configuration
    this.heatmapOptions = {
      radius: 25,
      blur: 15,
      maxZoom: 17,
      minOpacity: 0.1,
      gradient: {
        0.0: 'navy',
        0.2: 'blue',
        0.4: 'cyan',
        0.6: 'lime',
        0.8: 'yellow',
        1.0: 'red'
      }
    }

    this.initializeElements()
    this.setupEventListeners()

    console.log('[Heatmap] Initialized')
  }

  /**
     * Initialize DOM elements
     */
  initializeElements () {
    this.heatmapButton = document.getElementById('heatmap-btn')

    // Replace inline onclick handler
    if (this.heatmapButton) {
      const existingOnclick = this.heatmapButton.getAttribute('onclick')
      if (existingOnclick && existingOnclick.includes('toggleHeatmap')) {
        this.heatmapButton.removeAttribute('onclick')
        this.heatmapButton.addEventListener('click', () => this.toggleHeatmap())
      }
    }

    // Check if leaflet-heat is available
    this.leafletHeatAvailable = typeof L !== 'undefined' && typeof L.heatLayer !== 'undefined'
    if (!this.leafletHeatAvailable) {
      console.warn('[Heatmap] leaflet-heat plugin not found. Heatmap functionality disabled.')
      if (this.heatmapButton) {
        this.heatmapButton.disabled = true
        this.heatmapButton.title = 'Heatmap requires leaflet-heat plugin'
      }
    }
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    // Listen for data changes that might affect heatmap
    this.eventBus.on('data:loaded', (data) => {
      if (data.type === 'election') {
        // If heatmap is active, update its data when new election data is loaded
        if (this.stateManager.getState('heatmapActive')) { // Check stateManager
          this.updateHeatmapData()
        }
      }
    })

    this.eventBus.on('ui:ppsFilterChanged', () => {
      if (this.stateManager.getState('heatmapActive')) { // Check stateManager
        this.updateHeatmapData()
      }
    })

    // Listen for heatmap configuration changes
    this.eventBus.on('features:heatmapConfigChanged', (data) => {
      this.updateHeatmapOptions(data.options)
      if (this.stateManager.getState('heatmapActive')) { // Check stateManager
        this.refreshHeatmap()
      }
    })

    // Subscribe to StateManager for heatmap active state changes
    this.stateManager.subscribe('heatmapActive', (stateChanges) => {
      this.handleHeatmapStateChange(stateChanges)
    })

    console.log('[Heatmap] Event listeners set up')
  }

  /**
   * Handle heatmap state change from StateManager
   */
  handleHeatmapStateChange (stateChanges) {
    if (stateChanges.hasOwnProperty('heatmapActive')) {
      const enabled = stateChanges.heatmapActive
      console.log(`[Heatmap] Toggling heatmap to: ${enabled}`)
      if (enabled && !this.isActive) {
        this.showHeatmap()
      } else if (!enabled && this.isActive) {
        this.hideHeatmap()
      }
    }
  }

  /**
     * Toggle heatmap on/off
     */
  toggleHeatmap () {
    if (!this.leafletHeatAvailable) {
      alert('Heatmap functionality requires the leaflet-heat plugin to be loaded.')
      return
    }

    // Toggle heatmapActive state in StateManager
    const currentHeatmapActive = this.stateManager.getState('heatmapActive') || false
    this.stateManager.setState({ heatmapActive: !currentHeatmapActive })
    console.log('[Heatmap] Toggled heatmapActive state to:', !currentHeatmapActive)
  }

  /**
     * Show heatmap overlay
     */
  async showHeatmap () {
    try {
      console.log('[Heatmap] Showing heatmap...')

      const heatData = await this.generateHeatmapData()

      if (heatData.length === 0) {
        alert('No valid vote data found for heatmap visualization.')
        return
      }

      // Create heatmap layer
      this.heatmapLayer = L.heatLayer(heatData, this.heatmapOptions)

      // Add to map
      const map = this.mapManager.map
      if (map) {
        this.heatmapLayer.addTo(map)
      }

      this.setActiveState(true)

      console.log(`[Heatmap] Heatmap created with ${heatData.length} data points`)

      this.eventBus.emit('heatmap:shown', {
        dataPoints: heatData.length,
        options: { ...this.heatmapOptions }
      })
    } catch (error) {
      console.error('[Heatmap] Failed to show heatmap:', error)
      this.eventBus.emit('heatmap:error', {
        error: error.message,
        context: 'showHeatmap'
      })
    }
  }

  /**
     * Hide heatmap overlay
     */
  hideHeatmap () {
    if (this.heatmapLayer) {
      const map = this.mapManager.map
      if (map) {
        map.removeLayer(this.heatmapLayer)
      }
      this.heatmapLayer = null
    }

    this.setActiveState(false)

    console.log('[Heatmap] Heatmap hidden')

    this.eventBus.emit('heatmap:hidden')
  }

  /**
     * Generate heatmap data from election data
     */
  async generateHeatmapData () {
    const electionData = this.stateManager.getState('electionData')
    if (!electionData) {
      console.warn('[Heatmap] No election data available')
      return []
    }

    const showPpsOnly = this.stateManager.getState('showPpsOnly')
    const heatData = []

    console.log('[Heatmap] Processing election data for heatmap...')

    electionData.features
      .filter(f => (!showPpsOnly || f.properties.is_pps_precinct) && f.properties.votes_total)
      .forEach(feature => {
        try {
          const coords = this.extractCentroidFromGeometry(feature.geometry)

          if (coords && this.isValidCoordinate(coords)) {
            const intensity = this.calculateIntensity(feature.properties)
            if (intensity > 0) {
              heatData.push([coords[0], coords[1], intensity])
            }
          }
        } catch (error) {
          console.warn('[Heatmap] Error processing feature:', error)
        }
      })

    console.log(`[Heatmap] Generated ${heatData.length} heatmap points`)
    return heatData
  }

  /**
     * Extract centroid coordinates from GeoJSON geometry
     */
  extractCentroidFromGeometry (geometry) {
    if (!geometry || !geometry.coordinates) {
      return null
    }

    try {
      if (geometry.type === 'Polygon') {
        return this.calculatePolygonCentroid(geometry.coordinates[0])
      } else if (geometry.type === 'MultiPolygon') {
        // Use the first polygon's centroid
        const firstPolygon = geometry.coordinates[0]
        if (firstPolygon && firstPolygon[0]) {
          return this.calculatePolygonCentroid(firstPolygon[0])
        }
      } else if (geometry.type === 'Point') {
        // For points, use coordinates directly (convert to lat/lng)
        const point = geometry.coordinates
        if (Array.isArray(point) && point.length >= 2) {
          return [point[1], point[0]] // [lat, lng]
        }
      }
    } catch (error) {
      console.warn('[Heatmap] Error extracting centroid:', error)
    }

    return null
  }

  /**
     * Calculate centroid of a polygon ring
     */
  calculatePolygonCentroid (ring) {
    if (!Array.isArray(ring) || ring.length < 3) {
      return null
    }

    let totalLng = 0
    let totalLat = 0
    let validPoints = 0

    ring.forEach(point => {
      if (Array.isArray(point) && point.length >= 2 &&
                typeof point[0] === 'number' && typeof point[1] === 'number' &&
                !isNaN(point[0]) && !isNaN(point[1])) {
        totalLng += point[0]
        totalLat += point[1]
        validPoints++
      }
    })

    if (validPoints > 0) {
      return [totalLat / validPoints, totalLng / validPoints] // [lat, lng]
    }

    return null
  }

  /**
     * Validate coordinate is within reasonable bounds
     */
  isValidCoordinate (coords) {
    if (!Array.isArray(coords) || coords.length < 2) {
      return false
    }

    const [lat, lng] = coords

    // Check for valid latitude and longitude ranges
    return lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180 &&
               // Additional check for Portland area (rough bounds)
               lat >= 45.0 && lat <= 46.0 && lng >= -123.0 && lng <= -122.0
  }

  /**
     * Calculate intensity value for heatmap
     */
  calculateIntensity (properties) {
    const votesTotal = properties.votes_total || 0

    if (votesTotal <= 0) {
      return 0
    }

    // Scale votes to reasonable intensity (divide by 50 to create good visual range)
    let intensity = Math.max(1, votesTotal / 50)

    // Cap intensity to prevent extreme values
    intensity = Math.min(intensity, 20)

    return intensity
  }

  /**
     * Update heatmap data when underlying data changes
     */
  async updateHeatmapData () {
    if (!this.isActive) {
      return
    }

    console.log('[Heatmap] Updating heatmap data...')

    try {
      // Hide current heatmap
      this.hideHeatmap()

      // Show updated heatmap
      await this.showHeatmap()
    } catch (error) {
      console.error('[Heatmap] Failed to update heatmap data:', error)
    }
  }

  /**
     * Refresh heatmap with current data
     */
  async refreshHeatmap () {
    if (this.isActive) {
      await this.updateHeatmapData()
    }
  }

  /**
     * Set active state and update UI
     */
  setActiveState (isActive) {
    this.isActive = isActive

    // Update StateManager
    this.stateManager.setState({ heatmapLayer: this.heatmapLayer })

    // Update button appearance
    if (this.heatmapButton) {
      if (isActive) {
        this.heatmapButton.classList.add('active')
        this.heatmapButton.textContent = '🔥 Heatmap ON'
      } else {
        this.heatmapButton.classList.remove('active')
        this.heatmapButton.textContent = '🔥 Vote Heatmap'
      }
    }
  }

  /**
     * Update heatmap options
     */
  updateHeatmapOptions (newOptions) {
    this.heatmapOptions = { ...this.heatmapOptions, ...newOptions }
    console.log('[Heatmap] Updated options:', this.heatmapOptions)
  }

  /**
     * Set custom gradient
     */
  setGradient (gradient) {
    this.updateHeatmapOptions({ gradient })

    if (this.heatmapLayer) {
      this.heatmapLayer.setOptions({ gradient })
    }
  }

  /**
     * Set heatmap radius
     */
  setRadius (radius) {
    this.updateHeatmapOptions({ radius })

    if (this.heatmapLayer) {
      this.heatmapLayer.setOptions({ radius })
    }
  }

  /**
     * Set heatmap blur
     */
  setBlur (blur) {
    this.updateHeatmapOptions({ blur })

    if (this.heatmapLayer) {
      this.heatmapLayer.setOptions({ blur })
    }
  }

  /**
     * Get predefined gradient schemes
     */
  getGradientSchemes () {
    return {
      default: {
        0.0: 'navy',
        0.2: 'blue',
        0.4: 'cyan',
        0.6: 'lime',
        0.8: 'yellow',
        1.0: 'red'
      },
      fire: {
        0.0: '#000080',
        0.3: '#FF0000',
        0.6: '#FFFF00',
        1.0: '#FFFFFF'
      },
      cool: {
        0.0: '#000080',
        0.5: '#0080FF',
        1.0: '#00FFFF'
      },
      warm: {
        0.0: '#800000',
        0.5: '#FF8000',
        1.0: '#FFFF00'
      },
      purple: {
        0.0: '#4B0082',
        0.5: '#8A2BE2',
        1.0: '#DDA0DD'
      }
    }
  }

  /**
     * Apply gradient scheme by name
     */
  applyGradientScheme (schemeName) {
    const schemes = this.getGradientSchemes()
    const scheme = schemes[schemeName]

    if (scheme) {
      this.setGradient(scheme)
      console.log(`[Heatmap] Applied gradient scheme: ${schemeName}`)
    } else {
      console.warn(`[Heatmap] Unknown gradient scheme: ${schemeName}`)
    }
  }

  /**
     * Get heatmap statistics
     */
  getHeatmapStats () {
    if (!this.isActive || !this.heatmapLayer) {
      return null
    }

    // Note: leaflet-heat doesn't expose data directly, so we'd need to
    // track this separately or regenerate the data
    return {
      isActive: this.isActive,
      hasLayer: this.heatmapLayer !== null,
      options: { ...this.heatmapOptions }
    }
  }

  /**
     * Export heatmap data for analysis
     */
  async exportHeatmapData () {
    const heatData = await this.generateHeatmapData()

    return {
      data: heatData,
      options: { ...this.heatmapOptions },
      metadata: {
        timestamp: new Date().toISOString(),
        dataPoints: heatData.length,
        showPpsOnly: this.stateManager.getState('showPpsOnly')
      }
    }
  }

  /**
     * Check if heatmap can be created
     */
  canCreateHeatmap () {
    const issues = []

    if (!this.leafletHeatAvailable) {
      issues.push('leaflet-heat plugin not available')
    }

    const electionData = this.stateManager.getState('electionData')
    if (!electionData) {
      issues.push('No election data loaded')
    }

    const map = this.mapManager.map
    if (!map) {
      issues.push('Map not initialized')
    }

    return {
      canCreate: issues.length === 0,
      issues
    }
  }

  /**
     * Get current heatmap state
     */
  getHeatmapState () {
    return {
      isActive: this.isActive,
      hasLayer: this.heatmapLayer !== null,
      options: { ...this.heatmapOptions },
      available: this.leafletHeatAvailable,
      canCreate: this.canCreateHeatmap().canCreate
    }
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.hideHeatmap()

    // Reset button state
    if (this.heatmapButton) {
      this.heatmapButton.classList.remove('active')
      this.heatmapButton.textContent = '🔥 Vote Heatmap'
    }

    console.log('[Heatmap] Destroyed')
  }
}
