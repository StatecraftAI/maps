/**
 * DemographicOverlays - Demographic Data Overlay Management
 *
 * Handles the button-style demographic overlay controls from the test file
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class DemographicOverlays {
  constructor (mapManager, eventBus, supabaseClient) {
    this.mapManager = mapManager
    this.eventBus = eventBus
    this.supabaseClient = supabaseClient

    // Simple state tracking
    this.activeOverlays = new Set()
    this.overlayLayers = new Map()
    this.overlayData = new Map()

    // Available overlays with Supabase table mapping
    this.availableOverlays = [
      {
        id: 'voter-hexagons',
        name: 'Voter Hexagons',
        table: 'voter_hexagons',
        badgeId: 'hex-count'
      },
      {
        id: 'block-groups',
        name: 'Block Groups',
        table: 'voter_block_groups',
        badgeId: 'bg-count'
      },
      {
        id: 'household-demographics',
        name: 'Household Demographics',
        table: 'household_demographics_pps',
        badgeId: 'household-count'
      },
      {
        id: 'pps-district',
        name: 'PPS District',
        table: 'pps_district_summary',
        badgeId: 'district-count'
      }
    ]

    this.initializeElements()
    this.setupEventListeners()
    this.loadOverlayData()

    console.log('[DemographicOverlays] Initialized')
  }

  initializeElements () {
    // Find overlay control divs (not checkboxes)
    this.overlayControls = new Map()

    this.availableOverlays.forEach(overlay => {
      const control = document.querySelector(`[data-layer="${overlay.id}"]`)
      if (control) {
        this.overlayControls.set(overlay.id, control)
        console.log(`[DemographicOverlays] Found control for: ${overlay.id}`)
      } else {
        console.warn(`[DemographicOverlays] Control not found for: ${overlay.id}`)
      }
    })

    console.log(`[DemographicOverlays] Found ${this.overlayControls.size} overlay controls`)
  }

  setupEventListeners () {
    // Add click listeners to the control divs
    this.overlayControls.forEach((control, overlayId) => {
      control.addEventListener('click', (e) => {
        console.log(`[DemographicOverlays] Control clicked: ${overlayId}`)
        this.toggleOverlay(overlayId)
      })

      // Add hover effects
      control.style.cursor = 'pointer'
    })
  }

  async loadOverlayData () {
    if (!this.supabaseClient) {
      console.warn('[DemographicOverlays] No Supabase client available')
      this.updateAllBadges('No DB')
      return
    }

    console.log('[DemographicOverlays] Loading overlay data from Supabase...')

    for (const overlay of this.availableOverlays) {
      try {
        console.log(`[DemographicOverlays] Loading ${overlay.table}...`)

        const { data, error, count } = await this.supabaseClient
          .from(overlay.table)
          .select('*', { count: 'exact' })

        if (error) {
          console.error(`[DemographicOverlays] Error loading ${overlay.table}:`, error)
          this.updateBadge(overlay.badgeId, 'Error')
          continue
        }

        if (data && data.length > 0) {
          // Transform to GeoJSON
          const geoJsonData = this.transformToGeoJSON(data, overlay.table)
          this.overlayData.set(overlay.id, geoJsonData)

          this.updateBadge(overlay.badgeId, count || data.length)
          console.log(`[DemographicOverlays] Loaded ${overlay.name}: ${data.length} features`)
        } else {
          this.updateBadge(overlay.badgeId, '0')
          console.log(`[DemographicOverlays] No data found for ${overlay.name}`)
        }
      } catch (error) {
        console.error(`[DemographicOverlays] Failed to load ${overlay.name}:`, error)
        this.updateBadge(overlay.badgeId, 'Error')
      }
    }
  }

  transformToGeoJSON (data, tableName) {
    const features = data.map(row => {
      // Handle both Polygon and MultiPolygon geometries
      const geometry = row.geometry

      // Ensure geometry is properly formatted
      if (geometry && typeof geometry === 'object') {
        // PostGIS returns geometry as object, ensure it has proper GeoJSON structure
        if (!geometry.type) {
          console.warn(`[DemographicOverlays] Invalid geometry in ${tableName}:`, geometry)
          return null
        }
      }

      // Create properties object (exclude geometry)
      const properties = { ...row }
      delete properties.geometry

      return {
        type: 'Feature',
        geometry,
        properties
      }
    }).filter(feature => feature !== null) // Remove invalid features

    return {
      type: 'FeatureCollection',
      features,
      metadata: {
        source: 'supabase',
        table: tableName,
        count: features.length,
        loadedAt: new Date().toISOString()
      }
    }
  }

  updateBadge (badgeId, value) {
    const badge = document.getElementById(badgeId)
    if (badge) {
      badge.textContent = value.toLocaleString()
    }
  }

  updateAllBadges (value) {
    this.availableOverlays.forEach(overlay => {
      this.updateBadge(overlay.badgeId, value)
    })
  }

  toggleOverlay (overlayId) {
    if (this.activeOverlays.has(overlayId)) {
      this.removeOverlay(overlayId)
    } else {
      this.addOverlay(overlayId)
    }
  }

  async addOverlay (overlayId) {
    if (this.activeOverlays.has(overlayId)) {
      console.log(`[DemographicOverlays] Overlay ${overlayId} already active`)
      return
    }

    console.log(`[DemographicOverlays] Adding overlay: ${overlayId}`)

    try {
      const overlay = this.availableOverlays.find(o => o.id === overlayId)
      if (!overlay) {
        console.error(`[DemographicOverlays] Unknown overlay: ${overlayId}`)
        return
      }

      // Get data from cache
      const geoJsonData = this.overlayData.get(overlayId)
      if (!geoJsonData || !geoJsonData.features.length) {
        console.warn(`[DemographicOverlays] No data available for ${overlayId}`)
        return
      }

      // Create layer with appropriate styling
      const layer = L.geoJSON(geoJsonData, {
        style: (feature) => this.getOverlayStyle(overlayId, feature),
        onEachFeature: (feature, layer) => {
          if (feature.properties) {
            layer.bindPopup(this.createOverlayPopup(feature.properties, overlay.name))
          }
        }
      })

      // Add to map
      this.mapManager.addLayer(overlayId, layer)
      this.overlayLayers.set(overlayId, layer)
      this.activeOverlays.add(overlayId)

      // Update UI
      this.setControlActive(overlayId, true)

      console.log(`[DemographicOverlays] Successfully added overlay: ${overlayId}`)
    } catch (error) {
      console.error(`[DemographicOverlays] Error adding overlay ${overlayId}:`, error)
    }
  }

  removeOverlay (overlayId) {
    if (!this.activeOverlays.has(overlayId)) {
      console.log(`[DemographicOverlays] Overlay ${overlayId} not active`)
      return
    }

    console.log(`[DemographicOverlays] Removing overlay: ${overlayId}`)

    const layer = this.overlayLayers.get(overlayId)
    if (layer) {
      this.mapManager.removeLayer(overlayId)
      this.overlayLayers.delete(overlayId)
    }

    this.activeOverlays.delete(overlayId)

    // Update UI
    this.setControlActive(overlayId, false)

    console.log(`[DemographicOverlays] Successfully removed overlay: ${overlayId}`)
  }

  setControlActive (overlayId, isActive) {
    const control = this.overlayControls.get(overlayId)
    if (control) {
      if (isActive) {
        control.classList.add('active')
      } else {
        control.classList.remove('active')
      }
    }
  }

  getOverlayStyle (overlayId, feature) {
    const styles = {
      'voter-hexagons': {
        fillColor: this.getVoterDensityColor(feature.properties),
        weight: 1,
        opacity: 0.8,
        color: 'white',
        fillOpacity: 0.6
      },
      'block-groups': {
        fillColor: this.getBlockGroupColor(feature.properties),
        weight: 2,
        opacity: 0.8,
        color: '#0078ff',
        fillOpacity: 0.3
      },
      'household-demographics': {
        fillColor: this.getHouseholdColor(feature.properties),
        weight: 1,
        opacity: 0.8,
        color: '#00ff78',
        fillOpacity: 0.4
      },
      'pps-district': {
        fillColor: 'transparent',
        weight: 3,
        opacity: 1,
        color: '#ff0000',
        fillOpacity: 0,
        dashArray: '5, 5'
      }
    }

    return styles[overlayId] || {
      fillColor: '#cccccc',
      weight: 1,
      opacity: 0.8,
      color: 'white',
      fillOpacity: 0.3
    }
  }

  getVoterDensityColor (properties) {
    // Color based on voter density
    const density = properties.voter_density || properties.density || 0
    const maxDensity = 100 // Adjust based on your data
    const intensity = Math.min(density / maxDensity, 1)

    // Blue gradient for voter density
    const blue = Math.round(255 - (intensity * 100))
    return `rgb(${blue}, ${blue}, 255)`
  }

  getBlockGroupColor (properties) {
    // Color based on PPS percentage or similar metric
    const ppsPercent = properties.pps_percent || properties.percentage || 0
    const intensity = ppsPercent / 100

    // Green gradient
    const green = Math.round(255 - (intensity * 100))
    return `rgb(${green}, 255, ${green})`
  }

  getHouseholdColor (properties) {
    // Color based on household demographics
    const value = properties.household_value || properties.value || 0
    const maxValue = 1000 // Adjust based on your data
    const intensity = Math.min(value / maxValue, 1)

    // Orange gradient
    const orange = Math.round(255 - (intensity * 100))
    return `rgb(255, ${orange}, ${orange})`
  }

  createOverlayPopup (properties, overlayName) {
    let content = `<h4>${overlayName}</h4>`

    // Show relevant properties
    const relevantProps = Object.entries(properties)
      .filter(([key, value]) =>
        !key.toLowerCase().includes('geometry') &&
        value !== null &&
        value !== undefined &&
        value !== ''
      )
      .slice(0, 8) // Show more properties for demographic data

    if (relevantProps.length > 0) {
      content += '<table style="font-size: 12px; width: 100%;">'
      relevantProps.forEach(([key, value]) => {
        const displayKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
        const displayValue = typeof value === 'number' ? value.toLocaleString() : value
        content += `<tr><td><strong>${displayKey}:</strong></td><td>${displayValue}</td></tr>`
      })
      content += '</table>'
    }

    return content
  }

  // Public methods
  isOverlayActive (overlayId) {
    return this.activeOverlays.has(overlayId)
  }

  getActiveOverlays () {
    return Array.from(this.activeOverlays)
  }

  clearAllOverlays () {
    console.log('[DemographicOverlays] Clearing all overlays')

    this.activeOverlays.forEach(overlayId => {
      this.removeOverlay(overlayId)
    })
  }

  destroy () {
    this.clearAllOverlays()
    console.log('[DemographicOverlays] Destroyed')
  }
}
