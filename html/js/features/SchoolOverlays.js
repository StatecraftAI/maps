/**
 * SchoolOverlays - School Location and Boundary Overlay Management
 *
 * Updated to use Supabase PostGIS tables instead of static GeoJSON files
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class SchoolOverlays {
  constructor (stateManager, eventBus, mapManager, supabaseClient) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager
    this.supabaseClient = supabaseClient

    // Layer management
    this.overlayLayers = new Map()
    this.overlayData = new Map()

    // Available overlays with Supabase table mapping
    this.availableOverlays = [
      {
        id: 'high-schools',
        name: 'High Schools',
        table: 'pps_high_school_locations',
        type: 'location',
        icon: 'high'
      },
      {
        id: 'middle-schools',
        name: 'Middle Schools',
        table: 'pps_middle_school_locations',
        type: 'location',
        icon: 'middle'
      },
      {
        id: 'elementary-schools',
        name: 'Elementary Schools',
        table: 'pps_elementary_school_locations',
        type: 'location',
        icon: 'elementary'
      },
      {
        id: 'k8-schools',
        name: 'K-8 Schools',
        table: 'pps_k8_school_locations',
        type: 'location',
        icon: 'elementary'
      },
      {
        id: 'high-boundaries',
        name: 'High School Boundaries',
        table: 'pps_high_school_boundaries',
        type: 'boundary'
      },
      {
        id: 'middle-boundaries',
        name: 'Middle School Boundaries',
        table: 'pps_middle_school_boundaries',
        type: 'boundary'
      },
      {
        id: 'elementary-boundaries',
        name: 'Elementary Boundaries',
        table: 'pps_elementary_school_boundaries',
        type: 'boundary'
      },
      {
        id: 'district-boundary',
        name: 'District Boundary',
        table: 'pps_district_boundary',
        type: 'boundary'
      }
    ]

    this.initializeElements()
    this.setupEventListeners()

    console.log('🏫 SchoolOverlays initialized with Supabase integration')
  }

  initializeElements () {
    // Find all school overlay checkboxes
    this.elements = {}

    this.availableOverlays.forEach(overlay => {
      const checkbox = document.getElementById(`show-${overlay.id}`)
      if (checkbox) {
        this.elements[overlay.id] = checkbox
      }
    })

    console.log('🏫 Found school overlay elements:', Object.keys(this.elements))
  }

  setupEventListeners () {
    // Set up checkbox event listeners
    Object.entries(this.elements).forEach(([overlayId, checkbox]) => {
      checkbox.addEventListener('change', (e) => {
        if (e.target.checked) {
          this.addOverlay(overlayId)
        } else {
          this.removeOverlay(overlayId)
        }
      })
    })
  }

  async addOverlay (overlayId) {
    console.log(`🏫 Adding school overlay: ${overlayId}`)

    try {
      // Find overlay configuration
      const overlayConfig = this.availableOverlays.find(o => o.id === overlayId)
      if (!overlayConfig) {
        console.error(`❌ Unknown overlay: ${overlayId}`)
        return
      }

      // Check if we have Supabase client
      if (!this.supabaseClient) {
        console.error('❌ Supabase client not available for school overlays')
        this.showFallbackMessage(overlayId)
        return
      }

      // Load data from Supabase
      const data = await this.loadOverlayData(overlayConfig)
      if (!data || data.length === 0) {
        console.warn(`⚠️ No data found for ${overlayId}`)
        return
      }

      // Transform to GeoJSON
      const geoJsonData = this.transformToGeoJSON(data, overlayConfig.table)

      // Create and add layer
      const layer = this.createOverlayLayer(geoJsonData, overlayConfig)
      if (layer) {
        this.mapManager.addLayer(`school-${overlayId}`, layer)
        this.overlayLayers.set(overlayId, layer)
        this.overlayData.set(overlayId, geoJsonData)

        console.log(`✅ Added ${overlayId} overlay with ${data.length} features`)
      }
    } catch (error) {
      console.error(`❌ Failed to add ${overlayId} overlay:`, error)
      this.showErrorMessage(overlayId, error.message)
    }
  }

  async loadOverlayData (overlayConfig) {
    console.log(`📊 Loading data from table: ${overlayConfig.table}`)

    try {
      const { data, error } = await this.supabaseClient
        .from(overlayConfig.table)
        .select('*')

      if (error) {
        console.error(`❌ Supabase error loading ${overlayConfig.table}:`, error)
        return null
      }

      console.log(`✅ Loaded ${data.length} records from ${overlayConfig.table}`)
      return data
    } catch (error) {
      console.error(`❌ Error loading ${overlayConfig.table}:`, error)
      return null
    }
  }

  transformToGeoJSON (data, tableName) {
    console.log(`🔄 Transforming ${data.length} records to GeoJSON`)

    const features = data.map(record => {
      // Extract geometry
      let geometry = null

      if (record.geometry) {
        try {
          // Handle different geometry formats
          if (typeof record.geometry === 'string') {
            geometry = JSON.parse(record.geometry)
          } else if (typeof record.geometry === 'object') {
            geometry = record.geometry
          }
        } catch (e) {
          console.warn('⚠️ Failed to parse geometry:', e)
        }
      }

      // Create properties (exclude geometry field)
      const properties = { ...record }
      delete properties.geometry

      return {
        type: 'Feature',
        geometry,
        properties
      }
    }).filter(feature => feature.geometry !== null)

    return {
      type: 'FeatureCollection',
      features
    }
  }

  createOverlayLayer (geoJsonData, overlayConfig) {
    if (overlayConfig.type === 'location') {
      return this.createLocationLayer(geoJsonData, overlayConfig)
    } else if (overlayConfig.type === 'boundary') {
      return this.createBoundaryLayer(geoJsonData, overlayConfig)
    }
    return null
  }

  createLocationLayer (geoJsonData, overlayConfig) {
    return L.geoJSON(geoJsonData, {
      pointToLayer: (feature, latlng) => {
        const icon = this.getSchoolIcon(overlayConfig.icon)
        return L.marker(latlng, { icon })
      },
      onEachFeature: (feature, layer) => {
        const popup = this.createSchoolPopup(feature.properties, overlayConfig.name)
        layer.bindPopup(popup)
      }
    })
  }

  createBoundaryLayer (geoJsonData, overlayConfig) {
    return L.geoJSON(geoJsonData, {
      style: this.getBoundaryStyle(overlayConfig.id),
      onEachFeature: (feature, layer) => {
        const popup = this.createBoundaryPopup(feature.properties, overlayConfig.name)
        layer.bindPopup(popup)
      }
    })
  }

  removeOverlay (overlayId) {
    console.log(`🏫 Removing school overlay: ${overlayId}`)

    // Remove from map
    this.mapManager.removeLayer(`school-${overlayId}`)

    // Clean up references
    this.overlayLayers.delete(overlayId)
    this.overlayData.delete(overlayId)

    console.log(`✅ Removed ${overlayId} overlay`)
  }

  getSchoolIcon (iconType) {
    const iconConfigs = {
      high: {
        html: '<div style="background: #4E3A6D; color: white; border-radius: 50%; width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: bold; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">H</div>',
        iconSize: [24, 24],
        iconAnchor: [12, 12]
      },
      middle: {
        html: '<div style="background: #4F4F4F; color: white; border-radius: 50%; width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: bold; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">M</div>',
        iconSize: [24, 24],
        iconAnchor: [12, 12]
      },
      elementary: {
        html: '<div style="background: #000000; color: white; border-radius: 50%; width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: bold; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">E</div>',
        iconSize: [24, 24],
        iconAnchor: [12, 12]
      }
    }

    const config = iconConfigs[iconType] || iconConfigs.elementary

    return L.divIcon({
      html: config.html,
      iconSize: config.iconSize,
      iconAnchor: config.iconAnchor,
      className: 'school-marker'
    })
  }

  getBoundaryStyle (overlayId) {
    const styles = {
      'high-boundaries': {
        color: '#d62728',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.1,
        dashArray: '5, 5'
      },
      'middle-boundaries': {
        color: '#2ca02c',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.1,
        dashArray: '5, 5'
      },
      'elementary-boundaries': {
        color: '#1f77b4',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.1,
        dashArray: '5, 5'
      },
      'district-boundary': {
        color: '#ff7f0e',
        weight: 3,
        opacity: 0.9,
        fillOpacity: 0.05,
        dashArray: '10, 5'
      }
    }

    return styles[overlayId] || styles['district-boundary']
  }

  createSchoolPopup (properties, overlayName) {
    const schoolName = properties.school_name || properties.sitename || properties.common_name || 'Unknown School'
    const address = properties.siteaddress || properties.address || 'Address not available'
    const gradeGroup = properties.school_gradegroup || properties.grade_group || 'Grades not specified'
    const capacity = properties.site_capacity_name || properties.capacity || 'Capacity not specified'

    return `
      <div class="school-popup">
        <h4>${schoolName}</h4>
        <p><strong>Type:</strong> ${overlayName}</p>
        <p><strong>Grades:</strong> ${gradeGroup}</p>
        <p><strong>Capacity:</strong> ${capacity}</p>
        <p><strong>Address:</strong> ${address}</p>
      </div>
    `
  }

  createBoundaryPopup (properties, overlayName) {
    const name = properties.name || properties.school_name || properties.district_name || 'Unnamed Boundary'
    const area = properties.area || properties.shape_area || 'Area not specified'

    return `
      <div class="boundary-popup">
        <h4>${name}</h4>
        <p><strong>Type:</strong> ${overlayName}</p>
        <p><strong>Area:</strong> ${area}</p>
      </div>
    `
  }

  showFallbackMessage (overlayId) {
    console.warn(`⚠️ Showing fallback message for ${overlayId}`)
    // Could show a user-friendly message about data not being available
  }

  showErrorMessage (overlayId, message) {
    console.error(`❌ Error with ${overlayId}: ${message}`)
    // Could show user-friendly error message
  }

  isOverlayActive (overlayId) {
    return this.overlayLayers.has(overlayId)
  }

  getActiveOverlays () {
    return Array.from(this.overlayLayers.keys())
  }

  clearAllOverlays () {
    console.log('🏫 Clearing all school overlays')

    // Remove all layers
    this.overlayLayers.forEach((layer, overlayId) => {
      this.removeOverlay(overlayId)
    })

    // Uncheck all checkboxes
    Object.values(this.elements).forEach(checkbox => {
      checkbox.checked = false
    })

    console.log('✅ All school overlays cleared')
  }

  destroy () {
    this.clearAllOverlays()
    this.overlayLayers.clear()
    this.overlayData.clear()
    console.log('🏫 SchoolOverlays destroyed')
  }
}
