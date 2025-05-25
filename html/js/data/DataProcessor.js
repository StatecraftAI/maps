/**
 * DataProcessor - GeoJSON Data Processing and Analysis
 *
 * Handles:
 * - Field detection and registry management
 * - Data range calculations
 * - Data validation and cleaning
 * - Layer organization and categorization
 * - Metadata extraction and processing
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class DataProcessor {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // Field categorization patterns
    this.fieldCategories = {
      electoral: [
        /^votes?_/, /^vote_pct_/, /^vote_margin/, /^leading_candidate/,
        /^turnout/, /^political_lean/, /^competitiveness/
      ],
      analytical: [
        /^vote_impact/, /^net_margin/, /^swing_contribution/,
        /^power_index/, /^precinct_influence/, /^competitive_balance/,
        /^vote_efficiency/, /^margin_volatility/, /^divergence_from/
      ],
      demographic: [
        /^reg_pct_/, /^total_voters/, /^precinct_size/,
        /^major_party_pct/, /^dem_advantage/
      ],
      administrative: [
        /^precinct/, /^district/, /^zone/, /^is_pps/, /^has_/,
        /^participated/, /^complete_record/
      ]
    }

    console.log('[DataProcessor] Initialized')
  }

  /**
     * Process loaded GeoJSON data
     */
  async processElectionData (geoJsonData, datasetKey) {
    console.log(`[DataProcessor] Processing election data for ${datasetKey}...`)

    try {
      // Validate data structure
      this.validateGeoJsonStructure(geoJsonData)

      // Extract field information
      const fieldInfo = this.extractFieldInformation(geoJsonData)

      // Calculate data ranges
      const dataRanges = this.calculateDataRanges(geoJsonData, fieldInfo.numeric)

      // Organize layers by category
      const layerOrganization = this.organizeLayersByCategory(fieldInfo.available)

      // Extract metadata
      const metadata = this.extractMetadata(geoJsonData)

      const processedData = {
        originalData: geoJsonData,
        fieldInfo,
        dataRanges,
        layerOrganization,
        metadata,
        processed: true,
        processedAt: new Date().toISOString()
      }

      console.log(`[DataProcessor] Processing completed for ${datasetKey}`)
      console.log('Field info:', fieldInfo)
      console.log('Data ranges calculated for', Object.keys(dataRanges).length, 'fields')

      this.eventBus.emit('data:processed', {
        datasetKey,
        processedData
      })

      return processedData
    } catch (error) {
      console.error(`[DataProcessor] Processing failed for ${datasetKey}:`, error)
      this.eventBus.emit('data:processingError', {
        datasetKey,
        error: error.message
      })
      throw error
    }
  }

  /**
     * Validate GeoJSON structure
     */
  validateGeoJsonStructure (geoJsonData) {
    if (!geoJsonData) {
      throw new Error('GeoJSON data is null or undefined')
    }

    if (!geoJsonData.features || !Array.isArray(geoJsonData.features)) {
      throw new Error('Invalid GeoJSON: missing or invalid features array')
    }

    if (geoJsonData.features.length === 0) {
      throw new Error('GeoJSON contains no features')
    }

    // Validate sample feature structure
    const sampleFeature = geoJsonData.features[0]
    if (!sampleFeature.properties || typeof sampleFeature.properties !== 'object') {
      throw new Error('Invalid GeoJSON: features missing properties')
    }

    if (!sampleFeature.geometry) {
      throw new Error('Invalid GeoJSON: features missing geometry')
    }

    console.log(`[DataProcessor] Validated GeoJSON with ${geoJsonData.features.length} features`)
  }

  /**
     * Extract field information from GeoJSON data
     */
  extractFieldInformation (geoJsonData) {
    console.log('[DataProcessor] Extracting field information...')

    // Use field registry if available
    if (geoJsonData.metadata?.field_registry) {
      return this.extractFromFieldRegistry(geoJsonData.metadata.field_registry)
    }

    // Fallback to property analysis
    return this.extractFromProperties(geoJsonData.features)
  }

  /**
     * Extract field information from metadata field registry
     */
  extractFromFieldRegistry (fieldRegistry) {
    console.log('[DataProcessor] Using field registry for field information')

    const fieldInfo = {
      available: fieldRegistry.available_fields || [],
      visualization: fieldRegistry.visualization_fields || [],
      numeric: [
        ...(fieldRegistry.numeric_fields || []),
        ...(fieldRegistry.percentage_fields || []),
        ...(fieldRegistry.count_fields || [])
      ],
      categorical: fieldRegistry.categorical_fields || [],
      displayNames: fieldRegistry.display_names || {},
      explanations: fieldRegistry.explanations || {},
      source: 'registry'
    }

    console.log(`[DataProcessor] Found ${fieldInfo.available.length} total fields`)
    console.log(`[DataProcessor] Found ${fieldInfo.visualization.length} visualization fields`)
    console.log(`[DataProcessor] Found ${fieldInfo.numeric.length} numeric fields`)

    return fieldInfo
  }

  /**
     * Extract field information from feature properties (fallback)
     */
  extractFromProperties (features) {
    console.log('[DataProcessor] Analyzing feature properties (fallback method)')

    if (features.length === 0) {
      return { available: [], visualization: [], numeric: [], categorical: [] }
    }

    const sampleProperties = features[0].properties
    const allFields = Object.keys(sampleProperties).filter(field => field !== 'geometry')
    const numericFields = []
    const categoricalFields = []

    // Analyze field types
    allFields.forEach(field => {
      const value = sampleProperties[field]
      if (typeof value === 'number') {
        numericFields.push(field)
      } else if (typeof value === 'string') {
        categoricalFields.push(field)
      }
    })

    const fieldInfo = {
      available: allFields,
      visualization: allFields, // All fields available for visualization in fallback
      numeric: numericFields,
      categorical: categoricalFields,
      displayNames: {},
      explanations: {},
      source: 'properties'
    }

    console.log(`[DataProcessor] Detected ${allFields.length} fields from properties`)
    console.warn('[DataProcessor] Using fallback method - field categorization may be incomplete')

    return fieldInfo
  }

  /**
     * Calculate data ranges for numeric fields
     */
  calculateDataRanges (geoJsonData, numericFields) {
    console.log('[DataProcessor] Calculating data ranges...')

    const ranges = {}
    const features = geoJsonData.features

    // Apply PPS filter if enabled
    const showPpsOnly = this.stateManager.getState('showPpsOnly')
    const filteredFeatures = showPpsOnly
      ? features.filter(f => f.properties.is_pps_precinct)
      : features

    numericFields.forEach(field => {
      const values = filteredFeatures
        .map(f => f.properties[field])
        .filter(v => v !== null && v !== undefined && !isNaN(v))

      if (values.length > 0) {
        ranges[field] = {
          min: Math.min(...values),
          max: Math.max(...values),
          count: values.length,
          mean: values.reduce((a, b) => a + b, 0) / values.length
        }
      }
    })

    console.log(`[DataProcessor] Calculated ranges for ${Object.keys(ranges).length} numeric fields`)

    return ranges
  }

  /**
     * Organize layers by category for UI organization
     */
  organizeLayersByCategory (availableFields) {
    console.log('[DataProcessor] Organizing layers by category...')

    const categories = {
      electoral: [],
      analytical: [],
      demographic: [],
      administrative: [],
      other: []
    }

    availableFields.forEach(field => {
      let categorized = false

      // Check each category pattern
      for (const [category, patterns] of Object.entries(this.fieldCategories)) {
        if (patterns.some(pattern => pattern.test(field))) {
          categories[category].push(field)
          categorized = true
          break
        }
      }

      // If no pattern matched, put in 'other'
      if (!categorized) {
        categories.other.push(field)
      }
    })

    // Log categorization results
    Object.entries(categories).forEach(([category, fields]) => {
      if (fields.length > 0) {
        console.log(`[DataProcessor] ${category}: ${fields.length} fields`)
      }
    })

    return categories
  }

  /**
     * Extract and process metadata from GeoJSON
     */
  extractMetadata (geoJsonData) {
    const metadata = geoJsonData.metadata || {}

    // Extract candidate information if available
    const candidates = this.extractCandidates(geoJsonData)

    // Process candidate colors
    const candidateColors = this.processCandidateColors(metadata.candidate_colors, candidates)

    return {
      ...metadata,
      candidates,
      candidateColors,
      featureCount: geoJsonData.features.length,
      extractedAt: new Date().toISOString()
    }
  }

  /**
     * Extract candidate names from data
     */
  extractCandidates (geoJsonData) {
    if (geoJsonData.features.length === 0) {
      return []
    }

    const sampleProperties = geoJsonData.features[0].properties
    const candidates = []

    Object.keys(sampleProperties).forEach(prop => {
      if (prop.startsWith('vote_pct_') &&
                !prop.startsWith('vote_pct_contribution_') &&
                prop !== 'vote_pct_contribution_total_votes') {
        const candidateName = prop.replace('vote_pct_', '')

        // Filter out administrative fields
        const skipList = [
          'leading', 'second_place', 'total', 'write_in',
          'Write In', 'Leading', 'Second Place', 'tie'
        ]

        if (!skipList.includes(candidateName) && candidateName.length > 2) {
          candidates.push(candidateName)
        }
      }
    })

    console.log(`[DataProcessor] Extracted ${candidates.length} candidates:`, candidates)

    return candidates
  }

  /**
     * Process candidate colors from metadata
     */
  processCandidateColors (candidateColorsFromMetadata, detectedCandidates) {
    const candidateColors = {}

    // Use metadata colors if available
    if (candidateColorsFromMetadata) {
      console.log('[DataProcessor] Using candidate colors from metadata')
      Object.assign(candidateColors, candidateColorsFromMetadata)
    } else {
      console.log('[DataProcessor] Generating candidate colors automatically')

      // Color palette for automatic assignment
      const colorPalette = [
        '#0571b0', '#fd8d3c', '#238b45', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
      ]

      // Assign colors to detected candidates
      detectedCandidates.forEach((candidate, index) => {
        candidateColors[candidate] = colorPalette[index % colorPalette.length]
      })
    }

    return candidateColors
  }

  /**
     * Update data ranges for filtered data
     */
  updateDataRanges (geoJsonData, numericFields, filterPpsOnly = null) {
    const showPpsOnly = filterPpsOnly !== null ? filterPpsOnly : this.stateManager.getState('showPpsOnly')
    console.log(`[DataProcessor] Updating data ranges (PPS filter: ${showPpsOnly})`)

    const updatedRanges = this.calculateDataRanges(geoJsonData, numericFields)

    this.eventBus.emit('data:rangesUpdated', {
      ranges: updatedRanges,
      ppsFilter: showPpsOnly
    })

    return updatedRanges
  }

  /**
     * Get field display name with fallbacks
     */
  getFieldDisplayName (fieldKey, fieldInfo) {
    // Try display names from field info first
    if (fieldInfo.displayNames && fieldInfo.displayNames[fieldKey]) {
      return fieldInfo.displayNames[fieldKey]
    }

    // Generate display name from field key
    return this.generateDisplayName(fieldKey)
  }

  /**
     * Generate human-readable display name from field key
     */
  generateDisplayName (fieldKey) {
    // Handle candidate fields
    if (fieldKey.startsWith('votes_') && fieldKey !== 'votes_total') {
      const candidateName = fieldKey.replace('votes_', '')
      return `Vote Count - ${this.toTitleCase(candidateName)}`
    }

    if (fieldKey.startsWith('vote_pct_') && !fieldKey.startsWith('vote_pct_contribution_')) {
      const candidateName = fieldKey.replace('vote_pct_', '')
      return `Vote % - ${this.toTitleCase(candidateName)}`
    }

    if (fieldKey.startsWith('vote_pct_contribution_')) {
      const candidateName = fieldKey.replace('vote_pct_contribution_', '')
      return `Vote Contribution % - ${this.toTitleCase(candidateName)}`
    }

    if (fieldKey.startsWith('reg_pct_')) {
      const party = fieldKey.replace('reg_pct_', '').toUpperCase()
      return `Registration % - ${party}`
    }

    // Default: convert to title case
    return this.toTitleCase(fieldKey)
  }

  /**
     * Convert snake_case to Title Case
     */
  toTitleCase (str) {
    if (!str) return ''
    return str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
  }

  /**
     * Get processing statistics
     */
  getStats () {
    return {
      // Add any processing statistics here
      timestamp: new Date().toISOString()
    }
  }
}
