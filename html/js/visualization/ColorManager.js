/**
 * ColorManager - Color Schemes and Gradient Generation
 *
 * Handles:
 * - Color-blind friendly color schemes
 * - Dynamic gradient generation
 * - Categorical and continuous color mappings
 * - Candidate-specific color management
 * - Range-based color calculations
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class ColorManager {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // Initialize color schemes
    this.initializeColorSchemes()

    // Update state with initial color schemes
    this.stateManager.setState({
      colorSchemes: { ...this.colorSchemes }
    }, { source: 'ColorManager.constructor' })

    // Color palettes for automatic assignment
    this.candidateColorPalette = [
      '#0571b0', '#fd8d3c', '#238b45', '#d62728', '#9467bd',
      '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
    ]

    console.log('[ColorManager] Initialized')
  }

  /**
     * Initialize all color schemes
     */
  initializeColorSchemes () {
    this.colorSchemes = {
      political_lean: {
        'Strong Dem': '#0571b0', // Strong blue
        'Lean Dem': '#74a9cf', // Light blue
        Competitive: '#fee391', // Light yellow
        'Lean Rep': '#fd8d3c', // Orange
        'Strong Rep': '#d94701' // Strong orange/red
      },
      competitiveness: {
        Safe: '#2166ac', // Dark blue (less competitive)
        Likely: '#762a83', // Purple
        Competitive: '#f1a340', // Orange
        Tossup: '#d73027', // Red (most competitive)
        'No Election Data': '#f7f7f7' // Light gray
      },
      leading_candidate: {
        Tie: '#636363', // Gray
        'No Election Data': '#f7f7f7', // Light gray
        'No Data': '#f7f7f7' // Light gray
        // Candidate colors will be added dynamically
      },
      turnout_quartile: {
        Low: '#fee391', // Light yellow (low)
        'Med-Low': '#fec44f', // Medium yellow
        Medium: '#fe9929', // Orange
        'Med-High': '#d95f0e', // Dark orange
        High: '#993404', // Very dark orange (high)
        Single: '#f7f7f7' // Light gray
      },
      margin_category: {
        'Very Close': '#fee391', // Light (close)
        Close: '#fec44f', // Medium light
        Clear: '#d95f0e', // Darker (clear)
        Landslide: '#993404' // Darkest (landslide)
      },
      precinct_size_category: {
        Small: '#fee391', // Light (small)
        Medium: '#fec44f', // Medium light
        Large: '#d95f0e', // Dark (large)
        'Extra Large': '#993404' // Darkest (extra large)
      }
    }
  }

  /**
     * Get feature color based on current field and properties
     */
  getFeatureColor (properties, currentField = null) {
    if (!currentField) {
      currentField = this.stateManager.getState('currentField')
    }

    const value = properties[currentField]

    // Handle categorical fields with defined color schemes
    if (this.colorSchemes[currentField]) {
      return this.getCategoricalColor(currentField, value)
    }

    // Handle numeric fields with gradient generation
    if (typeof value === 'number') {
      return this.getNumericColor(currentField, value, properties)
    }

    // Default fallback
    return '#808080'
  }

  /**
     * Get color for categorical fields
     */
  getCategoricalColor (fieldKey, value) {
    const scheme = this.colorSchemes[fieldKey]

    // Special handling for leading_candidate with name normalization
    if (fieldKey === 'leading_candidate' && value) {
      const normalizedValue = this.normalizeCandidateName(value)
      return scheme[normalizedValue] || scheme[value] || '#808080'
    }

    return scheme[value] || '#808080'
  }

  /**
     * Get color for numeric fields using gradients
     */
  getNumericColor (fieldKey, value, properties) {
    const range = this.getCurrentRange(fieldKey)

    if (!range) {
      return this.getFallbackNumericColor(value)
    }

    const normalized = Math.max(0, Math.min(1, (value - range.min) / (range.max - range.min)))

    // Check for candidate-specific fields
    if (fieldKey.startsWith('vote_pct_') && !fieldKey.startsWith('vote_pct_contribution_')) {
      return this.getCandidateGradientColor(fieldKey, normalized)
    }

    if (fieldKey.startsWith('votes_') && fieldKey !== 'votes_total') {
      return this.getCandidateGradientColor(fieldKey, normalized)
    }

    // Field-specific gradient schemes
    return this.getGradientColor(fieldKey, normalized)
  }

  /**
     * Get candidate-specific gradient color
     */
  getCandidateGradientColor (fieldKey, normalized) {
    let candidateName

    if (fieldKey.startsWith('vote_pct_')) {
      candidateName = fieldKey.replace('vote_pct_', '')
    } else if (fieldKey.startsWith('votes_')) {
      candidateName = fieldKey.replace('votes_', '')
    }

    if (candidateName) {
      const candidateColor = this.getCandidateColor(candidateName)
      if (candidateColor) {
        return this.createGradientFromColor(candidateColor, normalized)
      }
    }

    // Fallback to default gradient
    return this.getViridisColor(normalized)
  }

  /**
     * Create gradient from candidate color to white
     */
  createGradientFromColor (candidateColor, intensity) {
    const hex = candidateColor.replace('#', '')
    const r = parseInt(hex.substr(0, 2), 16)
    const g = parseInt(hex.substr(2, 2), 16)
    const b = parseInt(hex.substr(4, 2), 16)

    // Interpolate from white (255,255,255) to candidate color
    const finalR = Math.round(255 + (r - 255) * intensity)
    const finalG = Math.round(255 + (g - 255) * intensity)
    const finalB = Math.round(255 + (b - 255) * intensity)

    return `rgb(${finalR}, ${finalG}, ${finalB})`
  }

  /**
     * Get gradient color based on field type
     */
  getGradientColor (fieldKey, normalized) {
    // Percentage fields - viridis-like gradient
    if (fieldKey.includes('vote_pct_') || fieldKey === 'turnout_rate' ||
            fieldKey === 'major_party_pct') {
      return this.getViridisColor(normalized)
    }

    // Count fields - plasma-like gradient
    if (fieldKey.includes('votes_') || fieldKey === 'total_voters') {
      return this.getPlasmaColor(normalized)
    }

    // Diverging fields (advantage/efficiency)
    if (fieldKey === 'dem_advantage' || fieldKey === 'vote_efficiency_dem') {
      return this.getDivergingColor(normalized, 'blue')
    }

    if (fieldKey === 'divergence_from_tie') {
      return this.getDivergingColor(normalized, 'green')
    }

    // Default - cividis-like gradient
    return this.getCividisColor(normalized)
  }

  /**
     * Viridis color scheme (color-blind friendly)
     */
  getViridisColor (normalized) {
    const colors = [
      [68, 1, 84], // Dark purple (low)
      [59, 82, 139], // Blue
      [33, 145, 140], // Teal
      [94, 201, 98], // Green
      [253, 231, 37] // Yellow (high)
    ]

    return this.interpolateColors(colors, normalized)
  }

  /**
     * Plasma color scheme (color-blind friendly)
     */
  getPlasmaColor (normalized) {
    const colors = [
      [13, 8, 135], // Dark blue (low)
      [84, 2, 163], // Purple
      [139, 10, 165], // Pink
      [185, 50, 137], // Red
      [224, 93, 106], // Orange
      [253, 231, 37] // Yellow (high)
    ]

    return this.interpolateColors(colors, normalized)
  }

  /**
     * Cividis color scheme (color-blind friendly)
     */
  getCividisColor (normalized) {
    const colors = [
      [0, 32, 76], // Dark blue (low)
      [0, 67, 88], // Blue
      [0, 104, 87], // Teal
      [87, 134, 58], // Green
      [188, 163, 23], // Yellow
      [255, 221, 0] // Bright yellow (high)
    ]

    return this.interpolateColors(colors, normalized)
  }

  /**
     * Diverging color scheme for advantage/efficiency fields
     */
  getDivergingColor (normalized, scheme = 'blue') {
    const range = this.getCurrentRange(this.stateManager.getState('currentField'))
    if (!range) return '#808080'

    const value = range.min + (range.max - range.min) * normalized

    if (scheme === 'blue') {
      if (value >= 0) {
        // Positive values: white to blue
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const blueIntensity = Math.round(intensity * 150)
        return `rgb(${255 - blueIntensity}, ${255 - blueIntensity}, 255)`
      } else {
        // Negative values: white to red
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const redIntensity = Math.round(intensity * 150)
        return `rgb(255, ${255 - redIntensity}, ${255 - redIntensity})`
      }
    } else if (scheme === 'green') {
      if (value >= 0) {
        // Positive values: white to green
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const greenIntensity = Math.round(intensity * 150)
        return `rgb(${255 - greenIntensity}, 255, ${255 - greenIntensity})`
      } else {
        // Negative values: white to red
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const redIntensity = Math.round(intensity * 150)
        return `rgb(255, ${255 - redIntensity}, ${255 - redIntensity})`
      }
    }

    return '#808080'
  }

  /**
     * Interpolate between colors in a palette
     */
  interpolateColors (colors, normalized) {
    // Validate inputs
    if (!colors || colors.length === 0) {
      console.warn('[ColorManager] No colors provided for interpolation');
      return 'rgb(128, 128, 128)'; // Gray fallback
    }
    
    if (colors.length === 1) {
      const color = colors[0];
      return `rgb(${color[0]}, ${color[1]}, ${color[2]})`;
    }
    
    // Clamp normalized value between 0 and 1
    const clampedNormalized = Math.max(0, Math.min(1, normalized));
    
    const colorIndex = clampedNormalized * (colors.length - 1)
    const lowerIndex = Math.floor(colorIndex)
    const upperIndex = Math.min(lowerIndex + 1, colors.length - 1)
    const fraction = colorIndex - lowerIndex

    const lowerColor = colors[lowerIndex]
    const upperColor = colors[upperIndex]
    
    // Additional validation
    if (!lowerColor || !upperColor) {
      console.warn('[ColorManager] Invalid color data in interpolation');
      return 'rgb(128, 128, 128)'; // Gray fallback
    }

    const r = Math.round(lowerColor[0] + (upperColor[0] - lowerColor[0]) * fraction)
    const g = Math.round(lowerColor[1] + (upperColor[1] - lowerColor[1]) * fraction)
    const b = Math.round(lowerColor[2] + (upperColor[2] - lowerColor[2]) * fraction)

    return `rgb(${r}, ${g}, ${b})`
  }

  /**
     * Get current data range for a field
     */
  getCurrentRange (fieldKey) {
    const customRange = this.stateManager.getState('customRange')
    if (customRange && customRange.field === fieldKey) {
      return customRange
    }

    const actualRanges = this.stateManager.getState('actualDataRanges')
    return actualRanges ? actualRanges[fieldKey] : null
  }

  /**
     * Get candidate color from metadata or generate automatically
     */
  getCandidateColor (candidateName) {
    // Check metadata first
    const electionData = this.stateManager.getState('electionData')
    if (electionData?.metadata?.candidate_colors) {
      const normalizedName = this.normalizeCandidateName(candidateName)
      return electionData.metadata.candidate_colors[candidateName] ||
                   electionData.metadata.candidate_colors[normalizedName]
    }

    // Check color schemes
    const normalizedName = this.normalizeCandidateName(candidateName)
    if (this.colorSchemes.leading_candidate[normalizedName]) {
      return this.colorSchemes.leading_candidate[normalizedName]
    }

    return null
  }

  /**
     * Normalize candidate name for consistent lookup
     */
  normalizeCandidateName (name) {
    if (!name) return ''
    return name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '')
  }

  /**
     * Add candidate color to schemes
     */
  addCandidateColor (candidateName, color) {
    const normalizedName = this.normalizeCandidateName(candidateName)
    this.colorSchemes.leading_candidate[normalizedName] = color

    // Update state with new color schemes
    this.stateManager.setState({
      colorSchemes: { ...this.colorSchemes }
    }, { source: 'ColorManager.addCandidateColor' })

    console.log(`[ColorManager] Added candidate color: ${candidateName} -> ${color}`)
  }

  /**
     * Build candidate color schemes from data
     */
  buildCandidateColorSchemes (candidates, candidateColors = null) {
    // Reset leading_candidate scheme
    this.colorSchemes.leading_candidate = {
      Tie: '#636363',
      'No Election Data': '#f7f7f7',
      'No Data': '#f7f7f7'
    }

    if (candidateColors) {
      // Use provided colors
      Object.entries(candidateColors).forEach(([name, color]) => {
        this.addCandidateColor(name, color)
      })
    } else {
      // Auto-assign colors
      let colorIndex = 0
      candidates.forEach(candidate => {
        if (this.isValidCandidate(candidate)) {
          const color = this.candidateColorPalette[colorIndex % this.candidateColorPalette.length]
          this.addCandidateColor(candidate, color)
          colorIndex++
        }
      })
    }

    // Update state with new color schemes
    this.stateManager.setState({
      colorSchemes: { ...this.colorSchemes }
    }, { source: 'ColorManager.buildCandidateColorSchemes' })

    console.log('[ColorManager] Built candidate color schemes:', this.colorSchemes.leading_candidate)
  }

  /**
     * Check if candidate name is valid (not administrative field)
     */
  isValidCandidate (candidateName) {
    const skipList = [
      'leading', 'second_place', 'total', 'write_in',
      'Write In', 'Leading', 'Second Place', 'tie'
    ]

    return !skipList.includes(candidateName) &&
               !skipList.includes(candidateName.toLowerCase()) &&
               !candidateName.startsWith('vote_') &&
               !candidateName.startsWith('reg_') &&
               candidateName.length > 2 &&
               !candidateName.includes('_total') &&
               !candidateName.includes('_pct') &&
               candidateName !== candidateName.toUpperCase()
  }

  /**
     * Get fallback color for numeric fields without range
     */
  getFallbackNumericColor (value) {
    const intensity = Math.min(Math.abs(value) / 100, 1)
    return `hsl(220, 70%, ${90 - (intensity * 50)}%)`
  }

  /**
     * Get color scheme for a field
     */
  getColorScheme (fieldKey) {
    return this.colorSchemes[fieldKey] || null
  }

  /**
     * Check if field has categorical color scheme
     */
  isCategorical (fieldKey) {
    return this.colorSchemes[fieldKey] !== undefined
  }

  /**
     * Generate legend colors for current field
     */
  generateLegendColors (fieldKey, steps = 5) {
    if (this.isCategorical(fieldKey)) {
      return this.getCategoricalLegendColors(fieldKey)
    } else {
      return this.getContinuousLegendColors(fieldKey, steps)
    }
  }

  /**
     * Get categorical legend colors
     */
  getCategoricalLegendColors (fieldKey) {
    const scheme = this.colorSchemes[fieldKey]
    if (!scheme) return []

    return Object.entries(scheme)
      .filter(([key]) => !['Tie', 'No Data', 'No Election Data'].includes(key))
      .map(([label, color]) => ({ label, color }))
  }

  /**
     * Get continuous legend colors
     */
  getContinuousLegendColors (fieldKey, steps) {
    const colors = []
    const range = this.getCurrentRange(fieldKey)

    if (!range) return []

    for (let i = 0; i < steps; i++) {
      const value = range.min + (i / (steps - 1)) * (range.max - range.min)
      const color = this.getNumericColor(fieldKey, value, {})
      colors.push({ value, color })
    }

    return colors
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.colorSchemes = null
    console.log('[ColorManager] Destroyed')
  }
}
