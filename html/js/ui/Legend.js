/**
 * Legend - Map Color Scale Legend
 *
 * Handles:
 * - Color scale legend display
 * - Categorical and continuous legend types
 * - Dynamic legend updates based on current field
 * - Clean horizontal layout with proper styling
 * - Range display and color gradient generation
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Legend {
  constructor (stateManager, eventBus, colorManager = null) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.colorManager = colorManager

    // Legend elements
    this.legendContainer = null
    this.legendTitle = null
    this.legendBar = null
    this.legendLabels = null

    // Color schemes for categorical legends
    this.colorSchemes = {
      political_lean: {
        'Strong Dem': '#0571b0',
        'Lean Dem': '#74a9cf',
        Competitive: '#fee391',
        'Lean Rep': '#fd8d3c',
        'Strong Rep': '#d94701'
      },
      competitiveness: {
        Safe: '#2166ac',
        Likely: '#762a83',
        Competitive: '#f1a340',
        Tossup: '#d73027',
        'No Election Data': '#f7f7f7'
      },
      leading_candidate: {
        Tie: '#636363',
        'No Election Data': '#f7f7f7',
        'No Data': '#f7f7f7'
      },
      turnout_quartile: {
        Low: '#fee391',
        'Med-Low': '#fec44f',
        Medium: '#fe9929',
        'Med-High': '#d95f0e',
        High: '#993404',
        Single: '#f7f7f7'
      },
      margin_category: {
        'Very Close': '#fee391',
        Close: '#fec44f',
        Clear: '#d95f0e',
        Landslide: '#993404'
      },
      precinct_size_category: {
        Small: '#fee391',
        Medium: '#fec44f',
        Large: '#d95f0e',
        'Extra Large': '#993404'
      }
    }

    this.initializeElements()
    this.setupEventListeners()

    console.log('[Legend] Initialized')
  }

  /**
     * Initialize DOM elements
     */
  initializeElements () {
    this.legendContainer = document.getElementById('color-scale-legend')
    if (!this.legendContainer) {
      console.warn('[Legend] Legend container not found')
      return
    }

    this.legendTitle = document.getElementById('legend-title')
    this.legendBar = document.getElementById('legend-bar')
    this.legendLabels = this.legendContainer.querySelector('.legend-labels')

    // Set initial legend
    this.updateLegend()
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    // Listen for field changes
    this.stateManager.subscribe('currentField', () => {
      this.updateLegend()
    })

    // Listen for data changes
    this.eventBus.on('data:loaded', () => {
      this.updateLegend()
    })

    // Listen for candidate color scheme updates
    this.eventBus.on('candidates:colorsUpdated', (data) => {
      this.updateCandidateColors(data.colors)
      this.updateLegend()
    })

    // Listen for custom range changes
    this.stateManager.subscribe('customRange', () => {
      this.updateLegend()
    })

    // Listen for filter changes that affect data ranges
    this.stateManager.subscribe('showPpsOnly', () => {
      this.updateLegend()
    })
  }

  /**
     * Update legend based on current field
     */
  updateLegend () {
    if (!this.legendContainer) return

    const currentField = this.stateManager.getState('currentField')
    const fieldDisplayName = this.getFieldDisplayName(currentField)
    const isCategorical = this.isCategoricalField(currentField)

    console.log(`[Legend] Updating legend for field: ${currentField}, categorical: ${isCategorical}`)

    // Prepare legend data for integration
    let legendData = {
      title: fieldDisplayName,
      field: currentField
    }

    // Handle "none" layer selection
    if (currentField === 'none') {
      this.showBaseMapLegend()
      legendData.type = 'none'
      this.eventBus.emit('legend:updated', legendData)
      return
    }

    // Check if this is a categorical field
    if (isCategorical) {
      this.showCategoricalLegend(currentField, fieldDisplayName)
      const colorScheme = this.getColorScheme(currentField)
      if (colorScheme) {
        legendData.type = 'categorical'
        legendData.items = Object.entries(colorScheme)
          .filter(([key]) => this.shouldShowCategory(currentField, key))
          .map(([value, color]) => ({
            label: this.formatCategoryValue(currentField, value),
            color: color
          }))
      }
    } else {
      this.showContinuousLegend(currentField, fieldDisplayName)
      const range = this.getFieldRange(currentField)
      if (range) {
        const gradientColors = this.generateGradientColors(currentField, range)
        const { minLabel, maxLabel } = this.formatRangeLabels(currentField, range)
        
        legendData.type = 'continuous'
        legendData.gradient = `linear-gradient(to right, ${gradientColors.join(', ')})`
        legendData.min = minLabel
        legendData.max = maxLabel
      }
    }

    // Emit legend data for integration into InfoPanel
    this.eventBus.emit('legend:updated', legendData)
  }

  /**
     * Show base map legend (no data overlay)
     */
  showBaseMapLegend () {
    this.legendContainer.innerHTML = `
            <div class="legend-horizontal">
                <div class="legend-title">Base Map Only</div>
                <div style="color: var(--color-text-secondary); font-style: italic; font-size: var(--font-size-xs);">
                    Showing precinct boundaries without data overlay
                </div>
            </div>
        `
  }

  /**
     * Show categorical legend
     */
  showCategoricalLegend (field, displayName) {
    const colorScheme = this.getColorScheme(field)
    if (!colorScheme) {
      this.showNoDataLegend(displayName)
      return
    }

    const categoricalItems = Object.entries(colorScheme)
      .filter(([key]) => this.shouldShowCategory(field, key))
      .map(([value, color]) => {
        const displayValue = this.formatCategoryValue(field, value)
        return `<div class="legend-item">
                            <div class="legend-color-dot" style="background-color: ${color};"></div>
                            <span>${displayValue}</span>
                        </div>`
      }).join('')

    this.legendContainer.innerHTML = `
            <div class="legend-horizontal">
                <div class="legend-title">${displayName}</div>
                <div class="legend-categorical">${categoricalItems}</div>
            </div>
        `
  }

  /**
     * Show continuous legend
     */
  showContinuousLegend (field, displayName) {
    const range = this.getFieldRange(field)
    if (!range) {
      this.showNoDataLegend(displayName)
      return
    }

    const gradientColors = this.generateGradientColors(field, range)
    const { minLabel, maxLabel } = this.formatRangeLabels(field, range)

    this.legendContainer.innerHTML = `
            <div class="legend-horizontal">
                <div class="legend-title">${displayName}</div>
                <div class="legend-colorbar-container">
                    <div class="legend-colorbar" style="background: linear-gradient(to right, ${gradientColors.join(', ')});"></div>
                    <div class="legend-range">
                        <span>${minLabel}</span>
                        <span>${maxLabel}</span>
                    </div>
                </div>
            </div>
        `
  }

  /**
     * Show no data legend
     */
  showNoDataLegend (displayName) {
    this.legendContainer.innerHTML = `
            <div class="legend-horizontal">
                <div class="legend-title">${displayName}</div>
                <div style="color: var(--color-text-secondary); font-style: italic; font-size: var(--font-size-xs); margin-top: var(--space-2);">
                    No data available
                </div>
            </div>
        `
  }

  /**
     * Check if field is categorical
     */
  isCategoricalField (field) {
    // Check ColorManager first if available
    if (this.colorManager && typeof this.colorManager.isCategorical === 'function') {
      return this.colorManager.isCategorical(field)
    }

    // Check state color schemes
    const colorSchemes = this.stateManager.getState('colorSchemes')
    if (colorSchemes && colorSchemes.hasOwnProperty(field)) {
      return true
    }

    // Fallback to internal schemes
    return this.colorSchemes.hasOwnProperty(field) ||
               field === 'leading_candidate' ||
               field.includes('_category')
  }

  /**
     * Get color scheme for field
     */
  getColorScheme (field) {
    // Get color schemes from ColorManager via state
    const colorSchemes = this.stateManager.getState('colorSchemes')
    
    if (colorSchemes && colorSchemes[field]) {
      return colorSchemes[field]
    }

    // Fallback to internal schemes if state not available
    if (this.colorSchemes[field]) {
      return this.colorSchemes[field]
    }

    // Check for dynamic candidate colors
    if (field === 'leading_candidate') {
      const candidateColors = this.stateManager.getState('candidateColors')
      if (candidateColors) {
        return { ...this.colorSchemes.leading_candidate, ...candidateColors }
      }
    }

    return null
  }

  /**
     * Get field data range
     */
  getFieldRange (field) {
    // Check for custom range first
    const customRange = this.stateManager.getState('customRange')
    if (customRange && customRange.field === field) {
      return customRange
    }

    // Get calculated range from data - try multiple state keys
    const actualDataRanges = this.stateManager.getState('actualDataRanges')
    if (actualDataRanges && actualDataRanges[field]) {
      return actualDataRanges[field]
    }

    const dataRanges = this.stateManager.getState('dataRanges')
    if (dataRanges && dataRanges[field]) {
      return dataRanges[field]
    }

    // Fallback range
    return { min: 0, max: 100 }
  }

  /**
     * Generate gradient colors for continuous legend
     */
  generateGradientColors (field, range, steps = 5) {
    const colors = []

    for (let i = 0; i < steps; i++) {
      const value = range.min + (i / (steps - 1)) * (range.max - range.min)
      const color = this.getFeatureColor(field, value, range)
      colors.push(color)
    }

    return colors
  }

  /**
     * Get feature color for a specific value using ColorManager
     */
  getFeatureColor (field, value, range) {
    // Use injected ColorManager if available
    if (this.colorManager && typeof this.colorManager.getFeatureColor === 'function') {
      // Use ColorManager for consistent colors
      const mockProperties = { [field]: value }
      return this.colorManager.getFeatureColor(mockProperties, field)
    }
    
    // Fallback to internal color generation
    const normalized = Math.max(0, Math.min(1, (value - range.min) / (range.max - range.min)))

    // Different color schemes based on field type
    if (field.includes('vote_pct_') || field === 'turnout_rate' || field === 'major_party_pct') {
      // Viridis-like color scheme for percentages
      const colors = [
        [68, 1, 84], // Dark purple (low)
        [59, 82, 139], // Blue
        [33, 145, 140], // Teal
        [94, 201, 98], // Green
        [253, 231, 37] // Yellow (high)
      ]
      return this.interpolateColors(colors, normalized)
    } else if (field.includes('votes_') || field === 'total_voters') {
      // Plasma-like color scheme for counts
      const colors = [
        [13, 8, 135], // Dark blue (low)
        [84, 2, 163], // Purple
        [139, 10, 165], // Pink
        [185, 50, 137], // Red
        [224, 93, 106], // Orange
        [253, 231, 37] // Yellow (high)
      ]
      return this.interpolateColors(colors, normalized)
    } else if (field === 'dem_advantage' || field === 'vote_efficiency_dem') {
      // Diverging color scheme
      if (value >= 0) {
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const blueIntensity = Math.round(intensity * 150)
        return `rgb(${255 - blueIntensity}, ${255 - blueIntensity}, 255)`
      } else {
        const intensity = Math.abs(value) / Math.max(Math.abs(range.min), Math.abs(range.max))
        const redIntensity = Math.round(intensity * 150)
        return `rgb(255, ${255 - redIntensity}, ${255 - redIntensity})`
      }
    } else {
      // Default cividis-like color scheme
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
  }

  /**
     * Interpolate between colors
     */
  interpolateColors (colors, normalized) {
    const colorIndex = normalized * (colors.length - 1)
    const lowerIndex = Math.floor(colorIndex)
    const upperIndex = Math.min(lowerIndex + 1, colors.length - 1)
    const fraction = colorIndex - lowerIndex

    const lowerColor = colors[lowerIndex]
    const upperColor = colors[upperIndex]

    const r = Math.round(lowerColor[0] + (upperColor[0] - lowerColor[0]) * fraction)
    const g = Math.round(lowerColor[1] + (upperColor[1] - lowerColor[1]) * fraction)
    const b = Math.round(lowerColor[2] + (upperColor[2] - lowerColor[2]) * fraction)

    return `rgb(${r}, ${g}, ${b})`
  }

  /**
     * Format range labels
     */
  formatRangeLabels (field, range) {
    let minLabel = range.min.toFixed(1)
    let maxLabel = range.max.toFixed(1)

    // Add % for percentage fields
    if (field.includes('_pct_') || field === 'turnout_rate' ||
            field === 'dem_advantage' || field === 'engagement_rate') {
      minLabel += '%'
      maxLabel += '%'
    }

    // Add comma separators for vote counts
    if (field.startsWith('votes_')) {
      minLabel = Math.round(range.min).toLocaleString()
      maxLabel = Math.round(range.max).toLocaleString()
    }

    return { minLabel, maxLabel }
  }

  /**
     * Should show category in legend
     */
  shouldShowCategory (field, category) {
    if (field === 'leading_candidate') {
      return !['Tie', 'No Election Data', 'No Data'].includes(category)
    }
    return true
  }

  /**
     * Format category value for display
     */
  formatCategoryValue (field, value) {
    if (field === 'leading_candidate' && value === 'write_in') {
      return 'Write In'
    }

    // Format candidate names
    if (field === 'leading_candidate' && value !== 'Tie' && value !== 'No Election Data' && value !== 'No Data') {
      return this.formatCandidateName(value)
    }

    return value
  }

  /**
     * Format candidate name
     */
  formatCandidateName (candidateName) {
    if (!candidateName) return ''

    return candidateName.replace(/_/g, ' ')
      .replace(/\b\w/g, l => l.toUpperCase())
  }

  /**
     * Get field display name
     */
  getFieldDisplayName (fieldKey) {
    if (fieldKey === 'none') {
      return 'Base Map'
    }

    // This would typically come from DataProcessor or be passed in
    // For now, simple formatting
    return fieldKey.replace(/_/g, ' ')
      .replace(/\b\w/g, l => l.toUpperCase())
      .replace('Vote Pct', 'Vote %')
      .replace('Reg Pct', 'Registration %')
  }

  /**
     * Update candidate colors from external source
     */
  updateCandidateColors (candidateColors) {
    if (candidateColors) {
      this.colorSchemes.leading_candidate = {
        ...this.colorSchemes.leading_candidate,
        ...candidateColors
      }
    }
  }

  /**
     * Show/hide legend
     */
  setVisible (visible) {
    if (this.legendContainer) {
      this.legendContainer.style.display = visible ? 'flex' : 'none'
    }
  }

  /**
     * Get legend visibility
     */
  isVisible () {
    if (!this.legendContainer) return false
    return this.legendContainer.style.display !== 'none'
  }

  /**
     * Get current legend type
     */
  getLegendType () {
    const currentField = this.stateManager.getState('currentField')

    if (currentField === 'none') {
      return 'none'
    } else if (this.isCategoricalField(currentField)) {
      return 'categorical'
    } else {
      return 'continuous'
    }
  }

  /**
     * Get legend data for export
     */
  getLegendData () {
    const currentField = this.stateManager.getState('currentField')
    const legendType = this.getLegendType()

    const data = {
      field: currentField,
      type: legendType,
      displayName: this.getFieldDisplayName(currentField)
    }

    if (legendType === 'categorical') {
      data.categories = this.getColorScheme(currentField)
    } else if (legendType === 'continuous') {
      data.range = this.getFieldRange(currentField)
      data.colors = this.generateGradientColors(currentField, data.range)
    }

    return data
  }

  /**
     * Refresh legend display
     */
  refresh () {
    this.updateLegend()
  }

  /**
     * Clean up resources
     */
  destroy () {
    if (this.legendContainer) {
      this.legendContainer.innerHTML = ''
    }

    console.log('[Legend] Destroyed')
  }
}
