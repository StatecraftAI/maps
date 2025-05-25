/**
 * Tooltip - Map Feature Hover Tooltips
 *
 * Handles:
 * - Showing quick precinct information on hover
 * - Positioning tooltips relative to mouse cursor
 * - Formatting data values appropriately
 * - Clean tooltip styling and animations
 *
 * This replaces the hover functionality that was previously in InfoPanel.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Tooltip {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // DOM elements
    this.tooltipElement = null
    this.isVisible = false

    // State
    this.currentData = null
    this.mousePosition = { x: 0, y: 0 }

    this.initializeTooltip()
    this.setupEventListeners()

    console.log('[Tooltip] Initialized')
  }

  /**
   * Initialize the tooltip DOM element
   */
  initializeTooltip () {
    // Create tooltip element
    this.tooltipElement = document.createElement('div')
    this.tooltipElement.id = 'map-tooltip'
    this.tooltipElement.className = 'map-tooltip'
    this.tooltipElement.style.cssText = `
      position: absolute;
      background: rgba(0, 0, 0, 0.9);
      color: white;
      padding: 8px 12px;
      border-radius: 4px;
      font-size: 13px;
      line-height: 1.4;
      pointer-events: none;
      z-index: 1000;
      opacity: 0;
      transition: opacity 0.2s ease;
      max-width: 250px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
      border: 1px solid rgba(255, 255, 255, 0.1);
    `

    // Add to document body
    document.body.appendChild(this.tooltipElement)
  }

  /**
   * Set up event listeners
   */
  setupEventListeners () {
    // Listen for feature hover events
    this.eventBus.on('map:featureHover', (data) => {
      this.showTooltip(data)
    })

    // Listen for feature mouse out events
    this.eventBus.on('map:featureMouseOut', () => {
      this.hideTooltip()
    })

    // Track mouse position for tooltip positioning
    document.addEventListener('mousemove', (e) => {
      this.mousePosition = { x: e.clientX, y: e.clientY }
      if (this.isVisible) {
        this.updateTooltipPosition()
      }
    })

    console.log('[Tooltip] Event listeners set up')
  }

  /**
   * Show tooltip with precinct data
   */
  showTooltip (data) {
    if (!data || !data.properties) return

    const props = data.properties
    const currentField = data.currentField || this.stateManager.getState('currentField')

    // Generate tooltip content
    const content = this.generateTooltipContent(props, currentField)
    
    // Update tooltip
    this.tooltipElement.innerHTML = content
    this.currentData = data
    
    // Show tooltip
    this.tooltipElement.style.opacity = '1'
    this.isVisible = true
    
    // Position tooltip
    this.updateTooltipPosition()
  }

  /**
   * Hide tooltip
   */
  hideTooltip () {
    this.tooltipElement.style.opacity = '0'
    this.isVisible = false
    this.currentData = null
  }

  /**
   * Generate tooltip content based on precinct data and current field
   */
  generateTooltipContent (properties, currentField) {
    const precinctName = properties.precinct || 'Unknown'
    let content = `<div class="tooltip-header"><strong>Precinct ${precinctName}</strong></div>`

    // Add current field value if available
    if (currentField && currentField !== 'none') {
      const fieldValue = this.getValueForField(properties, currentField)
      const displayName = this.getFieldDisplayName(currentField)
      
      if (fieldValue !== null && fieldValue !== undefined) {
        const formattedValue = this.formatValue(fieldValue, currentField)
        content += `<div class="tooltip-field">${displayName}: <strong>${formattedValue}</strong></div>`
      }
    }

    // Add basic precinct info
    const voterCount = properties.total_voters || properties.registered_voters
    const voteCount = properties.votes_total || properties.total_votes
    const turnout = properties.turnout_rate || properties.turnout_pct

    if (voterCount) {
      content += `<div class="tooltip-info">Voters: ${voterCount.toLocaleString()}</div>`
    }

    if (voteCount) {
      content += `<div class="tooltip-info">Votes: ${voteCount.toLocaleString()}</div>`
    }

    if (turnout) {
      content += `<div class="tooltip-info">Turnout: ${turnout.toFixed(1)}%</div>`
    }

    content += `<div class="tooltip-footer"><em>Click for details</em></div>`

    return content
  }

  /**
   * Get value for a specific field from properties
   */
  getValueForField (properties, fieldKey) {
    return properties[fieldKey]
  }

  /**
   * Get display name for a field
   */
  getFieldDisplayName (fieldKey) {
    // Handle candidate fields
    if (fieldKey.startsWith('votes_') && fieldKey !== 'votes_total') {
      const candidateName = fieldKey.replace('votes_', '').replace(/_/g, ' ')
      return `${this.toTitleCase(candidateName)} Votes`
    }

    if (fieldKey.startsWith('vote_pct_') && !fieldKey.startsWith('vote_pct_contribution_')) {
      const candidateName = fieldKey.replace('vote_pct_', '').replace(/_/g, ' ')
      return `${this.toTitleCase(candidateName)} %`
    }

    // Common field mappings
    const fieldNames = {
      'political_lean': 'Political Lean',
      'competitiveness': 'Competitiveness',
      'leading_candidate': 'Leading Candidate',
      'turnout_rate': 'Turnout Rate',
      'turnout_quartile': 'Turnout Quartile',
      'margin_category': 'Victory Margin',
      'precinct_size_category': 'Precinct Size',
      'total_voters': 'Total Voters',
      'votes_total': 'Total Votes',
      'dem_advantage': 'Dem Advantage'
    }

    return fieldNames[fieldKey] || this.toTitleCase(fieldKey)
  }

  /**
   * Format value based on field type
   */
  formatValue (value, fieldKey) {
    if (typeof value === 'string') {
      // Format categorical values
      if (fieldKey === 'leading_candidate') {
        return this.toTitleCase(value)
      }
      return value
    }

    if (typeof value === 'number') {
      // Percentage fields
      if (fieldKey.includes('_pct') || fieldKey === 'turnout_rate' || fieldKey === 'dem_advantage') {
        return `${value.toFixed(1)}%`
      }
      
      // Vote count fields
      if (fieldKey.includes('votes_') || fieldKey === 'total_voters') {
        return value.toLocaleString()
      }
      
      // Default number formatting
      if (value >= 1000) {
        return value.toLocaleString()
      } else if (value % 1 !== 0) {
        return value.toFixed(1)
      } else {
        return value.toString()
      }
    }

    return value?.toString() || 'N/A'
  }

  /**
   * Convert string to title case
   */
  toTitleCase (str) {
    if (!str) return ''
    return str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
  }

  /**
   * Update tooltip position based on mouse cursor
   */
  updateTooltipPosition () {
    if (!this.isVisible || !this.tooltipElement) return

    const tooltip = this.tooltipElement
    const mouseX = this.mousePosition.x
    const mouseY = this.mousePosition.y

    // Get tooltip dimensions
    const tooltipRect = tooltip.getBoundingClientRect()
    const tooltipWidth = tooltipRect.width
    const tooltipHeight = tooltipRect.height

    // Get viewport dimensions
    const viewportWidth = window.innerWidth
    const viewportHeight = window.innerHeight

    // Calculate position with offset from cursor
    let left = mouseX + 15
    let top = mouseY - tooltipHeight - 15

    // Adjust if tooltip would go off screen
    if (left + tooltipWidth > viewportWidth) {
      left = mouseX - tooltipWidth - 15
    }

    if (top < 0) {
      top = mouseY + 15
    }

    // Ensure tooltip stays within viewport
    left = Math.max(5, Math.min(left, viewportWidth - tooltipWidth - 5))
    top = Math.max(5, Math.min(top, viewportHeight - tooltipHeight - 5))

    // Apply position
    tooltip.style.left = `${left}px`
    tooltip.style.top = `${top}px`
  }

  /**
   * Check if tooltip is currently visible
   */
  isTooltipVisible () {
    return this.isVisible
  }

  /**
   * Get current tooltip data
   */
  getCurrentData () {
    return this.currentData
  }

  /**
   * Clean up resources
   */
  destroy () {
    if (this.tooltipElement && this.tooltipElement.parentNode) {
      this.tooltipElement.parentNode.removeChild(this.tooltipElement)
    }

    // Remove event listeners
    document.removeEventListener('mousemove', this.updateTooltipPosition)

    this.tooltipElement = null
    this.currentData = null
    this.isVisible = false

    console.log('[Tooltip] Destroyed')
  }
} 