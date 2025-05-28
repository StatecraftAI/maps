/**
 * ControlPanel - Left Sidebar Map Controls
 *
 * Manages the control panel UI including:
 * - Dataset selection
 * - Layer selection with custom dropdown
 * - PPS zone filtering
 * - Opacity controls
 * - Range controls
 * - Base map selection
 * - Accordion sections (Location/Search, Advanced Features, School Overlays)
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'
import { SELECTORS } from '../config/constants.js'
import { Accordion } from './Accordion.js'
import { LayerSelector } from './LayerSelector.js'

export class ControlPanel {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // UI component references - now managed by ComponentOrchestrator
    // Note: accordion and layerSelector are now managed by ComponentOrchestrator

    // DOM element references
    this.container = null
    this.datasetSelect = null
    this.ppsFilter = null
    this.opacitySlider = null
    this.basemapSelect = null
    this.rangeControls = null
    this.layerHelpBtn = null

    // State tracking
    this.isInitialized = false

    console.log('[ControlPanel] Initialized')
  }

  /**
     * Initialize the control panel
     */
  initialize () {
    if (this.isInitialized) {
      console.warn('[ControlPanel] Already initialized')
      return
    }

    try {
      this.findDOMElements()
      this.initializeSubComponents()
      this.setupEventListeners()
      this.restoreState()

      this.isInitialized = true
      console.log('[ControlPanel] Successfully initialized')

      this.eventBus.emit('ui:controlPanelReady')
    } catch (error) {
      console.error('[ControlPanel] Failed to initialize:', error)
      this.eventBus.emit('ui:error', {
        component: 'ControlPanel',
        error: error.message
      })
    }
  }

  /**
     * Find and cache DOM element references
     */
  findDOMElements () {
    this.container = document.querySelector('.control-panel')
    if (!this.container) {
      throw new Error('Control panel container not found')
    }

    // Find form elements
    this.datasetSelect = document.getElementById('dataset-select')
    this.ppsFilter = document.getElementById('pps-only')
    this.opacitySlider = document.getElementById('opacity-slider')
    this.basemapSelect = document.getElementById('basemap-select')
    this.rangeControls = document.getElementById('range-control')
    this.layerHelpBtn = document.getElementById('layer-help-btn')

    // Validate required elements
    const requiredElements = {
      datasetSelect: this.datasetSelect,
      ppsFilter: this.ppsFilter,
      opacitySlider: this.opacitySlider,
      basemapSelect: this.basemapSelect
    }

    for (const [name, element] of Object.entries(requiredElements)) {
      if (!element) {
        throw new Error(`Required element not found: ${name}`)
      }
    }

    console.log('[ControlPanel] Found all required DOM elements')
  }

  /**
     * Initialize sub-components
     */
  initializeSubComponents () {
    // Note: Both Accordion and LayerSelector are now created by ComponentOrchestrator
    // to avoid duplicate instances and EventBus conflicts

    console.log('[ControlPanel] Sub-components initialized')
  }

  /**
     * Set up event listeners for all controls
     */
  setupEventListeners () {
    // Main form control event listeners
    this.setupFormControlListeners()

    // Range controls
    this.setupRangeControls()

    // Layer help button
    if (this.layerHelpBtn) {
      this.layerHelpBtn.addEventListener('click', () => {
        this.toggleLayerHelp()
      })
    }

    // Subscribe to StateManager for changes in controlled state
    this.stateManager.subscribe(['currentDataset', 'currentField', 'mapOpacity', 'showPpsOnly', 'customRange', 'basemap'], (stateChanges) => {
      this.updateFromState(stateChanges)
      // Trigger specific UI updates based on certain state changes if needed
      if (stateChanges.hasOwnProperty('currentField')) {
        this.updateRangeControlsVisibility(stateChanges.currentField)
      }
      if (stateChanges.hasOwnProperty('customRange')) {
        this.updateRangeDisplayValues(this.stateManager.getState('currentField')) // Update display when range changes
      }
    })

    // Listen for external events that populate options or trigger specific UI updates
    this.eventBus.on('data:discoveryComplete', (data) => {
      console.log('[ControlPanel] Received data:discoveryComplete event:', data)
      console.log('[ControlPanel] Datasets to populate:', Object.keys(data.datasets || {}))
      this.populateDatasetOptions(data.datasets)
      // Also update from state after populating options, in case state was set by URL before discovery
      this.updateFromState(this.stateManager.getState())
    })

    this.eventBus.on('data:loaded', (data) => {
      if (data.type === 'election') {
        // Request layer options update from LayerSelector
        this.eventBus.emit('ui:layerOptionsUpdateRequested')
      }
    })

    console.log('[ControlPanel] Event listeners set up')
  }

  /**
   * Set up event listeners for main form controls
   */
  setupFormControlListeners () {
    // Dataset selector
    if (this.datasetSelect) {
      this.datasetSelect.addEventListener('change', (e) => {
        const selectedDataset = e.target.value
        console.log('[ControlPanel] Dataset changed to:', selectedDataset)
        this.stateManager.setState({ currentDataset: selectedDataset }, { source: 'ControlPanel' })
        this.eventBus.emit('ui:datasetChanged', { dataset: selectedDataset })
      })
    }

    // PPS filter checkbox
    if (this.ppsFilter) {
      this.ppsFilter.addEventListener('change', (e) => {
        const showPpsOnly = e.target.checked
        console.log('[ControlPanel] PPS filter changed to:', showPpsOnly)
        this.stateManager.setState({ showPpsOnly }, { source: 'ControlPanel' })
        this.eventBus.emit('ui:ppsFilterChanged', { showPpsOnly })
      })
    }

    // Opacity slider
    if (this.opacitySlider) {
      this.opacitySlider.addEventListener('input', (e) => {
        const opacity = parseFloat(e.target.value)
        console.log('[ControlPanel] Opacity changed to:', opacity)
        this.stateManager.setState({ mapOpacity: opacity }, { source: 'ControlPanel' })
        this.eventBus.emit('ui:opacityChanged', { opacity })

        // Update opacity display
        const opacityValue = document.getElementById('opacity-value')
        if (opacityValue) {
          opacityValue.textContent = Math.round(opacity * 100) + '%'
        }
      })
    }

    // Basemap selector
    if (this.basemapSelect) {
      this.basemapSelect.addEventListener('change', (e) => {
        const selectedBasemap = e.target.value
        console.log('[ControlPanel] Basemap changed to:', selectedBasemap)
        this.stateManager.setState({ basemap: selectedBasemap }, { source: 'ControlPanel' })
        this.eventBus.emit('ui:basemapChanged', { basemap: selectedBasemap })
      })
    }

    console.log('[ControlPanel] Form control listeners set up')
  }

  /**
     * Set up range control event listeners
     */
  setupRangeControls () {
    const rangeMin = document.getElementById('range-min')
    const rangeMax = document.getElementById('range-max')
    const resetRangeBtn = document.getElementById('reset-range-btn')

    if (rangeMin && rangeMax) {
      rangeMin.addEventListener('change', () => {
        this.handleRangeChange()
      })

      rangeMax.addEventListener('change', () => {
        this.handleRangeChange()
      })
    }

    if (resetRangeBtn) {
      resetRangeBtn.addEventListener('click', () => {
        this.resetRange()
      })
    }
  }

  /**
     * Handle range control changes
     */
  handleRangeChange () {
    const rangeMin = document.getElementById('range-min')
    const rangeMax = document.getElementById('range-max')
    const currentField = this.stateManager.getState('currentField')

    if (!rangeMin || !rangeMax || !currentField) return

    const min = parseFloat(rangeMin.value)
    const max = parseFloat(rangeMax.value)

    if (!isNaN(min) && !isNaN(max) && min < max) {
      const customRange = { field: currentField, min, max }

      this.stateManager.setState({ customRange })

      this.eventBus.emit('ui:rangeChanged', { customRange })

      console.log('[ControlPanel] Custom range set:', customRange)
    }
  }

  /**
     * Reset range to auto-calculated values
     */
  resetRange () {
    this.stateManager.setState({ customRange: null })

    this.eventBus.emit('ui:rangeReset')

    console.log('[ControlPanel] Range reset to auto')
  }

  /**
     * Toggle layer help display
     */
  toggleLayerHelp () {
    const explanationDiv = document.getElementById('layer-explanation')
    const currentField = this.stateManager.getState('currentField')

    if (!explanationDiv) return

    const isVisible = explanationDiv.style.display !== 'none'

    if (isVisible) {
      explanationDiv.style.display = 'none'
    } else {
      // Show explanation for current field
      const explanation = this.getLayerExplanation(currentField)
      explanationDiv.innerHTML = explanation
      explanationDiv.style.display = 'block'
    }
  }

  /**
   * Get explanation text for a data layer
   */
  getLayerExplanation (fieldKey) {
    const explanations = {
      none: 'Shows only the base map with precinct boundaries and no data overlay.',

      political_lean: 'Shows the political lean of each precinct based on historical voting patterns. Ranges from Strong Democratic to Strong Republican.',

      competitiveness: 'Indicates how competitive each precinct is in elections. Safe seats rarely change hands, while Tossup precincts are highly competitive.',

      leading_candidate: 'Shows which candidate received the most votes in each precinct. Colors correspond to each candidate.',

      turnout_rate: 'Percentage of registered voters who cast ballots. Higher percentages indicate greater civic engagement.',

      turnout_quartile: 'Precincts grouped into quartiles (Low, Med-Low, Medium, Med-High, High) based on voter turnout rates.',

      margin_category: 'Victory margin categories: Very Close (0-5%), Close (5-10%), Clear (10-20%), Landslide (20%+).',

      precinct_size_category: 'Precincts categorized by number of registered voters: Small (<500), Medium (500-1000), Large (1000-2000), Extra Large (2000+).',

      total_voters: 'Total number of registered voters in each precinct.',

      votes_total: 'Total number of votes cast in each precinct.',

      dem_advantage: 'Democratic advantage percentage - positive values favor Democrats, negative favor Republicans.',

      vote_efficiency_dem: 'How efficiently Democratic votes are distributed - measures wasted votes and gerrymandering effects.'
    }

    // Handle candidate-specific vote fields
    if (fieldKey && fieldKey.startsWith('votes_')) {
      const candidateName = fieldKey.replace('votes_', '').replace(/_/g, ' ')
      return `Shows the number of votes received by ${candidateName} in each precinct. Darker colors indicate more votes.`
    }

    if (fieldKey && fieldKey.startsWith('vote_pct_')) {
      const candidateName = fieldKey.replace('vote_pct_', '').replace(/_/g, ' ')
      return `Shows the percentage of votes received by ${candidateName} in each precinct. Darker colors indicate higher vote percentages.`
    }

    return explanations[fieldKey] || `Data layer showing ${fieldKey ? fieldKey.replace(/_/g, ' ') : 'selected information'} for each precinct.`
  }

  /**
   * Populate dataset dropdown options
   */
  populateDatasetOptions (datasets) {
    console.log('[ControlPanel] populateDatasetOptions called with:', datasets)
    console.log('[ControlPanel] datasetSelect element:', this.datasetSelect)

    if (!this.datasetSelect) {
      console.warn('[ControlPanel] datasetSelect element not found!')
      return
    }

    this.datasetSelect.innerHTML = ''

    // Add "No Data" option first
    const noDataOption = document.createElement('option')
    noDataOption.value = 'none'
    noDataOption.textContent = 'No Data - Base Map Only'
    this.datasetSelect.appendChild(noDataOption)

    // Add separator
    const separator = document.createElement('option')
    separator.disabled = true
    separator.textContent = '──────────────────────'
    this.datasetSelect.appendChild(separator)

    // Add actual datasets
    Object.entries(datasets).forEach(([key, config]) => {
      const option = document.createElement('option')
      option.value = key
      option.textContent = config.title
      this.datasetSelect.appendChild(option)
    })

    console.log(`[ControlPanel] Populated ${Object.keys(datasets).length + 1} dataset options (including No Data)`)
  }

  /**
     * Update range controls visibility based on current layer
     */
  updateRangeControlsVisibility (layerKey) {
    if (!this.rangeControls) return

    // Get current data state to determine if layer is numeric
    const electionData = this.stateManager.getState('electionData')
    const colorSchemes = this.stateManager.getState('colorSchemes')

    // Hide range controls for "none" layer or categorical fields
    if (layerKey === 'none' || (colorSchemes && colorSchemes[layerKey])) {
      this.rangeControls.style.display = 'none'
    } else {
      this.rangeControls.style.display = 'block'
      this.updateRangeDisplayValues(layerKey)
    }
  }

  /**
     * Update range control display values
     */
  updateRangeDisplayValues (layerKey) {
    const rangeMin = document.getElementById('range-min')
    const rangeMax = document.getElementById('range-max')
    const rangeDisplay = document.getElementById('range-display')

    if (!rangeMin || !rangeMax || !rangeDisplay) return

    const dataRanges = this.stateManager.getState('actualDataRanges')
    const customRange = this.stateManager.getState('customRange')

    let range
    if (customRange && customRange.field === layerKey) {
      range = customRange
    } else if (dataRanges && dataRanges[layerKey]) {
      range = dataRanges[layerKey]
    }

    if (range) {
      rangeMin.value = range.min.toFixed(1)
      rangeMax.value = range.max.toFixed(1)
      rangeDisplay.textContent = `Current: ${range.min.toFixed(1)} - ${range.max.toFixed(1)}`
    }
  }

  /**
     * Restore state from StateManager
     */
  restoreState () {
    // Restore dataset selection
    const currentDataset = this.stateManager.getState('currentDataset')
    if (currentDataset && this.datasetSelect) {
      this.datasetSelect.value = currentDataset
    }

    // Restore PPS filter
    const showPpsOnly = this.stateManager.getState('showPpsOnly')
    if (this.ppsFilter) {
      this.ppsFilter.checked = showPpsOnly
    }

    // Restore opacity
    const mapOpacity = this.stateManager.getState('mapOpacity') || 0.7
    if (this.opacitySlider) {
      this.opacitySlider.value = mapOpacity
    }

    // Restore basemap
    const currentBasemap = this.stateManager.getState('currentBasemap') || 'streets'
    if (this.basemapSelect) {
      this.basemapSelect.value = currentBasemap
    }

    console.log('[ControlPanel] State restored')
  }

  /**
     * Update control values from external state changes
     */
  updateFromState (stateChanges) {
    Object.keys(stateChanges).forEach(key => {
      switch (key) {
        case 'currentDataset':
          if (this.datasetSelect && this.datasetSelect.value !== stateChanges[key]) {
            this.datasetSelect.value = stateChanges[key]
          }
          break

        case 'showPpsOnly':
          if (this.ppsFilter && this.ppsFilter.checked !== stateChanges[key]) {
            this.ppsFilter.checked = stateChanges[key]
          }
          break

        case 'mapOpacity':
          if (this.opacitySlider && parseFloat(this.opacitySlider.value) !== stateChanges[key]) {
            this.opacitySlider.value = stateChanges[key]
            // Manually update output text as handleOpacityChange is no longer called directly
            const opacityValue = document.getElementById('opacity-value')
            if (opacityValue) {
              opacityValue.textContent = Math.round(stateChanges[key] * 100) + '%'
            }
          }
          break

        case 'basemap':
          if (this.basemapSelect && this.basemapSelect.value !== stateChanges[key]) {
            this.basemapSelect.value = stateChanges[key]
          }
          break

        case 'currentField':
          // This is handled by LayerSelector
          break

        case 'customRange':
          // This is handled by updateRangeDisplayValues triggered by subscription callback
          break
      }
    })
  }

  /**
     * Enable/disable controls
     */
  setEnabled (enabled) {
    const controls = [
      this.datasetSelect,
      this.ppsFilter,
      this.opacitySlider,
      this.basemapSelect
    ]

    controls.forEach(control => {
      if (control) {
        control.disabled = !enabled
      }
    })

    // Emit event for LayerSelector to handle instead of direct method call
    this.eventBus.emit('ui:layerSelectorEnabledChanged', { enabled })
  }

  /**
     * Show loading state
     */
  showLoading (message = 'Loading...') {
    this.setEnabled(false)

    // You could add a loading indicator here
    console.log(`[ControlPanel] Loading: ${message}`)
  }

  /**
     * Hide loading state
     */
  hideLoading () {
    this.setEnabled(true)

    console.log('[ControlPanel] Loading complete')
  }

  /**
     * Get current control values
     */
  getValues () {
    return {
      dataset: this.datasetSelect?.value,
      showPpsOnly: this.ppsFilter?.checked,
      mapOpacity: parseFloat(this.opacitySlider?.value || 0.7),
      basemap: this.basemapSelect?.value
    }
  }

  /**
     * Validate control values
     */
  validate () {
    const values = this.getValues()
    const issues = []

    if (!values.dataset) {
      issues.push('No dataset selected')
    }

    if (isNaN(values.mapOpacity) || values.mapOpacity < 0 || values.mapOpacity > 1) {
      issues.push('Invalid opacity value')
    }

    return {
      isValid: issues.length === 0,
      issues
    }
  }

  /**
     * Clean up and destroy component
     */
  destroy () {
    // Remove event listeners
    if (this.datasetSelect) {
      this.datasetSelect.removeEventListener('change', this.handleDatasetChange)
    }

    // ComponentOrchestrator will handle sub-component cleanup
    // No need to call destroy on accordion or layerSelector

    this.isInitialized = false

    console.log('[ControlPanel] Destroyed')
  }
}
