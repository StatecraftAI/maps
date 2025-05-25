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
    // Dataset selection
    this.datasetSelect.addEventListener('change', (e) => {
      this.handleDatasetChange(e.target.value)
    })

    // PPS filter toggle
    this.ppsFilter.addEventListener('change', (e) => {
      this.handlePpsFilterChange(e.target.checked)
    })

    // Opacity control
    this.opacitySlider.addEventListener('input', (e) => {
      this.handleOpacityChange(parseFloat(e.target.value))
    })

    // Base map selection
    this.basemapSelect.addEventListener('change', (e) => {
      this.handleBasemapChange(e.target.value)
    })

    // Range controls
    this.setupRangeControls()

    // Layer help button
    if (this.layerHelpBtn) {
      this.layerHelpBtn.addEventListener('click', () => {
        this.toggleLayerHelp()
      })
    }

    // Listen for external state changes
    this.eventBus.on('data:discoveryComplete', (data) => {
      this.populateDatasetOptions(data.datasets)
    })

    this.eventBus.on('data:loaded', (data) => {
      if (data.type === 'election') {
        this.updateLayerOptions()
      }
    })

    this.eventBus.on('map:layerChanged', (data) => {
      this.updateRangeControlsVisibility(data.layerKey)
    })

    console.log('[ControlPanel] Event listeners set up')
  }

  /**
     * Set up range control event listeners
     */
  setupRangeControls () {
    const rangeMin = document.getElementById('range-min')
    const rangeMax = document.getElementById('range-max')

    if (rangeMin && rangeMax) {
      rangeMin.addEventListener('change', () => {
        this.handleRangeChange()
      })

      rangeMax.addEventListener('change', () => {
        this.handleRangeChange()
      })
    }
  }

  /**
     * Handle dataset selection change
     */
  handleDatasetChange (datasetKey) {
    console.log(`[ControlPanel] Dataset changed to: ${datasetKey}`)

    this.stateManager.setState({
      currentDataset: datasetKey,
      customRange: null // Reset custom range when changing datasets
    })

    this.eventBus.emit('ui:datasetChanged', { datasetKey })
  }

  /**
     * Handle PPS filter toggle
     */
  handlePpsFilterChange (showPpsOnly) {
    console.log(`[ControlPanel] PPS filter changed to: ${showPpsOnly}`)

    this.stateManager.setState({ showPpsOnly })

    this.eventBus.emit('ui:ppsFilterChanged', { showPpsOnly })
  }

  /**
     * Handle opacity slider change
     */
  handleOpacityChange (opacity) {
    const opacityValue = document.getElementById('opacity-value')
    if (opacityValue) {
      opacityValue.textContent = Math.round(opacity * 100) + '%'
    }

    this.stateManager.setState({ mapOpacity: opacity })

    this.eventBus.emit('ui:opacityChanged', { opacity })
  }

  /**
     * Handle base map selection change
     */
  handleBasemapChange (basemapKey) {
    console.log(`[ControlPanel] Base map changed to: ${basemapKey}`)

    this.stateManager.setState({ currentBasemap: basemapKey })

    this.eventBus.emit('ui:basemapChanged', { basemapKey })
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
      this.eventBus.emit('ui:layerHelpRequested', { fieldKey: currentField })
      explanationDiv.style.display = 'block'
    }
  }

  /**
     * Populate dataset dropdown options
     */
  populateDatasetOptions (datasets) {
    if (!this.datasetSelect) return

    this.datasetSelect.innerHTML = ''

    Object.entries(datasets).forEach(([key, config]) => {
      const option = document.createElement('option')
      option.value = key
      option.textContent = config.title
      this.datasetSelect.appendChild(option)
    })

    console.log(`[ControlPanel] Populated ${Object.keys(datasets).length} dataset options`)
  }

  /**
     * Update layer options when new data is loaded
     */
  updateLayerOptions() {
    // Emit event for LayerSelector to handle instead of direct method call
    this.eventBus.emit('ui:layerOptionsUpdateRequested');
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
      this.handleOpacityChange(mapOpacity)
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
          if (this.datasetSelect) {
            this.datasetSelect.value = stateChanges[key]
          }
          break

        case 'showPpsOnly':
          if (this.ppsFilter) {
            this.ppsFilter.checked = stateChanges[key]
          }
          break

        case 'mapOpacity':
          if (this.opacitySlider) {
            this.opacitySlider.value = stateChanges[key]
            this.handleOpacityChange(stateChanges[key])
          }
          break

        case 'currentBasemap':
          if (this.basemapSelect) {
            this.basemapSelect.value = stateChanges[key]
          }
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
    this.eventBus.emit('ui:layerSelectorEnabledChanged', { enabled });
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
      opacity: parseFloat(this.opacitySlider?.value || 0.7),
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

    if (isNaN(values.opacity) || values.opacity < 0 || values.opacity > 1) {
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
    destroy() {
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
