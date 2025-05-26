/**
 * Export - Map Image Export
 *
 * Handles:
 * - Map export as PNG images using dom-to-image
 * - UI element hiding/showing for clean exports
 * - Filename generation with current state
 * - Export quality and format options
 * - Error handling and fallback strategies
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Export {
  constructor (stateManager, eventBus, mapManager) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager

    // Export configuration
    this.exportOptions = {
      quality: 0.95,
      format: 'png',
      maxWidth: 3840, // 4K width
      maxHeight: 2160, // 4K height
      backgroundColor: '#ffffff',
      cacheBust: true
    }

    // UI elements to hide during export
    this.elementsToHide = [
      '.control-panel',
      '.info-panel',
      '.leaflet-control-zoom',
      '.leaflet-control-fullscreen',
      '#color-scale-legend'
    ]

    this.isExporting = false

    this.initializeElements()
    this.setupEventListeners()

    console.log('[Export] Initialized')
  }

  /**
     * Initialize DOM elements
     */
  initializeElements () {
    this.exportButton = document.querySelector('[onclick="exportMapImage()"]')

    // Replace inline onclick handler
    if (this.exportButton) {
      this.exportButton.removeAttribute('onclick')
      this.exportButton.addEventListener('click', () => this.exportMapImage())
    }

    // Check if dom-to-image is available
    this.domToImageAvailable = typeof window.domtoimage !== 'undefined'
    if (!this.domToImageAvailable) {
      console.warn('[Export] dom-to-image library not found. Export functionality may be limited.')
    }
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    // Listen for export requests from other components
    this.eventBus.on('export:requestMapImage', (data) => {
      this.exportMapImage(data.options)
    })

    // Listen for export configuration changes
    this.eventBus.on('export:configChanged', (data) => {
      this.updateExportOptions(data.options)
    })
  }

  /**
     * Export map as image
     */
  async exportMapImage (customOptions = {}) {
    if (this.isExporting) {
      console.warn('[Export] Export already in progress')
      return
    }

    if (!this.domToImageAvailable) {
      this.showExportError('Export library not available. Please check that dom-to-image is loaded.')
      return
    }

    try {
      this.setExportingState(true)
      console.log('[Export] Starting map image export...')

      const options = { ...this.exportOptions, ...customOptions }
      const filename = this.generateFilename()

      // Hide UI elements for clean export
      const hiddenElements = this.hideUIElements()

      // Wait for tiles to load
      await this.waitForTilesToLoad()

      // Export the map
      const dataUrl = await this.captureMapImage(options)

      // Restore UI elements
      this.showUIElements(hiddenElements)

      // Download the image
      this.downloadImage(dataUrl, filename)

      this.eventBus.emit('export:success', {
        filename,
        size: dataUrl.length,
        format: options.format
      })

      console.log(`[Export] Export completed: ${filename}`)
    } catch (error) {
      console.error('[Export] Export failed:', error)
      this.showExportError(`Failed to export map image: ${error.message}`)

      this.eventBus.emit('export:error', {
        error: error.message,
        context: 'exportMapImage'
      })
    } finally {
      this.setExportingState(false)
    }
  }

  /**
     * Capture map image using dom-to-image
     */
  async captureMapImage (options) {
    const mapElement = document.getElementById('map')
    if (!mapElement) {
      throw new Error('Map element not found')
    }

    const captureOptions = {
      quality: options.quality,
      width: Math.min(mapElement.offsetWidth, options.maxWidth),
      height: Math.min(mapElement.offsetHeight, options.maxHeight),
      backgroundColor: options.backgroundColor,
      cacheBust: options.cacheBust,
      style: {
        transform: 'scale(1)',
        transformOrigin: 'top left'
      }
    }

    console.log('[Export] Capturing image with options:', captureOptions)

    // Use dom-to-image to capture the map
    return await window.domtoimage.toPng(mapElement, captureOptions)
  }

  /**
     * Hide UI elements for clean export
     */
  hideUIElements () {
    const hiddenElements = []

    this.elementsToHide.forEach(selector => {
      const elements = document.querySelectorAll(selector)
      elements.forEach(element => {
        if (element && element.style.display !== 'none') {
          hiddenElements.push({
            element,
            originalDisplay: element.style.display
          })
          element.style.display = 'none'
        }
      })
    })

    console.log(`[Export] Hid ${hiddenElements.length} UI elements for export`)
    return hiddenElements
  }

  /**
     * Show UI elements after export
     */
  showUIElements (hiddenElements) {
    hiddenElements.forEach(({ element, originalDisplay }) => {
      element.style.display = originalDisplay
    })

    console.log(`[Export] Restored ${hiddenElements.length} UI elements after export`)
  }

  /**
     * Wait for map tiles to load
     */
  async waitForTilesToLoad (timeout = 2000) {
    return new Promise((resolve) => {
      // Simple timeout-based approach
      // In a more sophisticated version, you could check tile loading status
      setTimeout(() => {
        console.log('[Export] Waited for tiles to load')
        resolve()
      }, timeout)
    })
  }

  /**
     * Generate filename based on current state
     */
  generateFilename () {
    const currentField = this.stateManager.getState('currentField') || 'map'
    const currentDataset = this.stateManager.getState('currentDataset') || 'data'
    const timestamp = new Date().toISOString().slice(0, 10) // YYYY-MM-DD

    // Clean field name for filename
    const cleanField = currentField
      .replace(/[^a-z0-9_]/gi, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '')

    const cleanDataset = currentDataset
      .replace(/[^a-z0-9_]/gi, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '')

    return `election_map_${cleanDataset}_${cleanField}_${timestamp}.png`
  }

  /**
     * Download image file
     */
  downloadImage (dataUrl, filename) {
    try {
      const link = document.createElement('a')
      link.href = dataUrl
      link.download = filename

      // Temporarily add to DOM for download
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)

      console.log(`[Export] Downloaded image: ${filename}`)
    } catch (error) {
      console.error('[Export] Download failed:', error)
      this.showFallbackDownload(dataUrl, filename)
    }
  }

  /**
     * Show fallback download option
     */
  showFallbackDownload (dataUrl, filename) {
    const dialog = document.createElement('div')
    dialog.style.cssText = `
            position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
            background: var(--color-surface); border: 1px solid var(--color-border);
            border-radius: var(--border-radius); padding: var(--space-6);
            box-shadow: var(--shadow-lg); z-index: 10000; max-width: 90vw;
            font-family: var(--font-family);
        `

    dialog.innerHTML = `
            <h3 style="margin: 0 0 var(--space-4) 0;">Export Complete</h3>
            <p style="margin: 0 0 var(--space-4) 0;">Right-click the image below and select "Save As..." to download:</p>
            <div style="text-align: center; margin-bottom: var(--space-4);">
                <img src="${dataUrl}" alt="Exported map" style="max-width: 100%; max-height: 300px; border: 1px solid var(--color-border); border-radius: var(--border-radius);">
            </div>
            <div style="text-align: center;">
                <button onclick="this.parentElement.parentElement.remove()"
                        style="padding: var(--space-2) var(--space-4); background: var(--color-primary);
                               color: white; border: none; border-radius: var(--border-radius); cursor: pointer;">
                    Close
                </button>
            </div>
        `

    document.body.appendChild(dialog)
  }

  /**
     * Set exporting state
     */
  setExportingState (isExporting) {
    this.isExporting = isExporting

    // Update button state
    if (this.exportButton) {
      this.exportButton.disabled = isExporting
      this.exportButton.textContent = isExporting
        ? '📸 Exporting...'
        : '📸 Export Image'
    }

    // Update cursor for map element
    const mapElement = document.getElementById('map')
    if (mapElement) {
      mapElement.style.cursor = isExporting ? 'wait' : ''
    }
  }

  /**
     * Show export error
     */
  showExportError (message) {
    alert(`Export Error: ${message}`)
    console.error('[Export]', message)
  }

  /**
     * Update export options
     */
  updateExportOptions (newOptions) {
    this.exportOptions = { ...this.exportOptions, ...newOptions }
    console.log('[Export] Updated export options:', this.exportOptions)
  }

  /**
     * Export with specific format
     */
  async exportAs (format, customOptions = {}) {
    const options = {
      ...customOptions,
      format
    }

    return await this.exportMapImage(options)
  }

  /**
     * Export as PNG (default)
     */
  async exportAsPNG (options = {}) {
    return await this.exportAs('png', options)
  }

  /**
     * Export as JPEG
     */
  async exportAsJPEG (options = {}) {
    if (!this.domToImageAvailable) {
      this.showExportError('JPEG export requires dom-to-image library')
      return
    }

    try {
      this.setExportingState(true)

      const mapElement = document.getElementById('map')
      if (!mapElement) {
        throw new Error('Map element not found')
      }

      const hiddenElements = this.hideUIElements()
      await this.waitForTilesToLoad()

      const dataUrl = await window.domtoimage.toJpeg(mapElement, {
        quality: options.quality || 0.9,
        backgroundColor: options.backgroundColor || '#ffffff'
      })

      this.showUIElements(hiddenElements)

      const filename = this.generateFilename().replace('.png', '.jpg')
      this.downloadImage(dataUrl, filename)

      console.log(`[Export] JPEG export completed: ${filename}`)
    } catch (error) {
      console.error('[Export] JPEG export failed:', error)
      this.showExportError(`Failed to export as JPEG: ${error.message}`)
    } finally {
      this.setExportingState(false)
    }
  }

  /**
     * Export with custom dimensions
     */
  async exportWithDimensions (width, height, options = {}) {
    const customOptions = {
      ...options,
      maxWidth: width,
      maxHeight: height
    }

    return await this.exportMapImage(customOptions)
  }

  /**
     * Export high quality version
     */
  async exportHighQuality () {
    const highQualityOptions = {
      quality: 1.0,
      maxWidth: 3840,
      maxHeight: 2160,
      cacheBust: true
    }

    return await this.exportMapImage(highQualityOptions)
  }

  /**
     * Get export capabilities
     */
  getExportCapabilities () {
    return {
      domToImageAvailable: this.domToImageAvailable,
      supportedFormats: this.domToImageAvailable ? ['png', 'jpeg'] : [],
      maxDimensions: {
        width: this.exportOptions.maxWidth,
        height: this.exportOptions.maxHeight
      },
      defaultQuality: this.exportOptions.quality
    }
  }

  /**
     * Get current export state
     */
  getExportState () {
    return {
      isExporting: this.isExporting,
      options: { ...this.exportOptions },
      capabilities: this.getExportCapabilities()
    }
  }

  /**
     * Validate export prerequisites
     */
  validateExportPrerequisites () {
    const issues = []

    if (!this.domToImageAvailable) {
      issues.push('dom-to-image library not loaded')
    }

    const mapElement = document.getElementById('map')
    if (!mapElement) {
      issues.push('Map element not found')
    }

    const map = this.mapManager.map
    if (!map) {
      issues.push('Map not initialized')
    }

    return {
      valid: issues.length === 0,
      issues
    }
  }

  /**
     * Pre-export checklist
     */
  async preExportChecklist () {
    const validation = this.validateExportPrerequisites()

    if (!validation.valid) {
      console.warn('[Export] Pre-export validation failed:', validation.issues)
      return false
    }

    // Additional checks could go here
    // - Check if data is loaded
    // - Check if map has content
    // - Verify network status for tile loading

    return true
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.setExportingState(false)

    // Remove any export dialogs
    const dialogs = document.querySelectorAll('[style*="position: fixed"]')
    dialogs.forEach(dialog => {
      if (dialog.textContent.includes('Export Complete')) {
        dialog.remove()
      }
    })

    console.log('[Export] Destroyed')
  }
}
