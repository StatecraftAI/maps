/**
 * CoordinateDisplay Feature Module
 * Handles coordinate display on hover and click with copy functionality
 */

import { EventBus } from '../core/EventBus.js'
import { StateManager } from '../core/StateManager.js'

export class CoordinateDisplay {
  constructor (mapManager) {
    this.mapManager = mapManager
    this.map = mapManager.getMap()
    this.coordinateInfoBox = null
    this.isEnabled = false

    this.init()
  }

  init () {
    this.setupEventListeners()
    this.connectToUI()
  }

  setupEventListeners () {
    // Listen for coordinate display toggle events
    EventBus.on('FEATURE_COORDINATES_TOGGLED', (data) => {
      this.handleToggle(data.enabled)
    })

    // Listen for state changes
    EventBus.on('STATE_CHANGED', (data) => {
      if (data.key === 'coordinateDisplay') {
        this.handleToggle(data.value)
      }
    })
  }

  connectToUI () {
    // Find and connect the coordinate display button
    this.coordinateButton = document.getElementById('coordinates-btn')
    if (this.coordinateButton) {
      // Remove any existing onclick handlers
      this.coordinateButton.removeAttribute('onclick')
      this.coordinateButton.addEventListener('click', () => this.toggleCoordinateDisplay())
    }
  }

  toggleCoordinateDisplay () {
    const newState = !StateManager.getState('coordinateDisplay')
    StateManager.setState('coordinateDisplay', newState)

    // Update button appearance
    if (this.coordinateButton) {
      if (newState) {
        this.coordinateButton.classList.add('active')
        this.coordinateButton.textContent = '🔢 Coordinates ON'
      } else {
        this.coordinateButton.classList.remove('active')
        this.coordinateButton.textContent = '🔢 Show Coordinates'
      }
    }

    // Emit event for other components
    EventBus.emit('FEATURE_COORDINATES_TOGGLED', {
      enabled: newState,
      context: 'user-toggle'
    })
  }

  handleToggle (enabled) {
    if (enabled && !this.isEnabled) {
      this.enableCoordinateDisplay()
    } else if (!enabled && this.isEnabled) {
      this.disableCoordinateDisplay()
    }
  }

  enableCoordinateDisplay () {
    if (this.isEnabled) return

    this.isEnabled = true

    // Create coordinate info box
    this.coordinateInfoBox = L.control({ position: 'topright' })
    this.coordinateInfoBox.onAdd = () => {
      const div = L.DomUtil.create('div', 'coordinate-info')
      div.style.cssText = `
        background: var(--color-surface);
        border: 1px solid var(--color-border);
        border-radius: var(--border-radius);
        padding: var(--space-3);
        font-size: var(--font-size-sm);
        font-family: monospace;
        box-shadow: var(--shadow);
        min-width: 180px;
        z-index: 1002;
      `
      div.innerHTML = '<strong>Coordinates</strong><br>Move mouse to see coordinates'
      return div
    }
    this.coordinateInfoBox.addTo(this.map)

    // Add mouse move and click listeners
    this.map.on('mousemove', this.updateCoordinateDisplay.bind(this))
    this.map.on('click', this.showCoordinatePopup.bind(this))

    console.log('Coordinate display enabled')
  }

  disableCoordinateDisplay () {
    if (!this.isEnabled) return

    this.isEnabled = false

    // Remove coordinate info box
    if (this.coordinateInfoBox) {
      this.map.removeControl(this.coordinateInfoBox)
      this.coordinateInfoBox = null
    }

    // Remove event listeners
    this.map.off('mousemove', this.updateCoordinateDisplay.bind(this))
    this.map.off('click', this.showCoordinatePopup.bind(this))

    console.log('Coordinate display disabled')
  }

  updateCoordinateDisplay (e) {
    if (this.coordinateInfoBox && this.coordinateInfoBox._container) {
      this.coordinateInfoBox._container.innerHTML = `
        <strong>Coordinates</strong><br>
        Lat: ${e.latlng.lat.toFixed(6)}<br>
        Lng: ${e.latlng.lng.toFixed(6)}<br>
        <small>Click to copy</small>
      `
    }
  }

  showCoordinatePopup (e) {
    if (!this.isEnabled) return

    const coordText = `${e.latlng.lat.toFixed(6)}, ${e.latlng.lng.toFixed(6)}`

    // Create popup content with copy button
    const popupContent = document.createElement('div')
    popupContent.style.textAlign = 'center'
    popupContent.innerHTML = `
      <strong>Coordinates</strong><br>
      <span style="font-family: monospace;">${coordText}</span><br>
    `

    const copyButton = document.createElement('button')
    copyButton.textContent = '📋 Copy'
    copyButton.style.cssText = `
      margin-top: var(--space-2);
      padding: var(--space-1) var(--space-2);
      border: 1px solid var(--color-border);
      border-radius: var(--border-radius);
      background: var(--color-surface);
      cursor: pointer;
    `
    copyButton.addEventListener('click', () => this.copyToClipboard(coordText))

    popupContent.appendChild(copyButton)

    L.popup()
      .setLatLng(e.latlng)
      .setContent(popupContent)
      .openOn(this.map)
  }

  copyToClipboard (text) {
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(() => {
        this.showCopySuccess()
      }).catch(() => {
        this.fallbackCopyToClipboard(text)
      })
    } else {
      this.fallbackCopyToClipboard(text)
    }
  }

  fallbackCopyToClipboard (text) {
    // Fallback for older browsers or non-secure contexts
    const textArea = document.createElement('textarea')
    textArea.value = text
    textArea.style.position = 'fixed'
    textArea.style.left = '-999999px'
    textArea.style.top = '-999999px'
    document.body.appendChild(textArea)
    textArea.focus()
    textArea.select()

    try {
      document.execCommand('copy')
      this.showCopySuccess()
    } catch (err) {
      console.error('Failed to copy coordinates:', err)
      alert('Failed to copy coordinates to clipboard')
    }

    document.body.removeChild(textArea)
  }

  showCopySuccess () {
    // Show a brief success message
    const successDiv = document.createElement('div')
    successDiv.textContent = 'Coordinates copied!'
    successDiv.style.cssText = `
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      background: var(--color-success);
      color: white;
      padding: var(--space-3) var(--space-4);
      border-radius: var(--border-radius);
      box-shadow: var(--shadow-lg);
      z-index: 10000;
      font-size: var(--font-size-sm);
      font-weight: 500;
    `

    document.body.appendChild(successDiv)

    // Remove after 2 seconds
    setTimeout(() => {
      if (successDiv.parentNode) {
        successDiv.parentNode.removeChild(successDiv)
      }
    }, 2000)
  }

  // Public API
  isCoordinateDisplayEnabled () {
    return this.isEnabled
  }

  // Cleanup method
  destroy () {
    this.disableCoordinateDisplay()

    if (this.coordinateButton) {
      this.coordinateButton.removeEventListener('click', this.toggleCoordinateDisplay)
    }

    EventBus.off('FEATURE_COORDINATES_TOGGLED')
    EventBus.off('STATE_CHANGED')
  }
}
