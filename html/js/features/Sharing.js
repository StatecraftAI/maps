/**
 * Sharing - URL and Social Media Sharing
 *
 * Handles:
 * - Map state serialization to shareable URLs
 * - Social media sharing (Twitter, Facebook, LinkedIn)
 * - URL parameter restoration
 * - Share link generation with full state preservation
 * - Clipboard integration
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Sharing {
  constructor (stateManager, eventBus, mapManager) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager

    // Social media configurations
    this.socialPlatforms = {
      twitter: {
        baseUrl: 'https://twitter.com/intent/tweet',
        params: ['text', 'url']
      },
      facebook: {
        baseUrl: 'https://www.facebook.com/sharer/sharer.php',
        params: ['u', 'quote']
      },
      linkedin: {
        baseUrl: 'https://www.linkedin.com/sharing/share-offsite/',
        params: ['url', 'title', 'summary']
      }
    }

    this.initializeElements()
    this.setupEventListeners()

    console.log('[Sharing] Initialized')
  }

  /**
     * Initialize DOM elements
     */
  initializeElements () {
    this.shareButton = document.querySelector('[onclick="shareMapView()"]')
    this.twitterButton = document.querySelector('[onclick="shareToSocial(\'twitter\')"]')
    this.facebookButton = document.querySelector('[onclick="shareToSocial(\'facebook\')"]')
    this.linkedinButton = document.querySelector('[onclick="shareToSocial(\'linkedin\')"]')

    // Replace inline onclick handlers
    if (this.shareButton) {
      this.shareButton.removeAttribute('onclick')
      this.shareButton.addEventListener('click', () => this.shareMapView())
    }

    if (this.twitterButton) {
      this.twitterButton.removeAttribute('onclick')
      this.twitterButton.addEventListener('click', () => this.shareToSocial('twitter'))
    }

    if (this.facebookButton) {
      this.facebookButton.removeAttribute('onclick')
      this.facebookButton.addEventListener('click', () => this.shareToSocial('facebook'))
    }

    if (this.linkedinButton) {
      this.linkedinButton.removeAttribute('onclick')
      this.linkedinButton.addEventListener('click', () => this.shareToSocial('linkedin'))
    }
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    // Listen for state changes that might affect sharing
    this.eventBus.on('map:layerChanged', () => {
      this.updateShareState()
    })

    this.eventBus.on('ui:layerSelected', () => {
      this.updateShareState()
    })

    this.eventBus.on('map:viewChanged', () => {
      this.updateShareState()
    })

    // Handle URL parameter restoration on page load
    this.eventBus.on('app:initialized', () => {
      this.checkAndRestoreFromUrl()
    })
  }

  /**
     * Share current map view
     */
  shareMapView () {
    try {
      const mapState = this.captureMapState()
      const shareUrl = this.generateShareUrl(mapState)

      console.log('[Sharing] Generated share URL:', shareUrl)

      // Try to copy to clipboard first
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(shareUrl).then(() => {
          this.showShareSuccess('Shareable link copied to clipboard!\n\nAnyone with this link will see the exact same map view, layer, and settings.')

          this.eventBus.emit('sharing:linkCopied', {
            url: shareUrl,
            method: 'clipboard'
          })
        }).catch((error) => {
          console.warn('[Sharing] Clipboard copy failed:', error)
          this.showShareUrlDialog(shareUrl)
        })
      } else {
        this.showShareUrlDialog(shareUrl)
      }
    } catch (error) {
      console.error('[Sharing] Failed to generate share URL:', error)
      alert('Failed to generate shareable link. Please try again.')

      this.eventBus.emit('sharing:error', {
        error: error.message,
        context: 'shareMapView'
      })
    }
  }

  /**
     * Share to social media platform
     */
  shareToSocial (platform) {
    try {
      const mapState = this.captureMapState()
      const shareUrl = this.generateShareUrl(mapState)
      const socialUrl = this.generateSocialUrl(platform, shareUrl)

      if (!socialUrl) {
        console.error('[Sharing] Unknown social platform:', platform)
        return
      }

      console.log(`[Sharing] Sharing to ${platform}:`, socialUrl)

      // Open in new window
      const popup = window.open(
        socialUrl,
                `share-${platform}`,
                'width=600,height=400,scrollbars=yes,resizable=yes'
      )

      if (!popup) {
        alert('Please allow popups to share on social media.')
      }

      this.eventBus.emit('sharing:socialShare', {
        platform,
        url: shareUrl,
        socialUrl
      })
    } catch (error) {
      console.error(`[Sharing] Failed to share to ${platform}:`, error)
      alert('Failed to open social sharing. Please try the direct share link instead.')

      this.eventBus.emit('sharing:error', {
        error: error.message,
        context: `shareToSocial:${platform}`
      })
    }
  }

  /**
     * Generate social media sharing URL
     */
  generateSocialUrl (platform, shareUrl) {
    const config = this.socialPlatforms[platform]
    if (!config) return null

    const currentField = this.stateManager.getState('currentField')
    const currentDataset = this.stateManager.getState('currentDataset')

    const title = `2025 Portland School Board Election Map - ${this.getFieldDisplayName(currentField)}`
    const description = `Interactive election results showing ${this.getFieldDisplayName(currentField)} data. Current view: ${currentDataset?.toUpperCase()} dataset.`

    const url = new URL(config.baseUrl)

    switch (platform) {
      case 'twitter':
        const twitterText = `${title}\n\n${description}`
        url.searchParams.set('text', twitterText)
        url.searchParams.set('url', shareUrl)
        break

      case 'facebook':
        url.searchParams.set('u', shareUrl)
        url.searchParams.set('quote', description)
        break

      case 'linkedin':
        url.searchParams.set('url', shareUrl)
        url.searchParams.set('title', title)
        url.searchParams.set('summary', description)
        break
    }

    return url.toString()
  }

  /**
     * Capture current map state for sharing
     */
  captureMapState () {
    const map = this.mapManager.map
    const center = map ? map.getCenter() : { lat: 45.5152, lng: -122.6784 }
    const zoom = map ? map.getZoom() : 11

    // Get current state from StateManager
    const currentField = this.stateManager.getState('currentField')
    const currentDataset = this.stateManager.getState('currentDataset')
    const showPpsOnly = this.stateManager.getState('showPpsOnly')
    const customRange = this.stateManager.getState('customRange')

    // Get UI state
    const mapOpacity = document.getElementById('opacity-slider')?.value || 0.7
    const basemap = document.getElementById('basemap-select')?.value || 'streets'

    // Get school overlay states
    const schoolOverlays = this.getSchoolOverlayStates()

    // Get feature states
    const heatmapActive = this.stateManager.getState('heatmapLayer') !== null

    const state = {
      currentDataset: this.stateManager.getState('currentDataset'),
      currentField: this.stateManager.getState('currentField'),
      opacity: this.stateManager.getState('opacity'),
      basemap: this.stateManager.getState('basemap'),
      coordinates: center,
      zoom,
      showPpsOnly,
      timestamp: Date.now()
    }

    return state
  }

  /**
     * Get school overlay states
     */
  getSchoolOverlayStates () {
    const overlayIds = [
      'high-schools', 'middle-schools', 'elementary-schools',
      'high-boundaries', 'middle-boundaries', 'elementary-boundaries',
      'district-boundary'
    ]

    const overlays = {}
    overlayIds.forEach(layerId => {
      const checkbox = document.getElementById(`show-${layerId}`)
      if (checkbox) {
        overlays[layerId] = checkbox.checked
      }
    })

    return overlays
  }

  /**
     * Generate shareable URL from map state
     */
  generateShareUrl (state) {
    const url = new URL(window.location.href.split('?')[0]) // Remove existing params

    // Add all state parameters
    url.searchParams.set('lat', state.coordinates.lat.toFixed(6))
    url.searchParams.set('lng', state.coordinates.lng.toFixed(6))
    url.searchParams.set('zoom', state.zoom.toFixed(2))

    if (state.currentDataset) {
      url.searchParams.set('dataset', state.currentDataset)
    }

    if (state.currentField) {
      url.searchParams.set('layer', state.currentField)
    }

    url.searchParams.set('pps', state.showPpsOnly ? '1' : '0')
    url.searchParams.set('opacity', state.opacity.toFixed(1))

    if (state.basemap && state.basemap !== 'streets') {
      url.searchParams.set('basemap', state.basemap)
    }

    // Custom range parameters
    if (state.customRange) {
      url.searchParams.set('rangeMin', state.customRange.min.toFixed(2))
      url.searchParams.set('rangeMax', state.customRange.max.toFixed(2))
      url.searchParams.set('rangeField', state.customRange.field)
    }

    // Heatmap state
    if (state.heatmap) {
      url.searchParams.set('heatmap', '1')
    }

    // School overlays
    const activeOverlays = Object.keys(state.schoolOverlays)
      .filter(key => state.schoolOverlays[key])
    if (activeOverlays.length > 0) {
      url.searchParams.set('overlays', activeOverlays.join(','))
    }

    return url.toString()
  }

  /**
     * Check URL parameters and restore state if present
     */
  checkAndRestoreFromUrl () {
    const params = new URLSearchParams(window.location.search)

    // Only proceed if we have URL parameters that look like map state
    if (!params.has('lat') && !params.has('dataset') && !params.has('layer')) {
      return false // No state to restore
    }

    console.log('[Sharing] Restoring map state from URL parameters...')

    try {
      const stateUpdates = this.parseUrlParameters(params)
      this.restoreMapState(stateUpdates)

      this.eventBus.emit('sharing:stateRestored', {
        parameters: Object.fromEntries(params),
        restoredState: stateUpdates
      })

      return true
    } catch (error) {
      console.error('[Sharing] Error restoring map state from URL:', error)
      this.eventBus.emit('sharing:error', {
        error: error.message,
        context: 'restoreFromUrl'
      })
      return false
    }
  }

  /**
     * Parse URL parameters into state object
     */
  parseUrlParameters (params) {
    const state = {}

    // Map view parameters
    if (params.has('lat') && params.has('lng') && params.has('zoom')) {
      state.coordinates = {
        lat: parseFloat(params.get('lat')),
        lng: parseFloat(params.get('lng')),
        zoom: parseFloat(params.get('zoom'))
      }
    }

    // Data parameters
    if (params.has('dataset')) {
      state.currentDataset = params.get('dataset')
    }

    if (params.has('layer')) {
      state.currentField = params.get('layer')
    }

    // Filter parameters
    if (params.has('pps')) {
      state.showPpsOnly = params.get('pps') === '1'
    }

    // UI parameters
    if (params.has('opacity')) {
      state.opacity = parseFloat(params.get('opacity'))
    }

    if (params.has('basemap')) {
      state.basemap = params.get('basemap')
    }

    // Custom range parameters
    if (params.has('rangeMin') && params.has('rangeMax') && params.has('rangeField')) {
      state.customRange = {
        field: params.get('rangeField'),
        min: parseFloat(params.get('rangeMin')),
        max: parseFloat(params.get('rangeMax'))
      }
    }

    // Feature parameters
    if (params.has('heatmap') && params.get('heatmap') === '1') {
      state.heatmap = true
    }

    if (params.has('overlays')) {
      state.schoolOverlays = params.get('overlays').split(',')
    }

    return state
  }

  /**
     * Restore map state from parsed parameters
     */
  restoreMapState (state) {
    // Update StateManager with restored values
    if (state.currentDataset) {
      this.stateManager.setState({ currentDataset: state.currentDataset })
      const datasetSelect = document.getElementById('dataset-select')
      if (datasetSelect) {
        datasetSelect.value = state.currentDataset
      }
    }

    if (state.currentField) {
      this.stateManager.setState({ currentField: state.currentField })
    }

    if (state.showPpsOnly !== undefined) {
      this.stateManager.setState({ showPpsOnly: state.showPpsOnly })
      const ppsCheckbox = document.getElementById('pps-only')
      if (ppsCheckbox) {
        ppsCheckbox.checked = state.showPpsOnly
      }
    }

    if (state.customRange) {
      this.stateManager.setState({ customRange: state.customRange })
    }

    // Restore UI elements
    if (state.opacity !== undefined) {
      const opacitySlider = document.getElementById('opacity-slider')
      const opacityValue = document.getElementById('opacity-value')
      if (opacitySlider) {
        opacitySlider.value = state.opacity
        if (opacityValue) {
          opacityValue.textContent = Math.round(state.opacity * 100) + '%'
        }
      }
    }

    if (state.basemap) {
      const basemapSelect = document.getElementById('basemap-select')
      if (basemapSelect) {
        basemapSelect.value = state.basemap
        // Trigger basemap change
        this.eventBus.emit('ui:basemapChanged', { basemap: state.basemap })
      }
    }

    // Restore map view (with delay to ensure map is ready)
    if (state.coordinates) {
      setTimeout(() => {
        const map = this.mapManager.map
        if (map) {
          map.setView([state.coordinates.lat, state.coordinates.lng], state.coordinates.zoom)
        }
      }, 500)
    }

    // Restore school overlays (with delay)
    if (state.schoolOverlays) {
      setTimeout(() => {
        state.schoolOverlays.forEach(layerId => {
          const checkbox = document.getElementById(`show-${layerId}`)
          if (checkbox) {
            checkbox.checked = true
            this.eventBus.emit('features:schoolOverlayToggled', {
              layerId,
              enabled: true
            })
          }
        })
      }, 1000)
    }

    // Restore heatmap (with delay)
    if (state.heatmap) {
      setTimeout(() => {
        this.eventBus.emit('features:heatmapToggled', { enabled: true })
      }, 1000)
    }
  }

  /**
     * Show share URL dialog for manual copying
     */
  showShareUrlDialog (shareUrl) {
    const dialog = document.createElement('div')
    dialog.style.cssText = `
            position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
            background: var(--color-surface); border: 1px solid var(--color-border);
            border-radius: var(--border-radius); padding: var(--space-6);
            box-shadow: var(--shadow-lg); z-index: 10000; max-width: 90vw;
            font-family: var(--font-family);
        `

    dialog.innerHTML = `
            <h3 style="margin: 0 0 var(--space-4) 0;">Share Map View</h3>
            <p style="margin: 0 0 var(--space-4) 0;">Copy this link to share the current map view:</p>
            <input type="text" value="${shareUrl}" readonly
                   style="width: 100%; padding: var(--space-3); margin-bottom: var(--space-4);
                          border: 1px solid var(--color-border); border-radius: var(--border-radius);
                          font-family: monospace; font-size: var(--font-size-sm);"
                   onclick="this.select()" id="share-url-input">
            <div style="text-align: right; display: flex; gap: var(--space-2); justify-content: flex-end;">
                <button onclick="this.parentElement.parentElement.parentElement.querySelector('#share-url-input').select(); document.execCommand('copy'); alert('Copied to clipboard!');"
                        style="padding: var(--space-2) var(--space-4); background: var(--color-primary);
                               color: white; border: none; border-radius: var(--border-radius); cursor: pointer;">
                    Copy
                </button>
                <button onclick="this.parentElement.parentElement.parentElement.remove()"
                        style="padding: var(--space-2) var(--space-4); background: var(--color-border);
                               color: var(--color-text-primary); border: none; border-radius: var(--border-radius); cursor: pointer;">
                    Close
                </button>
            </div>
        `

    document.body.appendChild(dialog)

    // Auto-select the URL text
    const input = dialog.querySelector('#share-url-input')
    input.focus()
    input.select()

    this.eventBus.emit('sharing:dialogShown', { url: shareUrl })
  }

  /**
     * Show share success message
     */
  showShareSuccess (message) {
    alert(message)
  }

  /**
     * Update share state (for future use)
     */
  updateShareState () {
    // This could be used to update UI indicators, etc.
    const state = this.captureMapState()
    this.stateManager.setState({ lastShareableState: state })
  }

  /**
     * Get field display name (helper method)
     */
  getFieldDisplayName (fieldKey) {
    // This would typically come from a utility or the DataProcessor
    // For now, simple fallback
    if (!fieldKey) return 'Election Data'

    return fieldKey.replace(/_/g, ' ')
      .replace(/\b\w/g, l => l.toUpperCase())
      .replace('Vote Pct', 'Vote %')
      .replace('Reg Pct', 'Registration %')
  }

  /**
     * Get current sharing capabilities
     */
  getSharingCapabilities () {
    return {
      clipboard: !!(navigator.clipboard && window.isSecureContext),
      socialPlatforms: Object.keys(this.socialPlatforms),
      urlParameters: true
    }
  }

  /**
     * Generate share data for programmatic use
     */
  generateShareData () {
    const mapState = this.captureMapState()
    const shareUrl = this.generateShareUrl(mapState)

    return {
      url: shareUrl,
      state: mapState,
      metadata: {
        title: '2025 Portland School Board Election Map',
        description: 'Interactive election results map',
        timestamp: new Date().toISOString()
      }
    }
  }

  /**
     * Clean up resources
     */
  destroy () {
    // Remove any dialogs
    const dialogs = document.querySelectorAll('[style*="position: fixed"]')
    dialogs.forEach(dialog => {
      if (dialog.textContent.includes('Share Map View')) {
        dialog.remove()
      }
    })

    console.log('[Sharing] Destroyed')
  }
}
 