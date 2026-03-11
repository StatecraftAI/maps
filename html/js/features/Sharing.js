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
  constructor (stateManager, eventBus, mapManager, demographicOverlays = null) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    this.mapManager = mapManager
    this.demographicOverlays = demographicOverlays

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

    // Bind methods to ensure correct 'this' context in callbacks
    this.showShareUrlDialog = this.showShareUrlDialog.bind(this)
    this.captureMapState = this.captureMapState.bind(this)
    this.generateShareUrl = this.generateShareUrl.bind(this)
    this.parseUrlParameters = this.parseUrlParameters.bind(this)
    this.checkAndRestoreFromUrl = this.checkAndRestoreFromUrl.bind(this)
    this.shareMapView = this.shareMapView.bind(this)
    this.shareToSocial = this.shareToSocial.bind(this)

    this.initializeElements()
    this.setupEventListeners()

    console.log('[Sharing] Initialized')
  }

  /**
     * Initialize DOM elements
     */
  initializeElements () {
    // Select buttons by their IDs after removing inline handlers in HTML
    this.shareButton = document.getElementById('share-btn')
    this.twitterButton = document.getElementById('share-twitter-btn')
    this.facebookButton = document.getElementById('share-facebook-btn')
    this.linkedinButton = document.getElementById('share-linkedin-btn')

    // Attach event listeners
    if (this.shareButton) {
      this.shareButton.addEventListener('click', () => this.shareMapView())
    }

    if (this.twitterButton) {
      this.twitterButton.addEventListener('click', () => this.shareToSocial('twitter'))
    }

    if (this.facebookButton) {
      this.facebookButton.addEventListener('click', () => this.shareToSocial('facebook'))
    }

    if (this.linkedinButton) {
      this.linkedinButton.addEventListener('click', () => this.shareToSocial('linkedin'))
    }

    console.log('[Sharing] Initialized elements and attached listeners.')
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
    console.log('[Sharing] shareMapView called')
    try {
      console.log('[Sharing] Capturing map state...')
      const mapState = this.captureMapState()
      console.log('[Sharing] Map state captured:', mapState)

      console.log('[Sharing] Generating share URL...')
      const shareUrl = this.generateShareUrl(mapState)
      console.log('[Sharing] Share URL generated:', shareUrl)

      console.log('[Sharing] Checking clipboard and secure context...')

      // Try to copy to clipboard first
      if (navigator.clipboard && window.isSecureContext) {
        console.log('[Sharing] Attempting clipboard copy...')
        navigator.clipboard.writeText(shareUrl).then(() => {
          console.log('[Sharing] Clipboard copy successful.')
          this.showShareSuccess('Shareable link copied to clipboard!\n\nAnyone with this link will see the exact same map view, layer, and settings.')

          this.eventBus.emit('sharing:linkCopied', {
            url: shareUrl,
            method: 'clipboard'
          })
        }).catch((error) => {
          console.warn('[Sharing] Clipboard copy failed:', error)
          console.log('[Sharing] Falling back to URL dialog after clipboard failure, explicitly calling dialog function...')
          this.showShareUrlDialog(shareUrl)
        })
      } else {
        console.log('[Sharing] Clipboard or secure context not available, showing URL dialog.')
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

    // Get demographic overlay states
    const demographicOverlays = this.getDemographicOverlayStates()

    // Get feature states
    const heatmapActive = this.stateManager.getState('heatmapActive') || false

    const state = {
      currentDataset,
      currentField,
      opacity: mapOpacity,
      basemap,
      coordinates: center,
      zoom,
      showPpsOnly,
      customRange,
      schoolOverlays,
      demographicOverlays,
      heatmapActive,
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
     * Get demographic overlay states
     */
  getDemographicOverlayStates () {
    if (!this.demographicOverlays) {
      return []
    }

    // Get active overlays from the DemographicOverlays component
    return this.demographicOverlays.getActiveOverlays()
  }

  /**
     * Generate shareable URL from map state
     */
  generateShareUrl (state) {
    const url = new URL(window.location.href.split('?')[0]) // Remove existing params

    // Add all state parameters with checks for existence and type
    if (state.coordinates && typeof state.coordinates.lat === 'number' && typeof state.coordinates.lng === 'number') {
      url.searchParams.set('lat', state.coordinates.lat.toFixed(6))
      url.searchParams.set('lng', state.coordinates.lng.toFixed(6))
    }

    if (typeof state.zoom === 'number') {
      url.searchParams.set('zoom', state.zoom.toFixed(2))
    }

    if (state.currentDataset) {
      url.searchParams.set('dataset', state.currentDataset)
    }

    if (state.currentField) {
      url.searchParams.set('layer', state.currentField)
    }

    // showPpsOnly is a boolean, convert to '1' or '0'
    url.searchParams.set('pps', state.showPpsOnly ? '1' : '0')

    if (typeof state.opacity === 'number') {
      url.searchParams.set('opacity', state.opacity.toFixed(1))
    }

    if (state.basemap && state.basemap !== 'streets') {
      url.searchParams.set('basemap', state.basemap)
    }

    // Custom range parameters
    if (state.customRange && typeof state.customRange.min === 'number' && typeof state.customRange.max === 'number' && state.customRange.field) {
      url.searchParams.set('rangeMin', state.customRange.min.toFixed(2))
      url.searchParams.set('rangeMax', state.customRange.max.toFixed(2))
      url.searchParams.set('rangeField', state.customRange.field)
    }

    // Heatmap state (boolean)
    if (state.heatmapActive) {
      url.searchParams.set('heatmap', '1')
    }

    // School overlays (object with boolean values)
    if (state.schoolOverlays && typeof state.schoolOverlays === 'object') {
      const activeOverlays = Object.keys(state.schoolOverlays).filter(key => state.schoolOverlays[key])
      if (activeOverlays.length > 0) {
        url.searchParams.set('overlays', activeOverlays.join(','))
      }
    }

    // Demographic overlays (array of active overlay IDs)
    if (state.demographicOverlays && Array.isArray(state.demographicOverlays) && state.demographicOverlays.length > 0) {
      url.searchParams.set('demographicOverlays', state.demographicOverlays.join(','))
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

    if (params.has('demographicOverlays')) {
      state.demographicOverlays = params.get('demographicOverlays').split(',')
    }

    return state
  }

  /**
     * Restore map state from parsed parameters
     */
  restoreMapState (state) {
    // Update StateManager with restored values
    const stateUpdates = {} // Use a single object for updates

    if (state.currentDataset) {
      stateUpdates.currentDataset = state.currentDataset
      const datasetSelect = document.getElementById('dataset-select')
      if (datasetSelect) {
        datasetSelect.value = state.currentDataset
      }
    }

    if (state.currentField) {
      stateUpdates.currentField = state.currentField
    }

    if (state.showPpsOnly !== undefined) {
      stateUpdates.showPpsOnly = state.showPpsOnly
      const ppsCheckbox = document.getElementById('pps-only')
      if (ppsCheckbox) {
        ppsCheckbox.checked = state.showPpsOnly
      }
    }

    if (state.customRange) {
      stateUpdates.customRange = state.customRange
    }

    // Restore UI elements
    if (state.opacity !== undefined) {
      stateUpdates.opacity = state.opacity
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
      stateUpdates.basemap = state.basemap
      const basemapSelect = document.getElementById('basemap-select')
      if (basemapSelect) {
        basemapSelect.value = state.basemap
        // Trigger basemap change - StateManager subscriber will handle this
      }
    }

    // Restore map view (with delay to ensure map is ready)
    if (state.coordinates) {
      // Add coordinates to stateUpdates, MapManager can subscribe to this
      stateUpdates.initialCoordinates = state.coordinates // Use a different key to avoid conflict

      // The actual map view update will be handled by MapManager subscribing to this state change
      // No need for setTimeout here if MapManager reacts to initialCoordinates state
    }

    // Restore school overlays - Add to stateUpdates instead of emitting event
    if (state.schoolOverlays && Array.isArray(state.schoolOverlays) && state.schoolOverlays.length > 0) {
      console.log('[Sharing] Found school overlays in URL, adding to state updates:', state.schoolOverlays)
      stateUpdates.activeSchoolOverlays = state.schoolOverlays // New state key
    }

    // Restore demographic overlays
    if (state.demographicOverlays && Array.isArray(state.demographicOverlays) && state.demographicOverlays.length > 0) {
      console.log('[Sharing] Found demographic overlays in URL, restoring:', state.demographicOverlays)
      // Restore demographic overlays directly through the component
      if (this.demographicOverlays) {
        // Clear existing overlays first
        this.demographicOverlays.clearAllOverlays()
        // Add each overlay from the URL
        state.demographicOverlays.forEach(overlayId => {
          this.demographicOverlays.addOverlay(overlayId)
        })
      }
    }

    // Restore heatmap - Add to stateUpdates instead of emitting event
    if (state.heatmap !== undefined) { // Check explicitly for presence
      console.log('[Sharing] Found heatmap state in URL, adding to state updates:', state.heatmap)
      stateUpdates.heatmapActive = state.heatmap // Existing state key
    }

    // Apply all restored state updates at once through StateManager
    if (Object.keys(stateUpdates).length > 0) {
      console.log('[Sharing] Applying all restored state updates via StateManager:', stateUpdates)
      this.stateManager.setState(stateUpdates, { source: 'Sharing.restoreFromUrl' })
    } else {
      console.log('[Sharing] No state updates to apply from URL.')
    }
  }

  /**
     * Show share URL dialog for manual copying
     */
  showShareUrlDialog (shareUrl) {
    console.log('[Sharing] showShareUrlDialog called with URL:', shareUrl)

    // Remove any existing dialog
    const existingDialog = document.querySelector('.share-url-dialog')
    if (existingDialog) {
      existingDialog.remove()
    }

    // Create dialog container
    const dialog = document.createElement('div')
    dialog.className = 'share-url-dialog'
    dialog.style.cssText = `
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      background: white;
      padding: 2rem;
      border-radius: 8px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.3);
      max-width: 500px;
      width: 90%;
      text-align: center;
      z-index: 10000;
      border: 1px solid #ddd;
    `

    // Create dialog content
    dialog.innerHTML = `
      <h3 style="margin-top: 0; color: #333;">Share Map View</h3>
      <p style="margin-bottom: 1rem; color: #666;">
        Copy this URL to share the current map view:
      </p>
      <div style="margin-bottom: 1.5rem;">
        <input type="text" value="${shareUrl}" readonly
               style="width: 100%; padding: 0.75rem; border: 1px solid #ddd; border-radius: 4px; font-family: monospace; font-size: 0.9rem;"
               id="share-url-input">
      </div>
      <div style="display: flex; gap: 0.5rem; justify-content: center;">
        <button id="copy-url-btn" style="
          background: #2563eb;
          color: white;
          border: none;
          padding: 0.75rem 1.5rem;
          border-radius: 4px;
          cursor: pointer;
          font-size: 0.9rem;
        ">📋 Copy URL</button>
        <button id="close-dialog-btn" style="
          background: #6b7280;
          color: white;
          border: none;
          padding: 0.75rem 1.5rem;
          border-radius: 4px;
          cursor: pointer;
          font-size: 0.9rem;
        ">Close</button>
      </div>
    `

    // Add to page
    document.body.appendChild(dialog)

    // Set up event listeners
    const urlInput = dialog.querySelector('#share-url-input')
    const copyBtn = dialog.querySelector('#copy-url-btn')
    const closeBtn = dialog.querySelector('#close-dialog-btn')

    // Select URL text when clicked
    urlInput.addEventListener('click', () => {
      urlInput.select()
    })

    // Copy button functionality
    copyBtn.addEventListener('click', () => {
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(shareUrl).then(() => {
          copyBtn.textContent = '✅ Copied!'
          copyBtn.style.background = '#10b981'
          setTimeout(() => {
            copyBtn.textContent = '📋 Copy URL'
            copyBtn.style.background = '#2563eb'
          }, 2000)
        }).catch(() => {
          this.fallbackCopy(urlInput)
        })
      } else {
        this.fallbackCopy(urlInput)
      }
    })

    // Close button functionality
    closeBtn.addEventListener('click', () => {
      dialog.remove()
    })

    // Close on escape key
    const handleEscape = (e) => {
      if (e.key === 'Escape') {
        dialog.remove()
        document.removeEventListener('keydown', handleEscape)
      }
    }
    document.addEventListener('keydown', handleEscape)

    // Close on backdrop click
    dialog.addEventListener('click', (e) => {
      if (e.target === dialog) {
        dialog.remove()
      }
    })

    console.log('[Sharing] Share URL dialog displayed')
  }

  /**
   * Fallback copy method for older browsers
   */
  fallbackCopy (input) {
    try {
      input.select()
      document.execCommand('copy')
      const copyBtn = document.querySelector('#copy-url-btn')
      if (copyBtn) {
        copyBtn.textContent = '✅ Copied!'
        copyBtn.style.background = '#10b981'
        setTimeout(() => {
          copyBtn.textContent = '📋 Copy URL'
          copyBtn.style.background = '#2563eb'
        }, 2000)
      }
    } catch (err) {
      console.error('[Sharing] Copy failed:', err)
      alert('Please manually copy the URL from the text field above.')
    }
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
    console.log('[Sharing] updateShareState called')
    // This could be used to update UI indicators, etc.
    const state = this.captureMapState()
    console.log('[Sharing] Setting lastShareableState in StateManager')
    this.stateManager.setState({ lastShareableState: state })
    console.log('[Sharing] Finished updateShareState')
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
