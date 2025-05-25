/**
 * EventBus - Event-driven communication system for Election Map Application
 *
 * Replaces direct function calls and tightly coupled component interactions
 * with a clean publish-subscribe event system.
 *
 * Features:
 * - Loose coupling between components
 * - Type-safe event definitions
 * - Event history and debugging
 * - Async event handling support
 * - Event filtering and middleware
 */

export class EventBus {
  constructor () {
    // Event listeners storage
    this.listeners = new Map()

    // Event history for debugging
    this.eventHistory = []
    this.maxHistorySize = 100

    // Event middleware functions
    this.middleware = []

    // Debug mode flag
    this.debugMode = false

    console.log('📡 EventBus initialized')
  }

  /**
     * Register an event listener
     * @param {string} eventName - Name of the event to listen for
     * @param {Function} callback - Function to call when event is emitted
     * @param {Object} options - Optional configuration
     * @returns {Function} Unsubscribe function
     */
  on (eventName, callback, options = {}) {
    const { once = false, priority = 0 } = options

    if (!this.listeners.has(eventName)) {
      this.listeners.set(eventName, [])
    }

    const listener = {
      callback,
      once,
      priority,
      id: Math.random().toString(36).substr(2, 9)
    }

    // Insert listener maintaining priority order (higher priority first)
    const listeners = this.listeners.get(eventName)
    const insertIndex = listeners.findIndex(l => l.priority < priority)

    if (insertIndex === -1) {
      listeners.push(listener)
    } else {
      listeners.splice(insertIndex, 0, listener)
    }

    if (this.debugMode) {
      console.log(`📡 Listener registered for '${eventName}' (ID: ${listener.id})`)
    }

    // Return unsubscribe function
    return () => this.off(eventName, listener.id)
  }

  /**
     * Register a one-time event listener
     * @param {string} eventName - Name of the event to listen for
     * @param {Function} callback - Function to call when event is emitted
     * @returns {Function} Unsubscribe function
     */
  once (eventName, callback) {
    return this.on(eventName, callback, { once: true })
  }

  /**
     * Remove an event listener
     * @param {string} eventName - Name of the event
     * @param {string} listenerId - ID of the listener to remove
     */
  off (eventName, listenerId) {
    if (!this.listeners.has(eventName)) {
      return
    }

    const listeners = this.listeners.get(eventName)
    const index = listeners.findIndex(l => l.id === listenerId)

    if (index !== -1) {
      listeners.splice(index, 1)

      if (this.debugMode) {
        console.log(`📡 Listener removed from '${eventName}' (ID: ${listenerId})`)
      }

      // Clean up empty listener arrays
      if (listeners.length === 0) {
        this.listeners.delete(eventName)
      }
    }
  }

  /**
     * Emit an event to all registered listeners
     * @param {string} eventName - Name of the event to emit
     * @param {*} data - Data to pass to listeners
     * @param {Object} options - Emit options
     * @returns {Promise} Promise that resolves when all listeners complete
     */
  async emit (eventName, data = null, options = {}) {
    const { async = false, bubbles = false } = options

    // Add to event history
    this.addToHistory({
      eventName,
      data,
      timestamp: Date.now(),
      options
    })

    if (this.debugMode) {
      console.log(`📡 Emitting '${eventName}' with data:`, data)
    }

    // Apply middleware
    const processedData = await this.applyMiddleware(eventName, data)

    if (!this.listeners.has(eventName)) {
      if (this.debugMode) {
        console.log(`📡 No listeners for '${eventName}'`)
      }
      return []
    }

    const listeners = [...this.listeners.get(eventName)]
    const results = []

    // Remove once listeners before calling them
    const onceListeners = listeners.filter(l => l.once)
    onceListeners.forEach(l => this.off(eventName, l.id))

    // Call listeners
    for (const listener of listeners) {
      try {
        let result

        if (async) {
          result = await Promise.resolve(listener.callback(processedData, eventName))
        } else {
          result = listener.callback(processedData, eventName)
        }

        results.push(result)
      } catch (error) {
        console.error(`❌ Error in event listener for '${eventName}':`, error)
        results.push({ error })
      }
    }

    return results
  }

  /**
     * Emit an event synchronously
     * @param {string} eventName - Name of the event to emit
     * @param {*} data - Data to pass to listeners
     * @returns {Array} Results from all listeners
     */
  emitSync (eventName, data = null) {
    return this.emit(eventName, data, { async: false })
  }

  /**
     * Emit an event asynchronously
     * @param {string} eventName - Name of the event to emit
     * @param {*} data - Data to pass to listeners
     * @returns {Promise<Array>} Promise resolving to results from all listeners
     */
  emitAsync (eventName, data = null) {
    return this.emit(eventName, data, { async: true })
  }

  /**
     * Add middleware function for processing events
     * @param {Function} middlewareFunction - Function to process events
     */
  addMiddleware (middlewareFunction) {
    this.middleware.push(middlewareFunction)
    console.log('📡 Middleware added to EventBus')
  }

  /**
     * Apply middleware to event data
     * @param {string} eventName - Name of the event
     * @param {*} data - Original event data
     * @returns {*} Processed event data
     */
  async applyMiddleware (eventName, data) {
    let processedData = data

    for (const middleware of this.middleware) {
      try {
        processedData = await Promise.resolve(
          middleware(eventName, processedData)
        )
      } catch (error) {
        console.error('❌ Error in EventBus middleware:', error)
      }
    }

    return processedData
  }

  /**
     * Wait for a specific event to be emitted
     * @param {string} eventName - Name of the event to wait for
     * @param {number} timeout - Timeout in milliseconds (optional)
     * @returns {Promise} Promise that resolves with event data
     */
  waitFor (eventName, timeout = null) {
    return new Promise((resolve, reject) => {
      let timeoutId = null

      const unsubscribe = this.once(eventName, (data) => {
        if (timeoutId) clearTimeout(timeoutId)
        resolve(data)
      })

      if (timeout) {
        timeoutId = setTimeout(() => {
          unsubscribe()
          reject(new Error(`Timeout waiting for event '${eventName}'`))
        }, timeout)
      }
    })
  }

  /**
     * Get all listeners for an event (for debugging)
     * @param {string} eventName - Name of the event
     * @returns {Array} Array of listener objects
     */
  getListeners (eventName) {
    return this.listeners.get(eventName) || []
  }

  /**
     * Get all registered event names
     * @returns {Array} Array of event names
     */
  getEventNames () {
    return Array.from(this.listeners.keys())
  }

  /**
     * Add entry to event history
     * @param {Object} entry - History entry
     */
  addToHistory (entry) {
    this.eventHistory.unshift(entry)

    // Trim history to max size
    if (this.eventHistory.length > this.maxHistorySize) {
      this.eventHistory = this.eventHistory.slice(0, this.maxHistorySize)
    }
  }

  /**
     * Get event history for debugging
     * @param {number} limit - Number of history entries to return
     * @returns {Array} Recent events
     */
  getHistory (limit = 20) {
    return this.eventHistory.slice(0, limit)
  }

  /**
     * Clear all listeners and history
     */
  clear () {
    this.listeners.clear()
    this.eventHistory = []
    this.middleware = []
    console.log('📡 EventBus cleared')
  }

  /**
     * Enable or disable debug mode
     * @param {boolean} enabled - Whether to enable debug mode
     */
  setDebugMode (enabled) {
    this.debugMode = enabled
    console.log(`📡 EventBus debug mode: ${enabled ? 'enabled' : 'disabled'}`)
  }

  /**
     * Get debug information about the EventBus
     * @returns {Object} Debug information
     */
  getDebugInfo () {
    const eventCounts = {}
    this.listeners.forEach((listeners, eventName) => {
      eventCounts[eventName] = listeners.length
    })

    return {
      totalEvents: this.listeners.size,
      totalListeners: Array.from(this.listeners.values())
        .reduce((sum, listeners) => sum + listeners.length, 0),
      eventCounts,
      historyLength: this.eventHistory.length,
      middlewareCount: this.middleware.length,
      debugMode: this.debugMode
    }
  }
}

/**
 * Pre-defined event types for the Election Map Application
 * This helps with consistency and IDE autocompletion
 */
export const EventTypes = {
  // Data events
  DATA_LOADING: 'data:loading',
  DATA_LOADED: 'data:loaded',
  DATA_ERROR: 'data:error',
  DATA_PROCESSED: 'data:processed',
  DATASET_CHANGED: 'data:dataset-changed',
  DATASET_DISCOVERED: 'data:dataset-discovered',

  // Map events
  MAP_READY: 'map:ready',
  MAP_LAYER_CHANGED: 'map:layer-changed',
  MAP_FEATURE_CLICKED: 'map:feature-clicked',
  MAP_FEATURE_HOVERED: 'map:feature-hovered',
  MAP_BOUNDS_CHANGED: 'map:bounds-changed',
  MAP_ZOOM_CHANGED: 'map:zoom-changed',
  MAP_BASEMAP_CHANGED: 'map:basemap-changed',

  // UI events
  UI_PANEL_TOGGLED: 'ui:panel-toggled',
  UI_LAYER_SELECTED: 'ui:layer-selected',
  UI_RANGE_CHANGED: 'ui:range-changed',
  UI_OPACITY_CHANGED: 'ui:opacity-changed',
  UI_FILTER_CHANGED: 'ui:filter-changed',
  UI_ACCORDION_TOGGLED: 'ui:accordion-toggled',

  // Feature events
  FEATURE_SEARCH_STARTED: 'feature:search-started',
  FEATURE_SEARCH_COMPLETE: 'feature:search-complete',
  FEATURE_SEARCH_ERROR: 'feature:search-error',
  FEATURE_LOCATION_FOUND: 'feature:location-found',
  FEATURE_EXPORT_STARTED: 'feature:export-started',
  FEATURE_EXPORT_COMPLETE: 'feature:export-complete',
  FEATURE_HEATMAP_TOGGLED: 'feature:heatmap-toggled',
  FEATURE_COMPARISON_TOGGLED: 'feature:comparison-toggled',
  FEATURE_COORDINATES_TOGGLED: 'feature:coordinates-toggled',

  // School overlay events
  SCHOOLS_LAYER_TOGGLED: 'schools:layer-toggled',
  SCHOOLS_DATA_LOADED: 'schools:data-loaded',

  // Error events
  ERROR_CRITICAL: 'error:critical',
  ERROR_WARNING: 'error:warning',
  ERROR_NETWORK: 'error:network',

  // Application lifecycle events
  APP_INITIALIZING: 'app:initializing',
  APP_READY: 'app:ready',
  APP_ERROR: 'app:error'
}

// Export singleton instance
export const eventBus = new EventBus()
