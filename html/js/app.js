/**
 * Election Map Application - Main Entry Point
 *
 * This is the new modular application entry point, replacing the monolithic
 * JavaScript code from election_map.html with a fully componentized architecture.
 *
 * The ComponentOrchestrator manages all component initialization, lifecycle,
 * and cross-component communication through the EventBus and StateManager.
 */

import { ComponentOrchestrator } from './integration/ComponentOrchestrator.js'

// Global application instance
let app = null

/**
 * Initialize the election map application
 */
async function initializeApp () {
  try {
    console.log('[App] Starting Portland School Board Election Map...')

    // Create and initialize the component orchestrator
    app = new ComponentOrchestrator()
    await app.initialize()

    // Hide loading screen and show main content
    hideLoadingScreen()

    // Make app globally available for debugging
    if (typeof window !== 'undefined') {
      window.ElectionMapApp = app

      // Add development helpers in debug mode
      const state = app.getComponent('stateManager')?.getState()
      if (state?.debug) {
        window.getComponent = (name) => app.getComponent(name)
        window.getState = () => app.getComponent('stateManager')?.getState()
        window.setState = (newState) => app.getComponent('stateManager')?.setState(newState)
        window.getMetrics = () => app.getMetrics()
        console.log('[App] Debug mode enabled. Available globals: ElectionMapApp, getComponent, getState, setState, getMetrics')
      }
    }

    console.log('[App] Application started successfully')
  } catch (error) {
    console.error('[App] Failed to initialize application:', error)
    
    // Hide loading screen even on error
    hideLoadingScreen()
    
    // Show user-friendly error message
    showInitializationError(error)
    
    // Re-throw for any global error handlers
    throw error
  }
}

/**
 * Hide loading screen and show main content
 */
function hideLoadingScreen() {
  console.log('[App] Hiding loading screen...')
  
  // Hide loading screen
  const loadingElement = document.getElementById('loading')
  if (loadingElement) {
    loadingElement.style.display = 'none'
  }
  
  // Show main content
  const mainContent = document.getElementById('main-content')
  if (mainContent) {
    mainContent.style.display = 'block'
    mainContent.style.visibility = 'visible'
  }
  
  // Show legend
  const legend = document.getElementById('color-scale-legend')
  if (legend) {
    legend.style.display = 'block'
    legend.style.visibility = 'visible'
  }
  
  console.log('[App] Loading screen hidden, main content visible')
}

/**
 * Show initialization error to user
 */
function showInitializationError (error) {
  const container = document.getElementById('map-container') || document.body

  const errorHtml = `
        <div style="
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 2rem;
            border-radius: 8px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            max-width: 500px;
            text-align: center;
            z-index: 10000;
        ">
            <h2 style="color: #dc2626; margin-bottom: 1rem;">Application Error</h2>
            <p style="margin-bottom: 1.5rem; color: #666;">
                The Portland School Board Election Map failed to load.
                This might be due to a network issue or browser compatibility.
            </p>
            <div style="margin-bottom: 1.5rem;">
                <button onclick="window.location.reload()" style="
                    background: #2563eb;
                    color: white;
                    border: none;
                    padding: 0.75rem 1.5rem;
                    border-radius: 4px;
                    cursor: pointer;
                    margin-right: 0.5rem;
                ">Refresh Page</button>
                <button onclick="this.parentElement.parentElement.style.display='none'" style="
                    background: #6b7280;
                    color: white;
                    border: none;
                    padding: 0.75rem 1.5rem;
                    border-radius: 4px;
                    cursor: pointer;
                ">Dismiss</button>
            </div>
            <details style="text-align: left; font-size: 0.875rem; color: #666;">
                <summary style="cursor: pointer; margin-bottom: 0.5rem;">Technical Details</summary>
                <pre style="background: #f5f5f5; padding: 1rem; border-radius: 4px; overflow-x: auto; white-space: pre-wrap;">
${error.message}
${error.stack ? '\n\nStack trace:\n' + error.stack : ''}
                </pre>
            </details>
        </div>
    `

  container.insertAdjacentHTML('beforeend', errorHtml)
}

/**
 * Handle page unload
 */
function handlePageUnload () {
  if (app) {
    console.log('[App] Page unloading, cleaning up...')
    app.cleanup()
  }
}

/**
 * Handle global errors
 */
function handleGlobalError (event) {
  console.error('[App] Global error:', event.error)

  // Emit error through app if available
  if (app) {
    const eventBus = app.getComponent('eventBus')
    eventBus?.emit('app:error', {
      type: 'global',
      error: event.error,
      fatal: false
    })
  }
}

/**
 * Handle unhandled promise rejections
 */
function handleUnhandledRejection (event) {
  console.error('[App] Unhandled promise rejection:', event.reason)

  // Emit error through app if available
  if (app) {
    const eventBus = app.getComponent('eventBus')
    eventBus?.emit('app:error', {
      type: 'promise',
      error: event.reason,
      fatal: false
    })
  }
}

/**
 * Setup global event listeners
 */
function setupGlobalEventListeners () {
  // Page unload cleanup
  window.addEventListener('beforeunload', handlePageUnload)

  // Global error handling
  window.addEventListener('error', handleGlobalError)
  window.addEventListener('unhandledrejection', handleUnhandledRejection)
}

/**
 * Wait for DOM to be ready
 */
function waitForDOM () {
  return new Promise((resolve) => {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', resolve)
    } else {
      resolve()
    }
  })
}

/**
 * Main application bootstrap
 */
async function bootstrap () {
  console.log('[App] Bootstrapping application...')

  // Setup global event listeners first
  setupGlobalEventListeners()

  // Wait for DOM to be ready
  await waitForDOM()

  // Initialize the application
  await initializeApp()
}

// Start the application
bootstrap().catch(error => {
  console.error('[App] Bootstrap failed:', error)
})

// Export for module usage
export { app as ElectionMapApp, initializeApp, bootstrap }
