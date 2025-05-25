/**
 * PanelMinimizer.js - Panel Minimize/Maximize Functionality
 *
 * Handles:
 * - Panel collapse/expand functionality
 * - Smooth animations
 * - State persistence
 * - Keyboard shortcuts
 * - Accessibility
 */

export class PanelMinimizer {
  constructor(stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus
    
    this.panels = new Map()
    this.initialized = false
  }

  /**
   * Initialize the PanelMinimizer component
   */
  async init() {
    try {
      this.setupPanels()
      this.setupEventListeners()
      this.setupKeyboardShortcuts()
      this.restoreState()
      
      this.initialized = true
      console.log('✅ PanelMinimizer initialized')
    } catch (error) {
      console.error('❌ PanelMinimizer initialization failed:', error)
      throw error
    }
  }

  /**
   * Set up panel configurations
   */
  setupPanels() {
    // Control Panel
    const controlPanel = document.querySelector('.control-panel')
    const controlMinimizeBtn = document.getElementById('control-panel-minimize')
    
    if (controlPanel && controlMinimizeBtn) {
      this.panels.set('control', {
        element: controlPanel,
        button: controlMinimizeBtn,
        isMinimized: false,
        originalWidth: '320px'
      })
    }

    // Info Panel
    const infoPanel = document.querySelector('.info-panel')
    const infoMinimizeBtn = document.getElementById('info-panel-minimize')
    
    if (infoPanel && infoMinimizeBtn) {
      this.panels.set('info', {
        element: infoPanel,
        button: infoMinimizeBtn,
        isMinimized: false,
        originalWidth: '380px'
      })
    }
  }

  /**
   * Set up event listeners
   */
  setupEventListeners() {
    this.panels.forEach((panel, panelId) => {
      if (panel.button) {
        panel.button.addEventListener('click', () => {
          this.togglePanel(panelId)
        })
      }
    })
  }

  /**
   * Set up keyboard shortcuts
   */
  setupKeyboardShortcuts() {
    document.addEventListener('keydown', (event) => {
      // Ctrl/Cmd + 1 = Toggle control panel
      if ((event.ctrlKey || event.metaKey) && event.key === '1') {
        event.preventDefault()
        this.togglePanel('control')
      }
      
      // Ctrl/Cmd + 2 = Toggle info panel
      if ((event.ctrlKey || event.metaKey) && event.key === '2') {
        event.preventDefault()
        this.togglePanel('info')
      }
      
      // Escape = Expand all panels
      if (event.key === 'Escape') {
        this.expandAllPanels()
      }
    })
  }

  /**
   * Toggle panel minimize/maximize state
   */
  togglePanel(panelId) {
    const panel = this.panels.get(panelId)
    if (!panel) return

    if (panel.isMinimized) {
      this.expandPanel(panelId)
    } else {
      this.minimizePanel(panelId)
    }
  }

  /**
   * Minimize a panel
   */
  minimizePanel(panelId) {
    const panel = this.panels.get(panelId)
    if (!panel || panel.isMinimized) return

    panel.element.classList.add('minimized')
    panel.isMinimized = true
    
    // Update button
    panel.button.innerHTML = ''
    panel.button.title = 'Maximize panel'
    panel.button.setAttribute('aria-label', `Maximize ${panelId} panel`)
    
    // Save state
    this.saveState()
    
    // Emit event
    this.eventBus.emit('panel:minimized', { panelId })
    
    console.log(`[PanelMinimizer] Minimized ${panelId} panel`)
  }

  /**
   * Expand a panel
   */
  expandPanel(panelId) {
    const panel = this.panels.get(panelId)
    if (!panel || !panel.isMinimized) return

    panel.element.classList.remove('minimized')
    panel.isMinimized = false
    
    // Update button
    panel.button.innerHTML = ''
    panel.button.title = 'Minimize panel'
    panel.button.setAttribute('aria-label', `Minimize ${panelId} panel`)
    
    // Save state
    this.saveState()
    
    // Emit event
    this.eventBus.emit('panel:expanded', { panelId })
    
    console.log(`[PanelMinimizer] Expanded ${panelId} panel`)
  }

  /**
   * Expand all panels
   */
  expandAllPanels() {
    this.panels.forEach((panel, panelId) => {
      if (panel.isMinimized) {
        this.expandPanel(panelId)
      }
    })
  }

  /**
   * Minimize all panels
   */
  minimizeAllPanels() {
    this.panels.forEach((panel, panelId) => {
      if (!panel.isMinimized) {
        this.minimizePanel(panelId)
      }
    })
  }

  /**
   * Save panel states to localStorage
   */
  saveState() {
    const state = {}
    this.panels.forEach((panel, panelId) => {
      state[panelId] = {
        isMinimized: panel.isMinimized
      }
    })
    
    try {
      localStorage.setItem('panelStates', JSON.stringify(state))
    } catch (error) {
      console.warn('[PanelMinimizer] Could not save state to localStorage:', error)
    }
  }

  /**
   * Restore panel states from localStorage
   */
  restoreState() {
    try {
      const savedState = localStorage.getItem('panelStates')
      if (!savedState) return

      const state = JSON.parse(savedState)
      
      this.panels.forEach((panel, panelId) => {
        if (state[panelId] && state[panelId].isMinimized) {
          this.minimizePanel(panelId)
        }
      })
    } catch (error) {
      console.warn('[PanelMinimizer] Could not restore state from localStorage:', error)
    }
  }

  /**
   * Get panel state
   */
  getPanelState(panelId) {
    const panel = this.panels.get(panelId)
    return panel ? { isMinimized: panel.isMinimized } : null
  }

  /**
   * Check if any panels are minimized
   */
  hasMinimizedPanels() {
    return Array.from(this.panels.values()).some(panel => panel.isMinimized)
  }

  /**
   * Get component status
   */
  getStatus() {
    return {
      name: 'PanelMinimizer',
      initialized: this.initialized,
      panelCount: this.panels.size,
      minimizedPanels: Array.from(this.panels.entries())
        .filter(([, panel]) => panel.isMinimized)
        .map(([id]) => id)
    }
  }

  /**
   * Cleanup and destroy the component
   */
  destroy() {
    // Remove event listeners
    this.panels.forEach((panel) => {
      if (panel.button) {
        panel.button.removeEventListener('click', this.togglePanel)
      }
    })
    
    this.panels.clear()
    this.initialized = false
    console.log('PanelMinimizer destroyed')
  }
} 