/**
 * ControlPanelTabs - Tabbed Interface for Control Panel
 *
 * Manages the tabbed interface for the control panel:
 * - Tab switching functionality
 * - State persistence
 * - Event coordination with other components
 *
 * Mirrors the InfoPanel tab functionality for consistency.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class ControlPanelTabs {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // DOM references
    this.tabs = null
    this.tabContents = null
    this.currentTab = 'data'

    // State tracking
    this.isInitialized = false

    console.log('[ControlPanelTabs] Initialized')
  }

  /**
   * Initialize the tab functionality
   */
  initialize () {
    if (this.isInitialized) {
      console.warn('[ControlPanelTabs] Already initialized')
      return
    }

    try {
      this.findDOMElements()
      this.setupEventListeners()
      this.restoreActiveTab()

      this.isInitialized = true
      console.log('[ControlPanelTabs] Successfully initialized')
    } catch (error) {
      console.error('[ControlPanelTabs] Failed to initialize:', error)
    }
  }

  /**
   * Find and cache DOM element references
   */
  findDOMElements () {
    this.tabs = document.querySelectorAll('.control-panel-tab')
    this.tabContents = document.querySelectorAll('.control-tab-content')

    if (this.tabs.length === 0) {
      throw new Error('No control panel tabs found')
    }

    if (this.tabContents.length === 0) {
      throw new Error('No control tab content found')
    }

    console.log(`[ControlPanelTabs] Found ${this.tabs.length} tabs and ${this.tabContents.length} content panels`)
  }

  /**
   * Set up event listeners
   */
  setupEventListeners () {
    this.tabs.forEach(tab => {
      tab.addEventListener('click', (e) => {
        e.preventDefault()
        const tabId = tab.getAttribute('data-tab')
        this.switchToTab(tabId)
      })

      // Keyboard navigation
      tab.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          const tabId = tab.getAttribute('data-tab')
          this.switchToTab(tabId)
        }
      })
    })

    console.log('[ControlPanelTabs] Event listeners set up')
  }

  /**
   * Switch to a specific tab
   */
  switchToTab (tabId) {
    if (!tabId) {
      console.warn('[ControlPanelTabs] No tab ID provided')
      return
    }

    const previousTab = this.currentTab

    // Update tab buttons
    this.tabs.forEach(tab => {
      if (tab.getAttribute('data-tab') === tabId) {
        tab.classList.add('active')
      } else {
        tab.classList.remove('active')
      }
    })

    // Update tab content
    this.tabContents.forEach(content => {
      if (content.id === `control-tab-${tabId}`) {
        content.classList.add('active')
        content.style.display = 'block'
      } else {
        content.classList.remove('active')
        content.style.display = 'none'
      }
    })

    // Handle special case: collapse expanded layers when switching away from layers tab
    if (previousTab === 'layers' && tabId !== 'layers') {
      this.collapseExpandedLayers()
    }

    // Update current tab
    this.currentTab = tabId

    // Save state
    this.saveActiveTab(tabId)

    // Emit event
    this.eventBus.emit('ui:controlTabChanged', {
      tabId,
      previousTab
    })

    console.log(`[ControlPanelTabs] Switched to tab: ${tabId}`)
  }

  /**
   * Collapse expanded layers to prevent UI overlap
   */
  collapseExpandedLayers () {
    const fullLayersSection = document.querySelector('.full-layers-section')
    const showMoreBtn = document.getElementById('show-more-layers')
    const controlPanel = document.querySelector('.control-panel')

    if (fullLayersSection && fullLayersSection.style.display !== 'none') {
      fullLayersSection.style.display = 'none'

      if (showMoreBtn) {
        showMoreBtn.classList.remove('expanded')
        const showMoreText = showMoreBtn.querySelector('.show-more-text')
        if (showMoreText) {
          showMoreText.textContent = 'Show All Layers'
        }
      }

      if (controlPanel) {
        controlPanel.classList.remove('layers-expanded')
      }

      console.log('[ControlPanelTabs] Collapsed expanded layers')
    }
  }

  /**
   * Get the currently active tab
   */
  getCurrentTab () {
    return this.currentTab
  }

  /**
   * Save active tab to localStorage
   */
  saveActiveTab (tabId) {
    try {
      localStorage.setItem('controlPanelActiveTab', tabId)
    } catch (error) {
      console.warn('[ControlPanelTabs] Failed to save active tab:', error)
    }
  }

  /**
   * Restore active tab from localStorage
   */
  restoreActiveTab () {
    try {
      const savedTab = localStorage.getItem('controlPanelActiveTab')
      if (savedTab) {
        this.switchToTab(savedTab)
      } else {
        // Default to first tab
        this.switchToTab('data')
      }
    } catch (error) {
      console.warn('[ControlPanelTabs] Failed to restore active tab:', error)
      this.switchToTab('data')
    }
  }

  /**
   * Enable/disable tab functionality
   */
  setEnabled (enabled) {
    this.tabs.forEach(tab => {
      tab.disabled = !enabled
      tab.style.pointerEvents = enabled ? 'auto' : 'none'
      tab.style.opacity = enabled ? '1' : '0.5'
    })

    console.log(`[ControlPanelTabs] ${enabled ? 'Enabled' : 'Disabled'} tab functionality`)
  }

  /**
   * Get list of all tab IDs
   */
  getTabIds () {
    return Array.from(this.tabs).map(tab => tab.getAttribute('data-tab'))
  }

  /**
   * Check if a tab exists
   */
  hasTab (tabId) {
    return Array.from(this.tabs).some(tab => tab.getAttribute('data-tab') === tabId)
  }

  /**
   * Validate tab state
   */
  validate () {
    const issues = []

    if (this.tabs.length === 0) {
      issues.push('No tabs found')
    }

    if (this.tabContents.length === 0) {
      issues.push('No tab content found')
    }

    if (this.tabs.length !== this.tabContents.length) {
      issues.push('Mismatch between number of tabs and content panels')
    }

    const activeTabCount = Array.from(this.tabs).filter(tab => tab.classList.contains('active')).length
    if (activeTabCount !== 1) {
      issues.push(`Expected 1 active tab, found ${activeTabCount}`)
    }

    return {
      isValid: issues.length === 0,
      issues
    }
  }

  /**
   * Clean up resources
   */
  destroy () {
    // Event listeners will be automatically removed when DOM elements are removed
    this.tabs = null
    this.tabContents = null
    this.isInitialized = false

    console.log('[ControlPanelTabs] Destroyed')
  }
}
