/**
 * Accordion - Collapsible Section Manager
 *
 * Manages accordion-style collapsible sections:
 * - Section state persistence (localStorage)
 * - Keyboard navigation support
 * - ARIA accessibility attributes
 * - Individual section toggle functionality
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class Accordion {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // State tracking
    this.isInitialized = false
    this.sections = new Map()

    console.log('[Accordion] Initialized')
  }

  /**
     * Initialize the accordion functionality
     */
  initialize () {
    if (this.isInitialized) {
      console.warn('[Accordion] Already initialized')
      return
    }

    try {
      this.findSections()
      this.setupEventListeners()
      this.restoreStates()
      this.setupAccessibility()

      this.isInitialized = true
      console.log('[Accordion] Successfully initialized')
    } catch (error) {
      console.error('[Accordion] Failed to initialize:', error)
    }
  }

  /**
     * Find and register all accordion sections
     */
  findSections () {
    const sectionElements = document.querySelectorAll('.section[data-section]')

    sectionElements.forEach(element => {
      const sectionId = element.getAttribute('data-section')
      const header = element.querySelector('.section-header')
      const content = element.querySelector('.section-content')

      if (sectionId && header && content) {
        this.sections.set(sectionId, {
          element,
          header,
          content,
          isCollapsed: element.classList.contains('collapsed')
        })

        console.log(`[Accordion] Registered section: ${sectionId}`)
      } else {
        console.warn(`[Accordion] Invalid section structure for: ${sectionId}`)
      }
    })

    console.log(`[Accordion] Found ${this.sections.size} sections`)
  }

  /**
     * Set up event listeners for all sections
     */
  setupEventListeners () {
    this.sections.forEach((section, sectionId) => {
      // Click event
      section.header.addEventListener('click', () => {
        this.toggleSection(sectionId)
      })

      // Keyboard navigation
      section.header.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          this.toggleSection(sectionId)
        }
      })
    })

    console.log('[Accordion] Event listeners set up')
  }

  /**
     * Set up accessibility attributes
     */
  setupAccessibility () {
    this.sections.forEach((section, sectionId) => {
      // Make header focusable and add role
      section.header.setAttribute('tabindex', '0')
      section.header.setAttribute('role', 'button')
      section.header.setAttribute('aria-expanded', !section.isCollapsed)

      // Set up content attributes
      section.content.setAttribute('aria-hidden', section.isCollapsed)

      // Add unique IDs for ARIA relationships
      const contentId = `section-content-${sectionId}`
      const headerId = `section-header-${sectionId}`

      section.content.id = contentId
      section.header.id = headerId
      section.header.setAttribute('aria-controls', contentId)
      section.content.setAttribute('aria-labelledby', headerId)
    })

    console.log('[Accordion] Accessibility attributes set up')
  }

  /**
     * Toggle a specific section
     */
  toggleSection (sectionId) {
    const section = this.sections.get(sectionId)
    if (!section) {
      console.warn(`[Accordion] Section not found: ${sectionId}`)
      return
    }

    const wasCollapsed = section.isCollapsed
    const willBeCollapsed = !wasCollapsed

    // Update visual state
    if (willBeCollapsed) {
      section.element.classList.add('collapsed')
    } else {
      section.element.classList.remove('collapsed')
    }

    // Update accessibility attributes
    section.header.setAttribute('aria-expanded', !willBeCollapsed)
    section.content.setAttribute('aria-hidden', willBeCollapsed)

    // Update internal state
    section.isCollapsed = willBeCollapsed

    // Save state to localStorage
    this.saveState(sectionId, !willBeCollapsed)

    // Emit event
    this.eventBus.emit('ui:sectionToggled', {
      sectionId,
      isExpanded: !willBeCollapsed,
      isCollapsed: willBeCollapsed
    })

    console.log(`[Accordion] Toggled section ${sectionId}: ${willBeCollapsed ? 'collapsed' : 'expanded'}`)
  }

  /**
     * Expand a specific section
     */
  expandSection (sectionId) {
    const section = this.sections.get(sectionId)
    if (!section || !section.isCollapsed) return

    this.toggleSection(sectionId)
  }

  /**
     * Collapse a specific section
     */
  collapseSection (sectionId) {
    const section = this.sections.get(sectionId)
    if (!section || section.isCollapsed) return

    this.toggleSection(sectionId)
  }

  /**
     * Expand all sections
     */
  expandAll () {
    this.sections.forEach((section, sectionId) => {
      if (section.isCollapsed) {
        this.toggleSection(sectionId)
      }
    })

    console.log('[Accordion] Expanded all sections')
  }

  /**
     * Collapse all sections
     */
  collapseAll () {
    this.sections.forEach((section, sectionId) => {
      if (!section.isCollapsed) {
        this.toggleSection(sectionId)
      }
    })

    console.log('[Accordion] Collapsed all sections')
  }

  /**
     * Check if a section is expanded
     */
  isExpanded (sectionId) {
    const section = this.sections.get(sectionId)
    return section ? !section.isCollapsed : false
  }

  /**
     * Check if a section is collapsed
     */
  isCollapsed (sectionId) {
    const section = this.sections.get(sectionId)
    return section ? section.isCollapsed : true
  }

  /**
     * Get the state of all sections
     */
  getStates () {
    const states = {}
    this.sections.forEach((section, sectionId) => {
      states[sectionId] = !section.isCollapsed
    })
    return states
  }

  /**
     * Set the state of all sections
     */
  setStates (states) {
    Object.entries(states).forEach(([sectionId, isExpanded]) => {
      const section = this.sections.get(sectionId)
      if (section && section.isCollapsed === isExpanded) {
        this.toggleSection(sectionId)
      }
    })
  }

  /**
     * Save section state to localStorage
     */
  saveState (sectionId, isExpanded) {
    const sectionStates = JSON.parse(localStorage.getItem('accordionStates') || '{}')
    sectionStates[sectionId] = isExpanded
    localStorage.setItem('accordionStates', JSON.stringify(sectionStates))
  }

  /**
     * Restore section states from localStorage
     */
  restoreStates () {
    const sectionStates = JSON.parse(localStorage.getItem('accordionStates') || '{}')

    this.sections.forEach((section, sectionId) => {
      if (sectionStates.hasOwnProperty(sectionId)) {
        const shouldBeExpanded = sectionStates[sectionId]
        const isCurrentlyExpanded = !section.isCollapsed

        if (shouldBeExpanded !== isCurrentlyExpanded) {
          this.toggleSection(sectionId)
        }
      }
    })

    console.log('[Accordion] States restored from localStorage')
  }

  /**
     * Clear all saved states
     */
  clearSavedStates () {
    localStorage.removeItem('accordionStates')
    console.log('[Accordion] Cleared saved states')
  }

  /**
     * Get list of all section IDs
     */
  getSectionIds () {
    return Array.from(this.sections.keys())
  }

  /**
     * Check if a section exists
     */
  hasSection (sectionId) {
    return this.sections.has(sectionId)
  }

  /**
     * Add a new section dynamically
     */
  addSection (sectionId, element) {
    if (this.sections.has(sectionId)) {
      console.warn(`[Accordion] Section already exists: ${sectionId}`)
      return false
    }

    const header = element.querySelector('.section-header')
    const content = element.querySelector('.section-content')

    if (!header || !content) {
      console.error(`[Accordion] Invalid section structure for: ${sectionId}`)
      return false
    }

    // Register the section
    this.sections.set(sectionId, {
      element,
      header,
      content,
      isCollapsed: element.classList.contains('collapsed')
    })

    // Set up event listeners
    header.addEventListener('click', () => this.toggleSection(sectionId))
    header.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault()
        this.toggleSection(sectionId)
      }
    })

    // Set up accessibility
    header.setAttribute('tabindex', '0')
    header.setAttribute('role', 'button')
    header.setAttribute('aria-expanded', !this.sections.get(sectionId).isCollapsed)

    const contentId = `section-content-${sectionId}`
    const headerId = `section-header-${sectionId}`

    content.id = contentId
    header.id = headerId
    header.setAttribute('aria-controls', contentId)
    content.setAttribute('aria-labelledby', headerId)
    content.setAttribute('aria-hidden', this.sections.get(sectionId).isCollapsed)

    console.log(`[Accordion] Added section: ${sectionId}`)
    return true
  }

  /**
     * Remove a section
     */
  removeSection (sectionId) {
    const section = this.sections.get(sectionId)
    if (!section) {
      console.warn(`[Accordion] Section not found: ${sectionId}`)
      return false
    }

    // Remove event listeners (if needed - depends on implementation)
    // Clean up DOM IDs and attributes
    section.header.removeAttribute('tabindex')
    section.header.removeAttribute('role')
    section.header.removeAttribute('aria-expanded')
    section.header.removeAttribute('aria-controls')
    section.header.removeAttribute('id')

    section.content.removeAttribute('aria-hidden')
    section.content.removeAttribute('aria-labelledby')
    section.content.removeAttribute('id')

    // Remove from internal tracking
    this.sections.delete(sectionId)

    console.log(`[Accordion] Removed section: ${sectionId}`)
    return true
  }

  /**
     * Enable/disable accordion functionality
     */
  setEnabled (enabled) {
    this.sections.forEach((section) => {
      section.header.style.pointerEvents = enabled ? 'auto' : 'none'
      section.header.setAttribute('tabindex', enabled ? '0' : '-1')
    })

    console.log(`[Accordion] ${enabled ? 'Enabled' : 'Disabled'} accordion functionality`)
  }

  /**
     * Validate accordion state
     */
  validate () {
    const issues = []

    this.sections.forEach((section, sectionId) => {
      if (!section.element.parentNode) {
        issues.push(`Section ${sectionId} is not in the DOM`)
      }

      if (!section.header.getAttribute('aria-expanded')) {
        issues.push(`Section ${sectionId} missing aria-expanded attribute`)
      }

      if (!section.content.getAttribute('aria-hidden')) {
        issues.push(`Section ${sectionId} missing aria-hidden attribute`)
      }
    })

    return {
      isValid: issues.length === 0,
      issues
    }
  }

  /**
     * Clean up resources
     */
  destroy () {
    // Clear all sections
    this.sections.clear()

    this.isInitialized = false

    console.log('[Accordion] Destroyed')
  }
}
