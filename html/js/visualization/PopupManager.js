/**
 * PopupManager - Precinct Popup Content and Chart Generation
 *
 * Handles:
 * - Dynamic popup content generation
 * - Chart.js integration for candidate results
 * - Precinct information display
 * - Performance optimization and chart cleanup
 * - Candidate detection and data formatting
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'

export class PopupManager {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // Chart management
    this.activeCharts = new Map()
    this.chartCleanupTimeouts = new Map()

    console.log('[PopupManager] Initialized')
  }

  /**
     * Create popup content for a precinct
     */
  createPopupContent (properties) {
    try {
      // Extract candidate information
      const candidates = this.extractCandidates(properties)

      // Generate unique popup ID
      const popupId = 'popup-' + Math.random().toString(36).substr(2, 9)

      // Build popup HTML
      let content = this.buildPopupHeader(properties)
      content += this.buildPopupStats(properties)

      // Add chart if candidates exist
      if (candidates.length > 0) {
        content += this.buildChartContainer(popupId)

        // Schedule chart creation after DOM insertion
        this.scheduleChartCreation(popupId, candidates, properties)
      }

      content += '</div>'

      return content
    } catch (error) {
      console.error('[PopupManager] Failed to create popup content:', error)
      return this.createErrorPopup(properties)
    }
  }

  /**
     * Extract candidate information from properties
     */
  extractCandidates (properties) {
    const candidates = []

    Object.keys(properties).forEach(prop => {
      if (prop.startsWith('vote_pct_') &&
                !prop.startsWith('vote_pct_contribution_') &&
                prop !== 'vote_pct_contribution_total_votes') {
        const candidateName = prop.replace('vote_pct_', '')
        const countProp = `votes_${candidateName}`
        const pctValue = properties[prop]
        const countValue = properties[countProp] || 0

        if (countValue > 0) {
          let displayName = this.formatCandidateName(candidateName)

          // Robust fallback for display names
          if (!displayName || displayName.trim() === '' ||
                        displayName === 'undefined' || displayName === 'null') {
            displayName = this.toTitleCase(candidateName) ||
                                    candidateName.replace(/_/g, ' ') ||
                                    'Unknown Candidate'
          }

          candidates.push({
            name: candidateName,
            displayName,
            count: countValue,
            pct: pctValue
          })
        }
      }
    })

    // Sort candidates by vote count (descending)
    candidates.sort((a, b) => b.count - a.count)

    return candidates
  }

  /**
     * Build popup header with precinct information
     */
  buildPopupHeader (properties) {
    return `
            <div style="width: 300px; max-width: 300px;">
                <h3>Precinct ${properties.precinct}</h3>
        `
  }

  /**
     * Build popup statistics section
     */
  buildPopupStats (properties) {
    return `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px;">
                <div>
                    <strong>Results:</strong><br>
                    <small>Total Votes: ${properties.votes_total || 'N/A'}</small><br>
                    <small>Turnout: ${properties.turnout_rate ? properties.turnout_rate.toFixed(1) + '%' : 'N/A'}</small><br>
                    <small>Leading: ${properties.leading_candidate ? this.formatCandidateName(properties.leading_candidate) : 'N/A'}</small>
                </div>
                <div>
                    <strong>Analysis:</strong><br>
                    <small>Political Lean: ${properties.political_lean || 'N/A'}</small><br>
                    <small>Competitiveness: ${properties.competitiveness || 'N/A'}</small><br>
                    <small>Margin: ${properties.vote_margin ? properties.vote_margin.toFixed(0) + ' votes' : 'N/A'}</small>
                </div>
            </div>
        `
  }

  /**
     * Build chart container HTML
     */
  buildChartContainer (popupId) {
    return `
            <div>
                <strong>Candidate Results:</strong>
                <div style="width: 280px; height: 180px; margin-top: 10px;">
                    <canvas id="${popupId}" class="popup-chart"></canvas>
                </div>
            </div>
        `
  }

  /**
     * Schedule chart creation after DOM insertion
     */
  scheduleChartCreation (popupId, candidates, properties) {
    setTimeout(() => {
      this.createChart(popupId, candidates, properties)
    }, 100)
  }

  /**
     * Create Chart.js chart for candidates
     */
  createChart (popupId, candidates, properties) {
    const canvas = document.getElementById(popupId)
    if (!canvas || candidates.length === 0) {
      console.warn(`[PopupManager] Chart canvas not found or no candidates: ${popupId}`)
      return
    }

    try {
      console.log(`[PopupManager] Creating chart for ${popupId} with candidates:`,
        candidates.map(c => c.displayName))

      // Set canvas dimensions
      canvas.width = 280
      canvas.height = 180

      // Get candidate colors
      const chartColors = this.getCandidateColors(candidates)

      // Clean up any existing chart
      this.cleanupChart(popupId)

      // Create new chart
      const chart = new Chart(canvas, {
        type: 'bar',
        data: {
          labels: candidates.map(c => c.displayName),
          datasets: [{
            label: 'Votes',
            data: candidates.map(c => c.count),
            backgroundColor: chartColors,
            borderColor: chartColors,
            borderWidth: 1
          }]
        },
        options: {
          responsive: false,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                afterLabel: function (context) {
                  const candidate = candidates[context.dataIndex]
                  return `${candidate.pct.toFixed(1)}%`
                }
              }
            }
          },
          scales: {
            y: {
              beginAtZero: true,
              title: { display: true, text: 'Votes' }
            }
          }
        }
      })

      // Store chart reference for cleanup
      this.activeCharts.set(popupId, chart)

      // Schedule cleanup after chart is no longer needed
      this.scheduleChartCleanup(popupId)

      console.log(`[PopupManager] Chart created successfully: ${popupId}`)
    } catch (error) {
      console.error(`[PopupManager] Failed to create chart ${popupId}:`, error)
      this.createFallbackChart(canvas, candidates)
    }
  }

  /**
     * Get colors for chart candidates
     */
  getCandidateColors (candidates) {
    const electionData = this.stateManager.getState('electionData')
    const defaultColor = '#cccccc'

    return candidates.map(candidate => {
      const normalizedName = this.normalizeCandidateName(candidate.name)
      let color = defaultColor

      // Try to get color from metadata
      if (electionData?.metadata?.candidate_colors) {
        if (electionData.metadata.candidate_colors[candidate.name]) {
          color = electionData.metadata.candidate_colors[candidate.name]
        } else if (electionData.metadata.candidate_colors[normalizedName]) {
          color = electionData.metadata.candidate_colors[normalizedName]
        }
      }

      // Try to get color from color schemes
      const colorSchemes = this.stateManager.getState('colorSchemes')
      if (colorSchemes?.leading_candidate?.[normalizedName]) {
        color = colorSchemes.leading_candidate[normalizedName]
      }

      return color
    })
  }

  /**
     * Create fallback chart when Chart.js fails
     */
  createFallbackChart (canvas, candidates) {
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    try {
      // Clear canvas
      ctx.clearRect(0, 0, canvas.width, canvas.height)

      // Draw simple text fallback
      ctx.fillStyle = '#333'
      ctx.font = '14px Arial'
      ctx.textAlign = 'center'
      ctx.fillText('Chart Error', canvas.width / 2, canvas.height / 2 - 10)
      ctx.fillText('Check console for details', canvas.width / 2, canvas.height / 2 + 10)

      // Draw candidate data as text
      let y = 40
      candidates.forEach((candidate, index) => {
        if (y < canvas.height - 20) {
          ctx.fillStyle = '#666'
          ctx.font = '12px Arial'
          ctx.textAlign = 'left'
          ctx.fillText(`${candidate.displayName}: ${candidate.count} (${candidate.pct.toFixed(1)}%)`,
            10, y)
          y += 20
        }
      })
    } catch (error) {
      console.error('[PopupManager] Fallback chart creation failed:', error)
    }
  }

  /**
     * Schedule chart cleanup to prevent memory leaks
     */
  scheduleChartCleanup (popupId) {
    // Clear any existing cleanup timeout
    if (this.chartCleanupTimeouts.has(popupId)) {
      clearTimeout(this.chartCleanupTimeouts.get(popupId))
    }

    // Schedule cleanup after 30 seconds
    const timeoutId = setTimeout(() => {
      this.cleanupChart(popupId)
    }, 30000)

    this.chartCleanupTimeouts.set(popupId, timeoutId)
  }

  /**
     * Clean up chart to prevent memory leaks
     */
  cleanupChart (popupId) {
    try {
      // Clean up chart instance
      if (this.activeCharts.has(popupId)) {
        const chart = this.activeCharts.get(popupId)
        chart.destroy()
        this.activeCharts.delete(popupId)
        console.log(`[PopupManager] Cleaned up chart: ${popupId}`)
      }

      // Clear cleanup timeout
      if (this.chartCleanupTimeouts.has(popupId)) {
        clearTimeout(this.chartCleanupTimeouts.get(popupId))
        this.chartCleanupTimeouts.delete(popupId)
      }
    } catch (error) {
      console.error(`[PopupManager] Failed to cleanup chart ${popupId}:`, error)
    }
  }

  /**
     * Clean up all charts
     */
  cleanupAllCharts () {
    console.log(`[PopupManager] Cleaning up ${this.activeCharts.size} active charts`)

    // Destroy all active charts
    this.activeCharts.forEach((chart, popupId) => {
      try {
        chart.destroy()
      } catch (error) {
        console.error(`[PopupManager] Error destroying chart ${popupId}:`, error)
      }
    })

    // Clear all timeouts
    this.chartCleanupTimeouts.forEach((timeoutId) => {
      clearTimeout(timeoutId)
    })

    // Clear maps
    this.activeCharts.clear()
    this.chartCleanupTimeouts.clear()
  }

  /**
     * Create error popup when content generation fails
     */
  createErrorPopup (properties) {
    return `
            <div style="width: 250px;">
                <h3>Precinct ${properties.precinct || 'Unknown'}</h3>
                <p style="color: #d32f2f;">
                    <strong>Error loading precinct data</strong>
                </p>
                <p style="font-size: 12px; color: #666;">
                    Basic information may be unavailable. Please try refreshing the page.
                </p>
            </div>
        `
  }

  /**
     * Format candidate name for display
     */
  formatCandidateName (candidateName) {
    const processedData = this.stateManager.getState('processedData')

    // Try to get display name from field info
    if (processedData?.fieldInfo?.displayNames) {
      const voteField = `vote_pct_${candidateName}`
      if (processedData.fieldInfo.displayNames[voteField]) {
        // Extract candidate name from display name like "Vote % - John Doe"
        const displayName = processedData.fieldInfo.displayNames[voteField]
        const match = displayName.match(/Vote % - (.+)/)
        if (match) {
          return match[1]
        }
      }
    }

    // Fallback to title case conversion
    return this.toTitleCase(candidateName)
  }

  /**
     * Convert snake_case to Title Case
     */
  toTitleCase (str) {
    if (!str) return ''
    return str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
  }

  /**
     * Normalize candidate name for consistent lookup
     */
  normalizeCandidateName (name) {
    if (!name) return ''
    return name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '')
  }

  /**
     * Get active chart count for monitoring
     */
  getActiveChartCount () {
    return this.activeCharts.size
  }

  /**
     * Get chart statistics
     */
  getChartStats () {
    return {
      activeCharts: this.activeCharts.size,
      pendingCleanups: this.chartCleanupTimeouts.size,
      chartIds: Array.from(this.activeCharts.keys())
    }
  }

  /**
     * Force cleanup of stale charts (older than threshold)
     */
  cleanupStaleCharts (maxAge = 60000) {
    const now = Date.now()
    const staleCharts = []

    // Identify stale charts (this is simplified - in a real implementation,
    // you'd track creation timestamps)
    this.activeCharts.forEach((chart, popupId) => {
      // For now, just cleanup charts that don't have DOM elements
      if (!document.getElementById(popupId)) {
        staleCharts.push(popupId)
      }
    })

    // Clean up stale charts
    staleCharts.forEach(popupId => {
      console.log(`[PopupManager] Cleaning up stale chart: ${popupId}`)
      this.cleanupChart(popupId)
    })

    return staleCharts.length
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.cleanupAllCharts()
    console.log('[PopupManager] Destroyed')
  }
}
