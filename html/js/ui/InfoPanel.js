/**
 * InfoPanel.js - Information Panel Management
 *
 * Manages the right-side information panel that displays:
 * - Election summary statistics
 * - Precinct information
 * - Candidate results breakdown
 * - Detailed voting data when a precinct is selected
 */

export class InfoPanel {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    this.container = null
    this.statsSection = null
    this.precinctSection = null

    this.initialized = false
    this.lastUpdatedStats = null
  }

  /**
     * Initialize the InfoPanel component
     */
  async init () {
    try {
      this.container = document.querySelector('.info-panel')
      if (!this.container) {
        throw new Error('Info panel container not found')
      }

      this.statsSection = document.getElementById('stats-summary')
      this.precinctSection = document.getElementById('precinct-info')
      this.insightsSection = document.getElementById('insights-content')
      this.targetSection = document.getElementById('target-list')
      this.deploymentSection = document.getElementById('deployment-content')
      this.oppositionSection = document.getElementById('opposition-content')
      this.coalitionSection = document.getElementById('coalition-content')
      this.legendSection = document.getElementById('integrated-legend')

      if (!this.statsSection || !this.precinctSection) {
        throw new Error('Required info panel sections not found')
      }

      this.setupEventListeners()
      this.setupTabHandlers()
      this.updateTitle()
      await this.loadInitialData()

      this.initialized = true
      console.log('✅ InfoPanel initialized')
    } catch (error) {
      console.error('❌ InfoPanel initialization failed:', error)
      throw error
    }
  }

  /**
     * Set up event listeners
     */
  setupEventListeners () {
    // Listen for data ready events to update statistics
    this.eventBus.on('data:ready', (data) => {
      console.log('[InfoPanel] Data ready, updating stats')
      this.updateStatsFromData(data)
    })

    // Listen for layer changes to update statistics
    this.eventBus.on('map:layerChanged', (data) => {
      console.log('[InfoPanel] Layer changed, updating stats')
      this.updateStatsFromLayer(data.layerKey)
    })

    // Listen for legend updates to integrate into panel
    this.eventBus.on('legend:updated', (data) => {
      console.log('[InfoPanel] Legend updated, integrating into panel')
      this.updateIntegratedLegend(data)
    })

    // Listen for state changes to update stats and layer info
    this.stateManager.subscribe('currentField', () => {
      console.log('[InfoPanel] Current field changed, updating stats and layer info')
      this.updateTitle() // Update title when field changes
      this.updateStatsFromCurrentState()
      const currentField = this.stateManager.getState('currentField')
      this.showLayerStatistics(currentField)
    })

    this.stateManager.subscribe('electionData', () => {
      console.log('[InfoPanel] Election data changed, updating stats')
      this.updateStatsFromCurrentState()
    })

    this.stateManager.subscribe('showPpsOnly', () => {
      console.log('[InfoPanel] Filter changed, updating stats')
      this.updateStatsFromCurrentState()
    })

    this.stateManager.subscribe('currentDataset', () => {
      console.log('[InfoPanel] Dataset changed, updating title and stats')
      this.updateTitle()
      this.updateStatsFromCurrentState()
    })

    // Listen for feature clicks to show precinct details
    this.eventBus.on('map:featureClick', (data) => {
      console.log('[InfoPanel] Feature clicked, showing details:', data)
      this.showPrecinctDetails(data)
    })

    // Listen for feature hover - no longer used for InfoPanel
    // Hover info will be handled by tooltips instead

    // Listen for mouse out - no longer used for InfoPanel
    // InfoPanel will maintain persistent layer statistics

    console.log('[InfoPanel] Event listeners set up')
  }

  /**
   * Set up tab handlers for the info panel
   */
  setupTabHandlers () {
    const tabs = document.querySelectorAll('.info-panel-tab')
    const tabContents = document.querySelectorAll('.tab-content')

    tabs.forEach(tab => {
      tab.addEventListener('click', () => {
        const targetTab = tab.dataset.tab

        // Remove active class from all tabs and contents
        tabs.forEach(t => t.classList.remove('active'))
        tabContents.forEach(content => {
          content.classList.remove('active')
          content.style.display = 'none'
        })

        // Add active class to clicked tab and show content
        tab.classList.add('active')
        const targetContent = document.getElementById(`tab-${targetTab}`)
        if (targetContent) {
          targetContent.classList.add('active')
          targetContent.style.display = 'block'
        }

        // Load content for the active tab
        this.loadTabContent(targetTab)
      })
    })
  }

  /**
   * Load content for a specific tab
   */
  loadTabContent (tabName) {
    switch (tabName) {
      case 'overview':
        this.showPlaceholderInsights()
        break
      case 'targeting':
        this.showPlaceholderTargeting()
        break
      case 'strategy':
        this.showPlaceholderStrategy()
        break
      case 'details':
        // Details tab content is already loaded
        break
    }
  }

  /**
   * Update the integrated legend in the right panel
   */
  updateIntegratedLegend (legendData) {
    if (!this.legendSection || !legendData) return

    try {
      if (legendData.type === 'continuous') {
        this.legendSection.innerHTML = `
          <div class="integrated-legend-title">${legendData.title}</div>
          <div class="integrated-legend-bar" style="background: ${legendData.gradient}"></div>
          <div class="integrated-legend-labels">
            <span>${legendData.min}</span>
            <span>${legendData.max}</span>
          </div>
        `
      } else if (legendData.type === 'categorical') {
        const items = legendData.items.map(item => `
          <div class="legend-item" style="display: flex; align-items: center; gap: var(--space-2); margin-bottom: var(--space-1);">
            <div class="legend-color-dot" style="background: ${item.color}"></div>
            <span style="font-size: var(--font-size-xs);">${item.label}</span>
          </div>
        `).join('')

        this.legendSection.innerHTML = `
          <div class="integrated-legend-title">${legendData.title}</div>
          <div style="display: flex; flex-direction: column; gap: var(--space-1);">
            ${items}
          </div>
        `
      } else {
        this.legendSection.innerHTML = `
          <div class="integrated-legend-title">No Legend Available</div>
          <p style="font-size: var(--font-size-xs); color: var(--color-text-secondary); font-style: italic;">
            Select a data layer to see the legend
          </p>
        `
      }
    } catch (error) {
      console.error('Error updating integrated legend:', error)
      this.legendSection.innerHTML = `
        <div class="integrated-legend-title">Legend Error</div>
        <p style="font-size: var(--font-size-xs); color: var(--color-error);">
          Unable to display legend
        </p>
      `
    }
  }

  /**
   * Show placeholder content for insights (until analytics are implemented)
   */
  showPlaceholderInsights () {
    if (this.insightsSection) {
      this.insightsSection.innerHTML = `
        <div class="insight-item">
          <span class="insight-priority high">Quick Wins</span>
          Identify precincts where small turnout increases could flip results
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Base Mobilization</span>
          Find strong supporter areas with low turnout for voter mobilization
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Persuadable Zones</span>
          Locate areas with high NAV registration or split voting patterns
        </div>
        <div class="insight-item">
          <span class="insight-priority low">Layer Info</span>
          Currently viewing: <strong>${this.getLayerDisplayName(this.stateManager.getState('currentField') || 'political_lean')}</strong>
        </div>
      `
    }
  }

  /**
   * Show placeholder content for targeting (until analytics are implemented)
   */
  showPlaceholderTargeting () {
    if (this.targetSection) {
      this.targetSection.innerHTML = `
        <li class="target-item">
          <span class="target-precinct">Precinct 101</span>
          <span class="target-score">High</span>
        </li>
        <li class="target-item">
          <span class="target-precinct">Precinct 205</span>
          <span class="target-score">High</span>
        </li>
        <li class="target-item">
          <span class="target-precinct">Precinct 143</span>
          <span class="target-score">Med</span>
        </li>
        <li class="target-item">
          <span class="target-precinct">Precinct 089</span>
          <span class="target-score">Med</span>
        </li>
        <li class="target-item">
          <span class="target-precinct">Precinct 167</span>
          <span class="target-score">Med</span>
        </li>
      `
    }

    if (this.deploymentSection) {
      this.deploymentSection.innerHTML = `
        <div class="insight-item">
          <span class="insight-priority high">Door-to-Door Priority</span>
          Focus on high-density residential areas with competitive voting patterns
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Phone Bank Targets</span>
          Reach voters in precincts with high turnout potential but lower accessibility
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Event Locations</span>
          Optimal locations for rallies and town halls based on supporter density
        </div>
        <div class="insight-item">
          <span class="insight-priority low">Resource Efficiency</span>
          Cost-per-vote analysis to maximize impact with limited campaign budget
        </div>
        <div class="action-buttons">
          <button class="action-btn primary" onclick="alert('Export targeting data - Coming Soon!')">📊 Export List</button>
          <button class="action-btn" onclick="alert('Share targeting plan - Coming Soon!')">🔗 Share</button>
        </div>
      `
    }
  }

  /**
   * Show placeholder content for strategy (until analytics are implemented)
   */
  showPlaceholderStrategy () {
    if (this.oppositionSection) {
      this.oppositionSection.innerHTML = `
        <div class="insight-item">
          <span class="insight-priority high">Opponent Vulnerabilities</span>
          Identify precincts where leading opponent shows weakness or declining support
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Strength Analysis</span>
          Map opponent strongholds to understand their base and avoid wasted resources
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Demographic Patterns</span>
          Correlate voter demographics with candidate preferences for targeted messaging
        </div>
        <div class="insight-item">
          <span class="insight-priority low">Trend Analysis</span>
          Track momentum shifts and identify areas of growing/declining support
        </div>
      `
    }

    if (this.coalitionSection) {
      this.coalitionSection.innerHTML = `
        <div class="insight-item">
          <span class="insight-priority high">Progressive Alliance</span>
          Find precincts where multiple progressive candidates/issues perform well
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Issue-Based Mapping</span>
          Identify communities likely to care about schools, housing, and local issues
        </div>
        <div class="insight-item">
          <span class="insight-priority medium">Stakeholder Zones</span>
          Locate areas with key community leaders and organizational presence
        </div>
        <div class="insight-item">
          <span class="insight-priority low">Demographic Clustering</span>
          Find communities of similar voters for coordinated outreach efforts
        </div>
      `
    }
  }

  /**
     * Load initial data and display default information
     */
  async loadInitialData () {
    const currentLayer = this.stateManager.getState('currentField')
    const precinctData = this.stateManager.getState('electionData')

    if (precinctData && currentLayer) {
      this.updateStats({
        layer: currentLayer,
        data: precinctData
      })
      this.showLayerStatistics(currentLayer)
    } else {
      this.showDefaultContent()
    }

    // Load placeholder content for the overview tab (initially active)
    this.showPlaceholderInsights()
  }

  /**
     * Update statistics display
     */
  updateStats (data) {
    if (!this.statsSection || !data) return

    try {
      const stats = this.calculateLayerStats(data)
      this.displayStats(stats)
      this.lastUpdatedStats = stats
    } catch (error) {
      console.error('Error updating stats:', error)
      this.showStatsError()
    }
  }

  /**
   * Update stats from data ready event
   */
  updateStatsFromData (data) {
    console.log('[InfoPanel] Updating stats from data event:', data)

    // Extract current layer and election data
    const currentLayer = this.stateManager.getState('currentField') || 'political_lean'
    const electionData = data.rawData || this.stateManager.getState('electionData')

    if (electionData && electionData.features) {
      this.updateStats({
        layer: currentLayer,
        data: electionData.features
      })
    }
  }

  /**
   * Update stats from layer change event
   */
  updateStatsFromLayer (layerKey) {
    console.log('[InfoPanel] Updating stats for layer:', layerKey)

    const electionData = this.stateManager.getState('electionData')
    if (electionData && electionData.features) {
      this.updateStats({
        layer: layerKey,
        data: electionData.features
      })
    }
  }

  /**
   * Update stats from current state
   */
  updateStatsFromCurrentState () {
    const currentField = this.stateManager.getState('currentField')
    const electionData = this.stateManager.getState('electionData')

    if (electionData && currentField) {
      this.updateStats({
        layer: currentField,
        data: electionData
      })
    }
  }

  /**
   * Update the panel title based on current dataset and layer
   */
  updateTitle () {
    const titleElement = document.getElementById('info-heading')
    if (!titleElement) return

    const currentDataset = this.stateManager.getState('currentDataset')
    const currentField = this.stateManager.getState('currentField')
    const datasets = this.stateManager.getState('datasets')

    // Get the main title
    let mainTitle = '2025 Portland Election Analysis'
    if (datasets && datasets[currentDataset]) {
      mainTitle = datasets[currentDataset].title || `Dataset: ${currentDataset}`
    } else if (currentDataset && currentDataset.startsWith('zone')) {
      const zoneNumber = currentDataset.replace('zone', '')
      mainTitle = `2025 School Board Zone ${zoneNumber}`
    } else if (currentDataset === 'bond') {
      mainTitle = '2025 Bond Election'
    } else if (currentDataset === 'voter_reg') {
      mainTitle = 'Voter Registration Data'
    }

    // Get the current layer context
    const layerName = this.getLayerDisplayName(currentField || 'political_lean')

    // Create dynamic title with context
    titleElement.innerHTML = `
      <div class="panel-title-main">${mainTitle}</div>
      <div class="panel-title-context">Currently viewing: ${layerName}</div>
    `
  }

  /**
     * Calculate statistics for the current layer
     */
  calculateLayerStats (data) {
    const precinctData = data.data || this.stateManager.getState('electionData')
    const currentLayer = data.layer || this.stateManager.getState('currentField')

    if (!precinctData || !currentLayer) {
      return null
    }

    const filteredData = this.getFilteredData(precinctData)

    // Calculate election-specific stats
    const electionStats = this.calculateElectionStats(filteredData)

    const stats = {
      totalPrecincts: filteredData.length,
      layerName: this.getLayerDisplayName(currentLayer),
      ...electionStats,
      ...this.calculateSpecificStats(filteredData, currentLayer)
    }

    return stats
  }

  /**
     * Get filtered data based on current filters
     */
  getFilteredData (data) {
    const ppsOnly = this.stateManager.getState('showPpsOnly')

    // Handle both array of features and GeoJSON structure
    const features = Array.isArray(data) ? data : (data.features || [])

    if (ppsOnly) {
      return features.filter(precinct =>
        precinct.properties && (precinct.properties.is_pps_precinct || precinct.properties.school_zone === 1)
      )
    }

    return features
  }

  /**
     * Calculate election-specific statistics
     */
  calculateElectionStats (data) {
    const stats = {}

    // Calculate total votes across all precincts
    let totalVotes = 0
    let totalRegistered = 0
    let precinctCount = 0

    // Detect candidates dynamically
    const candidateTotals = {}

    data.forEach(precinct => {
      const props = precinct.properties || {}

      // Add to totals
      if (props.votes_total) {
        totalVotes += props.votes_total
        precinctCount++
      }

      if (props.total_voters) {
        totalRegistered += props.total_voters
      }

      // Detect and sum candidate votes
      Object.keys(props).forEach(key => {
        if (key.startsWith('votes_') && key !== 'votes_total') {
          const candidateName = key.replace('votes_', '')
          if (!candidateTotals[candidateName]) {
            candidateTotals[candidateName] = 0
          }
          candidateTotals[candidateName] += props[key] || 0
        }
      })
    })

    stats.totalVotes = totalVotes
    stats.totalRegistered = totalRegistered
    stats.avgTurnout = totalRegistered > 0 ? (totalVotes / totalRegistered * 100) : 0
    stats.candidateTotals = candidateTotals

    return stats
  }

  /**
     * Calculate layer-specific statistics
     */
  calculateSpecificStats (data, layer) {
    const values = data.map(precinct => {
      const props = precinct.properties || {}
      return this.getValueForLayer(props, layer)
    }).filter(val => val !== null && val !== undefined && !isNaN(val))

    if (values.length === 0) {
      return { hasData: false }
    }

    const stats = {
      hasData: true,
      total: values.reduce((sum, val) => sum + val, 0),
      average: values.reduce((sum, val) => sum + val, 0) / values.length,
      min: Math.min(...values),
      max: Math.max(...values),
      count: values.length
    }

    // Add layer-specific calculations
    if (layer.includes('turnout')) {
      stats.averageTurnout = `${stats.average.toFixed(1)}%`
    } else if (layer.includes('votes') || layer.includes('total')) {
      stats.totalVotes = stats.total.toLocaleString()
    }

    return stats
  }

  /**
     * Extract value for a specific layer from precinct properties
     */
  getValueForLayer (properties, layer) {
    // Handle different layer types
    if (layer.includes('turnout_pct')) {
      return properties.turnout_pct
    } else if (layer.includes('total_votes')) {
      return properties.total_votes
    } else if (layer.includes('registered_voters')) {
      return properties.registered_voters
    } else if (layer.startsWith('candidate_')) {
      const candidateField = layer.replace('candidate_', '')
      return properties[candidateField]
    }

    return properties[layer] || null
  }

  /**
     * Get human-readable layer name
     */
  getLayerDisplayName (layer) {
    const layerNames = {
      turnout_pct: 'Voter Turnout',
      total_votes: 'Total Votes',
      registered_voters: 'Registered Voters',
      margin: 'Victory Margin'
    }

    if (layerNames[layer]) {
      return layerNames[layer]
    }

    if (layer.startsWith('candidate_')) {
      const candidate = layer.replace('candidate_', '').replace(/_/g, ' ')
      return `${candidate} Votes`
    }

    return layer.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
  }

  /**
     * Display calculated statistics
     */
  displayStats (stats) {
    if (!stats) {
      this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>No data available for the current selection.</p>
            `
      return
    }

    const formatValue = (value) => {
      if (typeof value === 'number') {
        return value >= 1000 ? value.toLocaleString() : value.toFixed(1)
      }
      return value
    }

    let tableContent = `
            <h4>Election Summary</h4>
            <table>
                <tr>
                    <th>Precincts:</th>
                    <td>${stats.totalPrecincts}</td>
                </tr>
    `

    // Add election-specific stats if available
    if (stats.totalVotes > 0) {
      tableContent += `
                <tr>
                    <th>Total Votes:</th>
                    <td>${stats.totalVotes.toLocaleString()}</td>
                </tr>
                <tr>
                    <th>Avg Turnout:</th>
                    <td>${stats.avgTurnout.toFixed(1)}%</td>
                </tr>
      `

      // Add candidate totals
      if (stats.candidateTotals && Object.keys(stats.candidateTotals).length > 0) {
        Object.entries(stats.candidateTotals)
          .sort(([, a], [, b]) => b - a) // Sort by vote count descending
          .forEach(([candidate, votes]) => {
            const percentage = stats.totalVotes > 0 ? (votes / stats.totalVotes * 100).toFixed(1) : '0.0'
            const displayName = candidate.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
            tableContent += `
                <tr>
                    <th>${displayName}:</th>
                    <td>${votes.toLocaleString()} (${percentage}%)</td>
                </tr>
            `
          })
      }
    } else if (stats.hasData !== false) {
      // Show layer-specific stats for non-election data
      tableContent += `
                <tr>
                    <th>Layer:</th>
                    <td>${stats.layerName}</td>
                </tr>
      `

      if (stats.average !== undefined) {
        tableContent += `
                <tr>
                    <th>Average:</th>
                    <td>${formatValue(stats.average)}</td>
                </tr>
        `
      }

      if (stats.min !== undefined && stats.max !== undefined) {
        tableContent += `
                <tr>
                    <th>Range:</th>
                    <td>${formatValue(stats.min)} - ${formatValue(stats.max)}</td>
                </tr>
        `
      }
    }

    tableContent += '</table>'
    this.statsSection.innerHTML = tableContent
  }

  /**
     * Show layer-specific helpful statistics
     */
  showLayerStatistics (currentField) {
    if (!this.precinctSection) return

    try {
      const layerStats = this.calculateLayerSpecificStats(currentField)
      const displayName = this.getLayerDisplayName(currentField)

      let content = '<h4>Layer Information</h4>'
      content += `<p><strong>Current Layer:</strong> ${displayName}</p>`

      if (layerStats.description) {
        content += `<p><em>${layerStats.description}</em></p>`
      }

      if (layerStats.insights && layerStats.insights.length > 0) {
        content += '<div class="layer-insights">'
        content += '<h5>Key Insights:</h5>'
        content += '<ul>'
        layerStats.insights.forEach(insight => {
          content += `<li>${insight}</li>`
        })
        content += '</ul></div>'
      }

      content += '<p class="help-text"><em>Hover over precincts for quick info, click for detailed results</em></p>'

      this.precinctSection.innerHTML = content
    } catch (error) {
      console.error('Error showing layer statistics:', error)
      this.showDefaultLayerInfo()
    }
  }

  /**
     * Calculate layer-specific statistics and insights
     */
  calculateLayerSpecificStats (currentField) {
    const electionData = this.stateManager.getState('electionData')
    const filteredData = this.getFilteredData(electionData)

    const stats = {
      description: this.getLayerDescription(currentField),
      insights: []
    }

    if (!filteredData || filteredData.length === 0) {
      return stats
    }

    // Calculate insights based on field type
    if (currentField === 'turnout_rate' || currentField === 'turnout_pct') {
      stats.insights = this.calculateTurnoutInsights(filteredData)
    } else if (currentField === 'leading_candidate') {
      stats.insights = this.calculateCandidateInsights(filteredData)
    } else if (currentField.startsWith('votes_')) {
      stats.insights = this.calculateVoteCountInsights(filteredData, currentField)
    } else if (currentField.startsWith('vote_pct_')) {
      stats.insights = this.calculateVotePercentageInsights(filteredData, currentField)
    } else if (currentField === 'political_lean') {
      stats.insights = this.calculatePoliticalLeanInsights(filteredData)
    } else if (currentField === 'competitiveness') {
      stats.insights = this.calculateCompetitivenessInsights(filteredData)
    } else if (currentField === 'precinct_size_category') {
      stats.insights = this.calculatePrecinctSizeInsights(filteredData)
    } else if (currentField === 'margin_category') {
      stats.insights = this.calculateMarginInsights(filteredData)
    }

    return stats
  }

  /**
     * Get description for a layer
     */
  getLayerDescription (field) {
    const descriptions = {
      turnout_rate: 'Shows voter participation rates across precincts',
      leading_candidate: 'Displays which candidate won each precinct',
      political_lean: 'Historical voting patterns from Democratic to Republican',
      competitiveness: 'How competitive each precinct typically is in elections',
      precinct_size_category: 'Precincts grouped by number of registered voters',
      margin_category: 'Victory margins categorized by closeness',
      none: 'Base map showing precinct boundaries only'
    }

    if (field && field.startsWith('votes_')) {
      const candidate = field.replace('votes_', '').replace(/_/g, ' ')
      return `Vote counts for ${candidate} across precincts`
    }

    if (field && field.startsWith('vote_pct_')) {
      const candidate = field.replace('vote_pct_', '').replace(/_/g, ' ')
      return `Vote percentages for ${candidate} across precincts`
    }

    return descriptions[field] || 'Data visualization for selected field'
  }

  /**
     * Calculate turnout-specific insights
     */
  calculateTurnoutInsights (data) {
    const turnoutValues = data.map(f => f.properties.turnout_rate || f.properties.turnout_pct).filter(v => v != null)
    if (turnoutValues.length === 0) return []

    const avg = turnoutValues.reduce((a, b) => a + b, 0) / turnoutValues.length
    const max = Math.max(...turnoutValues)
    const min = Math.min(...turnoutValues)
    const highTurnout = turnoutValues.filter(v => v > 70).length
    const lowTurnout = turnoutValues.filter(v => v < 40).length

    return [
      `Average turnout: ${avg.toFixed(1)}%`,
      `Range: ${min.toFixed(1)}% to ${max.toFixed(1)}%`,
      `${highTurnout} precincts with high turnout (>70%)`,
      `${lowTurnout} precincts with low turnout (<40%)`
    ]
  }

  /**
     * Calculate candidate-specific insights
     */
  calculateCandidateInsights (data) {
    const candidates = {}
    data.forEach(f => {
      const leader = f.properties.leading_candidate
      if (leader && leader !== 'Tie' && leader !== 'No Data') {
        candidates[leader] = (candidates[leader] || 0) + 1
      }
    })

    const insights = []
    const sortedCandidates = Object.entries(candidates).sort(([, a], [, b]) => b - a)

    if (sortedCandidates.length > 0) {
      const [topCandidate, topCount] = sortedCandidates[0]
      const totalPrecincts = data.length
      const percentage = ((topCount / totalPrecincts) * 100).toFixed(1)

      insights.push(`${topCandidate.replace(/_/g, ' ')} leads in ${topCount} precincts (${percentage}%)`)

      if (sortedCandidates.length > 1) {
        sortedCandidates.slice(0, 3).forEach(([candidate, count]) => {
          const pct = ((count / totalPrecincts) * 100).toFixed(1)
          insights.push(`${candidate.replace(/_/g, ' ')}: ${count} precincts (${pct}%)`)
        })
      }
    }

    return insights
  }

  /**
     * Calculate vote count insights
     */
  calculateVoteCountInsights (data, field) {
    const values = data.map(f => f.properties[field]).filter(v => v != null && v > 0)
    if (values.length === 0) return []

    const total = values.reduce((a, b) => a + b, 0)
    const avg = total / values.length
    const max = Math.max(...values)
    const min = Math.min(...values)

    return [
      `Total votes: ${total.toLocaleString()}`,
      `Average per precinct: ${Math.round(avg).toLocaleString()}`,
      `Highest: ${max.toLocaleString()} votes`,
      `Lowest: ${min.toLocaleString()} votes`
    ]
  }

  /**
     * Calculate vote percentage insights
     */
  calculateVotePercentageInsights (data, field) {
    const values = data.map(f => f.properties[field]).filter(v => v != null)
    if (values.length === 0) return []

    const avg = values.reduce((a, b) => a + b, 0) / values.length
    const max = Math.max(...values)
    const min = Math.min(...values)
    const strongPrecincts = values.filter(v => v > 60).length

    return [
      `Average: ${avg.toFixed(1)}%`,
      `Range: ${min.toFixed(1)}% to ${max.toFixed(1)}%`,
      `${strongPrecincts} precincts with >60% support`,
      `Competitive in ${values.filter(v => v >= 40 && v <= 60).length} precincts`
    ]
  }

  /**
     * Calculate political lean insights
     */
  calculatePoliticalLeanInsights (data) {
    const leanCounts = {}
    data.forEach(f => {
      const lean = f.properties.political_lean
      if (lean) {
        leanCounts[lean] = (leanCounts[lean] || 0) + 1
      }
    })

    const insights = []
    Object.entries(leanCounts).forEach(([lean, count]) => {
      const pct = ((count / data.length) * 100).toFixed(1)
      insights.push(`${lean}: ${count} precincts (${pct}%)`)
    })

    return insights
  }

  /**
     * Calculate competitiveness insights
     */
  calculateCompetitivenessInsights (data) {
    const compCounts = {}
    data.forEach(f => {
      const comp = f.properties.competitiveness
      if (comp) {
        compCounts[comp] = (compCounts[comp] || 0) + 1
      }
    })

    const insights = []
    const competitive = (compCounts.Competitive || 0) + (compCounts.Tossup || 0)
    const safe = (compCounts.Safe || 0) + (compCounts.Likely || 0)

    insights.push(`${competitive} competitive precincts`)
    insights.push(`${safe} safe precincts`)

    return insights
  }

  /**
     * Calculate precinct size insights
     */
  calculatePrecinctSizeInsights (data) {
    const sizeCounts = {}
    data.forEach(f => {
      const size = f.properties.precinct_size_category
      if (size) {
        sizeCounts[size] = (sizeCounts[size] || 0) + 1
      }
    })

    const insights = []
    Object.entries(sizeCounts).forEach(([size, count]) => {
      const pct = ((count / data.length) * 100).toFixed(1)
      insights.push(`${size}: ${count} precincts (${pct}%)`)
    })

    return insights
  }

  /**
     * Calculate margin insights
     */
  calculateMarginInsights (data) {
    const marginCounts = {}
    data.forEach(f => {
      const margin = f.properties.margin_category
      if (margin) {
        marginCounts[margin] = (marginCounts[margin] || 0) + 1
      }
    })

    const insights = []
    const close = (marginCounts['Very Close'] || 0) + (marginCounts.Close || 0)
    const decisive = (marginCounts.Clear || 0) + (marginCounts.Landslide || 0)

    insights.push(`${close} close races`)
    insights.push(`${decisive} decisive victories`)

    return insights
  }

  /**
     * Show default layer information
     */
  showDefaultLayerInfo () {
    if (!this.precinctSection) return

    this.precinctSection.innerHTML = `
      <h4>Layer Information</h4>
      <p>Select a data layer to see relevant statistics and insights.</p>
      <p class="help-text"><em>Hover over precincts for quick info, click for detailed results</em></p>
    `
  }

  /**
     * Show detailed information for a selected precinct
     */
  showPrecinctDetails (data) {
    if (!this.precinctSection || !data) return

    try {
      const props = data.properties || data
      const precinctProps = props.properties || props

      this.precinctSection.innerHTML = `
                <h4>Precinct ${precinctProps.precinct || 'N/A'}</h4>

                <div class="precinct-details">
                    <table>
                        <tr>
                            <th>Registered Voters:</th>
                            <td>${(precinctProps.registered_voters || precinctProps.total_voters || 0).toLocaleString()}</td>
                        </tr>
                        <tr>
                            <th>Total Votes:</th>
                            <td>${(precinctProps.total_votes || precinctProps.votes_total || 0).toLocaleString()}</td>
                        </tr>
                        <tr>
                            <th>Turnout:</th>
                            <td>${(precinctProps.turnout_pct || precinctProps.turnout_rate || 0).toFixed(1)}%</td>
                        </tr>
                        ${precinctProps.school_zone || precinctProps.is_pps_precinct
? `
                        <tr>
                            <th>School Zone:</th>
                            <td>${precinctProps.school_zone ? `Zone ${precinctProps.school_zone}` : 'PPS District'}</td>
                        </tr>
                        `
: ''}
                    </table>

                    ${this.renderCandidateResults(precinctProps)}
                </div>
            `
    } catch (error) {
      console.error('Error showing precinct details:', error)
      this.precinctSection.innerHTML = `
                <h4>Precinct Information</h4>
                <p>Error loading precinct details.</p>
            `
    }
  }

  /**
     * Render candidate results for the precinct
     */
  renderCandidateResults (properties) {
    const candidateFields = Object.keys(properties).filter(key =>
      key.startsWith('candidate_') ||
            (key.includes('_') && !['total_votes', 'registered_voters', 'turnout_pct', 'school_zone', 'precinct'].includes(key))
    )

    if (candidateFields.length === 0) {
      return ''
    }

    const candidates = candidateFields.map(field => ({
      name: field.replace(/^candidate_/, '').replace(/_/g, ' '),
      votes: properties[field] || 0
    })).sort((a, b) => b.votes - a.votes)

    const totalVotes = candidates.reduce((sum, c) => sum + c.votes, 0)

    return `
            <h5>Candidate Results</h5>
            <table class="candidate-results">
                ${candidates.map(candidate => `
                    <tr>
                        <th>${candidate.name}:</th>
                        <td>${candidate.votes.toLocaleString()}</td>
                        <td class="percentage">(${totalVotes > 0 ? ((candidate.votes / totalVotes) * 100).toFixed(1) : 0}%)</td>
                    </tr>
                `).join('')}
            </table>
        `
  }

  /**
     * Clear precinct details and show default content
     */
  clearPrecinctDetails () {
    if (!this.precinctSection) return

    this.precinctSection.innerHTML = `
            <h4>Precinct Information</h4>
            <p><strong>Click a precinct</strong> to see detailed results.</p>
            <p>Hover over precincts to see basic information, or click for detailed candidate results.</p>
        `
  }

  /**
     * Show default content when no data is available
     */
  showDefaultContent () {
    if (this.statsSection) {
      this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>Loading election data...</p>
            `
    }

    this.showDefaultLayerInfo()
  }

  /**
     * Show error message for stats
     */
  showStatsError () {
    if (this.statsSection) {
      this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>Error loading statistics. Please try refreshing the page.</p>
            `
    }
  }

  /**
     * Cleanup and destroy the component
     */
  destroy () {
    // No specific cleanup needed for InfoPanel
    // Event listeners are managed by EventBus
    this.initialized = false
    console.log('InfoPanel destroyed')
  }

  /**
     * Get component status
     */
  getStatus () {
    return {
      name: 'InfoPanel',
      initialized: this.initialized,
      hasContainer: !!this.container,
      hasStats: !!this.lastUpdatedStats
    }
  }
}
