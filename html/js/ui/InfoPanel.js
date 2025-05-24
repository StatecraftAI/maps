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
    constructor(stateManager, eventBus) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        
        this.container = null;
        this.statsSection = null;
        this.precinctSection = null;
        
        this.initialized = false;
        this.lastUpdatedStats = null;
    }

    /**
     * Initialize the InfoPanel component
     */
    async init() {
        try {
            this.container = document.querySelector('.info-panel');
            if (!this.container) {
                throw new Error('Info panel container not found');
            }

            this.statsSection = document.getElementById('stats-summary');
            this.precinctSection = document.getElementById('precinct-info');
            
            if (!this.statsSection || !this.precinctSection) {
                throw new Error('Required info panel sections not found');
            }

            this.setupEventListeners();
            await this.loadInitialData();
            
            this.initialized = true;
            console.log('✅ InfoPanel initialized');
            
        } catch (error) {
            console.error('❌ InfoPanel initialization failed:', error);
            throw error;
        }
    }

    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Listen for layer changes to update statistics
        this.eventBus.on('layerChanged', (data) => {
            this.updateStats(data);
        });

        // Listen for precinct selection
        this.eventBus.on('precinctSelected', (data) => {
            this.showPrecinctDetails(data);
        });

        // Listen for precinct deselection
        this.eventBus.on('precinctDeselected', () => {
            this.clearPrecinctDetails();
        });

        // Listen for data updates
        this.eventBus.on('dataLoaded', (data) => {
            this.updateStats(data);
        });

        // Listen for filter changes
        this.eventBus.on('filterChanged', (data) => {
            this.updateStats(data);
        });
    }

    /**
     * Load initial data and display default information
     */
    async loadInitialData() {
        const currentLayer = this.stateManager.getState('currentField');
        const precinctData = this.stateManager.getState('electionData');
        
        if (precinctData && currentLayer) {
            this.updateStats({ 
                layer: currentLayer, 
                data: precinctData 
            });
        } else {
            this.showDefaultContent();
        }
    }

    /**
     * Update statistics display
     */
    updateStats(data) {
        if (!this.statsSection || !data) return;

        try {
            const stats = this.calculateLayerStats(data);
            this.displayStats(stats);
            this.lastUpdatedStats = stats;
            
        } catch (error) {
            console.error('Error updating stats:', error);
            this.showStatsError();
        }
    }

    /**
     * Calculate statistics for the current layer
     */
    calculateLayerStats(data) {
        const precinctData = data.data || this.stateManager.getState('electionData');
        const currentLayer = data.layer || this.stateManager.getState('currentField');
        
        if (!precinctData || !currentLayer) {
            return null;
        }

        const filteredData = this.getFilteredData(precinctData);
        const stats = {
            totalPrecincts: filteredData.length,
            layerName: this.getLayerDisplayName(currentLayer),
            ...this.calculateSpecificStats(filteredData, currentLayer)
        };

        return stats;
    }

    /**
     * Get filtered data based on current filters
     */
    getFilteredData(data) {
        const ppsOnly = this.stateManager.getState('showPpsOnly');
        
        if (ppsOnly) {
            return data.filter(precinct => 
                precinct.properties && precinct.properties.school_zone === 1
            );
        }
        
        return data;
    }

    /**
     * Calculate layer-specific statistics
     */
    calculateSpecificStats(data, layer) {
        const values = data.map(precinct => {
            const props = precinct.properties || {};
            return this.getValueForLayer(props, layer);
        }).filter(val => val !== null && val !== undefined && !isNaN(val));

        if (values.length === 0) {
            return { hasData: false };
        }

        const stats = {
            hasData: true,
            total: values.reduce((sum, val) => sum + val, 0),
            average: values.reduce((sum, val) => sum + val, 0) / values.length,
            min: Math.min(...values),
            max: Math.max(...values),
            count: values.length
        };

        // Add layer-specific calculations
        if (layer.includes('turnout')) {
            stats.averageTurnout = `${stats.average.toFixed(1)}%`;
        } else if (layer.includes('votes') || layer.includes('total')) {
            stats.totalVotes = stats.total.toLocaleString();
        }

        return stats;
    }

    /**
     * Extract value for a specific layer from precinct properties
     */
    getValueForLayer(properties, layer) {
        // Handle different layer types
        if (layer.includes('turnout_pct')) {
            return properties.turnout_pct;
        } else if (layer.includes('total_votes')) {
            return properties.total_votes;
        } else if (layer.includes('registered_voters')) {
            return properties.registered_voters;
        } else if (layer.startsWith('candidate_')) {
            const candidateField = layer.replace('candidate_', '');
            return properties[candidateField];
        }
        
        return properties[layer] || null;
    }

    /**
     * Get human-readable layer name
     */
    getLayerDisplayName(layer) {
        const layerNames = {
            'turnout_pct': 'Voter Turnout',
            'total_votes': 'Total Votes',
            'registered_voters': 'Registered Voters',
            'margin': 'Victory Margin'
        };

        if (layerNames[layer]) {
            return layerNames[layer];
        }

        if (layer.startsWith('candidate_')) {
            const candidate = layer.replace('candidate_', '').replace(/_/g, ' ');
            return `${candidate} Votes`;
        }

        return layer.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }

    /**
     * Display calculated statistics
     */
    displayStats(stats) {
        if (!stats || !stats.hasData) {
            this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>No data available for the current selection.</p>
            `;
            return;
        }

        const formatValue = (value) => {
            if (typeof value === 'number') {
                return value >= 1000 ? value.toLocaleString() : value.toFixed(1);
            }
            return value;
        };

        this.statsSection.innerHTML = `
            <h4>Election Summary</h4>
            <table>
                <tr>
                    <th>Layer:</th>
                    <td>${stats.layerName}</td>
                </tr>
                <tr>
                    <th>Precincts:</th>
                    <td>${stats.totalPrecincts}</td>
                </tr>
                ${stats.totalVotes ? `
                <tr>
                    <th>Total Votes:</th>
                    <td>${stats.totalVotes}</td>
                </tr>
                ` : ''}
                ${stats.averageTurnout ? `
                <tr>
                    <th>Avg Turnout:</th>
                    <td>${stats.averageTurnout}</td>
                </tr>
                ` : `
                <tr>
                    <th>Average:</th>
                    <td>${formatValue(stats.average)}</td>
                </tr>
                `}
                <tr>
                    <th>Range:</th>
                    <td>${formatValue(stats.min)} - ${formatValue(stats.max)}</td>
                </tr>
            </table>
        `;
    }

    /**
     * Show detailed information for a selected precinct
     */
    showPrecinctDetails(data) {
        if (!this.precinctSection || !data) return;

        try {
            const precinct = data.feature || data;
            const props = precinct.properties || {};
            
            this.precinctSection.innerHTML = `
                <h4>Precinct ${props.precinct || 'N/A'}</h4>
                
                <div class="precinct-details">
                    <table>
                        <tr>
                            <th>Registered Voters:</th>
                            <td>${(props.registered_voters || 0).toLocaleString()}</td>
                        </tr>
                        <tr>
                            <th>Total Votes:</th>
                            <td>${(props.total_votes || 0).toLocaleString()}</td>
                        </tr>
                        <tr>
                            <th>Turnout:</th>
                            <td>${(props.turnout_pct || 0).toFixed(1)}%</td>
                        </tr>
                        ${props.school_zone ? `
                        <tr>
                            <th>School Zone:</th>
                            <td>Zone ${props.school_zone}</td>
                        </tr>
                        ` : ''}
                    </table>
                    
                    ${this.renderCandidateResults(props)}
                </div>
            `;
            
        } catch (error) {
            console.error('Error showing precinct details:', error);
            this.precinctSection.innerHTML = `
                <h4>Precinct Information</h4>
                <p>Error loading precinct details.</p>
            `;
        }
    }

    /**
     * Render candidate results for the precinct
     */
    renderCandidateResults(properties) {
        const candidateFields = Object.keys(properties).filter(key => 
            key.startsWith('candidate_') || 
            (key.includes('_') && !['total_votes', 'registered_voters', 'turnout_pct', 'school_zone', 'precinct'].includes(key))
        );

        if (candidateFields.length === 0) {
            return '';
        }

        const candidates = candidateFields.map(field => ({
            name: field.replace(/^candidate_/, '').replace(/_/g, ' '),
            votes: properties[field] || 0
        })).sort((a, b) => b.votes - a.votes);

        const totalVotes = candidates.reduce((sum, c) => sum + c.votes, 0);

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
        `;
    }

    /**
     * Clear precinct details and show default content
     */
    clearPrecinctDetails() {
        if (!this.precinctSection) return;
        
        this.precinctSection.innerHTML = `
            <h4>Precinct Information</h4>
            <p><strong>Click a precinct</strong> to see detailed results.</p>
            <p>Hover over precincts to see basic information, or click for detailed candidate results.</p>
        `;
    }

    /**
     * Show default content when no data is available
     */
    showDefaultContent() {
        if (this.statsSection) {
            this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>Loading election data...</p>
            `;
        }
        
        this.clearPrecinctDetails();
    }

    /**
     * Show error message for stats
     */
    showStatsError() {
        if (this.statsSection) {
            this.statsSection.innerHTML = `
                <h4>Election Summary</h4>
                <p>Error loading statistics. Please try refreshing the page.</p>
            `;
        }
    }

    /**
     * Cleanup and destroy the component
     */
    destroy() {
        // No specific cleanup needed for InfoPanel
        // Event listeners are managed by EventBus
        this.initialized = false;
        console.log('InfoPanel destroyed');
    }

    /**
     * Get component status
     */
    getStatus() {
        return {
            name: 'InfoPanel',
            initialized: this.initialized,
            hasContainer: !!this.container,
            hasStats: !!this.lastUpdatedStats
        };
    }
} 