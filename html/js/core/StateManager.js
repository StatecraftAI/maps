/**
 * StateManager - Centralized state management for Election Map Application
 * 
 * Replaces all global variables from the original monolithic code with
 * a reactive state management system following modern JavaScript patterns.
 * 
 * Features:
 * - Centralized state storage
 * - Reactive updates with subscriber notifications
 * - State persistence to localStorage
 * - Type-safe state management
 * - Debug-friendly state inspection
 */

export class StateManager {
    constructor() {
        // Initialize state with all former global variables
        this.state = {
            // === CORE MAP STATE ===
            map: null,                          // Leaflet map instance
            currentLayer: null,                 // Current GeoJSON layer
            currentField: 'political_lean',     // Currently selected data field
            currentDataset: 'zone1',           // Currently selected dataset
            
            // === DATA STATE ===
            electionData: null,                 // Loaded GeoJSON election data
            datasets: {},                       // Available dataset configurations
            actualDataRanges: {},              // Calculated min/max ranges for numeric fields
            baseDataPath: '',                  // Base path for data files (set by environment detection)
            
            // === UI STATE ===
            showPpsOnly: true,                 // Filter to show only Zone 1 precincts
            customRange: null,                 // User-defined color range override
            chartInstance: null,               // Chart.js instance for popups
            
            // === FEATURE STATE ===
            heatmapLayer: null,               // Heatmap overlay layer
            schoolLayers: {},                 // School overlay layers collection
            searchMarker: null,               // Address search result marker
            locationMarker: null,             // User's GPS location marker
            coordinateInfoBox: null,          // Coordinate display control
            
            // === FEATURE FLAGS ===
            coordinateDisplay: false,         // Show coordinates on hover/click
            comparisonMode: false,           // Layer comparison mode active
            
            // === UI ELEMENT REFERENCES ===
            // These will be populated as components initialize
            controlPanel: null,
            infoPanel: null,
            layerSelector: null,
            legend: null,
            
            // === CONFIGURATION ===
            opacity: 0.7,                    // Layer opacity setting
            basemap: 'streets',              // Selected base map
            
            // === COLOR SCHEMES ===
            // Moved from global colorSchemes object
            colorSchemes: {
                political_lean: {
                    'Strong Dem': '#0571b0',
                    'Lean Dem': '#74a9cf',
                    'Competitive': '#fee391',
                    'Lean Rep': '#fd8d3c',
                    'Strong Rep': '#d94701'
                },
                competitiveness: {
                    'Safe': '#2166ac',
                    'Likely': '#762a83',
                    'Competitive': '#f1a340',
                    'Tossup': '#d73027',
                    'No Election Data': '#f7f7f7'
                },
                leading_candidate: {
                    'Tie': '#636363',
                    'No Election Data': '#f7f7f7',
                    'No Data': '#f7f7f7'
                    // Candidate colors will be added dynamically
                },
                turnout_quartile: {
                    'Low': '#fee391',
                    'Med-Low': '#fec44f',
                    'Medium': '#fe9929',
                    'Med-High': '#d95f0e',
                    'High': '#993404',
                    'Single': '#f7f7f7'
                },
                margin_category: {
                    'Very Close': '#fee391',
                    'Close': '#fec44f',
                    'Clear': '#d95f0e',
                    'Landslide': '#993404'
                },
                precinct_size_category: {
                    'Small': '#fee391',
                    'Medium': '#fec44f',
                    'Large': '#d95f0e',
                    'Extra Large': '#993404'
                }
            }
        };

        // Subscriber management for reactive updates
        this.subscribers = new Map();
        
        // State change history for debugging
        this.history = [];
        this.maxHistorySize = 50;
        
        // Initialize environment-based settings
        this.initializeEnvironment();
        
        // Load persisted state from localStorage
        this.loadPersistedState();

        console.log('🏗️ StateManager initialized with state:', this.getDebugState());
    }

    /**
     * Initialize environment-specific settings
     */
    initializeEnvironment() {
        // Detect base data path based on environment (from original code)
        const hostname = window.location.hostname;
        
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
            this.state.baseDataPath = '../';
        } else if (hostname.endsWith('github.io')) {
            this.state.baseDataPath = '';
        }
        
        console.log(`🌍 Environment detected: ${hostname}, baseDataPath: '${this.state.baseDataPath}'`);
    }

    /**
     * Set state with reactive updates
     * @param {Object} updates - Object containing state updates
     * @param {Object} options - Update options
     */
    setState(updates, options = {}) {
        const { silent = false, source = 'unknown' } = options;
        
        // Store previous state for history
        const previousState = { ...this.state };
        
        // Apply updates
        Object.assign(this.state, updates);
        
        // Add to history
        this.addToHistory({
            timestamp: Date.now(),
            source,
            updates,
            previousState: previousState
        });

        if (!silent) {
            console.log(`🔄 State updated by ${source}:`, updates);
        }

        // Notify subscribers
        this.notifySubscribers(updates, source);
        
        // Persist important state changes
        this.persistState(Object.keys(updates));
    }

    /**
     * Get state value safely
     * @param {string} key - State key to retrieve
     * @returns {*} State value or undefined
     */
    getState(key) {
        if (key === undefined) {
            return { ...this.state }; // Return copy of entire state
        }
        
        // Support dot notation for nested access
        if (key.includes('.')) {
            return this.getNestedState(key);
        }
        
        return this.state[key];
    }

    /**
     * Get nested state value using dot notation
     * @param {string} path - Dot-separated path (e.g., 'colorSchemes.political_lean')
     * @returns {*} Nested value or undefined
     */
    getNestedState(path) {
        return path.split('.').reduce((obj, key) => obj?.[key], this.state);
    }

    /**
     * Subscribe to state changes
     * @param {string|Array} keys - State key(s) to watch
     * @param {Function} callback - Callback function
     * @returns {Function} Unsubscribe function
     */
    subscribe(keys, callback) {
        const keyArray = Array.isArray(keys) ? keys : [keys];
        const subscriberId = Math.random().toString(36).substr(2, 9);
        
        this.subscribers.set(subscriberId, {
            keys: keyArray,
            callback
        });

        console.log(`📡 Subscriber ${subscriberId} registered for keys:`, keyArray);

        // Return unsubscribe function
        return () => {
            this.subscribers.delete(subscriberId);
            console.log(`📡 Subscriber ${subscriberId} unsubscribed`);
        };
    }

    /**
     * Notify subscribers of state changes
     * @param {Object} updates - State updates
     * @param {string} source - Update source
     */
    notifySubscribers(updates, source) {
        const updatedKeys = Object.keys(updates);
        
        this.subscribers.forEach((subscriber, subscriberId) => {
            const { keys, callback } = subscriber;
            
            // Check if any subscribed keys were updated
            const hasMatchingKeys = keys.some(key => updatedKeys.includes(key));
            
            if (hasMatchingKeys) {
                try {
                    callback(updates, this.state, source);
                } catch (error) {
                    console.error(`❌ Error in subscriber ${subscriberId}:`, error);
                }
            }
        });
    }

    /**
     * Persist specific state keys to localStorage
     * @param {Array} keys - Keys to persist
     */
    persistState(keys) {
        const persistableKeys = [
            'currentField', 'currentDataset', 'showPpsOnly', 
            'opacity', 'basemap', 'coordinateDisplay', 'comparisonMode'
        ];
        
        const keysToPersist = keys.filter(key => persistableKeys.includes(key));
        
        if (keysToPersist.length > 0) {
            try {
                const persistedData = {};
                keysToPersist.forEach(key => {
                    persistedData[key] = this.state[key];
                });
                
                localStorage.setItem('electionMapState', JSON.stringify(persistedData));
                console.log('💾 State persisted:', keysToPersist);
            } catch (error) {
                console.warn('⚠️ Failed to persist state:', error);
            }
        }
    }

    /**
     * Load persisted state from localStorage
     */
    loadPersistedState() {
        try {
            const persistedData = localStorage.getItem('electionMapState');
            if (persistedData) {
                const parsed = JSON.parse(persistedData);
                Object.assign(this.state, parsed);
                console.log('📂 Loaded persisted state:', parsed);
            }
        } catch (error) {
            console.warn('⚠️ Failed to load persisted state:', error);
        }
    }

    /**
     * Add entry to state change history
     * @param {Object} entry - History entry
     */
    addToHistory(entry) {
        this.history.unshift(entry);
        
        // Trim history to max size
        if (this.history.length > this.maxHistorySize) {
            this.history = this.history.slice(0, this.maxHistorySize);
        }
    }

    /**
     * Get debug information about current state
     * @returns {Object} Debug state information
     */
    getDebugState() {
        return {
            stateKeys: Object.keys(this.state),
            subscriberCount: this.subscribers.size,
            historyLength: this.history.length,
            currentDataset: this.state.currentDataset,
            currentField: this.state.currentField,
            hasElectionData: !!this.state.electionData,
            mapInitialized: !!this.state.map
        };
    }

    /**
     * Get state change history for debugging
     * @param {number} limit - Number of history entries to return
     * @returns {Array} Recent state changes
     */
    getHistory(limit = 10) {
        return this.history.slice(0, limit);
    }

    /**
     * Reset state to initial values
     * @param {Array} keys - Specific keys to reset (optional)
     */
    reset(keys = null) {
        if (keys) {
            // Reset specific keys
            const initialState = new StateManager().state;
            const resetUpdates = {};
            
            keys.forEach(key => {
                resetUpdates[key] = initialState[key];
            });
            
            this.setState(resetUpdates, { source: 'reset' });
        } else {
            // Full reset
            this.state = new StateManager().state;
            this.history = [];
            this.subscribers.clear();
            
            // Clear localStorage
            localStorage.removeItem('electionMapState');
            
            console.log('🔄 StateManager fully reset');
        }
    }

    /**
     * Validate state integrity
     * @returns {Object} Validation results
     */
    validateState() {
        const issues = [];
        
        // Check for required state
        if (!this.state.currentField) {
            issues.push('currentField is missing');
        }
        
        if (!this.state.currentDataset) {
            issues.push('currentDataset is missing');
        }
        
        // Check for orphaned references
        if (this.state.currentLayer && !this.state.map) {
            issues.push('currentLayer exists without map');
        }
        
        if (this.state.chartInstance && !this.state.electionData) {
            issues.push('chartInstance exists without electionData');
        }
        
        return {
            valid: issues.length === 0,
            issues
        };
    }

    /**
     * Export state for debugging or backup
     * @returns {Object} Serializable state
     */
    exportState() {
        const exportableState = {};
        
        Object.keys(this.state).forEach(key => {
            const value = this.state[key];
            
            // Skip non-serializable objects
            if (value && typeof value === 'object' && 
                (value.constructor.name.includes('Layer') || 
                 value.constructor.name.includes('Map') ||
                 value.constructor.name.includes('Chart'))) {
                exportableState[key] = `[${value.constructor.name}]`;
            } else {
                exportableState[key] = value;
            }
        });
        
        return exportableState;
    }
}

// Export singleton instance
export const stateManager = new StateManager(); 