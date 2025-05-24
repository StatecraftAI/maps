/**
 * ComponentOrchestrator - Main Integration Layer
 * 
 * Handles:
 * - Component initialization and lifecycle management
 * - Cross-component communication coordination
 * - State synchronization across all modules
 * - Error handling and recovery
 * - Performance monitoring and optimization
 * 
 * This replaces the monolithic election_map.html JavaScript code with a fully modular architecture.
 */

// Core imports
import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';
import { MapManager } from '../core/MapManager.js';

// Data layer imports
import { DataLoader } from '../data/DataLoader.js';
import { DataProcessor } from '../data/DataProcessor.js';
import { CandidateManager } from '../data/CandidateManager.js';

// UI component imports
import { ControlPanel } from '../ui/ControlPanel.js';
import { LayerSelector } from '../ui/LayerSelector.js';
import { Accordion } from '../ui/Accordion.js';
import { InfoPanel } from '../ui/InfoPanel.js';
import { Legend } from '../ui/Legend.js';

// Visualization imports
import { MapRenderer } from '../visualization/MapRenderer.js';
import { ColorManager } from '../visualization/ColorManager.js';
import { PopupManager } from '../visualization/PopupManager.js';

// Feature imports
import { Search } from '../features/Search.js';
import { Sharing } from '../features/Sharing.js';
import { Export } from '../features/Export.js';
import { Heatmap } from '../features/Heatmap.js';
import { SchoolOverlays } from '../features/SchoolOverlays.js';
import { Comparison } from '../features/Comparison.js';

// Utilities
import { DOMUtils } from '../utils/domUtils.js';
import { URLUtils } from '../utils/urlUtils.js';

export class ComponentOrchestrator {
    constructor() {
        // Core components
        this.stateManager = null;
        this.eventBus = null;
        this.mapManager = null;
        
        // Data layer components
        this.dataLoader = null;
        this.dataProcessor = null;
        this.candidateManager = null;
        
        // UI components
        this.controlPanel = null;
        this.layerSelector = null;
        this.accordion = null;
        this.infoPanel = null;
        this.legend = null;
        
        // Visualization components
        this.mapRenderer = null;
        this.colorManager = null;
        this.popupManager = null;
        
        // Feature components
        this.search = null;
        this.sharing = null;
        this.export = null;
        this.heatmap = null;
        this.schoolOverlays = null;
        this.comparison = null;
        
        // State
        this.initialized = false;
        this.components = new Map();
        this.cleanupFunctions = [];
        
        // Performance monitoring
        this.initStartTime = null;
        this.metrics = {
            initTime: 0,
            componentsLoaded: 0,
            dataLoadTime: 0,
            renderTime: 0
        };
    }
    
    /**
     * Initialize the entire application
     */
    async initialize() {
        if (this.initialized) {
            console.warn('[ComponentOrchestrator] Already initialized');
            return;
        }
        
        this.initStartTime = performance.now();
        console.log('[ComponentOrchestrator] Starting application initialization...');
        
        try {
            // Phase 1: Initialize core components
            await this.initializeCoreComponents();
            
            // Phase 2: Initialize data layer
            await this.initializeDataLayer();
            
            // Phase 3: Initialize UI components
            await this.initializeUIComponents();
            
            // Phase 4: Initialize visualization layer
            await this.initializeVisualizationLayer();
            
            // Phase 5: Initialize features
            await this.initializeFeatures();
            
            // Phase 6: Setup cross-component communication
            this.setupComponentCommunication();
            
            // Phase 7: Restore state from URL if present
            await this.restoreStateFromUrl();
            
            // Phase 8: Initial data load
            await this.performInitialDataLoad();
            
            // Mark as initialized
            this.initialized = true;
            this.metrics.initTime = performance.now() - this.initStartTime;
            
            console.log('[ComponentOrchestrator] Application initialized successfully', {
                initTime: `${this.metrics.initTime.toFixed(2)}ms`,
                components: this.metrics.componentsLoaded
            });
            
            // Emit initialization complete event
            this.eventBus.emit('app:initialized', {
                metrics: this.metrics,
                components: Array.from(this.components.keys())
            });
            
        } catch (error) {
            console.error('[ComponentOrchestrator] Initialization failed:', error);
            this.handleInitializationError(error);
            throw error;
        }
    }
    
    /**
     * Initialize core foundation components
     */
    async initializeCoreComponents() {
        console.log('[ComponentOrchestrator] Initializing core components...');
        
        // Initialize StateManager first (foundation for everything)
        this.stateManager = new StateManager();
        this.components.set('stateManager', this.stateManager);
        this.metrics.componentsLoaded++;
        
        // Initialize EventBus second (communication backbone)
        this.eventBus = new EventBus();
        this.components.set('eventBus', this.eventBus);
        this.metrics.componentsLoaded++;
        
        // Initialize MapManager third (Leaflet map wrapper)
        this.mapManager = new MapManager(this.stateManager, this.eventBus);
        await this.mapManager.initializeMap('map');
        this.components.set('mapManager', this.mapManager);
        this.metrics.componentsLoaded++;
        
        console.log('[ComponentOrchestrator] Core components initialized');
    }
    
    /**
     * Initialize data layer components
     */
    async initializeDataLayer() {
        console.log('[ComponentOrchestrator] Initializing data layer...');
        
        // DataLoader - handles all data fetching
        this.dataLoader = new DataLoader(this.stateManager, this.eventBus);
        this.components.set('dataLoader', this.dataLoader);
        this.metrics.componentsLoaded++;
        
        // DataProcessor - processes GeoJSON and field data
        this.dataProcessor = new DataProcessor(this.stateManager, this.eventBus);
        this.components.set('dataProcessor', this.dataProcessor);
        this.metrics.componentsLoaded++;
        
        // CandidateManager - manages candidate detection and colors
        this.candidateManager = new CandidateManager(this.stateManager, this.eventBus);
        this.components.set('candidateManager', this.candidateManager);
        this.metrics.componentsLoaded++;
        
        console.log('[ComponentOrchestrator] Data layer initialized');
    }
    
    /**
     * Initialize UI components
     */
    async initializeUIComponents() {
        console.log('[ComponentOrchestrator] Initializing UI components...');
        
        // ControlPanel - left panel container (has initialize() method)
        this.controlPanel = new ControlPanel(this.stateManager, this.eventBus);
        this.controlPanel.initialize();
        this.components.set('controlPanel', this.controlPanel);
        this.metrics.componentsLoaded++;
        
        // LayerSelector - data field dropdown (has initialize() method)
        this.layerSelector = new LayerSelector(this.stateManager, this.eventBus);
        this.layerSelector.initialize();
        this.components.set('layerSelector', this.layerSelector);
        this.metrics.componentsLoaded++;
        
        // Accordion - collapsible sections (has initialize() method)
        this.accordion = new Accordion(this.stateManager, this.eventBus);
        this.accordion.initialize();
        this.components.set('accordion', this.accordion);
        this.metrics.componentsLoaded++;
        
        // InfoPanel - right panel stats (has init() method)
        this.infoPanel = new InfoPanel(this.stateManager, this.eventBus);
        await this.infoPanel.init();
        this.components.set('infoPanel', this.infoPanel);
        this.metrics.componentsLoaded++;
        
        // Legend - color scale legend (initializes in constructor)
        this.legend = new Legend(this.stateManager, this.eventBus);
        this.components.set('legend', this.legend);
        this.metrics.componentsLoaded++;
        
        console.log('[ComponentOrchestrator] UI components initialized');
    }
    
    /**
     * Initialize visualization layer
     */
    async initializeVisualizationLayer() {
        console.log('[ComponentOrchestrator] Initializing visualization layer...');
        
        // ColorManager - color schemes and gradients (initializes in constructor)
        this.colorManager = new ColorManager(this.stateManager, this.eventBus);
        this.components.set('colorManager', this.colorManager);
        this.metrics.componentsLoaded++;
        
        // PopupManager - map popup handling (initializes in constructor)
        this.popupManager = new PopupManager(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('popupManager', this.popupManager);
        this.metrics.componentsLoaded++;
        
        // MapRenderer - GeoJSON rendering (initializes in constructor)
        this.mapRenderer = new MapRenderer(this.stateManager, this.eventBus, this.mapManager, this.colorManager, this.popupManager);
        this.components.set('mapRenderer', this.mapRenderer);
        this.metrics.componentsLoaded++;
        
        console.log('[ComponentOrchestrator] Visualization layer initialized');
    }
    
    /**
     * Initialize feature components
     */
    async initializeFeatures() {
        console.log('[ComponentOrchestrator] Initializing features...');
        
        // Search - address search and GPS (initializes in constructor)
        this.search = new Search(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('search', this.search);
        this.metrics.componentsLoaded++;
        
        // Sharing - URL sharing and social media (initializes in constructor)
        this.sharing = new Sharing(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('sharing', this.sharing);
        this.metrics.componentsLoaded++;
        
        // Export - map image export (initializes in constructor)
        this.export = new Export(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('export', this.export);
        this.metrics.componentsLoaded++;
        
        // Heatmap - vote density heatmap (initializes in constructor)
        this.heatmap = new Heatmap(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('heatmap', this.heatmap);
        this.metrics.componentsLoaded++;
        
        // SchoolOverlays - school markers and boundaries (initializes in constructor)
        this.schoolOverlays = new SchoolOverlays(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('schoolOverlays', this.schoolOverlays);
        this.metrics.componentsLoaded++;
        
        // Comparison - split-screen layer comparison (initializes in constructor)
        this.comparison = new Comparison(this.stateManager, this.eventBus, this.mapManager);
        this.components.set('comparison', this.comparison);
        this.metrics.componentsLoaded++;
        
        console.log('[ComponentOrchestrator] Features initialized');
    }
    
    /**
     * Setup cross-component communication
     */
    setupComponentCommunication() {
        console.log('[ComponentOrchestrator] Setting up component communication...');
        
        // Setup global error handling
        this.eventBus.on('error', (error) => {
            console.error('[ComponentOrchestrator] Component error:', error);
            this.handleComponentError(error);
        });
        
        // Setup state change debugging
        if (this.stateManager.getState().debug) {
            this.eventBus.on('state:change', (data) => {
                console.log('[ComponentOrchestrator] State change:', data);
            });
        }
        
        // Setup performance monitoring
        this.eventBus.on('performance:metric', (metric) => {
            console.log('[ComponentOrchestrator] Performance metric:', metric);
        });
        
        // Setup cleanup on page unload
        const cleanup = () => this.cleanup();
        window.addEventListener('beforeunload', cleanup);
        this.cleanupFunctions.push(() => {
            window.removeEventListener('beforeunload', cleanup);
        });
    }
    
    /**
     * Restore application state from URL parameters
     */
    async restoreStateFromUrl() {
        const urlParams = URLUtils.parseUrlParameters();
        
        if (Object.keys(urlParams).length > 0) {
            console.log('[ComponentOrchestrator] Restoring state from URL:', urlParams);
            
            // Update state with URL parameters
            Object.keys(urlParams).forEach(key => {
                this.stateManager.setState({ [key]: urlParams[key] });
            });
            
            // Emit URL restoration event
            this.eventBus.emit('url:restored', urlParams);
        }
    }
    
    /**
     * Perform initial data load
     */
    async performInitialDataLoad() {
        console.log('[ComponentOrchestrator] Starting initial data load...');
        
        const dataLoadStart = performance.now();
        
        try {
            // Discover available datasets
            await this.dataLoader.discoverDatasets();
            
            // Load default dataset if specified
            const defaultDataset = this.stateManager.getState().currentDataset || 'zone1';
            await this.dataLoader.loadDataset(defaultDataset);
            
            this.metrics.dataLoadTime = performance.now() - dataLoadStart;
            
            console.log('[ComponentOrchestrator] Initial data load complete', {
                dataLoadTime: `${this.metrics.dataLoadTime.toFixed(2)}ms`
            });
            
        } catch (error) {
            console.error('[ComponentOrchestrator] Initial data load failed:', error);
            this.eventBus.emit('error', {
                type: 'dataLoad',
                message: 'Failed to load initial data',
                error
            });
        }
    }
    
    /**
     * Handle initialization errors
     */
    handleInitializationError(error) {
        console.error('[ComponentOrchestrator] Initialization error:', error);
        
        // Show user-friendly error message
        const errorContainer = DOMUtils.getElement('error-container') || document.body;
        const errorElement = DOMUtils.createErrorElement(
            'Failed to initialize application. Please refresh the page to try again.'
        );
        
        errorContainer.appendChild(errorElement);
        
        // Emit error event
        this.eventBus?.emit('app:error', {
            type: 'initialization',
            error,
            fatal: true
        });
    }
    
    /**
     * Handle component errors during runtime
     */
    handleComponentError(errorData) {
        console.error('[ComponentOrchestrator] Component error:', errorData);
        
        // For now, just log the error
        // In the future, we could implement recovery strategies
    }
    
    /**
     * Get a specific component instance
     */
    getComponent(name) {
        return this.components.get(name);
    }
    
    /**
     * Check if a component is available
     */
    hasComponent(name) {
        return this.components.has(name);
    }
    
    /**
     * Get all component names
     */
    getComponentNames() {
        return Array.from(this.components.keys());
    }
    
    /**
     * Get application metrics
     */
    getMetrics() {
        return { ...this.metrics };
    }
    
    /**
     * Cleanup all components and event listeners
     */
    cleanup() {
        console.log('[ComponentOrchestrator] Cleaning up components...');
        
        // Run all cleanup functions
        this.cleanupFunctions.forEach(cleanup => {
            try {
                cleanup();
            } catch (error) {
                console.error('[ComponentOrchestrator] Cleanup error:', error);
            }
        });
        
        // Cleanup all components
        this.components.forEach((component, name) => {
            try {
                if (typeof component.cleanup === 'function') {
                    component.cleanup();
                }
            } catch (error) {
                console.error(`[ComponentOrchestrator] Error cleaning up ${name}:`, error);
            }
        });
        
        // Clear component references
        this.components.clear();
        this.initialized = false;
        
        console.log('[ComponentOrchestrator] Cleanup complete');
    }
    
    /**
     * Restart the application
     */
    async restart() {
        console.log('[ComponentOrchestrator] Restarting application...');
        
        this.cleanup();
        await this.initialize();
        
        console.log('[ComponentOrchestrator] Application restarted');
    }
} 