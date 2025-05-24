/**
 * Comparison - Layer Comparison and Split-Screen Mode
 * 
 * Handles:
 * - Split-screen layer comparison mode
 * - Swipe comparison with divider control
 * - A/B comparison view with synchronized maps
 * - Comparison state management
 * - UI updates for comparison mode
 * - Performance optimization for dual rendering
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';

export class Comparison {
    constructor(stateManager, eventBus, mapManager) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        this.mapManager = mapManager;
        
        // Comparison state
        this.isComparisonMode = false;
        this.leftLayer = null;
        this.rightLayer = null;
        this.swipePosition = 50; // Percentage from left
        
        // UI elements
        this.comparisonContainer = null;
        this.leftPanel = null;
        this.rightPanel = null;
        this.swipeControl = null;
        this.isDragging = false;
        
        // Layer management
        this.comparisonLayers = new Map();
        this.originalLayer = null;
        
        this.initializeElements();
        this.setupEventListeners();
        
        console.log('[Comparison] Initialized');
    }
    
    /**
     * Initialize DOM elements for comparison mode
     */
    initializeElements() {
        // Find or create comparison toggle button
        const toggleButton = document.getElementById('comparison-toggle');
        if (toggleButton) {
            toggleButton.addEventListener('click', () => this.toggleComparisonMode());
        }
        
        // Create comparison controls if they don't exist
        this.createComparisonControls();
    }
    
    /**
     * Create comparison mode controls
     */
    createComparisonControls() {
        // Check if already exists
        if (document.getElementById('comparison-container')) {
            return;
        }
        
        // Create comparison container
        const container = document.createElement('div');
        container.id = 'comparison-container';
        container.className = 'comparison-container hidden';
        container.innerHTML = `
            <div class="comparison-header">
                <h3>Layer Comparison Mode</h3>
                <button id="close-comparison" class="close-button" title="Exit Comparison">×</button>
            </div>
            <div class="comparison-controls">
                <div class="layer-selector">
                    <label>Left Layer:</label>
                    <select id="left-layer-select">
                        <option value="">Select layer...</option>
                    </select>
                </div>
                <div class="layer-selector">
                    <label>Right Layer:</label>
                    <select id="right-layer-select">
                        <option value="">Select layer...</option>
                    </select>
                </div>
                <div class="comparison-options">
                    <label>
                        <input type="checkbox" id="sync-maps" checked>
                        Synchronize map views
                    </label>
                    <label>
                        <input type="range" id="swipe-position" min="0" max="100" value="50">
                        Swipe Position
                    </label>
                </div>
            </div>
        `;
        
        // Add to page
        const mapContainer = document.getElementById('map-container') || document.body;
        mapContainer.appendChild(container);
        
        // Store references
        this.comparisonContainer = container;
        
        // Set up event listeners for new elements
        this.setupComparisonControls();
    }
    
    /**
     * Set up event listeners for comparison controls
     */
    setupComparisonControls() {
        // Close button
        const closeButton = document.getElementById('close-comparison');
        if (closeButton) {
            closeButton.addEventListener('click', () => this.exitComparisonMode());
        }
        
        // Layer selectors
        const leftSelect = document.getElementById('left-layer-select');
        const rightSelect = document.getElementById('right-layer-select');
        
        if (leftSelect) {
            leftSelect.addEventListener('change', (e) => this.setLeftLayer(e.target.value));
        }
        
        if (rightSelect) {
            rightSelect.addEventListener('change', (e) => this.setRightLayer(e.target.value));
        }
        
        // Swipe position control
        const swipeControl = document.getElementById('swipe-position');
        if (swipeControl) {
            swipeControl.addEventListener('input', (e) => this.setSwipePosition(parseFloat(e.target.value)));
        }
        
        // Sync checkbox
        const syncCheckbox = document.getElementById('sync-maps');
        if (syncCheckbox) {
            syncCheckbox.addEventListener('change', (e) => this.setSyncMode(e.target.checked));
        }
    }
    
    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Listen for comparison mode requests
        this.eventBus.on('features:toggleComparison', () => {
            this.toggleComparisonMode();
        });
        
        // Listen for layer updates to refresh comparison options
        this.eventBus.on('data:fieldRegistryUpdated', () => {
            this.updateLayerOptions();
        });
        
        // Listen for state changes that affect comparison
        this.stateManager.subscribe('currentField', () => {
            if (!this.isComparisonMode) {
                this.updateLayerOptions();
            }
        });
        
        // Listen for map events for synchronization
        this.eventBus.on('map:viewChanged', (data) => {
            if (this.isComparisonMode && this.getSyncMode()) {
                this.syncMapView(data);
            }
        });
    }
    
    /**
     * Toggle comparison mode on/off
     */
    toggleComparisonMode() {
        if (this.isComparisonMode) {
            this.exitComparisonMode();
        } else {
            this.enterComparisonMode();
        }
    }
    
    /**
     * Enter comparison mode
     */
    async enterComparisonMode() {
        if (this.isComparisonMode) {
            console.log('[Comparison] Already in comparison mode');
            return;
        }
        
        console.log('[Comparison] Entering comparison mode');
        
        try {
            // Store current layer for restoration
            this.originalLayer = this.stateManager.getState('currentField');
            
            // Show comparison container
            if (this.comparisonContainer) {
                this.comparisonContainer.classList.remove('hidden');
            }
            
            // Update layer options
            this.updateLayerOptions();
            
            // Set initial layers if available
            await this.setInitialLayers();
            
            // Create swipe interface
            this.createSwipeInterface();
            
            // Update state
            this.isComparisonMode = true;
            this.stateManager.setState('comparisonMode', true);
            
            // Emit event
            this.eventBus.emit('comparison:modeEntered', {
                leftLayer: this.leftLayer,
                rightLayer: this.rightLayer
            });
            
            console.log('[Comparison] Comparison mode activated');
            
        } catch (error) {
            console.error('[Comparison] Failed to enter comparison mode:', error);
            this.eventBus.emit('comparison:error', { 
                action: 'enter',
                error: error.message 
            });
        }
    }
    
    /**
     * Exit comparison mode
     */
    exitComparisonMode() {
        if (!this.isComparisonMode) {
            return;
        }
        
        console.log('[Comparison] Exiting comparison mode');
        
        // Hide comparison container
        if (this.comparisonContainer) {
            this.comparisonContainer.classList.add('hidden');
        }
        
        // Destroy swipe interface
        this.destroySwipeInterface();
        
        // Restore original layer
        if (this.originalLayer) {
            this.stateManager.setState('currentField', this.originalLayer);
        }
        
        // Clean up comparison layers
        this.clearComparisonLayers();
        
        // Update state
        this.isComparisonMode = false;
        this.stateManager.setState('comparisonMode', false);
        
        // Reset values
        this.leftLayer = null;
        this.rightLayer = null;
        this.swipePosition = 50;
        
        // Emit event
        this.eventBus.emit('comparison:modeExited', {
            restoredLayer: this.originalLayer
        });
        
        console.log('[Comparison] Comparison mode deactivated');
    }
    
    /**
     * Set initial layers for comparison
     */
    async setInitialLayers() {
        const fieldRegistry = this.stateManager.getState('fieldRegistry');
        if (!fieldRegistry || Object.keys(fieldRegistry).length < 2) {
            return;
        }
        
        const fields = Object.keys(fieldRegistry);
        const currentField = this.stateManager.getState('currentField');
        
        // Set current field as left layer
        this.leftLayer = currentField;
        
        // Set first different field as right layer
        this.rightLayer = fields.find(field => field !== currentField) || fields[0];
        
        // Update UI
        this.updateLayerSelectors();
        
        // Apply layers
        await this.applyComparisonLayers();
    }
    
    /**
     * Update layer options in dropdowns
     */
    updateLayerOptions() {
        const fieldRegistry = this.stateManager.getState('fieldRegistry');
        if (!fieldRegistry) {
            return;
        }
        
        const leftSelect = document.getElementById('left-layer-select');
        const rightSelect = document.getElementById('right-layer-select');
        
        if (!leftSelect || !rightSelect) {
            return;
        }
        
        // Clear existing options (except first)
        [leftSelect, rightSelect].forEach(select => {
            while (select.children.length > 1) {
                select.removeChild(select.lastChild);
            }
        });
        
        // Add field options
        Object.entries(fieldRegistry).forEach(([field, info]) => {
            const leftOption = document.createElement('option');
            leftOption.value = field;
            leftOption.textContent = info.displayName || field;
            leftSelect.appendChild(leftOption);
            
            const rightOption = document.createElement('option');
            rightOption.value = field;
            rightOption.textContent = info.displayName || field;
            rightSelect.appendChild(rightOption);
        });
        
        // Update selection
        this.updateLayerSelectors();
    }
    
    /**
     * Update layer selector values
     */
    updateLayerSelectors() {
        const leftSelect = document.getElementById('left-layer-select');
        const rightSelect = document.getElementById('right-layer-select');
        
        if (leftSelect && this.leftLayer) {
            leftSelect.value = this.leftLayer;
        }
        
        if (rightSelect && this.rightLayer) {
            rightSelect.value = this.rightLayer;
        }
    }
    
    /**
     * Set left comparison layer
     */
    async setLeftLayer(field) {
        if (field === this.leftLayer) {
            return;
        }
        
        console.log(`[Comparison] Setting left layer: ${field}`);
        
        this.leftLayer = field;
        await this.applyComparisonLayers();
        
        this.eventBus.emit('comparison:layerChanged', {
            side: 'left',
            field: field
        });
    }
    
    /**
     * Set right comparison layer
     */
    async setRightLayer(field) {
        if (field === this.rightLayer) {
            return;
        }
        
        console.log(`[Comparison] Setting right layer: ${field}`);
        
        this.rightLayer = field;
        await this.applyComparisonLayers();
        
        this.eventBus.emit('comparison:layerChanged', {
            side: 'right',
            field: field
        });
    }
    
    /**
     * Apply comparison layers to map
     */
    async applyComparisonLayers() {
        if (!this.leftLayer || !this.rightLayer) {
            return;
        }
        
        try {
            // Clear existing layers
            this.clearComparisonLayers();
            
            // Create clipped layers for both sides
            await this.createClippedLayer('left', this.leftLayer);
            await this.createClippedLayer('right', this.rightLayer);
            
            // Update swipe interface
            this.updateSwipeInterface();
            
            console.log(`[Comparison] Applied layers: ${this.leftLayer} | ${this.rightLayer}`);
            
        } catch (error) {
            console.error('[Comparison] Failed to apply comparison layers:', error);
            this.eventBus.emit('comparison:error', {
                action: 'applyLayers',
                error: error.message
            });
        }
    }
    
    /**
     * Create clipped layer for one side of comparison
     */
    async createClippedLayer(side, field) {
        // Set field temporarily to load data
        const originalField = this.stateManager.getState('currentField');
        this.stateManager.setState('currentField', field, 'comparison');
        
        // Let other components render the layer
        await new Promise(resolve => setTimeout(resolve, 100));
        
        // Get the rendered layer (this would need integration with MapRenderer)
        const layerData = await this.getLayerData(field);
        
        if (layerData) {
            // Create clipped version
            const clippedLayer = this.createClippedGeoJSON(layerData, side);
            this.comparisonLayers.set(side, clippedLayer);
            
            // Add to map
            const map = this.mapManager.getMap();
            if (map) {
                clippedLayer.addTo(map);
            }
        }
        
        // Restore original field if it was different
        if (originalField !== field) {
            this.stateManager.setState('currentField', originalField, 'comparison');
        }
    }
    
    /**
     * Get layer data for a field
     */
    async getLayerData(field) {
        // This would integrate with MapRenderer to get the current layer data
        // For now, return a placeholder
        const electionData = this.stateManager.getState('electionData');
        return electionData;
    }
    
    /**
     * Create clipped GeoJSON layer
     */
    createClippedGeoJSON(data, side) {
        return L.geoJSON(data, {
            style: (feature) => {
                // Get styling from ColorManager (would need integration)
                return this.getFeatureStyle(feature, side);
            },
            onEachFeature: (feature, layer) => {
                // Apply clipping mask
                this.applyClippingMask(layer, side);
            }
        });
    }
    
    /**
     * Get feature styling (placeholder for ColorManager integration)
     */
    getFeatureStyle(feature, side) {
        const field = side === 'left' ? this.leftLayer : this.rightLayer;
        
        // Basic styling placeholder
        return {
            fillColor: side === 'left' ? '#ff0000' : '#0000ff',
            weight: 1,
            opacity: 1,
            color: '#333',
            fillOpacity: 0.7
        };
    }
    
    /**
     * Apply clipping mask to layer
     */
    applyClippingMask(layer, side) {
        const element = layer.getElement ? layer.getElement() : null;
        if (element) {
            const clipPath = side === 'left' 
                ? `inset(0 ${100 - this.swipePosition}% 0 0)`
                : `inset(0 0 0 ${this.swipePosition}%)`;
            
            element.style.clipPath = clipPath;
        }
    }
    
    /**
     * Create swipe interface
     */
    createSwipeInterface() {
        // Remove existing swipe control
        this.destroySwipeInterface();
        
        // Create swipe divider
        const swipeControl = document.createElement('div');
        swipeControl.className = 'swipe-control';
        swipeControl.style.cssText = `
            position: absolute;
            top: 0;
            bottom: 0;
            width: 4px;
            background: #fff;
            border: 1px solid #333;
            cursor: ew-resize;
            z-index: 1000;
            left: ${this.swipePosition}%;
            transform: translateX(-50%);
        `;
        
        // Add drag handle
        const handle = document.createElement('div');
        handle.style.cssText = `
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 20px;
            height: 40px;
            background: #333;
            border-radius: 10px;
            cursor: ew-resize;
        `;
        swipeControl.appendChild(handle);
        
        // Add to map container
        const mapContainer = document.getElementById('map');
        if (mapContainer) {
            mapContainer.appendChild(swipeControl);
        }
        
        this.swipeControl = swipeControl;
        
        // Set up drag events
        this.setupSwipeDrag();
    }
    
    /**
     * Set up swipe drag functionality
     */
    setupSwipeDrag() {
        if (!this.swipeControl) {
            return;
        }
        
        const handleMouseDown = (e) => {
            this.isDragging = true;
            e.preventDefault();
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
        };
        
        const handleMouseMove = (e) => {
            if (!this.isDragging) return;
            
            const mapContainer = document.getElementById('map');
            if (!mapContainer) return;
            
            const rect = mapContainer.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const percentage = Math.max(0, Math.min(100, (x / rect.width) * 100));
            
            this.setSwipePosition(percentage);
        };
        
        const handleMouseUp = () => {
            this.isDragging = false;
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
        };
        
        this.swipeControl.addEventListener('mousedown', handleMouseDown);
        
        // Touch events for mobile
        this.swipeControl.addEventListener('touchstart', (e) => {
            this.isDragging = true;
            e.preventDefault();
        });
        
        this.swipeControl.addEventListener('touchmove', (e) => {
            if (!this.isDragging) return;
            
            const touch = e.touches[0];
            const mapContainer = document.getElementById('map');
            if (!mapContainer) return;
            
            const rect = mapContainer.getBoundingClientRect();
            const x = touch.clientX - rect.left;
            const percentage = Math.max(0, Math.min(100, (x / rect.width) * 100));
            
            this.setSwipePosition(percentage);
            e.preventDefault();
        });
        
        this.swipeControl.addEventListener('touchend', () => {
            this.isDragging = false;
        });
    }
    
    /**
     * Set swipe position
     */
    setSwipePosition(percentage) {
        this.swipePosition = Math.max(0, Math.min(100, percentage));
        
        // Update swipe control position
        if (this.swipeControl) {
            this.swipeControl.style.left = `${this.swipePosition}%`;
        }
        
        // Update range input
        const rangeInput = document.getElementById('swipe-position');
        if (rangeInput) {
            rangeInput.value = this.swipePosition;
        }
        
        // Update layer clipping
        this.updateLayerClipping();
        
        // Emit event
        this.eventBus.emit('comparison:swipePositionChanged', {
            position: this.swipePosition
        });
    }
    
    /**
     * Update layer clipping based on swipe position
     */
    updateLayerClipping() {
        this.comparisonLayers.forEach((layer, side) => {
            layer.eachLayer((featureLayer) => {
                this.applyClippingMask(featureLayer, side);
            });
        });
    }
    
    /**
     * Update swipe interface
     */
    updateSwipeInterface() {
        if (this.swipeControl) {
            this.setSwipePosition(this.swipePosition);
        }
    }
    
    /**
     * Destroy swipe interface
     */
    destroySwipeInterface() {
        if (this.swipeControl && this.swipeControl.parentNode) {
            this.swipeControl.parentNode.removeChild(this.swipeControl);
        }
        this.swipeControl = null;
        this.isDragging = false;
    }
    
    /**
     * Clear comparison layers
     */
    clearComparisonLayers() {
        const map = this.mapManager.getMap();
        
        this.comparisonLayers.forEach((layer) => {
            if (map && map.hasLayer(layer)) {
                map.removeLayer(layer);
            }
        });
        
        this.comparisonLayers.clear();
    }
    
    /**
     * Get sync mode setting
     */
    getSyncMode() {
        const syncCheckbox = document.getElementById('sync-maps');
        return syncCheckbox ? syncCheckbox.checked : true;
    }
    
    /**
     * Set sync mode
     */
    setSyncMode(enabled) {
        const syncCheckbox = document.getElementById('sync-maps');
        if (syncCheckbox) {
            syncCheckbox.checked = enabled;
        }
        
        this.eventBus.emit('comparison:syncModeChanged', { enabled });
    }
    
    /**
     * Sync map view (placeholder for actual implementation)
     */
    syncMapView(data) {
        // Would synchronize map view between comparison layers
        console.log('[Comparison] Syncing map view:', data);
    }
    
    /**
     * Get comparison state
     */
    getComparisonState() {
        return {
            isActive: this.isComparisonMode,
            leftLayer: this.leftLayer,
            rightLayer: this.rightLayer,
            swipePosition: this.swipePosition,
            syncMode: this.getSyncMode()
        };
    }
    
    /**
     * Export comparison view
     */
    exportComparison() {
        if (!this.isComparisonMode) {
            return null;
        }
        
        return {
            leftLayer: this.leftLayer,
            rightLayer: this.rightLayer,
            swipePosition: this.swipePosition,
            timestamp: new Date().toISOString()
        };
    }
    
    /**
     * Clean up resources
     */
    destroy() {
        if (this.isComparisonMode) {
            this.exitComparisonMode();
        }
        
        this.destroySwipeInterface();
        this.clearComparisonLayers();
        
        // Remove comparison container
        if (this.comparisonContainer && this.comparisonContainer.parentNode) {
            this.comparisonContainer.parentNode.removeChild(this.comparisonContainer);
        }
        
        console.log('[Comparison] Destroyed');
    }
} 