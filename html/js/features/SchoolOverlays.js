/**
 * SchoolOverlays - School Location and Boundary Overlays
 * 
 * Handles:
 * - School location markers (elementary, middle, high schools)
 * - School boundary overlays (attendance zones)
 * - Custom school icons and styling
 * - School data loading and caching
 * - Layer toggle functionality
 * - School information popups
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';

export class SchoolOverlays {
    constructor(stateManager, eventBus, mapManager) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        this.mapManager = mapManager;
        
        // School layers storage
        this.schoolLayers = new Map();
        
        // School overlay configuration
        this.overlayConfig = {
            'high-schools': {
                type: 'location',
                dataUrl: '../data/geospatial/pps_high_school_locations.geojson',
                iconColor: '#4E3A6D',
                iconShape: 'square',
                name: 'High Schools'
            },
            'middle-schools': {
                type: 'location', 
                dataUrl: '../data/geospatial/pps_middle_school_locations.geojson',
                iconColor: '#4F4F4F',
                iconShape: 'triangle',
                name: 'Middle Schools'
            },
            'elementary-schools': {
                type: 'location',
                dataUrl: '../data/geospatial/pps_elementary_school_locations.geojson',
                iconColor: '#000000',
                iconShape: 'circle',
                name: 'Elementary Schools'
            },
            'high-boundaries': {
                type: 'boundary',
                dataUrl: '../data/geospatial/pps_high_school_boundaries.geojson',
                color: '#d62728',
                name: 'High School Boundaries'
            },
            'middle-boundaries': {
                type: 'boundary',
                dataUrl: '../data/geospatial/pps_middle_school_boundaries.geojson',
                color: '#2ca02c',
                name: 'Middle School Boundaries'
            },
            'elementary-boundaries': {
                type: 'boundary',
                dataUrl: '../data/geospatial/pps_elementary_school_boundaries.geojson',
                color: '#1f77b4',
                name: 'Elementary School Boundaries'
            },
            'district-boundary': {
                type: 'boundary',
                dataUrl: '../data/geospatial/pps_district_boundary.geojson',
                color: '#ff7f0e',
                weight: 3,
                name: 'District Boundary'
            }
        };
        
        // Data cache
        this.dataCache = new Map();
        this.loadingStates = new Map();
        
        this.initializeElements();
        this.setupEventListeners();
        
        console.log('[SchoolOverlays] Initialized');
    }
    
    /**
     * Initialize DOM elements and replace onclick handlers
     */
    initializeElements() {
        const overlayIds = Object.keys(this.overlayConfig);
        
        overlayIds.forEach(layerId => {
            const checkbox = document.getElementById(`show-${layerId}`);
            if (checkbox) {
                // Replace any existing event listeners
                checkbox.removeEventListener('change', this.handleOverlayToggle);
                checkbox.addEventListener('change', (e) => this.handleOverlayToggle(e, layerId));
            }
        });
    }
    
    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Listen for overlay toggle requests from other components
        this.eventBus.on('features:schoolOverlayToggled', (data) => {
            this.toggleOverlay(data.layerId, data.enabled);
        });
        
        // Listen for school data loading requests
        this.eventBus.on('features:loadSchoolData', () => {
            this.preloadAllSchoolData();
        });
        
        // Listen for map ready event to preload data
        this.eventBus.on('map:ready', () => {
            this.preloadAllSchoolData();
        });
    }
    
    /**
     * Handle overlay checkbox toggle
     */
    handleOverlayToggle(event, layerId) {
        const isEnabled = event.target.checked;
        this.toggleOverlay(layerId, isEnabled);
    }
    
    /**
     * Toggle overlay on/off
     */
    async toggleOverlay(layerId, enabled) {
        try {
            console.log(`[SchoolOverlays] Toggling ${layerId}: ${enabled}`);
            
            if (enabled) {
                await this.showOverlay(layerId);
            } else {
                this.hideOverlay(layerId);
            }
            
            // Update checkbox state
            const checkbox = document.getElementById(`show-${layerId}`);
            if (checkbox) {
                checkbox.checked = enabled;
            }
            
            // Emit event
            this.eventBus.emit('schoolOverlays:toggled', {
                layerId: layerId,
                enabled: enabled,
                hasData: this.schoolLayers.has(layerId)
            });
            
        } catch (error) {
            console.error(`[SchoolOverlays] Failed to toggle ${layerId}:`, error);
            
            // Reset checkbox on error
            const checkbox = document.getElementById(`show-${layerId}`);
            if (checkbox) {
                checkbox.checked = false;
            }
            
            this.eventBus.emit('schoolOverlays:error', {
                layerId: layerId,
                error: error.message
            });
        }
    }
    
    /**
     * Show overlay on map
     */
    async showOverlay(layerId) {
        // Check if already visible
        if (this.schoolLayers.has(layerId)) {
            const layer = this.schoolLayers.get(layerId);
            const map = this.mapManager.getMap();
            if (map && map.hasLayer(layer)) {
                console.log(`[SchoolOverlays] ${layerId} already visible`);
                return;
            }
        }
        
        // Load data if needed
        const data = await this.loadSchoolData(layerId);
        if (!data) {
            throw new Error(`Failed to load data for ${layerId}`);
        }
        
        // Create layer if not exists
        if (!this.schoolLayers.has(layerId)) {
            const layer = this.createLayer(layerId, data);
            this.schoolLayers.set(layerId, layer);
        }
        
        // Add to map
        const layer = this.schoolLayers.get(layerId);
        const map = this.mapManager.getMap();
        if (map && layer) {
            layer.addTo(map);
            console.log(`[SchoolOverlays] Added ${layerId} to map`);
        }
    }
    
    /**
     * Hide overlay from map
     */
    hideOverlay(layerId) {
        const layer = this.schoolLayers.get(layerId);
        if (layer) {
            const map = this.mapManager.getMap();
            if (map && map.hasLayer(layer)) {
                map.removeLayer(layer);
                console.log(`[SchoolOverlays] Removed ${layerId} from map`);
            }
        }
    }
    
    /**
     * Load school data for a specific overlay
     */
    async loadSchoolData(layerId) {
        // Check cache first
        if (this.dataCache.has(layerId)) {
            console.log(`[SchoolOverlays] Using cached data for ${layerId}`);
            return this.dataCache.get(layerId);
        }
        
        // Check if already loading
        if (this.loadingStates.get(layerId)) {
            console.log(`[SchoolOverlays] Already loading ${layerId}, waiting...`);
            return this.loadingStates.get(layerId);
        }
        
        const config = this.overlayConfig[layerId];
        if (!config) {
            throw new Error(`Unknown overlay: ${layerId}`);
        }
        
        console.log(`[SchoolOverlays] Loading data for ${layerId} from ${config.dataUrl}`);
        
        // Create loading promise
        const loadingPromise = this.fetchSchoolData(config.dataUrl, layerId);
        this.loadingStates.set(layerId, loadingPromise);
        
        try {
            const data = await loadingPromise;
            this.dataCache.set(layerId, data);
            this.loadingStates.delete(layerId);
            
            console.log(`[SchoolOverlays] Loaded ${data.features?.length || 0} features for ${layerId}`);
            return data;
            
        } catch (error) {
            this.loadingStates.delete(layerId);
            console.error(`[SchoolOverlays] Failed to load ${layerId}:`, error);
            throw error;
        }
    }
    
    /**
     * Fetch school data from URL
     */
    async fetchSchoolData(url, layerId) {
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Validate GeoJSON structure
        if (!data || !data.features || !Array.isArray(data.features)) {
            throw new Error(`Invalid GeoJSON data for ${layerId}`);
        }
        
        return data;
    }
    
    /**
     * Create Leaflet layer from school data
     */
    createLayer(layerId, data) {
        const config = this.overlayConfig[layerId];
        
        if (config.type === 'location') {
            return this.createLocationLayer(layerId, data, config);
        } else if (config.type === 'boundary') {
            return this.createBoundaryLayer(layerId, data, config);
        } else {
            throw new Error(`Unknown layer type: ${config.type}`);
        }
    }
    
    /**
     * Create location marker layer
     */
    createLocationLayer(layerId, data, config) {
        return L.geoJSON(data, {
            pointToLayer: (feature, latlng) => {
                const icon = this.createSchoolIcon(config);
                return L.marker(latlng, { icon });
            },
            onEachFeature: (feature, layer) => {
                layer.bindPopup(this.createSchoolPopup(feature, config));
            }
        });
    }
    
    /**
     * Create boundary polygon layer
     */
    createBoundaryLayer(layerId, data, config) {
        return L.geoJSON(data, {
            style: {
                fillColor: config.color,
                weight: config.weight || 2,
                opacity: 0.8,
                color: config.color,
                fillOpacity: 0.0  // Transparent fill, only outline
            },
            onEachFeature: (feature, layer) => {
                layer.bindPopup(this.createBoundaryPopup(feature, config));
            }
        });
    }
    
    /**
     * Create custom school icon
     */
    createSchoolIcon(config) {
        const iconHtml = this.generateIconSVG(config);
        
        return L.divIcon({
            html: iconHtml,
            className: 'custom-school-icon',
            iconSize: [20, 20],
            iconAnchor: [10, 10]
        });
    }
    
    /**
     * Generate SVG icon based on config
     */
    generateIconSVG(config) {
        const { iconColor, iconShape } = config;
        
        switch (iconShape) {
            case 'square':
                return `
                    <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <rect x="2" y="2" width="12" height="12" fill="${iconColor}" stroke="#000" stroke-width="0.5" rx="1"/>
                        <rect x="4" y="4" width="8" height="8" fill="#FFFFFF" stroke="${iconColor}" stroke-width="0.5"/>
                        <circle cx="8" cy="8" r="2" fill="${iconColor}"/>
                    </svg>
                `;
                
            case 'triangle':
                return `
                    <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <polygon points="8,2 14,14 2,14" fill="${iconColor}" stroke="#000" stroke-width="0.5"/>
                        <polygon points="8,4 12,12 4,12" fill="#FFFFFF" stroke="${iconColor}" stroke-width="0.5"/>
                        <circle cx="8" cy="10" r="1.5" fill="${iconColor}"/>
                    </svg>
                `;
                
            case 'circle':
            default:
                return `
                    <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <circle cx="8" cy="8" r="6" fill="${iconColor}" stroke="#4F4F4F" stroke-width="0.5"/>
                        <circle cx="8" cy="8" r="4" fill="#F4F4F3" stroke="${iconColor}" stroke-width="0.5"/>
                        <circle cx="8" cy="8" r="2" fill="${iconColor}"/>
                    </svg>
                `;
        }
    }
    
    /**
     * Create school location popup content
     */
    createSchoolPopup(feature, config) {
        const props = feature.properties;
        
        return `
            <div style="max-width: 250px;">
                <h4>${props.School_Name || props.simple_nm || 'School'}</h4>
                <p><strong>Address:</strong> ${props.SiteAddress || 'N/A'}</p>
                <p><strong>Type:</strong> ${props.School_Type || props.School_GradeGroup || config.name}</p>
                <p><strong>Status:</strong> ${props.Status || 'Active'}</p>
                ${props.HS_Cluster ? `<p><strong>HS Cluster:</strong> ${props.HS_Cluster}</p>` : ''}
                ${props.Enrollment ? `<p><strong>Enrollment:</strong> ${props.Enrollment}</p>` : ''}
            </div>
        `;
    }
    
    /**
     * Create school boundary popup content
     */
    createBoundaryPopup(feature, config) {
        const props = feature.properties;
        
        return `
            <div style="max-width: 200px;">
                <h4>${props.School_Name || props.SCHOOL_NAM || config.name}</h4>
                <p><strong>Type:</strong> ${config.name}</p>
                ${props.School_GradeGroup ? `<p><strong>Grades:</strong> ${props.School_GradeGroup}</p>` : ''}
                ${props.Capacity ? `<p><strong>Capacity:</strong> ${props.Capacity}</p>` : ''}
            </div>
        `;
    }
    
    /**
     * Preload all school data for better performance
     */
    async preloadAllSchoolData() {
        console.log('[SchoolOverlays] Preloading all school data...');
        
        const loadPromises = Object.keys(this.overlayConfig).map(async (layerId) => {
            try {
                await this.loadSchoolData(layerId);
                console.log(`[SchoolOverlays] Preloaded ${layerId}`);
            } catch (error) {
                console.warn(`[SchoolOverlays] Failed to preload ${layerId}:`, error.message);
            }
        });
        
        await Promise.allSettled(loadPromises);
        
        this.eventBus.emit('schoolOverlays:preloadComplete', {
            loaded: this.dataCache.size,
            total: Object.keys(this.overlayConfig).length
        });
        
        console.log(`[SchoolOverlays] Preloading complete: ${this.dataCache.size}/${Object.keys(this.overlayConfig).length} layers`);
    }
    
    /**
     * Get overlay states
     */
    getOverlayStates() {
        const states = {};
        const map = this.mapManager.getMap();
        
        Object.keys(this.overlayConfig).forEach(layerId => {
            const layer = this.schoolLayers.get(layerId);
            states[layerId] = {
                loaded: this.dataCache.has(layerId),
                visible: layer && map && map.hasLayer(layer),
                loading: this.loadingStates.has(layerId)
            };
        });
        
        return states;
    }
    
    /**
     * Show all overlays of a specific type
     */
    async showAllByType(type) {
        const layerIds = Object.keys(this.overlayConfig)
            .filter(id => this.overlayConfig[id].type === type);
        
        for (const layerId of layerIds) {
            await this.toggleOverlay(layerId, true);
        }
    }
    
    /**
     * Hide all overlays of a specific type
     */
    hideAllByType(type) {
        const layerIds = Object.keys(this.overlayConfig)
            .filter(id => this.overlayConfig[id].type === type);
        
        layerIds.forEach(layerId => {
            this.toggleOverlay(layerId, false);
        });
    }
    
    /**
     * Show all school locations
     */
    async showAllLocations() {
        await this.showAllByType('location');
    }
    
    /**
     * Show all school boundaries
     */
    async showAllBoundaries() {
        await this.showAllByType('boundary');
    }
    
    /**
     * Hide all overlays
     */
    hideAllOverlays() {
        Object.keys(this.overlayConfig).forEach(layerId => {
            this.hideOverlay(layerId);
            
            // Update checkbox
            const checkbox = document.getElementById(`show-${layerId}`);
            if (checkbox) {
                checkbox.checked = false;
            }
        });
    }
    
    /**
     * Get layer by ID
     */
    getLayer(layerId) {
        return this.schoolLayers.get(layerId);
    }
    
    /**
     * Check if layer is visible
     */
    isLayerVisible(layerId) {
        const layer = this.schoolLayers.get(layerId);
        const map = this.mapManager.getMap();
        return layer && map && map.hasLayer(layer);
    }
    
    /**
     * Get school overlay statistics
     */
    getOverlayStats() {
        const totalLayers = Object.keys(this.overlayConfig).length;
        const loadedLayers = this.dataCache.size;
        const visibleLayers = Object.keys(this.overlayConfig)
            .filter(id => this.isLayerVisible(id)).length;
        
        return {
            total: totalLayers,
            loaded: loadedLayers,
            visible: visibleLayers,
            loading: this.loadingStates.size,
            config: this.overlayConfig
        };
    }
    
    /**
     * Refresh overlay (reload data and update display)
     */
    async refreshOverlay(layerId) {
        console.log(`[SchoolOverlays] Refreshing ${layerId}`);
        
        // Clear cache
        this.dataCache.delete(layerId);
        
        // Remove existing layer
        this.hideOverlay(layerId);
        this.schoolLayers.delete(layerId);
        
        // Check if should be visible
        const checkbox = document.getElementById(`show-${layerId}`);
        if (checkbox && checkbox.checked) {
            await this.showOverlay(layerId);
        }
    }
    
    /**
     * Clean up resources
     */
    destroy() {
        // Hide all overlays
        this.hideAllOverlays();
        
        // Clear data structures
        this.schoolLayers.clear();
        this.dataCache.clear();
        this.loadingStates.clear();
        
        console.log('[SchoolOverlays] Destroyed');
    }
} 