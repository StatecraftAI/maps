/**
 * Search - Address Search and Location Finding
 * 
 * Handles:
 * - Address geocoding using Nominatim API
 * - GPS location finding
 * - Precinct identification from coordinates
 * - Search result management and UI
 * - Location markers and map navigation
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';

export class Search {
    constructor(stateManager, eventBus, mapManager) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        this.mapManager = mapManager;
        
        // Search state
        this.searchMarker = null;
        this.locationMarker = null;
        this.isSearching = false;
        
        // Search configuration
        this.nominatimBaseUrl = 'https://nominatim.openstreetmap.org/search';
        this.searchBounds = {
            viewbox: '-122.9,45.2,-122.4,45.8', // Portland area
            bounded: 1
        };
        
        this.initializeElements();
        this.setupEventListeners();
        
        console.log('[Search] Initialized');
    }
    
    /**
     * Initialize DOM elements
     */
    initializeElements() {
        this.searchInput = document.getElementById('address-search');
        this.searchButton = document.querySelector('[onclick="searchAddress()"]');
        this.searchResults = document.getElementById('search-results');
        this.locationButton = document.querySelector('[onclick="findMyLocation()"]');
        
        if (!this.searchInput || !this.searchResults) {
            console.warn('[Search] Required DOM elements not found');
            return;
        }
        
        // Replace inline onclick handlers
        if (this.searchButton) {
            this.searchButton.removeAttribute('onclick');
            this.searchButton.addEventListener('click', () => this.searchAddress());
        }
        
        if (this.locationButton) {
            this.locationButton.removeAttribute('onclick');
            this.locationButton.addEventListener('click', () => this.findMyLocation());
        }
    }
    
    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Search input events
        if (this.searchInput) {
            this.searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    this.searchAddress();
                }
            });
            
            this.searchInput.addEventListener('input', () => {
                if (this.searchInput.value.trim() === '') {
                    this.hideSearchResults();
                }
            });
        }
        
        // Listen for map state changes
        this.eventBus.on('data:loaded', () => {
            // Enable search after data is loaded
            this.enableSearch();
        });
        
        // Click outside to close search results
        document.addEventListener('click', (event) => {
            if (!this.searchInput?.contains(event.target) && 
                !this.searchResults?.contains(event.target)) {
                this.hideSearchResults();
            }
        });
    }
    
    /**
     * Search for address using Nominatim geocoding
     */
    async searchAddress() {
        if (!this.searchInput) return;
        
        const query = this.searchInput.value.trim();
        if (!query) {
            this.showSearchError('Please enter an address or location to search.');
            return;
        }
        
        if (this.isSearching) {
            console.warn('[Search] Search already in progress');
            return;
        }
        
        try {
            this.setSearching(true);
            console.log(`[Search] Searching for: "${query}"`);
            
            const results = await this.geocodeAddress(query);
            
            if (results.length === 0) {
                this.showSearchError('No results found. Try searching for "School Name" or "Street Address".');
            } else {
                this.displaySearchResults(results);
                this.eventBus.emit('search:resultsFound', { 
                    query, 
                    results: results.length 
                });
            }
            
        } catch (error) {
            console.error('[Search] Search failed:', error);
            this.showSearchError('Search failed. Please try again.');
            this.eventBus.emit('search:error', { query, error: error.message });
        } finally {
            this.setSearching(false);
        }
    }
    
    /**
     * Geocode address using Nominatim API
     */
    async geocodeAddress(query) {
        const searchUrl = new URL(this.nominatimBaseUrl);
        searchUrl.searchParams.set('q', `${query} Portland Oregon`);
        searchUrl.searchParams.set('format', 'json');
        searchUrl.searchParams.set('limit', '5');
        searchUrl.searchParams.set('addressdetails', '1');
        searchUrl.searchParams.set('viewbox', this.searchBounds.viewbox);
        searchUrl.searchParams.set('bounded', this.searchBounds.bounded);
        
        const response = await fetch(searchUrl.toString());
        
        if (!response.ok) {
            throw new Error(`Geocoding API error: ${response.status}`);
        }
        
        const results = await response.json();
        
        // Validate and format results
        return results.map(result => ({
            lat: parseFloat(result.lat),
            lng: parseFloat(result.lon),
            displayName: result.display_name,
            name: result.display_name.split(',')[0],
            type: result.type || 'location'
        })).filter(result => 
            !isNaN(result.lat) && 
            !isNaN(result.lng) &&
            result.lat >= 45.2 && result.lat <= 45.8 &&
            result.lng >= -122.9 && result.lng <= -122.4
        );
    }
    
    /**
     * Display search results
     */
    displaySearchResults(results) {
        if (!this.searchResults) return;
        
        let html = '<div style="max-height: 150px; overflow-y: auto; border: 1px solid var(--color-border); border-radius: var(--border-radius); background: var(--color-surface);">';
        
        results.forEach((result, index) => {
            html += `
                <div class="search-result-item" 
                     style="padding: var(--space-2); border-bottom: 1px solid var(--color-border); cursor: pointer; transition: background-color var(--transition);"
                     onmouseover="this.style.backgroundColor='var(--color-background)'"
                     onmouseout="this.style.backgroundColor='transparent'"
                     onclick="window.searchInstance?.selectSearchResult(${result.lat}, ${result.lng}, '${result.displayName.replace(/'/g, "\\'")}')">
                    <strong>${result.name}</strong><br>
                    <small style="color: var(--color-text-secondary);">${result.displayName}</small>
                </div>
            `;
        });
        
        html += '</div>';
        this.searchResults.innerHTML = html;
        this.searchResults.style.display = 'block';
        
        // Make this instance available for onclick handlers
        window.searchInstance = this;
    }
    
    /**
     * Select a search result and navigate to it
     */
    selectSearchResult(lat, lng, displayName) {
        try {
            console.log(`[Search] Selected result: ${displayName} (${lat}, ${lng})`);
            
            // Remove previous search marker
            this.clearSearchMarker();
            
            // Create new search marker
            this.searchMarker = L.marker([lat, lng], {
                icon: L.divIcon({
                    html: '🔍',
                    className: 'custom-search-icon',
                    iconSize: [25, 25],
                    iconAnchor: [12, 12]
                })
            });
            
            // Add marker to map
            const map = this.mapManager.getMap();
            if (map) {
                this.searchMarker.addTo(map);
                
                // Bind popup
                this.searchMarker.bindPopup(`
                    <div style="max-width: 200px;">
                        <h4>Search Result</h4>
                        <p>${displayName}</p>
                        <p><small>Lat: ${lat.toFixed(6)}, Lng: ${lng.toFixed(6)}</small></p>
                    </div>
                `).openPopup();
                
                // Zoom to location
                map.setView([lat, lng], 16);
            }
            
            // Check which precinct contains this location
            this.checkPrecinctAtLocation(lat, lng);
            
            // Clear search UI
            this.hideSearchResults();
            if (this.searchInput) {
                this.searchInput.value = '';
            }
            
            // Emit event
            this.eventBus.emit('search:locationSelected', {
                lat, lng, displayName,
                hasMarker: true
            });
            
        } catch (error) {
            console.error('[Search] Failed to select search result:', error);
        }
    }
    
    /**
     * Find user's current location using GPS
     */
    findMyLocation() {
        if (!navigator.geolocation) {
            alert('Geolocation is not supported by this browser.');
            return;
        }
        
        if (this.isSearching) {
            console.warn('[Search] Location search already in progress');
            return;
        }
        
        const btn = this.locationButton;
        if (!btn) return;
        
        const originalText = btn.textContent;
        btn.textContent = '📡 Finding Location...';
        btn.disabled = true;
        
        this.setSearching(true);
        
        navigator.geolocation.getCurrentPosition(
            (position) => {
                this.handleLocationSuccess(position, btn, originalText);
            },
            (error) => {
                this.handleLocationError(error, btn, originalText);
            },
            {
                enableHighAccuracy: true,
                timeout: 10000,
                maximumAge: 300000 // 5 minutes
            }
        );
    }
    
    /**
     * Handle successful location finding
     */
    handleLocationSuccess(position, btn, originalText) {
        try {
            const lat = position.coords.latitude;
            const lng = position.coords.longitude;
            const accuracy = position.coords.accuracy;
            
            console.log(`[Search] Location found: ${lat}, ${lng} (±${accuracy}m)`);
            
            // Remove previous location marker
            this.clearLocationMarker();
            
            // Create location marker
            this.locationMarker = L.marker([lat, lng], {
                icon: L.divIcon({
                    html: '📍',
                    className: 'custom-location-icon',
                    iconSize: [25, 25],
                    iconAnchor: [12, 12]
                })
            });
            
            // Add to map
            const map = this.mapManager.getMap();
            if (map) {
                this.locationMarker.addTo(map);
                
                this.locationMarker.bindPopup(`
                    <div style="max-width: 200px;">
                        <h4>Your Location</h4>
                        <p>Accuracy: ±${Math.round(accuracy)} meters</p>
                        <p><small>Lat: ${lat.toFixed(6)}, Lng: ${lng.toFixed(6)}</small></p>
                    </div>
                `).openPopup();
                
                // Zoom to location
                map.setView([lat, lng], 16);
            }
            
            // Check precinct
            this.checkPrecinctAtLocation(lat, lng);
            
            // Emit event
            this.eventBus.emit('search:locationFound', {
                lat, lng, accuracy,
                hasMarker: true
            });
            
        } catch (error) {
            console.error('[Search] Error handling location success:', error);
        } finally {
            this.resetLocationButton(btn, originalText);
        }
    }
    
    /**
     * Handle location finding error
     */
    handleLocationError(error, btn, originalText) {
        console.error('[Search] Geolocation error:', error);
        
        let message = 'Unable to get your location. ';
        switch (error.code) {
            case error.PERMISSION_DENIED:
                message += 'Please allow location access.';
                break;
            case error.POSITION_UNAVAILABLE:
                message += 'Location information unavailable.';
                break;
            case error.TIMEOUT:
                message += 'Location request timed out.';
                break;
            default:
                message += 'Unknown error occurred.';
                break;
        }
        
        alert(message);
        
        this.eventBus.emit('search:locationError', {
            error: error.message,
            code: error.code
        });
        
        this.resetLocationButton(btn, originalText);
    }
    
    /**
     * Reset location button state
     */
    resetLocationButton(btn, originalText) {
        this.setSearching(false);
        if (btn) {
            btn.textContent = originalText;
            btn.disabled = false;
        }
    }
    
    /**
     * Check which precinct contains given coordinates
     */
    checkPrecinctAtLocation(lat, lng) {
        const electionData = this.stateManager.getState('electionData');
        if (!electionData) {
            console.warn('[Search] No election data available for precinct check');
            return;
        }
        
        const point = [lng, lat]; // GeoJSON uses [lng, lat] order
        let foundPrecinct = null;
        
        // Check each precinct
        electionData.features.forEach(feature => {
            if (this.isPointInPolygon(point, feature.geometry)) {
                foundPrecinct = feature.properties;
            }
        });
        
        // Emit precinct found event
        this.eventBus.emit('search:precinctFound', {
            lat, lng, precinct: foundPrecinct
        });
        
        return foundPrecinct;
    }
    
    /**
     * Point-in-polygon test
     */
    isPointInPolygon(point, geometry) {
        if (geometry.type === 'Polygon') {
            return this.pointInPolygon(point, geometry.coordinates[0]);
        } else if (geometry.type === 'MultiPolygon') {
            return geometry.coordinates.some(polygon => 
                this.pointInPolygon(point, polygon[0])
            );
        }
        return false;
    }
    
    /**
     * Ray casting algorithm for point-in-polygon
     */
    pointInPolygon(point, polygon) {
        const x = point[0];
        const y = point[1];
        let inside = false;
        
        for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
            const xi = polygon[i][0];
            const yi = polygon[i][1];
            const xj = polygon[j][0];
            const yj = polygon[j][1];
            
            if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
                inside = !inside;
            }
        }
        
        return inside;
    }
    
    /**
     * Set searching state
     */
    setSearching(isSearching) {
        this.isSearching = isSearching;
        
        if (this.searchInput) {
            this.searchInput.disabled = isSearching;
        }
        
        if (this.searchButton) {
            this.searchButton.disabled = isSearching;
        }
        
        if (isSearching) {
            this.showSearchProgress();
        }
    }
    
    /**
     * Show search progress
     */
    showSearchProgress() {
        if (this.searchResults) {
            this.searchResults.innerHTML = '<p style="color: var(--color-text-secondary); font-style: italic; padding: var(--space-3);">Searching...</p>';
            this.searchResults.style.display = 'block';
        }
    }
    
    /**
     * Show search error
     */
    showSearchError(message) {
        if (this.searchResults) {
            this.searchResults.innerHTML = `<p style="color: var(--color-text-secondary); padding: var(--space-3);">${message}</p>`;
            this.searchResults.style.display = 'block';
        }
    }
    
    /**
     * Hide search results
     */
    hideSearchResults() {
        if (this.searchResults) {
            this.searchResults.style.display = 'none';
            this.searchResults.innerHTML = '';
        }
        
        // Clean up global reference
        if (window.searchInstance === this) {
            delete window.searchInstance;
        }
    }
    
    /**
     * Enable search functionality
     */
    enableSearch() {
        if (this.searchInput) {
            this.searchInput.disabled = false;
            this.searchInput.placeholder = 'Enter address, school name, or landmark';
        }
        
        if (this.searchButton) {
            this.searchButton.disabled = false;
        }
        
        if (this.locationButton) {
            this.locationButton.disabled = false;
        }
    }
    
    /**
     * Clear search marker
     */
    clearSearchMarker() {
        if (this.searchMarker) {
            const map = this.mapManager.getMap();
            if (map) {
                map.removeLayer(this.searchMarker);
            }
            this.searchMarker = null;
        }
    }
    
    /**
     * Clear location marker
     */
    clearLocationMarker() {
        if (this.locationMarker) {
            const map = this.mapManager.getMap();
            if (map) {
                map.removeLayer(this.locationMarker);
            }
            this.locationMarker = null;
        }
    }
    
    /**
     * Clear all markers
     */
    clearAllMarkers() {
        this.clearSearchMarker();
        this.clearLocationMarker();
    }
    
    /**
     * Get search state
     */
    getSearchState() {
        return {
            isSearching: this.isSearching,
            hasSearchMarker: this.searchMarker !== null,
            hasLocationMarker: this.locationMarker !== null,
            lastQuery: this.searchInput?.value || null
        };
    }
    
    /**
     * Clean up resources
     */
    destroy() {
        this.clearAllMarkers();
        this.hideSearchResults();
        
        // Clean up global reference
        if (window.searchInstance === this) {
            delete window.searchInstance;
        }
        
        console.log('[Search] Destroyed');
    }
} 