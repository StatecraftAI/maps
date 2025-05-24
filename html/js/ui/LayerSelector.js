/**
 * LayerSelector - Custom Layer Selection Dropdown
 * 
 * Manages the custom layer selector with categorized groups:
 * - Collapsible layer groups (Electoral, Analytical, Demographic, Administrative)
 * - Current selection display
 * - "None" option for base map only
 * - Layer explanations integration
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';

export class LayerSelector {
    constructor(stateManager, eventBus) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        
        // DOM references
        this.container = null;
        this.currentSelection = null;
        this.dropdown = null;
        
        // State tracking
        this.isInitialized = false;
        this.isOpen = false;
        
        // Category configuration
        this.categoryOrder = [
            { key: 'electoral', name: '📊 Results' },
            { key: 'analytical', name: '🔬 Analytics' },
            { key: 'demographic', name: '👥 Demographics' },
            { key: 'administrative', name: '📋 Admin' }
        ];
        
        console.log('[LayerSelector] Initialized');
    }
    
    /**
     * Initialize the layer selector
     */
    initialize() {
        if (this.isInitialized) {
            console.warn('[LayerSelector] Already initialized');
            return;
        }
        
        try {
            this.findContainer();
            this.setupEventListeners();
            this.restoreGroupStates();
            
            this.isInitialized = true;
            console.log('[LayerSelector] Successfully initialized');
            
        } catch (error) {
            console.error('[LayerSelector] Failed to initialize:', error);
        }
    }
    
    /**
     * Find the container element
     */
    findContainer() {
        this.container = document.getElementById('layer-selector');
        if (!this.container) {
            throw new Error('Layer selector container not found');
        }
    }
    
    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Listen for data changes
        this.eventBus.on('data:processed', (data) => {
            this.updateOptions(data.processedData);
        });
        
        // Listen for layer changes
        this.eventBus.on('map:layerChanged', (data) => {
            this.updateSelectionDisplay(data.layerKey);
        });
        
        // Close dropdown when clicking outside
        document.addEventListener('click', (event) => {
            if (!this.container.contains(event.target)) {
                this.closeDropdown();
            }
        });
        
        console.log('[LayerSelector] Event listeners set up');
    }
    
    /**
     * Update layer options based on processed data
     */
    updateOptions(processedData = null) {
        if (!processedData) {
            // Get current processed data from state
            processedData = this.stateManager.getState('processedData');
        }
        
        if (!processedData || !processedData.layerOrganization) {
            console.warn('[LayerSelector] No layer organization data available');
            return;
        }
        
        this.buildSelector(processedData.layerOrganization, processedData.fieldInfo);
        this.updateSelectionDisplay();
        
        console.log('[LayerSelector] Options updated');
    }
    
    /**
     * Build the custom selector UI
     */
    buildSelector(layersByCategory, fieldInfo) {
        this.container.innerHTML = '';
        
        // Current selection display
        this.currentSelection = document.createElement('div');
        this.currentSelection.className = 'layer-current-selection';
        this.currentSelection.innerHTML = `
            <span id="layer-current-text">Loading...</span>
            <span class="layer-dropdown-arrow">▼</span>
        `;
        this.currentSelection.onclick = () => this.toggleDropdown();
        this.container.appendChild(this.currentSelection);
        
        // Dropdown content
        this.dropdown = document.createElement('div');
        this.dropdown.id = 'layer-dropdown';
        this.dropdown.className = 'layer-selector';
        this.dropdown.style.display = 'none';
        
        // Add "None" option
        this.addNoneOption();
        
        // Add categorized layers
        this.categoryOrder.forEach(({ key, name }) => {
            const categoryLayers = layersByCategory[key];
            if (categoryLayers && categoryLayers.length > 0) {
                this.addLayerGroup(key, name, categoryLayers, fieldInfo);
            }
        });
        
        // Add uncategorized layers
        const uncategorized = layersByCategory.other || [];
        if (uncategorized.length > 0) {
            this.addLayerGroup('other', '🔹 Other', uncategorized, fieldInfo);
        }
        
        this.container.appendChild(this.dropdown);
    }
    
    /**
     * Add "None" option for base map only
     */
    addNoneOption() {
        const noneOption = document.createElement('div');
        noneOption.className = 'layer-option none-option';
        noneOption.dataset.value = 'none';
        noneOption.textContent = 'No Data Layer (Base Map Only)';
        noneOption.onclick = () => this.selectLayer('none');
        this.dropdown.appendChild(noneOption);
    }
    
    /**
     * Add a collapsible layer group
     */
    addLayerGroup(groupKey, groupName, layers, fieldInfo) {
        const group = document.createElement('div');
        group.className = 'layer-group collapsed';
        group.dataset.group = groupKey;
        
        // Group header (collapsible)
        const header = document.createElement('button');
        header.type = 'button';
        header.className = 'layer-group-header';
        header.innerHTML = `
            <span>${groupName}</span>
            <span class="layer-group-toggle">▼</span>
        `;
        header.onclick = () => this.toggleLayerGroup(groupKey);
        group.appendChild(header);
        
        // Group content
        const content = document.createElement('div');
        content.className = 'layer-group-content';
        
        // Sort layers within category alphabetically by display name
        const sortedLayers = layers
            .map(layer => ({
                value: layer,
                display: this.getFieldDisplayName(layer, fieldInfo)
            }))
            .sort((a, b) => a.display.localeCompare(b.display));
        
        sortedLayers.forEach(({ value, display }) => {
            const option = document.createElement('div');
            option.className = 'layer-option';
            option.dataset.value = value;
            option.textContent = display;
            option.onclick = () => this.selectLayer(value);
            content.appendChild(option);
        });
        
        group.appendChild(content);
        this.dropdown.appendChild(group);
    }
    
    /**
     * Get field display name with fallback
     */
    getFieldDisplayName(fieldKey, fieldInfo) {
        if (fieldInfo && fieldInfo.displayNames && fieldInfo.displayNames[fieldKey]) {
            return fieldInfo.displayNames[fieldKey];
        }
        
        // Fallback to generating display name
        return this.generateDisplayName(fieldKey);
    }
    
    /**
     * Generate display name from field key
     */
    generateDisplayName(fieldKey) {
        // Handle candidate fields
        if (fieldKey.startsWith('votes_') && fieldKey !== 'votes_total') {
            const candidateName = fieldKey.replace('votes_', '');
            return `Vote Count - ${this.toTitleCase(candidateName)}`;
        }
        
        if (fieldKey.startsWith('vote_pct_') && !fieldKey.startsWith('vote_pct_contribution_')) {
            const candidateName = fieldKey.replace('vote_pct_', '');
            return `Vote % - ${this.toTitleCase(candidateName)}`;
        }
        
        if (fieldKey.startsWith('vote_pct_contribution_')) {
            const candidateName = fieldKey.replace('vote_pct_contribution_', '');
            return `Vote Contribution % - ${this.toTitleCase(candidateName)}`;
        }
        
        if (fieldKey.startsWith('reg_pct_')) {
            const party = fieldKey.replace('reg_pct_', '').toUpperCase();
            return `Registration % - ${party}`;
        }
        
        // Default: convert to title case
        return this.toTitleCase(fieldKey);
    }
    
    /**
     * Convert snake_case to Title Case
     */
    toTitleCase(str) {
        if (!str) return '';
        return str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }
    
    /**
     * Toggle dropdown visibility
     */
    toggleDropdown() {
        if (this.isOpen) {
            this.closeDropdown();
        } else {
            this.openDropdown();
        }
    }
    
    /**
     * Open dropdown
     */
    openDropdown() {
        if (!this.dropdown) return;
        
        this.dropdown.style.display = 'block';
        this.currentSelection.classList.add('open');
        this.isOpen = true;
        
        // Restore group states
        this.restoreGroupStates();
    }
    
    /**
     * Close dropdown
     */
    closeDropdown() {
        if (!this.dropdown) return;
        
        this.dropdown.style.display = 'none';
        this.currentSelection.classList.remove('open');
        this.isOpen = false;
    }
    
    /**
     * Select a layer
     */
    selectLayer(layerValue) {
        console.log(`[LayerSelector] Layer selected: ${layerValue}`);
        
        // Update state
        this.stateManager.setState({
            currentField: layerValue,
            customRange: null // Reset custom range when changing layers
        });
        
        // Update UI
        this.updateSelectionDisplay(layerValue);
        this.closeDropdown();
        
        // Emit event
        this.eventBus.emit('ui:layerSelected', { layerKey: layerValue });
    }
    
    /**
     * Update the current selection display
     */
    updateSelectionDisplay(layerKey = null) {
        if (!layerKey) {
            layerKey = this.stateManager.getState('currentField') || 'political_lean';
        }
        
        const currentText = document.getElementById('layer-current-text');
        if (currentText) {
            const processedData = this.stateManager.getState('processedData');
            const displayName = this.getFieldDisplayName(layerKey, processedData?.fieldInfo);
            currentText.textContent = displayName;
        }
        
        // Update selected state in options
        document.querySelectorAll('.layer-option').forEach(option => {
            if (option.dataset.value === layerKey) {
                option.classList.add('selected');
            } else {
                option.classList.remove('selected');
            }
        });
    }
    
    /**
     * Toggle individual layer group
     */
    toggleLayerGroup(groupKey) {
        const group = document.querySelector(`[data-group="${groupKey}"]`);
        if (!group) return;
        
        group.classList.toggle('collapsed');
        
        // Save group state to localStorage
        this.saveGroupState(groupKey, !group.classList.contains('collapsed'));
    }
    
    /**
     * Save group state to localStorage
     */
    saveGroupState(groupKey, isExpanded) {
        const groupStates = JSON.parse(localStorage.getItem('layerGroupStates') || '{}');
        groupStates[groupKey] = isExpanded;
        localStorage.setItem('layerGroupStates', JSON.stringify(groupStates));
    }
    
    /**
     * Restore layer group states from localStorage
     */
    restoreGroupStates() {
        const groupStates = JSON.parse(localStorage.getItem('layerGroupStates') || '{}');
        
        document.querySelectorAll('.layer-group').forEach(group => {
            const groupKey = group.dataset.group;
            if (groupStates.hasOwnProperty(groupKey)) {
                if (groupStates[groupKey]) {
                    group.classList.remove('collapsed');
                } else {
                    group.classList.add('collapsed');
                }
            } else {
                // Keep default collapsed state for new groups
                group.classList.add('collapsed');
            }
        });
    }
    
    /**
     * Get currently selected layer
     */
    getSelectedLayer() {
        return this.stateManager.getState('currentField');
    }
    
    /**
     * Set enabled/disabled state
     */
    setEnabled(enabled) {
        if (this.currentSelection) {
            this.currentSelection.style.pointerEvents = enabled ? 'auto' : 'none';
            this.currentSelection.style.opacity = enabled ? '1' : '0.5';
        }
        
        if (this.dropdown) {
            this.dropdown.style.pointerEvents = enabled ? 'auto' : 'none';
        }
    }
    
    /**
     * Clear current selection
     */
    clear() {
        this.updateSelectionDisplay('none');
        this.closeDropdown();
    }
    
    /**
     * Validate current state
     */
    validate() {
        const selectedLayer = this.getSelectedLayer();
        const issues = [];
        
        if (!selectedLayer) {
            issues.push('No layer selected');
        }
        
        return {
            isValid: issues.length === 0,
            issues: issues
        };
    }
    
    /**
     * Clean up resources
     */
    destroy() {
        // Remove event listeners
        document.removeEventListener('click', this.closeDropdown);
        
        // Clear DOM references
        this.container = null;
        this.currentSelection = null;
        this.dropdown = null;
        
        this.isInitialized = false;
        
        console.log('[LayerSelector] Destroyed');
    }
} 