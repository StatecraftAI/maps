/**
 * Foundation Integration Tests
 * 
 * Tests that verify the core foundation components work together correctly:
 * - StateManager + EventBus integration
 * - Data flow between components
 * - UI component initialization
 * - Error propagation and handling
 */

import { StateManager } from '../../html/js/core/StateManager.js';
import { EventBus } from '../../html/js/core/EventBus.js';
import { DataLoader } from '../../html/js/data/DataLoader.js';
import { ControlPanel } from '../../html/js/ui/ControlPanel.js';

// Mock DOM for UI components
function setupMockDOM() {
    document.body.innerHTML = `
        <div class="control-panel">
            <select id="dataset-select"></select>
            <input type="checkbox" id="pps-only" checked>
            <input type="range" id="opacity-slider" value="0.7" min="0.1" max="1" step="0.1">
            <select id="basemap-select">
                <option value="streets">Streets</option>
                <option value="satellite">Satellite</option>
            </select>
            <div id="range-control" style="display: none;">
                <input type="number" id="range-min">
                <input type="number" id="range-max">
                <div id="range-display"></div>
            </div>
            <button id="layer-help-btn">?</button>
            <div id="layer-selector"></div>
            <output id="opacity-value">70%</output>
            
            <!-- Accordion sections -->
            <div class="section collapsed" data-section="data-display">
                <div class="section-header">
                    <h4>Data & Display</h4>
                    <span class="section-toggle">▼</span>
                </div>
                <div class="section-content">
                    <p>Data controls</p>
                </div>
            </div>
        </div>
        
        <div id="layer-explanation" style="display: none;"></div>
    `;
}

describe('Foundation Integration Tests', () => {
    let stateManager, eventBus, dataLoader, controlPanel;
    
    beforeEach(() => {
        setupMockDOM();
        
        // Initialize core components
        stateManager = new StateManager();
        eventBus = new EventBus();
        dataLoader = new DataLoader(stateManager, eventBus);
        controlPanel = new ControlPanel(stateManager, eventBus);
    });
    
    afterEach(() => {
        // Clean up components
        if (controlPanel) controlPanel.destroy();
        if (dataLoader) dataLoader.destroy();
        if (stateManager) stateManager.destroy();
        
        document.body.innerHTML = '';
    });
    
    describe('Core Component Integration', () => {
        test('should initialize all components without errors', () => {
            expect(() => {
                controlPanel.initialize();
            }).not.toThrow();
            
            expect(stateManager.getState('currentField')).toBeDefined();
            expect(controlPanel.isInitialized).toBe(true);
        });
        
        test('should handle state changes across components', () => {
            controlPanel.initialize();
            
            const eventSpy = jest.fn();
            eventBus.on('ui:datasetChanged', eventSpy);
            
            // Simulate dataset change through ControlPanel
            const datasetSelect = document.getElementById('dataset-select');
            datasetSelect.value = 'zone2';
            datasetSelect.dispatchEvent(new Event('change'));
            
            // Verify state was updated
            expect(stateManager.getState('currentDataset')).toBe('zone2');
            expect(eventSpy).toHaveBeenCalledWith({ datasetKey: 'zone2' });
        });
        
        test('should handle PPS filter toggle', () => {
            controlPanel.initialize();
            
            const eventSpy = jest.fn();
            eventBus.on('ui:ppsFilterChanged', eventSpy);
            
            // Toggle PPS filter
            const ppsFilter = document.getElementById('pps-only');
            ppsFilter.checked = false;
            ppsFilter.dispatchEvent(new Event('change'));
            
            expect(stateManager.getState('showPpsOnly')).toBe(false);
            expect(eventSpy).toHaveBeenCalledWith({ showPpsOnly: false });
        });
        
        test('should handle opacity changes', () => {
            controlPanel.initialize();
            
            const eventSpy = jest.fn();
            eventBus.on('ui:opacityChanged', eventSpy);
            
            // Change opacity
            const opacitySlider = document.getElementById('opacity-slider');
            opacitySlider.value = '0.5';
            opacitySlider.dispatchEvent(new Event('input'));
            
            expect(stateManager.getState('mapOpacity')).toBe(0.5);
            expect(eventSpy).toHaveBeenCalledWith({ opacity: 0.5 });
            
            // Check that UI was updated
            const opacityValue = document.getElementById('opacity-value');
            expect(opacityValue.textContent).toBe('50%');
        });
    });
    
    describe('Data Flow Integration', () => {
        test('should handle dataset discovery workflow', async () => {
            // Mock successful dataset discovery
            const mockDatasets = {
                zone1: { title: 'Zone 1', file: 'zone1.geojson' },
                zone2: { title: 'Zone 2', file: 'zone2.geojson' }
            };
            
            controlPanel.initialize();
            
            // Simulate dataset discovery event
            eventBus.emit('data:discoveryComplete', { datasets: mockDatasets });
            
            // Check that ControlPanel populated the dropdown
            const datasetSelect = document.getElementById('dataset-select');
            expect(datasetSelect.children.length).toBe(2);
            expect(datasetSelect.children[0].textContent).toBe('Zone 1');
            expect(datasetSelect.children[1].textContent).toBe('Zone 2');
        });
        
        test('should handle loading states', () => {
            controlPanel.initialize();
            
            // Simulate loading start
            controlPanel.showLoading('Loading test data...');
            
            // Controls should be disabled
            const datasetSelect = document.getElementById('dataset-select');
            const ppsFilter = document.getElementById('pps-only');
            
            expect(datasetSelect.disabled).toBe(true);
            expect(ppsFilter.disabled).toBe(true);
            
            // Simulate loading complete
            controlPanel.hideLoading();
            
            expect(datasetSelect.disabled).toBe(false);
            expect(ppsFilter.disabled).toBe(false);
        });
    });
    
    describe('Event Bus Integration', () => {
        test('should handle event propagation between components', () => {
            controlPanel.initialize();
            
            const stateChangeSpy = jest.fn();
            const uiEventSpy = jest.fn();
            
            // Subscribe to multiple event types
            eventBus.on('ui:basemapChanged', uiEventSpy);
            stateManager.subscribe('currentBasemap', stateChangeSpy);
            
            // Trigger basemap change
            const basemapSelect = document.getElementById('basemap-select');
            basemapSelect.value = 'satellite';
            basemapSelect.dispatchEvent(new Event('change'));
            
            expect(uiEventSpy).toHaveBeenCalledWith({ basemapKey: 'satellite' });
            expect(stateChangeSpy).toHaveBeenCalledWith('satellite', 'streets');
        });
        
        test('should handle error events', () => {
            const errorSpy = jest.fn();
            eventBus.on('ui:error', errorSpy);
            
            // Force an initialization error by removing required DOM element
            document.getElementById('dataset-select').remove();
            
            controlPanel.initialize();
            
            expect(errorSpy).toHaveBeenCalled();
            expect(errorSpy.mock.calls[0][0]).toMatchObject({
                component: 'ControlPanel',
                error: expect.stringContaining('Required element not found')
            });
        });
    });
    
    describe('State Persistence Integration', () => {
        test('should restore state on component initialization', () => {
            // Mock saved state
            const savedState = {
                currentDataset: 'zone2',
                showPpsOnly: false,
                mapOpacity: 0.8,
                currentBasemap: 'satellite'
            };
            
            localStorage.getItem.mockReturnValue(JSON.stringify(savedState));
            
            // Create new state manager and restore
            const newStateManager = new StateManager();
            newStateManager.restore(['currentDataset', 'showPpsOnly', 'mapOpacity', 'currentBasemap']);
            
            // Initialize ControlPanel with restored state
            const newControlPanel = new ControlPanel(newStateManager, eventBus);
            newControlPanel.initialize();
            
            // Check that UI reflects restored state
            expect(document.getElementById('pps-only').checked).toBe(false);
            expect(document.getElementById('opacity-slider').value).toBe('0.8');
            expect(document.getElementById('basemap-select').value).toBe('satellite');
            
            newControlPanel.destroy();
            newStateManager.destroy();
        });
    });
    
    describe('Component Lifecycle', () => {
        test('should handle component destruction gracefully', () => {
            controlPanel.initialize();
            
            const eventSpy = jest.fn();
            eventBus.on('ui:datasetChanged', eventSpy);
            
            // Destroy component
            controlPanel.destroy();
            
            // Trigger event that destroyed component should not handle
            const datasetSelect = document.getElementById('dataset-select');
            if (datasetSelect) {
                datasetSelect.value = 'zone2';
                datasetSelect.dispatchEvent(new Event('change'));
            }
            
            // Event should not be emitted since component is destroyed
            expect(eventSpy).not.toHaveBeenCalled();
        });
        
        test('should validate component state', () => {
            controlPanel.initialize();
            
            // Set some values
            document.getElementById('dataset-select').value = 'zone1';
            document.getElementById('opacity-slider').value = '0.5';
            
            const validation = controlPanel.validate();
            expect(validation.isValid).toBe(true);
            expect(validation.issues).toEqual([]);
            
            // Test invalid state
            document.getElementById('opacity-slider').value = '2.0'; // Invalid range
            
            const invalidValidation = controlPanel.validate();
            expect(invalidValidation.isValid).toBe(false);
            expect(invalidValidation.issues).toContain('Invalid opacity value');
        });
    });
    
    describe('Performance Integration', () => {
        test('should handle rapid state changes efficiently', () => {
            controlPanel.initialize();
            
            const startTime = performance.now();
            
            // Simulate rapid user interactions
            for (let i = 0; i < 100; i++) {
                stateManager.setState('currentField', `field_${i}`);
            }
            
            const endTime = performance.now();
            const duration = endTime - startTime;
            
            // Should complete quickly (less than 100ms for 100 operations)
            expect(duration).toBeLessThan(100);
            
            // Final state should be correct
            expect(stateManager.getState('currentField')).toBe('field_99');
        });
        
        test('should handle memory cleanup', () => {
            controlPanel.initialize();
            
            // Create many subscribers
            const callbacks = [];
            for (let i = 0; i < 50; i++) {
                const callback = jest.fn();
                callbacks.push(callback);
                stateManager.subscribe('currentField', callback);
            }
            
            // Destroy components
            controlPanel.destroy();
            stateManager.destroy();
            
            // All callbacks should be cleaned up
            stateManager.setState('currentField', 'test_after_destroy');
            callbacks.forEach(callback => {
                expect(callback).not.toHaveBeenCalled();
            });
        });
    });
}); 