/**
 * StateManager Test Suite
 * 
 * Tests for the core state management functionality including:
 * - State initialization and updates
 * - Subscriber notifications (reactive updates)
 * - State persistence to localStorage
 * - State restoration and history tracking
 * - Error handling and validation
 */

import { StateManager } from '../../html/js/core/StateManager.js';

describe('StateManager', () => {
    let stateManager;
    
    beforeEach(() => {
        // Create fresh instance for each test
        stateManager = new StateManager();
    });
    
    afterEach(() => {
        // Clean up
        if (stateManager) {
            stateManager.destroy();
        }
    });
    
    describe('Initialization', () => {
        test('should initialize with default state structure', () => {
            expect(stateManager.getState('map')).toBeNull();
            expect(stateManager.getState('currentField')).toBe('political_lean');
            expect(stateManager.getState('currentDataset')).toBe('zone1');
            expect(stateManager.getState('showPpsOnly')).toBe(true);
            expect(stateManager.getState('datasets')).toEqual({});
        });
        
        test('should initialize with environment detection', () => {
            const baseDataPath = stateManager.getState('baseDataPath');
            expect(typeof baseDataPath).toBe('string');
        });
        
        test('should log initialization', () => {
            expect(console.log).toHaveBeenCalledWith('[StateManager] Initialized with environment detection');
        });
    });
    
    describe('State Management', () => {
        test('should set single state value', () => {
            stateManager.setState('currentField', 'test_field');
            expect(stateManager.getState('currentField')).toBe('test_field');
        });
        
        test('should set multiple state values', () => {
            const updates = {
                currentField: 'new_field',
                showPpsOnly: false,
                mapOpacity: 0.8
            };
            
            stateManager.setState(updates);
            
            expect(stateManager.getState('currentField')).toBe('new_field');
            expect(stateManager.getState('showPpsOnly')).toBe(false);
            expect(stateManager.getState('mapOpacity')).toBe(0.8);
        });
        
        test('should return undefined for non-existent keys', () => {
            expect(stateManager.getState('nonExistentKey')).toBeUndefined();
        });
        
        test('should handle nested object updates', () => {
            const testData = { test: 'value', nested: { prop: 123 } };
            stateManager.setState('electionData', testData);
            
            expect(stateManager.getState('electionData')).toEqual(testData);
        });
    });
    
    describe('Reactive Updates (Subscribers)', () => {
        test('should notify subscribers on state changes', () => {
            const mockCallback = jest.fn();
            
            stateManager.subscribe('currentField', mockCallback);
            stateManager.setState('currentField', 'new_value');
            
            expect(mockCallback).toHaveBeenCalledWith('new_value', 'political_lean');
        });
        
        test('should handle multiple subscribers for same key', () => {
            const callback1 = jest.fn();
            const callback2 = jest.fn();
            
            stateManager.subscribe('currentField', callback1);
            stateManager.subscribe('currentField', callback2);
            
            stateManager.setState('currentField', 'test_value');
            
            expect(callback1).toHaveBeenCalledWith('test_value', 'political_lean');
            expect(callback2).toHaveBeenCalledWith('test_value', 'political_lean');
        });
        
        test('should not notify subscribers if value unchanged', () => {
            const mockCallback = jest.fn();
            
            stateManager.subscribe('currentField', mockCallback);
            stateManager.setState('currentField', 'political_lean'); // Same as default
            
            expect(mockCallback).not.toHaveBeenCalled();
        });
        
        test('should handle subscriber errors gracefully', () => {
            const errorCallback = jest.fn(() => {
                throw new Error('Subscriber error');
            });
            const goodCallback = jest.fn();
            
            stateManager.subscribe('currentField', errorCallback);
            stateManager.subscribe('currentField', goodCallback);
            
            stateManager.setState('currentField', 'test_value');
            
            expect(errorCallback).toHaveBeenCalled();
            expect(goodCallback).toHaveBeenCalled();
            expect(console.error).toHaveBeenCalled();
        });
        
        test('should allow unsubscribing', () => {
            const mockCallback = jest.fn();
            
            const unsubscribe = stateManager.subscribe('currentField', mockCallback);
            unsubscribe();
            
            stateManager.setState('currentField', 'new_value');
            expect(mockCallback).not.toHaveBeenCalled();
        });
    });
    
    describe('State Persistence', () => {
        test('should persist specified keys to localStorage', () => {
            stateManager.setState({
                currentField: 'test_field',
                showPpsOnly: false,
                mapOpacity: 0.9
            });
            
            stateManager.persist(['currentField', 'showPpsOnly']);
            
            expect(localStorage.setItem).toHaveBeenCalledWith(
                'electionMap_state',
                JSON.stringify({
                    currentField: 'test_field',
                    showPpsOnly: false
                })
            );
        });
        
        test('should restore state from localStorage', () => {
            // Mock localStorage data
            const savedState = {
                currentField: 'saved_field',
                showPpsOnly: false
            };
            localStorage.getItem.mockReturnValue(JSON.stringify(savedState));
            
            stateManager.restore(['currentField', 'showPpsOnly']);
            
            expect(stateManager.getState('currentField')).toBe('saved_field');
            expect(stateManager.getState('showPpsOnly')).toBe(false);
        });
        
        test('should handle invalid JSON in localStorage gracefully', () => {
            localStorage.getItem.mockReturnValue('invalid json');
            
            expect(() => {
                stateManager.restore(['currentField']);
            }).not.toThrow();
            
            expect(console.error).toHaveBeenCalled();
        });
        
        test('should handle missing localStorage gracefully', () => {
            localStorage.getItem.mockReturnValue(null);
            
            expect(() => {
                stateManager.restore(['currentField']);
            }).not.toThrow();
        });
    });
    
    describe('State History', () => {
        test('should track state changes in history', () => {
            stateManager.setState('currentField', 'field1');
            stateManager.setState('currentField', 'field2');
            stateManager.setState('showPpsOnly', false);
            
            const history = stateManager.getHistory();
            expect(history.length).toBe(3);
            expect(history[0].changes).toEqual({ currentField: 'field1' });
            expect(history[1].changes).toEqual({ currentField: 'field2' });
            expect(history[2].changes).toEqual({ showPpsOnly: false });
        });
        
        test('should limit history size', () => {
            // Make more than 50 changes (default limit)
            for (let i = 0; i < 60; i++) {
                stateManager.setState('currentField', `field_${i}`);
            }
            
            const history = stateManager.getHistory();
            expect(history.length).toBe(50);
        });
        
        test('should clear history', () => {
            stateManager.setState('currentField', 'test');
            stateManager.clearHistory();
            
            expect(stateManager.getHistory()).toEqual([]);
        });
    });
    
    describe('Debugging and Introspection', () => {
        test('should provide current state snapshot', () => {
            stateManager.setState({
                currentField: 'test_field',
                showPpsOnly: false
            });
            
            const snapshot = stateManager.getSnapshot();
            expect(snapshot).toHaveProperty('state');
            expect(snapshot).toHaveProperty('subscribers');
            expect(snapshot).toHaveProperty('history');
            expect(snapshot.state.currentField).toBe('test_field');
        });
        
        test('should provide performance metrics', () => {
            stateManager.setState('currentField', 'test');
            
            const metrics = stateManager.getPerformanceMetrics();
            expect(metrics).toHaveProperty('totalStateChanges');
            expect(metrics).toHaveProperty('totalSubscribers');
            expect(metrics.totalStateChanges).toBe(1);
        });
        
        test('should reset state to defaults', () => {
            stateManager.setState({
                currentField: 'changed_field',
                showPpsOnly: false
            });
            
            stateManager.reset();
            
            expect(stateManager.getState('currentField')).toBe('political_lean');
            expect(stateManager.getState('showPpsOnly')).toBe(true);
        });
    });
    
    describe('Error Handling', () => {
        test('should handle invalid state updates gracefully', () => {
            // Test with undefined value
            expect(() => {
                stateManager.setState('testKey', undefined);
            }).not.toThrow();
            
            // Test with function (should not be stored)
            expect(() => {
                stateManager.setState('testKey', () => {});
            }).not.toThrow();
        });
        
        test('should validate state keys', () => {
            expect(() => {
                stateManager.setState(null, 'value');
            }).not.toThrow();
            
            expect(() => {
                stateManager.setState('', 'value');
            }).not.toThrow();
        });
    });
    
    describe('Resource Cleanup', () => {
        test('should clean up resources on destroy', () => {
            const mockCallback = jest.fn();
            stateManager.subscribe('currentField', mockCallback);
            
            stateManager.destroy();
            stateManager.setState('currentField', 'after_destroy');
            
            expect(mockCallback).not.toHaveBeenCalled();
        });
        
        test('should clear localStorage on destroy if specified', () => {
            stateManager.setState('currentField', 'test');
            stateManager.persist(['currentField']);
            
            stateManager.destroy(true); // Clear localStorage
            
            expect(localStorage.removeItem).toHaveBeenCalledWith('electionMap_state');
        });
    });
    
    describe('Integration Tests', () => {
        test('should handle realistic election map workflow', () => {
            const datasetChangeCallback = jest.fn();
            const fieldChangeCallback = jest.fn();
            
            // Subscribe to changes
            stateManager.subscribe('currentDataset', datasetChangeCallback);
            stateManager.subscribe('currentField', fieldChangeCallback);
            
            // Simulate dataset discovery
            const datasets = {
                zone1: { title: 'Zone 1', file: 'zone1.geojson' },
                zone2: { title: 'Zone 2', file: 'zone2.geojson' }
            };
            stateManager.setState('datasets', datasets);
            
            // Simulate dataset change
            stateManager.setState('currentDataset', 'zone2');
            expect(datasetChangeCallback).toHaveBeenCalledWith('zone2', 'zone1');
            
            // Simulate field change
            stateManager.setState('currentField', 'vote_pct_candidate_a');
            expect(fieldChangeCallback).toHaveBeenCalledWith('vote_pct_candidate_a', 'political_lean');
            
            // Simulate data loading
            const mockElectionData = createMockGeoJSON(5);
            stateManager.setState('electionData', mockElectionData);
            
            expect(stateManager.getState('electionData').features).toHaveLength(5);
        });
        
        test('should handle state persistence workflow', () => {
            // Simulate user interactions
            stateManager.setState({
                currentField: 'vote_pct_candidate_a',
                showPpsOnly: false,
                mapOpacity: 0.8,
                currentBasemap: 'satellite'
            });
            
            // Persist user preferences
            const persistKeys = ['currentField', 'showPpsOnly', 'mapOpacity', 'currentBasemap'];
            stateManager.persist(persistKeys);
            
            // Simulate page reload - create new StateManager and restore
            const newStateManager = new StateManager();
            newStateManager.restore(persistKeys);
            
            expect(newStateManager.getState('currentField')).toBe('vote_pct_candidate_a');
            expect(newStateManager.getState('showPpsOnly')).toBe(false);
            expect(newStateManager.getState('mapOpacity')).toBe(0.8);
            
            newStateManager.destroy();
        });
    });
});