/**
 * Simple Test - Basic Infrastructure Validation
 */

describe('Basic Testing Infrastructure', () => {
    test('should have working test environment', () => {
        expect(true).toBe(true);
    });
    
    test('should have localStorage mock', () => {
        localStorage.setItem('test', 'value');
        expect(localStorage.getItem('test')).toBe('value');
    });
    
    test('should have fetch mock', () => {
        expect(typeof fetch).toBe('function');
    });
    
    test('should have console mocks', () => {
        console.log('test message');
        expect(console.log).toHaveBeenCalledWith('test message');
    });
    
    test('should have DOM environment', () => {
        const element = document.createElement('div');
        element.id = 'test';
        expect(element.id).toBe('test');
    });
    
    test('should have helper functions', () => {
        expect(typeof createMockElement).toBe('function');
        expect(typeof createMockGeoJSON).toBe('function');
    });
}); 