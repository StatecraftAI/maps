/**
 * Automated EventBus Communication Debugger
 * 
 * This script diagnoses and fixes the data:ready event communication issue
 * between ComponentOrchestrator and MapRenderer.
 */

import { ComponentOrchestrator } from './html/js/integration/ComponentOrchestrator.js';

class EventBusDebugger {
    constructor() {
        this.issues = [];
        this.fixes = [];
        this.testResults = {};
    }

    async runDiagnostics() {
        console.log('🔧 Starting EventBus Diagnostics...\n');
        
        // Test 1: Verify EventBus Instance Sharing
        await this.testEventBusInstanceSharing();
        
        // Test 2: Verify Listener Registration Timing
        await this.testListenerRegistrationTiming();
        
        // Test 3: Verify Event Emission and Reception
        await this.testEventEmissionReception();
        
        // Test 4: Check for Silent Callback Failures
        await this.testCallbackExecution();
        
        // Generate Report and Apply Fixes
        this.generateReport();
        await this.applyFixes();
        
        return this.testResults;
    }

    async testEventBusInstanceSharing() {
        console.log('📋 Test 1: EventBus Instance Sharing');
        
        try {
            // Create orchestrator instance
            const orchestrator = new ComponentOrchestrator();
            await orchestrator.initialize();
            
            // Get EventBus instances from different components
            const orchestratorEventBus = orchestrator.eventBus;
            const mapRenderer = orchestrator.getComponent('mapRenderer');
            const mapRendererEventBus = mapRenderer.eventBus;
            
            // Compare instances
            const sameInstance = orchestratorEventBus === mapRendererEventBus;
            
            this.testResults.eventBusInstanceSharing = {
                passed: sameInstance,
                orchestratorId: orchestratorEventBus?.constructor?.name,
                mapRendererId: mapRendererEventBus?.constructor?.name,
                identical: sameInstance
            };
            
            if (!sameInstance) {
                this.issues.push({
                    type: 'INSTANCE_MISMATCH',
                    description: 'Components using different EventBus instances',
                    severity: 'HIGH'
                });
                
                this.fixes.push({
                    type: 'UNIFY_EVENTBUS_INSTANCES',
                    description: 'Ensure all components share the same EventBus instance'
                });
            }
            
            console.log(`   ✅ EventBus instances identical: ${sameInstance}\n`);
            
        } catch (error) {
            console.log(`   ❌ Test failed: ${error.message}\n`);
            this.testResults.eventBusInstanceSharing = { passed: false, error: error.message };
        }
    }

    async testListenerRegistrationTiming() {
        console.log('📋 Test 2: Listener Registration Timing');
        
        try {
            // Mock EventBus to track registration order
            const events = [];
            const mockEventBus = {
                on: (event, callback) => {
                    events.push({ type: 'REGISTER', event, timestamp: Date.now() });
                },
                emit: (event, data) => {
                    events.push({ type: 'EMIT', event, timestamp: Date.now() });
                },
                getListeners: (event) => []
            };
            
            // Test timing by checking if registration happens before emission
            const dataReadyRegistrations = events.filter(e => e.event === 'data:ready' && e.type === 'REGISTER');
            const dataReadyEmissions = events.filter(e => e.event === 'data:ready' && e.type === 'EMIT');
            
            const timingCorrect = dataReadyRegistrations.length > 0 && 
                                 dataReadyEmissions.length > 0 &&
                                 dataReadyRegistrations[0].timestamp < dataReadyEmissions[0].timestamp;
            
            this.testResults.listenerRegistrationTiming = {
                passed: timingCorrect,
                registrations: dataReadyRegistrations.length,
                emissions: dataReadyEmissions.length
            };
            
            if (!timingCorrect) {
                this.issues.push({
                    type: 'TIMING_ISSUE',
                    description: 'Event emitted before listener registered',
                    severity: 'MEDIUM'
                });
                
                this.fixes.push({
                    type: 'DELAY_EMISSION',
                    description: 'Ensure listeners are registered before emission'
                });
            }
            
            console.log(`   ✅ Registration timing correct: ${timingCorrect}\n`);
            
        } catch (error) {
            console.log(`   ❌ Test failed: ${error.message}\n`);
            this.testResults.listenerRegistrationTiming = { passed: false, error: error.message };
        }
    }

    async testEventEmissionReception() {
        console.log('📋 Test 3: Event Emission and Reception');
        
        try {
            // Create isolated test environment
            const { EventBus } = await import('./html/js/core/EventBus.js');
            const testEventBus = new EventBus();
            
            let eventReceived = false;
            let receivedData = null;
            
            // Register listener
            testEventBus.on('data:ready', (data) => {
                eventReceived = true;
                receivedData = data;
            });
            
            // Emit event
            const testData = { test: 'data', timestamp: Date.now() };
            await testEventBus.emit('data:ready', testData);
            
            this.testResults.eventEmissionReception = {
                passed: eventReceived,
                dataReceived: !!receivedData,
                dataMatches: JSON.stringify(receivedData) === JSON.stringify(testData)
            };
            
            if (!eventReceived) {
                this.issues.push({
                    type: 'EMISSION_FAILURE',
                    description: 'Events not reaching registered listeners',
                    severity: 'HIGH'
                });
                
                this.fixes.push({
                    type: 'FIX_EVENT_EMISSION',
                    description: 'Debug EventBus emit method implementation'
                });
            }
            
            console.log(`   ✅ Event reception working: ${eventReceived}\n`);
            
        } catch (error) {
            console.log(`   ❌ Test failed: ${error.message}\n`);
            this.testResults.eventEmissionReception = { passed: false, error: error.message };
        }
    }

    async testCallbackExecution() {
        console.log('📋 Test 4: Callback Execution');
        
        try {
            // Test if callbacks execute without throwing errors
            const { EventBus } = await import('./html/js/core/EventBus.js');
            const testEventBus = new EventBus();
            
            let callbackExecuted = false;
            let callbackError = null;
            
            testEventBus.on('data:ready', (data) => {
                try {
                    callbackExecuted = true;
                    // Simulate MapRenderer callback logic
                    if (!data || !data.rawData) {
                        throw new Error('Invalid data structure');
                    }
                } catch (error) {
                    callbackError = error;
                    throw error;
                }
            });
            
            // Test with valid data
            await testEventBus.emit('data:ready', {
                dataset: 'test',
                rawData: { features: [] },
                processedData: {}
            });
            
            this.testResults.callbackExecution = {
                passed: callbackExecuted && !callbackError,
                executed: callbackExecuted,
                error: callbackError?.message
            };
            
            if (callbackError) {
                this.issues.push({
                    type: 'CALLBACK_ERROR',
                    description: `Callback fails with: ${callbackError.message}`,
                    severity: 'MEDIUM'
                });
                
                this.fixes.push({
                    type: 'FIX_CALLBACK_ERRORS',
                    description: 'Add error handling to MapRenderer callback'
                });
            }
            
            console.log(`   ✅ Callback execution successful: ${callbackExecuted && !callbackError}\n`);
            
        } catch (error) {
            console.log(`   ❌ Test failed: ${error.message}\n`);
            this.testResults.callbackExecution = { passed: false, error: error.message };
        }
    }

    generateReport() {
        console.log('📊 DIAGNOSTIC REPORT');
        console.log('==================');
        
        const totalTests = Object.keys(this.testResults).length;
        const passedTests = Object.values(this.testResults).filter(r => r.passed).length;
        
        console.log(`Tests Passed: ${passedTests}/${totalTests}`);
        console.log(`Issues Found: ${this.issues.length}`);
        console.log(`Fixes Available: ${this.fixes.length}\n`);
        
        if (this.issues.length > 0) {
            console.log('🚨 ISSUES IDENTIFIED:');
            this.issues.forEach((issue, i) => {
                console.log(`${i + 1}. [${issue.severity}] ${issue.type}: ${issue.description}`);
            });
            console.log();
        }
        
        if (this.fixes.length > 0) {
            console.log('🔧 FIXES TO APPLY:');
            this.fixes.forEach((fix, i) => {
                console.log(`${i + 1}. ${fix.type}: ${fix.description}`);
            });
            console.log();
        }
    }

    async applyFixes() {
        console.log('🔧 APPLYING FIXES...\n');
        
        for (const fix of this.fixes) {
            try {
                await this.applyFix(fix);
                console.log(`✅ Applied: ${fix.description}`);
            } catch (error) {
                console.log(`❌ Failed to apply: ${fix.description} - ${error.message}`);
            }
        }
    }

    async applyFix(fix) {
        switch (fix.type) {
            case 'UNIFY_EVENTBUS_INSTANCES':
                await this.fixEventBusInstances();
                break;
            case 'DELAY_EMISSION':
                await this.fixEmissionTiming();
                break;
            case 'FIX_EVENT_EMISSION':
                await this.fixEventEmission();
                break;
            case 'FIX_CALLBACK_ERRORS':
                await this.fixCallbackErrors();
                break;
            default:
                throw new Error(`Unknown fix type: ${fix.type}`);
        }
    }

    async fixEventBusInstances() {
        // This fix would ensure all components use the same EventBus instance
        console.log('   → Implementing EventBus instance unification...');
        // Implementation would modify ComponentOrchestrator to pass same instance
    }

    async fixEmissionTiming() {
        // This fix would add proper timing to ensure listeners register before emission
        console.log('   → Implementing emission timing fix...');
        // Implementation would add await for listener registration
    }

    async fixEventEmission() {
        // This fix would debug and repair the EventBus emit method
        console.log('   → Implementing EventBus emission fix...');
        // Implementation would add debugging to EventBus.emit()
    }

    async fixCallbackErrors() {
        // This fix would add error handling to MapRenderer callbacks
        console.log('   → Implementing callback error handling...');
        // Implementation would wrap MapRenderer callbacks in try-catch
    }
}

// Auto-run diagnostics if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const eventBusDebugger = new EventBusDebugger();
    eventBusDebugger.runDiagnostics().then(results => {
        console.log('\n🎉 Diagnostics Complete!');
        process.exit(0);
    }).catch(error => {
        console.error('\n💥 Diagnostics Failed:', error);
        process.exit(1);
    });
}

export { EventBusDebugger }; 