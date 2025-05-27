/**
 * ServiceContainer - Dependency Injection & Service Locator
 * 
 * Solves the tight coupling and fragile initialization issues by:
 * 1. Centralizing service registration and resolution
 * 2. Providing consistent dependency injection
 * 3. Managing service lifecycles
 * 4. Enabling loose coupling between components
 */

export class ServiceContainer {
  constructor() {
    this.services = new Map()
    this.singletons = new Map()
    this.factories = new Map()
    this.initializing = new Set()
    
    console.log('[ServiceContainer] Initialized')
  }

  /**
   * Register a singleton service
   * @param {string} name - Service name
   * @param {Function} factory - Factory function that creates the service
   * @param {Array} dependencies - Array of dependency service names
   */
  registerSingleton(name, factory, dependencies = []) {
    this.services.set(name, {
      type: 'singleton',
      factory,
      dependencies,
      instance: null
    })
    console.log(`[ServiceContainer] Registered singleton: ${name}`)
  }

  /**
   * Register a transient service (new instance each time)
   * @param {string} name - Service name
   * @param {Function} factory - Factory function that creates the service
   * @param {Array} dependencies - Array of dependency service names
   */
  registerTransient(name, factory, dependencies = []) {
    this.services.set(name, {
      type: 'transient',
      factory,
      dependencies
    })
    console.log(`[ServiceContainer] Registered transient: ${name}`)
  }

  /**
   * Register an existing instance
   * @param {string} name - Service name
   * @param {*} instance - The service instance
   */
  registerInstance(name, instance) {
    this.singletons.set(name, instance)
    console.log(`[ServiceContainer] Registered instance: ${name}`)
  }

  /**
   * Resolve a service and its dependencies
   * @param {string} name - Service name
   * @returns {*} The resolved service instance
   */
  resolve(name) {
    // Check for circular dependency
    if (this.initializing.has(name)) {
      throw new Error(`[ServiceContainer] Circular dependency detected: ${name}`)
    }

    // Return existing singleton instance
    if (this.singletons.has(name)) {
      return this.singletons.get(name)
    }

    // Get service definition
    const service = this.services.get(name)
    if (!service) {
      throw new Error(`[ServiceContainer] Service not found: ${name}`)
    }

    // Mark as initializing
    this.initializing.add(name)

    try {
      // Resolve dependencies
      const dependencies = service.dependencies.map(dep => this.resolve(dep))
      
      // Create instance
      const instance = service.factory(...dependencies)
      
      // Store singleton instance
      if (service.type === 'singleton') {
        this.singletons.set(name, instance)
        service.instance = instance
      }

      console.log(`[ServiceContainer] Resolved: ${name}`)
      return instance
    } finally {
      // Remove from initializing set
      this.initializing.delete(name)
    }
  }

  /**
   * Check if a service is registered
   * @param {string} name - Service name
   * @returns {boolean}
   */
  has(name) {
    return this.services.has(name) || this.singletons.has(name)
  }

  /**
   * Get all registered service names
   * @returns {Array<string>}
   */
  getServiceNames() {
    return [
      ...Array.from(this.services.keys()),
      ...Array.from(this.singletons.keys())
    ]
  }

  /**
   * Initialize all registered services
   * This resolves all singletons to ensure proper initialization order
   */
  async initializeAll() {
    console.log('[ServiceContainer] Initializing all services...')
    
    const serviceNames = Array.from(this.services.keys())
    const initialized = []
    
    for (const name of serviceNames) {
      try {
        const instance = this.resolve(name)
        
        // Call initialize method if it exists
        if (instance && typeof instance.initialize === 'function') {
          await instance.initialize()
        }
        
        initialized.push(name)
      } catch (error) {
        console.error(`[ServiceContainer] Failed to initialize ${name}:`, error)
        throw error
      }
    }
    
    console.log(`[ServiceContainer] Initialized ${initialized.length} services:`, initialized)
  }

  /**
   * Cleanup all services
   */
  cleanup() {
    console.log('[ServiceContainer] Cleaning up services...')
    
    // Call cleanup on all singleton instances
    for (const [name, instance] of this.singletons) {
      try {
        if (instance && typeof instance.cleanup === 'function') {
          instance.cleanup()
        }
      } catch (error) {
        console.error(`[ServiceContainer] Error cleaning up ${name}:`, error)
      }
    }
    
    // Clear all references
    this.services.clear()
    this.singletons.clear()
    this.factories.clear()
    this.initializing.clear()
    
    console.log('[ServiceContainer] Cleanup complete')
  }

  /**
   * Get service dependency graph for debugging
   * @returns {Object}
   */
  getDependencyGraph() {
    const graph = {}
    
    for (const [name, service] of this.services) {
      graph[name] = {
        type: service.type,
        dependencies: service.dependencies,
        initialized: this.singletons.has(name)
      }
    }
    
    return graph
  }
} 