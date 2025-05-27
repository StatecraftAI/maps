/**
 * ModernComponentOrchestrator - Robust Service-Based Architecture
 *
 * Solves the architectural problems with:
 * 1. Dependency injection via ServiceContainer
 * 2. Consistent constructor patterns
 * 3. Proper service lifecycle management
 * 4. Loose coupling between components
 * 5. Clear separation of concerns
 */

import { ServiceContainer } from '../core/ServiceContainer.js'

// Core imports
import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'
import { MapManager } from '../core/MapManager.js'

// Data layer imports
import { DataLoader } from '../data/DataLoader.js'
import { DataProcessor } from '../data/DataProcessor.js'
import { CandidateManager } from '../data/CandidateManager.js'

// UI component imports
import { ControlPanel } from '../ui/ControlPanel.js'
import { ControlPanelTabs } from '../ui/ControlPanelTabs.js'
import { LayerSelector } from '../ui/LayerSelector.js'
import { Accordion } from '../ui/Accordion.js'
import { InfoPanel } from '../ui/InfoPanel.js'
import { Legend } from '../ui/Legend.js'
import { Tooltip } from '../ui/Tooltip.js'
import { PanelMinimizer } from '../ui/PanelMinimizer.js'

// Visualization imports
import { MapRenderer } from '../visualization/MapRenderer.js'
import { ColorManager } from '../visualization/ColorManager.js'
import { PopupManager } from '../visualization/PopupManager.js'

// Feature imports
import { Search } from '../features/Search.js'
import { Sharing } from '../features/Sharing.js'
import { Export } from '../features/Export.js'
import { Heatmap } from '../features/Heatmap.js'
import { SchoolOverlays } from '../features/SchoolOverlays.js'
import { DemographicOverlays } from '../features/DemographicOverlays.js'

// Utilities
import { URLUtils } from '../utils/urlUtils.js'

export class ModernComponentOrchestrator {
  constructor() {
    this.container = new ServiceContainer()
    this.initialized = false
    this.supabaseClient = null
    
    // Performance metrics
    this.metrics = {
      initTime: 0,
      servicesRegistered: 0,
      servicesInitialized: 0
    }
    
    console.log('[ModernComponentOrchestrator] Created with ServiceContainer')
  }

  /**
   * Set Supabase client before initialization
   */
  setSupabaseClient(supabaseClient) {
    this.supabaseClient = supabaseClient
    console.log('[ModernComponentOrchestrator] Supabase client set')
  }

  /**
   * Register all services with the container
   */
  registerServices() {
    console.log('[ModernComponentOrchestrator] Registering services...')
    
    // Core services (no dependencies)
    this.container.registerSingleton('stateManager', () => new StateManager())
    this.container.registerSingleton('eventBus', () => new EventBus())
    
    // Map manager (depends on core services)
    this.container.registerSingleton('mapManager', (stateManager, eventBus) => {
      return new MapManager(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    // Data layer services
    this.container.registerSingleton('dataLoader', (stateManager, eventBus) => {
      const loader = new DataLoader(stateManager, eventBus)
      if (this.supabaseClient) {
        loader.initializeSupabase(this.supabaseClient)
      }
      return loader
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('dataProcessor', (stateManager, eventBus) => {
      return new DataProcessor(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('candidateManager', (stateManager, eventBus) => {
      return new CandidateManager(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    // Visualization services
    this.container.registerSingleton('colorManager', (stateManager, eventBus) => {
      return new ColorManager(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('popupManager', (stateManager, eventBus, mapManager) => {
      return new PopupManager(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('mapRenderer', (stateManager, eventBus, mapManager, colorManager, popupManager) => {
      return new MapRenderer(stateManager, eventBus, mapManager, colorManager, popupManager)
    }, ['stateManager', 'eventBus', 'mapManager', 'colorManager', 'popupManager'])
    
    // UI services
    this.container.registerSingleton('controlPanel', (stateManager, eventBus) => {
      return new ControlPanel(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('controlPanelTabs', (stateManager, eventBus) => {
      return new ControlPanelTabs(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('layerSelector', (stateManager, eventBus) => {
      return new LayerSelector(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('accordion', (stateManager, eventBus) => {
      return new Accordion(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('infoPanel', (stateManager, eventBus) => {
      return new InfoPanel(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('legend', (stateManager, eventBus, colorManager) => {
      return new Legend(stateManager, eventBus, colorManager)
    }, ['stateManager', 'eventBus', 'colorManager'])
    
    this.container.registerSingleton('tooltip', (stateManager, eventBus) => {
      return new Tooltip(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    this.container.registerSingleton('panelMinimizer', (stateManager, eventBus) => {
      return new PanelMinimizer(stateManager, eventBus)
    }, ['stateManager', 'eventBus'])
    
    // Feature services
    this.container.registerSingleton('search', (stateManager, eventBus, mapManager) => {
      return new Search(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('sharing', (stateManager, eventBus, mapManager) => {
      return new Sharing(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('export', (stateManager, eventBus, mapManager) => {
      return new Export(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('heatmap', (stateManager, eventBus, mapManager) => {
      return new Heatmap(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('schoolOverlays', (stateManager, eventBus, mapManager) => {
      return new SchoolOverlays(stateManager, eventBus, mapManager)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.container.registerSingleton('demographicOverlays', (stateManager, eventBus, mapManager) => {
      return new DemographicOverlays(stateManager, eventBus, mapManager, this.supabaseClient)
    }, ['stateManager', 'eventBus', 'mapManager'])
    
    this.metrics.servicesRegistered = this.container.getServiceNames().length
    console.log(`[ModernComponentOrchestrator] Registered ${this.metrics.servicesRegistered} services`)
  }

  /**
   * Initialize the entire application
   */
  async initialize() {
    if (this.initialized) {
      console.warn('[ModernComponentOrchestrator] Already initialized')
      return
    }

    const startTime = performance.now()
    console.log('[ModernComponentOrchestrator] Starting initialization...')

    try {
      // Register all services
      this.registerServices()
      
      // Initialize map first (special case)
      const mapManager = this.container.resolve('mapManager')
      await mapManager.initializeMap('map')
      
      // Initialize all other services
      await this.container.initializeAll()
      
      // Setup cross-component communication
      this.setupCommunication()
      
      // Restore state from URL
      await this.restoreStateFromUrl()
      
      // Perform initial data load
      await this.performInitialDataLoad()
      
      this.initialized = true
      this.metrics.initTime = performance.now() - startTime
      this.metrics.servicesInitialized = this.container.getServiceNames().length
      
      console.log('[ModernComponentOrchestrator] Initialization complete', {
        initTime: `${this.metrics.initTime.toFixed(2)}ms`,
        services: this.metrics.servicesInitialized
      })
      
      // Emit initialization complete event
      const eventBus = this.container.resolve('eventBus')
      eventBus.emit('app:initialized', {
        metrics: this.metrics,
        services: this.container.getServiceNames()
      })
      
    } catch (error) {
      console.error('[ModernComponentOrchestrator] Initialization failed:', error)
      throw error
    }
  }

  /**
   * Setup cross-component communication
   */
  setupCommunication() {
    console.log('[ModernComponentOrchestrator] Setting up communication...')
    
    const eventBus = this.container.resolve('eventBus')
    
    // Listen for dataset changes
    eventBus.on('ui:datasetChanged', async (data) => {
      console.log('[ModernComponentOrchestrator] Dataset changed:', data.dataset)
      await this.loadDatasetData(data.dataset)
    })
    
    // Global error handling
    eventBus.on('error', (error) => {
      console.error('[ModernComponentOrchestrator] Component error:', error)
    })
  }

  /**
   * Restore state from URL
   */
  async restoreStateFromUrl() {
    const urlParams = URLUtils.parseUrlParameters()
    
    if (Object.keys(urlParams).length > 0) {
      console.log('[ModernComponentOrchestrator] Restoring state from URL:', urlParams)
      
      const stateManager = this.container.resolve('stateManager')
      Object.keys(urlParams).forEach(key => {
        stateManager.setState({ [key]: urlParams[key] })
      })
      
      const eventBus = this.container.resolve('eventBus')
      eventBus.emit('url:restored', urlParams)
    }
  }

  /**
   * Perform initial data load
   */
  async performInitialDataLoad() {
    console.log('[ModernComponentOrchestrator] Starting initial data load...')
    
    const dataLoader = this.container.resolve('dataLoader')
    const dataProcessor = this.container.resolve('dataProcessor')
    const stateManager = this.container.resolve('stateManager')
    const eventBus = this.container.resolve('eventBus')
    const colorManager = this.container.resolve('colorManager')
    
    try {
      // Discover datasets
      const discoveryResult = await dataLoader.discoverDatasets()
      stateManager.setState({ datasets: discoveryResult.datasets })
      
      // Load default dataset
      const defaultDataset = stateManager.getState('currentDataset') || discoveryResult.defaultDataset || 'zone1'
      
      if (!discoveryResult.datasets[defaultDataset]) {
        throw new Error(`Dataset ${defaultDataset} not found`)
      }
      
      // Load and process data
      const electionData = await dataLoader.loadElectionData(defaultDataset)
      const processedData = await dataProcessor.processElectionData(electionData, defaultDataset)
      
      // Update state
      stateManager.setState({
        currentDataset: defaultDataset,
        electionData,
        processedData: processedData.originalData,
        fieldInfo: processedData.fieldInfo,
        actualDataRanges: processedData.dataRanges,
        layerOrganization: processedData.layerOrganization,
        metadata: processedData.metadata
      })
      
      // Build candidate color schemes
      if (processedData.metadata?.candidates) {
        colorManager.buildCandidateColorSchemes(
          processedData.metadata.candidates,
          processedData.metadata.candidateColors
        )
      }
      
      // Emit data ready event
      eventBus.emit('data:ready', {
        dataset: defaultDataset,
        rawData: electionData,
        processedData: processedData.originalData
      })
      
      console.log('[ModernComponentOrchestrator] Initial data load complete')
      
    } catch (error) {
      console.error('[ModernComponentOrchestrator] Initial data load failed:', error)
      eventBus.emit('error', {
        type: 'dataLoad',
        message: 'Failed to load initial data',
        error
      })
    }
  }

  /**
   * Load data for a specific dataset
   */
  async loadDatasetData(datasetKey) {
    try {
      console.log(`[ModernComponentOrchestrator] Loading dataset: ${datasetKey}`)
      
      const dataLoader = this.container.resolve('dataLoader')
      const dataProcessor = this.container.resolve('dataProcessor')
      const stateManager = this.container.resolve('stateManager')
      const eventBus = this.container.resolve('eventBus')
      const colorManager = this.container.resolve('colorManager')
      
      if (datasetKey === 'none') {
        // Clear data
        stateManager.setState({
          currentDataset: 'none',
          electionData: null,
          processedData: null,
          fieldInfo: null,
          actualDataRanges: null,
          layerOrganization: null,
          metadata: null,
          currentField: 'none'
        })
        
        eventBus.emit('data:cleared', { dataset: 'none' })
        return
      }
      
      // Load and process data
      const electionData = await dataLoader.loadElectionData(datasetKey)
      const processedData = await dataProcessor.processElectionData(electionData, datasetKey)
      
      // Update state
      stateManager.setState({
        currentDataset: datasetKey,
        electionData,
        processedData: processedData.originalData,
        fieldInfo: processedData.fieldInfo,
        actualDataRanges: processedData.dataRanges,
        layerOrganization: processedData.layerOrganization,
        metadata: processedData.metadata
      })
      
      // Build candidate color schemes
      if (processedData.metadata?.candidates) {
        colorManager.buildCandidateColorSchemes(
          processedData.metadata.candidates,
          processedData.metadata.candidateColors
        )
      }
      
      // Emit data ready event
      eventBus.emit('data:ready', {
        dataset: datasetKey,
        rawData: electionData,
        processedData: processedData.originalData
      })
      
    } catch (error) {
      console.error('[ModernComponentOrchestrator] Failed to load dataset:', error)
      const eventBus = this.container.resolve('eventBus')
      eventBus.emit('data:loadError', {
        dataset: datasetKey,
        error: error.message
      })
    }
  }

  /**
   * Get a service instance
   */
  getService(name) {
    return this.container.resolve(name)
  }

  /**
   * Get all service names
   */
  getServiceNames() {
    return this.container.getServiceNames()
  }

  /**
   * Get dependency graph for debugging
   */
  getDependencyGraph() {
    return this.container.getDependencyGraph()
  }

  /**
   * Get metrics
   */
  getMetrics() {
    return { ...this.metrics }
  }

  /**
   * Cleanup all services
   */
  cleanup() {
    console.log('[ModernComponentOrchestrator] Cleaning up...')
    this.container.cleanup()
    this.initialized = false
  }

  /**
   * Restart the application
   */
  async restart() {
    console.log('[ModernComponentOrchestrator] Restarting...')
    this.cleanup()
    await this.initialize()
  }
} 