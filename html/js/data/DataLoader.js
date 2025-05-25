/**
 * DataLoader - Centralized Data Fetching and Caching
 *
 * Handles all data loading operations including:
 * - Dataset discovery and configuration
 * - Election data loading with error handling
 * - School overlay data loading
 * - Caching and performance optimization
 * - Loading state management
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js'
import { EventBus } from '../core/EventBus.js'
import { APP_CONFIG, DATA_PATHS, MESSAGES } from '../config/constants.js'

export class DataLoader {
  constructor (stateManager, eventBus) {
    this.stateManager = stateManager
    this.eventBus = eventBus

    // Data caching
    this.cache = new Map()
    this.loadingPromises = new Map() // Prevent duplicate requests

    // Performance tracking
    this.metrics = {
      totalRequests: 0,
      cacheHits: 0,
      cacheMisses: 0,
      loadTimes: {}
    }

    // Determine base data path based on environment
    this.baseDataPath = this.determineBaseDataPath()

    console.log(`[DataLoader] Initialized with baseDataPath: '${this.baseDataPath}'`)
  }

  /**
     * Determine the base path for data files based on environment
     */
  determineBaseDataPath () {
    const hostname = window.location.hostname
    const pathname = window.location.pathname

    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return APP_CONFIG.DATA_BASE_PATHS.localhost
    } else if (this.isValidGithubIoHost(hostname)) {
      return APP_CONFIG.DATA_BASE_PATHS['github.io']
    }

    // Default fallback
    return ''
  }

  /**
   * Validate if a hostname is 'github.io' or a subdomain of it
   */
  isValidGithubIoHost(hostname) {
    const parts = hostname.split('.');
    return parts.length >= 2 && parts.slice(-2).join('.') === 'github.io';
  }

  /**
     * Discover and configure available datasets
     * Returns a map of discovered datasets
     */
  async discoverDatasets () {
    console.log('[DataLoader] Starting dataset discovery...')

    const startTime = performance.now()
    const datasets = {}
    let firstDiscoveredZone = null

    try {
      // Update loading state
      this.eventBus.emit('data:loadingStarted', { type: 'discovery' })

      // Discover zone-based datasets (zone1-zone8)
      const zoneDatasets = await this.discoverZoneDatasets()
      Object.assign(datasets, zoneDatasets)

      // Set first discovered zone as default
      const zoneKeys = Object.keys(zoneDatasets)
      if (zoneKeys.length > 0) {
        firstDiscoveredZone = zoneKeys[0]
      }

      // Discover static datasets
      const staticDatasets = await this.discoverStaticDatasets()
      Object.assign(datasets, staticDatasets)

      // Cache the result
      this.cache.set('datasets', datasets)

      const loadTime = performance.now() - startTime
      this.metrics.loadTimes.discovery = loadTime

      console.log(`[DataLoader] Discovery completed in ${loadTime.toFixed(2)}ms`)
      console.log(`[DataLoader] Found ${Object.keys(datasets).length} datasets:`, Object.keys(datasets))

      this.eventBus.emit('data:discoveryComplete', {
        datasets,
        defaultDataset: firstDiscoveredZone || Object.keys(datasets)[0],
        discoveryTime: loadTime
      })

      return { datasets, defaultDataset: firstDiscoveredZone }
    } catch (error) {
      console.error('[DataLoader] Dataset discovery failed:', error)
      this.eventBus.emit('data:error', {
        type: 'discovery',
        error: error.message
      })
      throw error
    }
  }

  /**
     * Discover zone-based election datasets (zone1-zone8)
     */
  async discoverZoneDatasets () {
    const datasets = {}
    const promises = []

    // Check for zone datasets in parallel
    for (const zone of DATA_PATHS.election.zones) {
      const zoneKey = `zone${zone}`
      const filePath = this.resolveDataPath(
        DATA_PATHS.election.pattern.replace('{zone}', zoneKey)
      )

      const promise = this.checkFileExists(filePath)
        .then(exists => {
          if (exists) {
            datasets[zoneKey] = {
              file: filePath,
              title: `2025 School Board Zone ${zone}`,
              layers: null, // Will be populated after loading
              type: 'election',
              zone
            }
            console.log(`[DataLoader] Found dataset: ${zoneKey}`)
            return zoneKey
          }
          return null
        })
        .catch(error => {
          console.log(`[DataLoader] Zone ${zone} check failed:`, error.message)
          return null
        })

      promises.push(promise)
    }

    await Promise.all(promises)
    return datasets
  }

  /**
     * Discover static datasets (voter registration, bond data)
     */
  async discoverStaticDatasets () {
    const datasets = {}

    // Check voter registration dataset
    try {
      const voterRegPath = this.resolveDataPath(DATA_PATHS.voter_registration)
      if (await this.checkFileExists(voterRegPath)) {
        datasets.voter_reg = {
          file: voterRegPath,
          title: 'Voter Registration Data',
          type: 'voter_registration',
          layers: [
            'political_lean', 'dem_advantage', 'major_party_pct',
            'reg_pct_dem', 'reg_pct_rep', 'reg_pct_nav', 'total_voters',
            'registration_competitiveness', 'precinct_size_category'
          ]
        }
        console.log('[DataLoader] Found dataset: voter_reg')
      }
    } catch (error) {
      console.log('[DataLoader] Voter registration dataset check failed:', error.message)
    }

    // Check bond dataset
    try {
      const bondPath = this.resolveDataPath(DATA_PATHS.bond)
      if (await this.checkFileExists(bondPath)) {
        datasets.bond = {
          file: bondPath,
          title: 'Bond Election Data',
          type: 'bond',
          layers: null // Will be discovered after loading
        }
        console.log('[DataLoader] Found dataset: bond')
      }
    } catch (error) {
      console.log('[DataLoader] Bond dataset check failed:', error.message)
    }

    return datasets
  }

  /**
     * Load election data for a specific dataset
     */
  async loadElectionData (datasetKey) {
    const startTime = performance.now()

    try {
      // Check if already loading
      if (this.loadingPromises.has(datasetKey)) {
        return await this.loadingPromises.get(datasetKey)
      }

      // Start loading
      this.eventBus.emit('data:loadingStarted', {
        type: 'election',
        dataset: datasetKey
      })

      // Create loading promise
      const loadingPromise = this._loadElectionDataInternal(datasetKey, startTime)
      this.loadingPromises.set(datasetKey, loadingPromise)

      const result = await loadingPromise

      // Clean up
      this.loadingPromises.delete(datasetKey)

      return result
    } catch (error) {
      this.loadingPromises.delete(datasetKey)
      console.error(`[DataLoader] Failed to load election data for ${datasetKey}:`, error)

      this.eventBus.emit('data:error', {
        type: 'election',
        dataset: datasetKey,
        error: error.message
      })

      throw error
    }
  }

  /**
     * Internal election data loading implementation
     */
  async _loadElectionDataInternal (datasetKey, startTime) {
    // Get dataset configuration
    const datasets = this.cache.get('datasets') || this.stateManager.getState('datasets')
    const config = datasets[datasetKey]

    if (!config || !config.file) {
      throw new Error(`Configuration for dataset '${datasetKey}' is missing or invalid.`)
    }

    // Check cache first
    const cacheKey = `election:${datasetKey}`
    if (this.cache.has(cacheKey)) {
      this.metrics.cacheHits++
      const cachedData = this.cache.get(cacheKey)

      this.eventBus.emit('data:loaded', {
        type: 'election',
        dataset: datasetKey,
        data: cachedData,
        fromCache: true
      })

      return cachedData
    }

    this.metrics.cacheMisses++
    this.metrics.totalRequests++

    // Fetch data
    console.log(`[DataLoader] Fetching election data from: ${config.file}`)

    const response = await fetch(config.file)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const electionData = await response.json()

    // Validate data structure
    if (!electionData.features || !Array.isArray(electionData.features)) {
      throw new Error('Invalid GeoJSON structure: missing or invalid features array')
    }

    console.log(`[DataLoader] Loaded GeoJSON with ${electionData.features.length} features for ${datasetKey}`)

    // Cache the data
    this.cache.set(cacheKey, electionData)

    const loadTime = performance.now() - startTime
    this.metrics.loadTimes[`election:${datasetKey}`] = loadTime

    console.log(`[DataLoader] Election data load completed in ${loadTime.toFixed(2)}ms`)

    this.eventBus.emit('data:loaded', {
      type: 'election',
      dataset: datasetKey,
      data: electionData,
      loadTime,
      fromCache: false
    })

    return electionData
  }

  /**
     * Load all school overlay data
     */
  async loadSchoolData () {
    const startTime = performance.now()

    try {
      this.eventBus.emit('data:loadingStarted', { type: 'schools' })

      const schoolData = {}
      const loadPromises = []

      // Load each school dataset
      for (const [key, relativePath] of Object.entries(DATA_PATHS.schools)) {
        const fullPath = this.resolveDataPath(relativePath)

        const loadPromise = this.loadSingleSchoolLayer(key, fullPath)
          .then(data => {
            if (data) {
              schoolData[key] = data
            }
          })
          .catch(error => {
            console.warn(`[DataLoader] Could not load ${key}:`, error.message)
          })

        loadPromises.push(loadPromise)
      }

      await Promise.all(loadPromises)

      const loadTime = performance.now() - startTime
      this.metrics.loadTimes.schools = loadTime

      console.log(`[DataLoader] School data loading completed in ${loadTime.toFixed(2)}ms`)
      console.log(`[DataLoader] Loaded ${Object.keys(schoolData).length} school layers`)

      this.eventBus.emit('data:loaded', {
        type: 'schools',
        data: schoolData,
        loadTime
      })

      return schoolData
    } catch (error) {
      console.error('[DataLoader] School data loading failed:', error)
      this.eventBus.emit('data:error', {
        type: 'schools',
        error: error.message
      })
      throw error
    }
  }

  /**
     * Load a single school layer
     */
  async loadSingleSchoolLayer (layerKey, filePath) {
    // Check cache first
    const cacheKey = `school:${layerKey}`
    if (this.cache.has(cacheKey)) {
      this.metrics.cacheHits++
      return this.cache.get(cacheKey)
    }

    this.metrics.cacheMisses++
    this.metrics.totalRequests++

    const response = await fetch(filePath)
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()

    // Validate GeoJSON structure
    if (!data.features || !Array.isArray(data.features)) {
      throw new Error(`Invalid GeoJSON structure for ${layerKey}`)
    }

    // Cache the data
    this.cache.set(cacheKey, data)

    console.log(`[DataLoader] Loaded ${layerKey}: ${data.features.length} features`)

    return data
  }

  /**
     * Generic data fetcher with caching
     */
  async fetchData (url, cacheKey = null) {
    const startTime = performance.now()

    try {
      // Use URL as cache key if not provided
      const key = cacheKey || url

      // Check cache first
      if (this.cache.has(key)) {
        this.metrics.cacheHits++
        return this.cache.get(key)
      }

      this.metrics.cacheMisses++
      this.metrics.totalRequests++

      const response = await fetch(url)
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const data = await response.json()

      // Cache the result
      this.cache.set(key, data)

      const loadTime = performance.now() - startTime
      console.log(`[DataLoader] Fetched data from ${url} in ${loadTime.toFixed(2)}ms`)

      return data
    } catch (error) {
      console.error(`[DataLoader] Failed to fetch data from ${url}:`, error)
      throw error
    }
  }

  /**
     * Check if a file exists (using HEAD request)
     */
  async checkFileExists (filePath) {
    try {
      const response = await fetch(filePath, { method: 'HEAD' })
      return response.ok
    } catch (error) {
      return false
    }
  }

  /**
     * Resolve relative data path to full path
     */
  resolveDataPath (relativePath) {
    return this.baseDataPath + relativePath
  }

  /**
     * Clear cache for specific keys or all data
     */
  clearCache (keys = null) {
    if (keys === null) {
      const oldSize = this.cache.size
      this.cache.clear()
      console.log(`[DataLoader] Cleared entire cache (${oldSize} entries)`)
    } else if (Array.isArray(keys)) {
      keys.forEach(key => {
        if (this.cache.has(key)) {
          this.cache.delete(key)
          console.log(`[DataLoader] Cleared cache entry: ${key}`)
        }
      })
    } else {
      if (this.cache.has(keys)) {
        this.cache.delete(keys)
        console.log(`[DataLoader] Cleared cache entry: ${keys}`)
      }
    }
  }

  /**
     * Get cache statistics
     */
  getCacheStats () {
    return {
      size: this.cache.size,
      keys: Array.from(this.cache.keys()),
      metrics: { ...this.metrics }
    }
  }

  /**
     * Get performance metrics
     */
  getMetrics () {
    return {
      ...this.metrics,
      cacheHitRate: this.metrics.totalRequests > 0
        ? (this.metrics.cacheHits / this.metrics.totalRequests * 100).toFixed(2) + '%'
        : 'N/A'
    }
  }

  /**
     * Preload data for better performance
     */
  async preloadData (datasetKeys) {
    console.log('[DataLoader] Starting data preload...', datasetKeys)

    const preloadPromises = datasetKeys.map(async (key) => {
      try {
        await this.loadElectionData(key)
        console.log(`[DataLoader] Preloaded: ${key}`)
      } catch (error) {
        console.warn(`[DataLoader] Failed to preload ${key}:`, error.message)
      }
    })

    await Promise.all(preloadPromises)
    console.log('[DataLoader] Preload completed')
  }

  /**
     * Clean up resources
     */
  destroy () {
    this.cache.clear()
    this.loadingPromises.clear()
    console.log('[DataLoader] Cleaned up resources')
  }
}
