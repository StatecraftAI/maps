/**
 * Jest Test Setup
 *
 * Global test configuration and mocks for the refactored election map application.
 */

// Mock localStorage
const localStorageMock = {
  getItem: globalThis.jest?.fn((key) => localStorageMock.store[key] || null) ||
           ((key) => localStorageMock.store[key] || null),
  setItem: globalThis.jest?.fn((key, value) => {
    localStorageMock.store[key] = value.toString()
  }) || ((key, value) => {
    localStorageMock.store[key] = value.toString()
  }),
  removeItem: globalThis.jest?.fn((key) => {
    delete localStorageMock.store[key]
  }) || ((key) => {
    delete localStorageMock.store[key]
  }),
  clear: globalThis.jest?.fn(() => {
    localStorageMock.store = {}
  }) || (() => {
    localStorageMock.store = {}
  }),
  store: {}
}

Object.defineProperty(globalThis, 'localStorage', {
  value: localStorageMock,
  writable: true
})

// Mock console.log in tests to reduce noise
globalThis.console = {
  ...console,
  log: globalThis.jest?.fn() || (() => {}),
  warn: globalThis.jest?.fn() || (() => {}),
  error: globalThis.jest?.fn() || (() => {})
}

// Mock performance.now for timing tests
Object.defineProperty(globalThis, 'performance', {
  value: {
    now: globalThis.jest?.fn(() => Date.now()) || (() => Date.now())
  },
  writable: true
})

// Mock fetch for data loading tests
globalThis.fetch = globalThis.jest?.fn(() =>
  Promise.resolve({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ features: [] }),
    text: () => Promise.resolve('')
  })
) || (() => Promise.resolve({
  ok: true,
  status: 200,
  json: () => Promise.resolve({ features: [] }),
  text: () => Promise.resolve('')
}))

// Mock Leaflet (for map-related tests)
globalThis.L = {
  map: globalThis.jest?.fn(() => ({
    setView: globalThis.jest?.fn() || (() => {}),
    addLayer: globalThis.jest?.fn() || (() => {}),
    removeLayer: globalThis.jest?.fn() || (() => {}),
    on: globalThis.jest?.fn() || (() => {}),
    off: globalThis.jest?.fn() || (() => {}),
    getCenter: globalThis.jest?.fn(() => ({ lat: 45.5152, lng: -122.6784 })) ||
              (() => ({ lat: 45.5152, lng: -122.6784 })),
    getZoom: globalThis.jest?.fn(() => 11) || (() => 11)
  })) || (() => ({
    setView: () => {},
    addLayer: () => {},
    removeLayer: () => {},
    on: () => {},
    off: () => {},
    getCenter: () => ({ lat: 45.5152, lng: -122.6784 }),
    getZoom: () => 11
  })),
  tileLayer: globalThis.jest?.fn(() => ({
    addTo: globalThis.jest?.fn() || (() => {})
  })) || (() => ({
    addTo: () => {}
  })),
  geoJSON: globalThis.jest?.fn(() => ({
    addTo: globalThis.jest?.fn() || (() => {}),
    removeFrom: globalThis.jest?.fn() || (() => {})
  })) || (() => ({
    addTo: () => {},
    removeFrom: () => {}
  }))
}

// Reset mocks before each test
globalThis.beforeEach(() => {
  // Reset localStorage
  localStorageMock.clear()

  // Reset fetch mock
  if (globalThis.fetch.mockClear) {
    globalThis.fetch.mockClear()
  }

  // Reset console mocks
  if (globalThis.console.log.mockClear) {
    globalThis.console.log.mockClear()
    globalThis.console.warn.mockClear()
    globalThis.console.error.mockClear()
  }

  // Reset performance mock
  if (globalThis.performance.now.mockClear) {
    globalThis.performance.now.mockClear()
  }
})

// Helper function to create DOM elements for testing
globalThis.createMockElement = (tag, id, className) => {
  const element = globalThis.document.createElement(tag)
  if (id) element.id = id
  if (className) element.className = className
  return element
}

// Helper function to create mock GeoJSON data
globalThis.createMockGeoJSON = (featureCount = 1) => ({
  type: 'FeatureCollection',
  features: Array.from({ length: featureCount }, (_, i) => ({
    type: 'Feature',
    properties: {
      precinct: `P${i + 1}`,
      votes_total: 100 + i * 10,
      vote_pct_candidate_a: 45.5 + i,
      vote_pct_candidate_b: 54.5 - i,
      is_pps_precinct: true,
      political_lean: 'Competitive'
    },
    geometry: {
      type: 'Polygon',
      coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
    }
  })),
  metadata: {
    field_registry: {
      available_fields: ['votes_total', 'vote_pct_candidate_a', 'vote_pct_candidate_b'],
      numeric_fields: ['votes_total', 'vote_pct_candidate_a', 'vote_pct_candidate_b'],
      categorical_fields: ['political_lean'],
      display_names: {
        votes_total: 'Total Votes',
        vote_pct_candidate_a: 'Candidate A %',
        vote_pct_candidate_b: 'Candidate B %'
      }
    },
    candidate_colors: {
      candidate_a: '#0571b0',
      candidate_b: '#fd8d3c'
    }
  }
})

// Global test timeout
globalThis.jest?.setTimeout(10000)
