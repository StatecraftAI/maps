/**
 * Constants and Configuration for Election Map Application
 *
 * Centralized configuration management for colors, base maps,
 * application settings, and other constants used throughout the app.
 */

/**
 * Application Configuration
 */
export const APP_CONFIG = {
  // Map defaults
  DEFAULT_CENTER: [45.5152, -122.6784], // Portland, OR
  DEFAULT_ZOOM: 11,
  MIN_ZOOM: 8,
  MAX_ZOOM: 18,

  // Data paths
  DATA_BASE_PATHS: {
    localhost: '../',
    'github.io': ''
  },

  // Performance settings
  DEBOUNCE_DELAY: 300,
  CHART_ANIMATION_DURATION: 750,
  MAP_TRANSITION_DURATION: 500,

  // UI settings
  SIDEBAR_WIDTH: 320,
  LEGEND_HEIGHT: 40,
  POPUP_MAX_WIDTH: 320,

  // Local storage
  STORAGE_KEY: 'electionMapState',
  STORAGE_VERSION: '1.0'
}

/**
 * Base Map Configurations
 */
export const BASE_MAPS = {
  streets: {
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '© OpenStreetMap contributors',
    maxZoom: 19,
    name: 'Streets'
  },
  satellite: {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: '© Esri, Digital Globe, GeoEye, Earthstar Geographics',
    maxZoom: 18,
    name: 'Satellite'
  },
  topo: {
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    attribution: '© OpenTopoMap contributors',
    maxZoom: 17,
    name: 'Topographic'
  },
  dark: {
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '© OpenStreetMap contributors, © CARTO',
    maxZoom: 19,
    name: 'Dark Mode'
  },
  'dark-nolabels': {
    url: 'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
    attribution: '© OpenStreetMap contributors, © CARTO',
    maxZoom: 19,
    name: 'Dark (No Labels)'
  }
}

/**
 * Color Schemes for Data Visualization
 *
 * Color-blind friendly palettes following best practices
 */
export const COLOR_SCHEMES = {
  // Political lean visualization
  political_lean: {
    'Strong Dem': '#0571b0', // Strong blue (colorbrewer)
    'Lean Dem': '#74a9cf', // Light blue
    Competitive: '#fee391', // Light yellow
    'Lean Rep': '#fd8d3c', // Orange
    'Strong Rep': '#d94701' // Strong orange/red
  },

  // Election competitiveness
  competitiveness: {
    Safe: '#2166ac', // Dark blue (less competitive = darker)
    Likely: '#762a83', // Purple
    Competitive: '#f1a340', // Orange
    Tossup: '#d73027', // Red (most competitive = darkest)
    'No Election Data': '#f7f7f7' // Light gray
  },

  // Leading candidate (colors added dynamically)
  leading_candidate: {
    Tie: '#636363', // Gray
    'No Election Data': '#f7f7f7', // Light gray
    'No Data': '#f7f7f7' // Light gray
    // Candidate colors will be added dynamically
  },

  // Turnout quartiles
  turnout_quartile: {
    Low: '#fee391', // Light yellow (low = light)
    'Med-Low': '#fec44f', // Medium yellow
    Medium: '#fe9929', // Orange
    'Med-High': '#d95f0e', // Dark orange
    High: '#993404', // Very dark orange (high = dark)
    Single: '#f7f7f7' // Light gray
  },

  // Victory margins
  margin_category: {
    'Very Close': '#fee391', // Light (close = light color)
    Close: '#fec44f', // Medium light
    Clear: '#d95f0e', // Darker (clear = darker)
    Landslide: '#993404' // Darkest (landslide = darkest)
  },

  // Precinct sizes
  precinct_size_category: {
    Small: '#fee391', // Light (small = light)
    Medium: '#fec44f', // Medium light
    Large: '#d95f0e', // Dark (large = dark)
    'Extra Large': '#993404' // Darkest (extra large = darkest)
  }
}

/**
 * Color palettes for dynamic candidate assignment
 */
export const CANDIDATE_COLOR_PALETTE = [
  '#0571b0', // Blue
  '#fd8d3c', // Orange
  '#238b45', // Green
  '#d62728', // Red
  '#9467bd', // Purple
  '#8c564b', // Brown
  '#e377c2', // Pink
  '#7f7f7f', // Gray
  '#bcbd22', // Olive
  '#17becf' // Cyan
]

/**
 * Gradient color schemes for continuous data
 */
export const GRADIENT_SCHEMES = {
  // Viridis-like for percentages
  percentage: [
    [68, 1, 84], // Dark purple (low)
    [59, 82, 139], // Blue
    [33, 145, 140], // Teal
    [94, 201, 98], // Green
    [253, 231, 37] // Yellow (high)
  ],

  // Plasma-like for counts
  count: [
    [13, 8, 135], // Dark blue (low)
    [84, 2, 163], // Purple
    [139, 10, 165], // Pink
    [185, 50, 137], // Red
    [224, 93, 106], // Orange
    [253, 231, 37] // Yellow (high)
  ],

  // Cividis-like for general numeric data
  numeric: [
    [0, 32, 76], // Dark blue (low)
    [0, 67, 88], // Blue
    [0, 104, 87], // Teal
    [87, 134, 58], // Green
    [188, 163, 23], // Yellow
    [255, 221, 0] // Bright yellow (high)
  ]
}

/**
 * School overlay configuration
 */
export const SCHOOL_CONFIG = {
  // School icon SVG definitions
  icons: {
    high: {
      viewBox: '0 0 16 16',
      paths: `
                <rect x="2" y="2" width="12" height="12" fill="#4E3A6D" stroke="#000" stroke-width="0.5" rx="1"/>
                <rect x="4" y="4" width="8" height="8" fill="#FFFFFF" stroke="#4E3A6D" stroke-width="0.5"/>
                <circle cx="8" cy="8" r="2" fill="#4E3A6D"/>
            `,
      color: '#4E3A6D'
    },
    middle: {
      viewBox: '0 0 16 16',
      paths: `
                <polygon points="8,2 14,14 2,14" fill="#4F4F4F" stroke="#000" stroke-width="0.5"/>
                <polygon points="8,4 12,12 4,12" fill="#FFFFFF" stroke="#4F4F4F" stroke-width="0.5"/>
                <circle cx="8" cy="10" r="1.5" fill="#4F4F4F"/>
            `,
      color: '#4F4F4F'
    },
    elementary: {
      viewBox: '0 0 16 16',
      paths: `
                <circle cx="8" cy="8" r="6" fill="#000000" stroke="#4F4F4F" stroke-width="0.5"/>
                <circle cx="8" cy="8" r="4" fill="#F4F4F3" stroke="#000000" stroke-width="0.5"/>
                <circle cx="8" cy="8" r="2" fill="#000000"/>
            `,
      color: '#000000'
    }
  },

  // Boundary styles
  boundaries: {
    high: {
      color: '#d62728',
      weight: 2,
      opacity: 0.8,
      fillOpacity: 0.0
    },
    middle: {
      color: '#2ca02c',
      weight: 2,
      opacity: 0.8,
      fillOpacity: 0.0
    },
    elementary: {
      color: '#1f77b4',
      weight: 2,
      opacity: 0.8,
      fillOpacity: 0.0
    },
    district: {
      color: '#ff7f0e',
      weight: 3,
      opacity: 0.8,
      fillOpacity: 0.0
    }
  }
}

/**
 * Data file paths and configurations
 */
export const DATA_PATHS = {
  // School data files
  schools: {
    'high-schools': 'data/geospatial/pps_high_school_locations.geojson',
    'middle-schools': 'data/geospatial/pps_middle_school_locations.geojson',
    'elementary-schools': 'data/geospatial/pps_elementary_school_locations.geojson',
    'high-boundaries': 'data/geospatial/pps_high_school_boundaries.geojson',
    'middle-boundaries': 'data/geospatial/pps_middle_school_boundaries.geojson',
    'elementary-boundaries': 'data/geospatial/pps_elementary_school_boundaries.geojson',
    'district-boundary': 'data/geospatial/pps_district_boundary.geojson'
  },

  // Dataset patterns
  election: {
    pattern: 'data/geospatial/2025_election_{zone}_total_votes_results.geojson',
    zones: [1, 4, 5, 6]
  },

  // Static datasets
  voter_registration: 'data/geospatial/multnomah_election_precincts.geojson',
  bond: 'data/geospatial/2025_election_bond_total_votes_results.geojson'
}

/**
 * UI Element Selectors
 */
export const SELECTORS = {
  // Main containers
  map: '#map',
  controlPanel: '.control-panel',
  infoPanel: '.info-panel',
  legend: '#color-scale-legend',

  // Form elements
  datasetSelect: '#dataset-select',
  layerSelector: '#layer-selector',
  opacitySlider: '#opacity-slider',
  basemapSelect: '#basemap-select',
  ppsFilter: '#pps-only',

  // Feature controls
  searchInput: '#address-search',
  rangeMin: '#range-min',
  rangeMax: '#range-max',

  // Loading and error states
  loading: '#loading',
  errorDisplay: '#error-display'
}

/**
 * Animation and transition settings
 */
export const ANIMATIONS = {
  // Map transitions
  mapPan: {
    duration: 0.5,
    easeLinearity: 0.1
  },

  // Chart animations
  chart: {
    duration: 750,
    easing: 'easeInOutQuart'
  },

  // UI animations
  accordion: {
    duration: 300,
    easing: 'ease-in-out'
  },

  // Legend updates
  legend: {
    duration: 200,
    easing: 'ease'
  }
}

/**
 * Error messages and user feedback
 */
export const MESSAGES = {
  loading: {
    data: 'Loading election data...',
    map: 'Initializing map...',
    schools: 'Loading school data...',
    search: 'Searching...'
  },

  errors: {
    mapInit: 'Failed to initialize map. Please refresh and try again.',
    dataLoad: 'Failed to load election data. Please check your connection.',
    schoolData: 'Could not load school overlay data.',
    search: 'Search failed. Please try again.',
    geolocation: 'Unable to determine your location.'
  },

  success: {
    dataLoaded: 'Election data loaded successfully',
    mapReady: 'Map initialized',
    searchComplete: 'Location found',
    export: 'Map exported successfully'
  }
}

/**
 * Feature flags for conditional functionality
 */
export const FEATURE_FLAGS = {
  enableHeatmap: true,
  enableComparison: true,
  enableCoordinateDisplay: true,
  enableExport: true,
  enableSchoolOverlays: true,
  enableSocialSharing: true,
  enableAdvancedAnalytics: false, // Future feature
  enableOfflineMode: false // Future feature
}

/**
 * Performance and debugging settings
 */
export const DEBUG_CONFIG = {
  // Console logging
  enableEventLogging: false,
  enableStateLogging: false,
  enablePerformanceLogging: true,

  // History limits
  maxEventHistory: 100,
  maxStateHistory: 50,

  // Performance monitoring
  performanceMetrics: true,
  memoryMonitoring: false
}
