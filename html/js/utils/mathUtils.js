/**
 * Math Utilities - Mathematical Operations and Calculations
 *
 * Handles:
 * - Data range calculations
 * - Statistical operations
 * - Percentage formatting
 * - Number formatting and validation
 * - Geometric calculations
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

/**
 * Calculate min and max values from an array
 */
export function getMinMax (values) {
  if (!Array.isArray(values) || values.length === 0) {
    return { min: 0, max: 0 }
  }

  const validValues = values.filter(v => v !== null && v !== undefined && !isNaN(v))
  if (validValues.length === 0) {
    return { min: 0, max: 0 }
  }

  return {
    min: Math.min(...validValues),
    max: Math.max(...validValues)
  }
}

/**
 * Calculate data range with padding
 */
export function calculateRangeWithPadding (values, paddingPercent = 5) {
  const { min, max } = getMinMax(values)
  const range = max - min
  const padding = range * (paddingPercent / 100)

  return {
    min: Math.max(0, min - padding),
    max: max + padding
  }
}

/**
 * Normalize value to 0-1 range
 */
export function normalize (value, min, max) {
  if (max === min) return 0
  return Math.max(0, Math.min(1, (value - min) / (max - min)))
}

/**
 * Clamp value between min and max
 */
export function clamp (value, min, max) {
  return Math.max(min, Math.min(max, value))
}

/**
 * Round to specified decimal places
 */
export function roundTo (value, decimals = 2) {
  const factor = Math.pow(10, decimals)
  return Math.round(value * factor) / factor
}

/**
 * Format number with commas
 */
export function formatNumber (value, decimals = 0) {
  if (value === null || value === undefined || isNaN(value)) {
    return 'N/A'
  }

  const rounded = roundTo(value, decimals)
  return rounded.toLocaleString()
}

/**
 * Format percentage
 */
export function formatPercentage (value, decimals = 1) {
  if (value === null || value === undefined || isNaN(value)) {
    return 'N/A'
  }

  return `${roundTo(value, decimals)}%`
}

/**
 * Format value based on field type
 */
export function formatValue (fieldKey, value) {
  if (value === null || value === undefined) {
    return 'N/A'
  }

  if (typeof value === 'number') {
    if (fieldKey.includes('vote_pct_') ||
            fieldKey.includes('reg_pct_') ||
            fieldKey === 'turnout_rate' ||
            fieldKey === 'major_party_pct' ||
            fieldKey === 'dem_advantage' ||
            fieldKey === 'pct_victory_margin' ||
            fieldKey === 'engagement_rate') {
      return formatPercentage(value)
    } else {
      return formatNumber(value)
    }
  }

  return value.toString()
}

/**
 * Calculate average from array
 */
export function average (values) {
  const validValues = values.filter(v => v !== null && v !== undefined && !isNaN(v))
  if (validValues.length === 0) return 0

  const sum = validValues.reduce((acc, val) => acc + val, 0)
  return sum / validValues.length
}

/**
 * Calculate median from array
 */
export function median (values) {
  const validValues = values.filter(v => v !== null && v !== undefined && !isNaN(v))
  if (validValues.length === 0) return 0

  const sorted = validValues.sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)

  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2
  } else {
    return sorted[mid]
  }
}

/**
 * Calculate standard deviation
 */
export function standardDeviation (values) {
  const validValues = values.filter(v => v !== null && v !== undefined && !isNaN(v))
  if (validValues.length === 0) return 0

  const avg = average(validValues)
  const squaredDifferences = validValues.map(value => Math.pow(value - avg, 2))
  const avgSquaredDiff = average(squaredDifferences)

  return Math.sqrt(avgSquaredDiff)
}

/**
 * Calculate polygon centroid
 */
export function calculatePolygonCentroid (coordinates) {
  if (!Array.isArray(coordinates) || coordinates.length < 3) {
    return null
  }

  let totalLng = 0
  let totalLat = 0
  let validPoints = 0

  coordinates.forEach(point => {
    if (Array.isArray(point) && point.length >= 2 &&
            typeof point[0] === 'number' && typeof point[1] === 'number' &&
            !isNaN(point[0]) && !isNaN(point[1])) {
      totalLng += point[0]
      totalLat += point[1]
      validPoints++
    }
  })

  if (validPoints > 0) {
    return [totalLat / validPoints, totalLng / validPoints] // [lat, lng]
  }

  return null
}

/**
 * Check if coordinates are valid
 */
export function isValidCoordinate (coords) {
  if (!Array.isArray(coords) || coords.length < 2) {
    return false
  }

  const [lat, lng] = coords

  // Check for valid latitude and longitude ranges
  return lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180
}

/**
 * Check if coordinates are within Portland area (rough bounds)
 */
export function isInPortlandArea (coords) {
  if (!isValidCoordinate(coords)) return false

  const [lat, lng] = coords
  return lat >= 45.0 && lat <= 46.0 && lng >= -123.0 && lng <= -122.0
}

/**
 * Distance between two coordinates (Haversine formula)
 */
export function distanceBetweenCoords (coord1, coord2) {
  if (!isValidCoordinate(coord1) || !isValidCoordinate(coord2)) {
    return null
  }

  const [lat1, lng1] = coord1
  const [lat2, lng2] = coord2

  const R = 6371 // Earth's radius in kilometers
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLng = (lng2 - lng1) * Math.PI / 180

  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLng / 2) * Math.sin(dLng / 2)

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

  return R * c // Distance in kilometers
}

/**
 * Interpolate between two values
 */
export function interpolate (value1, value2, factor) {
  return value1 + (value2 - value1) * clamp(factor, 0, 1)
}

/**
 * Map value from one range to another
 */
export function mapRange (value, fromMin, fromMax, toMin, toMax) {
  const normalized = normalize(value, fromMin, fromMax)
  return interpolate(toMin, toMax, normalized)
}

/**
 * Check if value is numeric
 */
export function isNumeric (value) {
  return !isNaN(parseFloat(value)) && isFinite(value)
}

/**
 * Safe division (avoid division by zero)
 */
export function safeDivision (numerator, denominator, fallback = 0) {
  if (denominator === 0 || !isNumeric(numerator) || !isNumeric(denominator)) {
    return fallback
  }
  return numerator / denominator
}

/**
 * Calculate percentile
 */
export function percentile (values, percentile) {
  const validValues = values.filter(v => v !== null && v !== undefined && !isNaN(v))
  if (validValues.length === 0) return 0

  const sorted = validValues.sort((a, b) => a - b)
  const index = (percentile / 100) * (sorted.length - 1)

  if (index === Math.floor(index)) {
    return sorted[index]
  } else {
    const lower = sorted[Math.floor(index)]
    const upper = sorted[Math.ceil(index)]
    return interpolate(lower, upper, index - Math.floor(index))
  }
}

/**
 * Calculate quartiles
 */
export function calculateQuartiles (values) {
  return {
    q1: percentile(values, 25),
    q2: percentile(values, 50), // median
    q3: percentile(values, 75)
  }
}

/**
 * Parse numeric value safely
 */
export function parseNumeric (value, fallback = 0) {
  if (value === null || value === undefined) return fallback

  const parsed = parseFloat(value)
  return isNaN(parsed) ? fallback : parsed
}

/**
 * Math utilities object for backward compatibility
 */
export const MathUtils = {
  getMinMax,
  calculateRangeWithPadding,
  normalize,
  clamp,
  roundTo,
  formatNumber,
  formatPercentage,
  formatValue,
  average,
  median,
  standardDeviation,
  calculatePolygonCentroid,
  isValidCoordinate,
  isInPortlandArea,
  distanceBetweenCoords,
  interpolate,
  mapRange,
  isNumeric,
  safeDivision,
  percentile,
  calculateQuartiles,
  parseNumeric
}
