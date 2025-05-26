/**
 * URL Utilities - URL Parameter Management and Navigation
 *
 * Handles:
 * - URL parameter parsing and serialization
 * - State restoration from URLs
 * - Share link generation
 * - Path resolution and validation
 * - Base path detection
 *
 * Extracted from the monolithic election_map.html JavaScript code.
 */

/**
 * Parse URL parameters into object
 */
export function parseUrlParameters (url = window.location.href) {
  try {
    const urlObj = new URL(url)
    const params = {}

    for (const [key, value] of urlObj.searchParams.entries()) {
      // Try to parse numeric values
      if (!isNaN(value) && value !== '') {
        params[key] = parseFloat(value)
      } else if (value === 'true') {
        params[key] = true
      } else if (value === 'false') {
        params[key] = false
      } else {
        params[key] = value
      }
    }

    return params
  } catch (error) {
    console.error('[URLUtils] Error parsing URL parameters:', error)
    return {}
  }
}

/**
 * Serialize object to URL parameters
 */
export function serializeParameters (params) {
  if (!params || typeof params !== 'object') return ''

  const urlParams = new URLSearchParams()

  Object.keys(params).forEach(key => {
    const value = params[key]
    if (value !== null && value !== undefined) {
      urlParams.set(key, value.toString())
    }
  })

  return urlParams.toString()
}

/**
 * Update URL without page reload
 */
export function updateUrl (params, replaceState = false) {
  try {
    const url = new URL(window.location)

    // Clear existing parameters if starting fresh
    if (replaceState) {
      url.search = ''
    }

    // Add new parameters
    Object.keys(params).forEach(key => {
      const value = params[key]
      if (value !== null && value !== undefined) {
        url.searchParams.set(key, value.toString())
      } else {
        url.searchParams.delete(key)
      }
    })

    // Update browser history
    if (replaceState) {
      window.history.replaceState(null, '', url.toString())
    } else {
      window.history.pushState(null, '', url.toString())
    }

    return url.toString()
  } catch (error) {
    console.error('[URLUtils] Error updating URL:', error)
    return window.location.href
  }
}

/**
 * Generate shareable URL with state parameters
 */
export function generateShareUrl (stateParams, baseUrl = window.location.href) {
  try {
    const url = new URL(baseUrl.split('?')[0]) // Remove existing params

    // Add state parameters
    Object.keys(stateParams).forEach(key => {
      const value = stateParams[key]
      if (value !== null && value !== undefined) {
        url.searchParams.set(key, value.toString())
      }
    })

    return url.toString()
  } catch (error) {
    console.error('[URLUtils] Error generating share URL:', error)
    return baseUrl
  }
}

/**
 * Check if URL has specific parameters
 */
export function hasUrlParameters (requiredParams = []) {
  const params = parseUrlParameters()

  if (requiredParams.length === 0) {
    return Object.keys(params).length > 0
  }

  return requiredParams.some(param => params.hasOwnProperty(param))
}

/**
 * Get specific URL parameter with default value
 */
export function getUrlParameter (paramName, defaultValue = null) {
  const params = parseUrlParameters()
  return params.hasOwnProperty(paramName) ? params[paramName] : defaultValue
}

/**
 * Remove specific parameters from URL
 */
export function removeUrlParameters (paramsToRemove) {
  try {
    const url = new URL(window.location)

    paramsToRemove.forEach(param => {
      url.searchParams.delete(param)
    })

    window.history.replaceState(null, '', url.toString())
    return url.toString()
  } catch (error) {
    console.error('[URLUtils] Error removing URL parameters:', error)
    return window.location.href
  }
}

/**
 * Clear all URL parameters
 */
export function clearUrlParameters () {
  try {
    const url = new URL(window.location)
    url.search = ''
    window.history.replaceState(null, '', url.toString())
    return url.toString()
  } catch (error) {
    console.error('[URLUtils] Error clearing URL parameters:', error)
    return window.location.href
  }
}

/**
 * Detect base data path based on environment
 */
export function detectBaseDataPath () {
  const hostname = window.location.hostname
  const pathname = window.location.pathname

  // Local development
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return '../'
  }

  // GitHub Pages
  const allowedGitHubHosts = ['github.io']
  if (allowedGitHubHosts.some(allowedHost => hostname === allowedHost || hostname.endsWith(`.${allowedHost}`))) {
    return ''
  }

  // Custom domain or other hosting
  if (pathname.includes('/maps/')) {
    return '../'
  }

  // Default fallback
  return './'
}

/**
 * Resolve data file path
 */
export function resolveDataPath (relativePath) {
  const basePath = detectBaseDataPath()
  return basePath + relativePath
}

/**
 * Build absolute URL from relative path
 */
export function buildAbsoluteUrl (relativePath) {
  try {
    return new URL(relativePath, window.location.origin).toString()
  } catch (error) {
    console.error('[URLUtils] Error building absolute URL:', error)
    return relativePath
  }
}

/**
 * Validate URL format
 */
export function isValidUrl (urlString) {
  try {
    new URL(urlString)
    return true
  } catch (error) {
    return false
  }
}

/**
 * Extract domain from URL
 */
export function extractDomain (url) {
  try {
    const urlObj = new URL(url)
    return urlObj.hostname
  } catch (error) {
    console.error('[URLUtils] Error extracting domain:', error)
    return null
  }
}

/**
 * Check if URL is external
 */
export function isExternalUrl (url) {
  try {
    const urlObj = new URL(url, window.location.origin)
    return urlObj.origin !== window.location.origin
  } catch (error) {
    return false
  }
}

/**
 * Generate social media sharing URLs
 */
export function generateSocialUrls (shareUrl, title = '', description = '') {
  const encodedUrl = encodeURIComponent(shareUrl)
  const encodedTitle = encodeURIComponent(title)
  const encodedDescription = encodeURIComponent(description)

  return {
    twitter: `https://twitter.com/intent/tweet?text=${encodedTitle}&url=${encodedUrl}`,
    facebook: `https://www.facebook.com/sharer/sharer.php?u=${encodedUrl}&quote=${encodedDescription}`,
    linkedin: `https://www.linkedin.com/sharing/share-offsite/?url=${encodedUrl}&title=${encodedTitle}&summary=${encodedDescription}`,
    email: `mailto:?subject=${encodedTitle}&body=${encodedDescription}%0A%0A${encodedUrl}`,
    copy: shareUrl
  }
}

/**
 * Parse hash fragment into parameters
 */
export function parseHashParameters (hash = window.location.hash) {
  if (!hash || !hash.startsWith('#')) return {}

  const hashParams = {}
  const hashString = hash.substring(1)

  if (hashString.includes('=')) {
    // Hash contains key=value pairs
    const pairs = hashString.split('&')
    pairs.forEach(pair => {
      const [key, value] = pair.split('=')
      if (key && value) {
        hashParams[decodeURIComponent(key)] = decodeURIComponent(value)
      }
    })
  }

  return hashParams
}

/**
 * Set hash parameters
 */
export function setHashParameters (params) {
  const hashPairs = Object.keys(params).map(key =>
        `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`
  )

  window.location.hash = hashPairs.join('&')
}

/**
 * Get file extension from URL
 */
export function getFileExtension (url) {
  try {
    const urlObj = new URL(url)
    const pathname = urlObj.pathname
    const lastDot = pathname.lastIndexOf('.')

    if (lastDot > 0) {
      return pathname.substring(lastDot + 1).toLowerCase()
    }

    return ''
  } catch (error) {
    return ''
  }
}

/**
 * Check if URL points to a specific file type
 */
export function isFileType (url, extensions) {
  const fileExt = getFileExtension(url)
  const targetExtensions = Array.isArray(extensions) ? extensions : [extensions]

  return targetExtensions.some(ext => ext.toLowerCase() === fileExt)
}

/**
 * Sanitize URL for safe usage
 */
export function sanitizeUrl (url) {
  if (!url) return ''

  // Remove dangerous protocols
  const dangerousProtocols = ['javascript:', 'data:', 'vbscript:']
  const lowerUrl = url.toLowerCase()

  if (dangerousProtocols.some(protocol => lowerUrl.startsWith(protocol))) {
    return ''
  }

  try {
    const urlObj = new URL(url, window.location.origin)
    return urlObj.toString()
  } catch (error) {
    // If parsing fails, return empty string for safety
    return ''
  }
}

/**
 * URL utilities object for backward compatibility
 */
export const URLUtils = {
  parseUrlParameters,
  serializeParameters,
  updateUrl,
  generateShareUrl,
  hasUrlParameters,
  getUrlParameter,
  removeUrlParameters,
  clearUrlParameters,
  detectBaseDataPath,
  resolveDataPath,
  buildAbsoluteUrl,
  isValidUrl,
  extractDomain,
  isExternalUrl,
  generateSocialUrls,
  parseHashParameters,
  setHashParameters,
  getFileExtension,
  isFileType,
  sanitizeUrl
}
