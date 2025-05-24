/**
 * Name Utilities - Name Normalization and Formatting
 * 
 * Handles:
 * - Candidate name normalization for consistent lookups
 * - Field name formatting for display
 * - Snake case and title case conversions
 * - Name validation and sanitization
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

/**
 * Convert string to snake_case
 */
export function toSnakeCase(str) {
    if (!str) return '';
    return str.toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]/g, '');
}

/**
 * Convert string to Title Case
 */
export function toTitleCase(str) {
    if (!str) return '';
    return str.replace(/_/g, ' ')
        .replace(/\b\w/g, l => l.toUpperCase());
}

/**
 * Normalize candidate name for consistent key lookup
 */
export function normalizeCandidateName(name) {
    if (!name) return '';
    // Convert to snake_case for consistent key lookup
    return toSnakeCase(name);
}

/**
 * Format candidate name for display
 */
export function displayCandidateName(name) {
    if (!name) return '';
    // Convert to Title Case for display
    return toTitleCase(name);
}

/**
 * Format field name for display
 */
export function formatFieldName(fieldKey) {
    if (!fieldKey) return '';
    
    // Handle special cases
    const specialCases = {
        'vote_pct': 'Vote %',
        'reg_pct': 'Registration %',
        'dem_advantage': 'Democratic Advantage',
        'rep_advantage': 'Republican Advantage',
        'turnout_rate': 'Turnout Rate',
        'votes_total': 'Total Votes',
        'total_voters': 'Total Voters',
        'major_party_pct': 'Major Party %',
        'political_lean': 'Political Lean',
        'leading_candidate': 'Leading Candidate',
        'second_place_candidate': 'Second Place Candidate',
        'vote_margin': 'Vote Margin',
        'margin_category': 'Margin Category',
        'competitiveness': 'Competitiveness',
        'precinct_size_category': 'Precinct Size',
        'turnout_quartile': 'Turnout Quartile'
    };
    
    if (specialCases[fieldKey]) {
        return specialCases[fieldKey];
    }
    
    // Handle dynamic candidate fields
    if (fieldKey.startsWith('votes_') && fieldKey !== 'votes_total') {
        const candidateName = fieldKey.replace('votes_', '');
        return `Vote Count - ${displayCandidateName(candidateName)}`;
    }
    
    if (fieldKey.startsWith('vote_pct_') && !fieldKey.startsWith('vote_pct_contribution_')) {
        const candidateName = fieldKey.replace('vote_pct_', '');
        return `Vote % - ${displayCandidateName(candidateName)}`;
    }
    
    if (fieldKey.startsWith('vote_pct_contribution_')) {
        const candidateName = fieldKey.replace('vote_pct_contribution_', '');
        return `Vote Contribution % - ${displayCandidateName(candidateName)}`;
    }
    
    if (fieldKey.startsWith('reg_pct_')) {
        const party = fieldKey.replace('reg_pct_', '').toUpperCase();
        return `Registration % - ${party}`;
    }
    
    // Default formatting
    return toTitleCase(fieldKey);
}

/**
 * Validate candidate name
 */
export function isValidCandidateName(name) {
    if (!name || typeof name !== 'string') return false;
    
    // Skip problematic entries
    const skipEntries = [
        'Tie', 'No Data', 'No Election Data',
        'leading', 'second_place', 'total', 'write_in',
        'Write In', 'Leading', 'Second Place'
    ];
    
    if (skipEntries.includes(name)) return false;
    
    // Additional validation
    if (name.length < 2) return false;
    if (name.startsWith('vote_') || name.startsWith('reg_')) return false;
    if (name.includes('_total') || name.includes('_pct')) return false;
    if (name === name.toUpperCase()) return false; // Skip ALL CAPS
    
    return true;
}

/**
 * Clean field name for safe usage
 */
export function sanitizeFieldName(fieldName) {
    if (!fieldName) return '';
    
    return fieldName
        .replace(/[^a-zA-Z0-9_]/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '')
        .toLowerCase();
}

/**
 * Generate display name with fallback
 */
export function getDisplayNameWithFallback(name, fallback = 'Unknown') {
    const displayName = displayCandidateName(name);
    
    if (!displayName || 
        displayName.trim() === '' || 
        displayName === 'undefined' || 
        displayName === 'null') {
        return fallback;
    }
    
    return displayName;
}

/**
 * Check if name looks like a candidate
 */
export function looksLikeCandidateName(name) {
    if (!name || typeof name !== 'string') return false;
    
    // Should be reasonable length
    if (name.length < 2 || name.length > 50) return false;
    
    // Should contain letters
    if (!/[a-zA-Z]/.test(name)) return false;
    
    // Should not be all uppercase (likely a data field)
    if (name === name.toUpperCase() && name.length > 3) return false;
    
    // Should not contain suspicious patterns
    const suspiciousPatterns = [
        /^vote_/i,
        /^reg_/i,
        /_total$/i,
        /_pct$/i,
        /^leading$/i,
        /^second$/i,
        /^margin$/i,
        /^turnout$/i
    ];
    
    return !suspiciousPatterns.some(pattern => pattern.test(name));
}

/**
 * Extract candidate names from field list
 */
export function extractCandidateNames(fields) {
    const candidates = new Set();
    
    fields.forEach(field => {
        if (field.startsWith('vote_pct_') && 
            !field.startsWith('vote_pct_contribution_') &&
            field !== 'vote_pct_contribution_total_votes') {
            
            const candidateName = field.replace('vote_pct_', '');
            if (isValidCandidateName(candidateName)) {
                candidates.add(candidateName);
            }
        }
    });
    
    return Array.from(candidates).sort();
}

/**
 * Format name for filename
 */
export function formatNameForFilename(name) {
    if (!name) return '';
    
    return name
        .replace(/[^a-z0-9_]/gi, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '')
        .toLowerCase();
}

/**
 * Compare names for sorting (case-insensitive)
 */
export function compareNames(nameA, nameB) {
    const a = (nameA || '').toLowerCase();
    const b = (nameB || '').toLowerCase();
    
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

/**
 * Name utilities object for backward compatibility
 */
export const NameUtils = {
    toSnakeCase,
    toTitleCase,
    normalizeCandidateName,
    displayCandidateName,
    formatFieldName,
    isValidCandidateName,
    sanitizeFieldName,
    getDisplayNameWithFallback,
    looksLikeCandidateName,
    extractCandidateNames,
    formatNameForFilename,
    compareNames
}; 