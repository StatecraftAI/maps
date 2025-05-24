/**
 * CandidateManager - Dynamic Candidate Detection and Management
 * 
 * Handles:
 * - Dynamic candidate detection from election data
 * - Candidate name normalization and display formatting
 * - Color scheme generation for candidates
 * - Candidate-specific data processing
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

import { StateManager } from '../core/StateManager.js';
import { EventBus } from '../core/EventBus.js';
import { CANDIDATE_COLOR_PALETTE, COLOR_SCHEMES } from '../config/constants.js';

export class CandidateManager {
    constructor(stateManager, eventBus) {
        this.stateManager = stateManager;
        this.eventBus = eventBus;
        
        // Candidate tracking
        this.detectedCandidates = [];
        this.candidateColors = {};
        this.colorIndex = 0;
        
        // Normalization utilities
        this.skipList = new Set([
            'leading', 'second_place', 'total', 'write_in',
            'Write In', 'Leading', 'Second Place', 'tie',
            'Tie', 'No Data', 'No Election Data'
        ]);
        
        console.log('[CandidateManager] Initialized');
    }
    
    /**
     * Detect candidates from election data
     */
    detectCandidates(geoJsonData) {
        console.log('[CandidateManager] Detecting candidates from data...');
        
        if (!geoJsonData?.features?.length) {
            console.warn('[CandidateManager] No features available for candidate detection');
            return [];
        }
        
        const sampleProperties = geoJsonData.features[0].properties;
        const candidates = new Set();
        
        // Look for vote percentage fields to identify candidates
        Object.keys(sampleProperties).forEach(prop => {
            if (this.isVotePercentageField(prop)) {
                const candidateName = prop.replace('vote_pct_', '');
                
                if (this.isValidCandidateName(candidateName)) {
                    candidates.add(candidateName);
                }
            }
        });
        
        this.detectedCandidates = Array.from(candidates);
        
        console.log(`[CandidateManager] Detected ${this.detectedCandidates.length} candidates:`, this.detectedCandidates);
        
        this.eventBus.emit('candidates:detected', {
            candidates: this.detectedCandidates,
            count: this.detectedCandidates.length
        });
        
        return this.detectedCandidates;
    }
    
    /**
     * Check if a field is a vote percentage field
     */
    isVotePercentageField(fieldName) {
        return fieldName.startsWith('vote_pct_') &&
               !fieldName.startsWith('vote_pct_contribution_') &&
               fieldName !== 'vote_pct_contribution_total_votes';
    }
    
    /**
     * Validate candidate name
     */
    isValidCandidateName(candidateName) {
        // Skip administrative fields
        if (this.skipList.has(candidateName)) {
            return false;
        }
        
        // Skip single letters or very short entries
        if (candidateName.length <= 2) {
            return false;
        }
        
        // Skip fields with underscore patterns that suggest they're not candidates
        if (candidateName.includes('_total') || candidateName.includes('_pct')) {
            return false;
        }
        
        // Skip ALL CAPS entries (likely administrative)
        if (candidateName === candidateName.toUpperCase() && candidateName.length > 3) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Build candidate color schemes
     */
    buildCandidateColorSchemes(geoJsonData) {
        console.log('[CandidateManager] Building candidate color schemes...');
        
        // Reset the color scheme
        this.candidateColors = {
            'Tie': '#636363',
            'No Election Data': '#f7f7f7',
            'No Data': '#f7f7f7'
        };
        
        // Check for metadata colors first
        if (geoJsonData.metadata?.candidate_colors) {
            this.useCandidateColorsFromMetadata(geoJsonData.metadata.candidate_colors);
        } else {
            this.generateAutomaticCandidateColors();
        }
        
        // Update the global color schemes
        COLOR_SCHEMES.leading_candidate = { ...this.candidateColors };
        
        console.log('[CandidateManager] Built candidate color scheme:', this.candidateColors);
        
        this.eventBus.emit('candidates:colorsBuilt', {
            colors: this.candidateColors,
            source: geoJsonData.metadata?.candidate_colors ? 'metadata' : 'automatic'
        });
        
        return this.candidateColors;
    }
    
    /**
     * Use candidate colors from metadata
     */
    useCandidateColorsFromMetadata(candidateColorsFromMetadata) {
        console.log('[CandidateManager] Using candidate colors from metadata');
        
        Object.keys(candidateColorsFromMetadata).forEach(candidateName => {
            // Filter out problematic entries
            if (!this.skipList.has(candidateName)) {
                const normalizedName = this.normalizeCandidateName(candidateName);
                this.candidateColors[normalizedName] = candidateColorsFromMetadata[candidateName];
            }
        });
    }
    
    /**
     * Generate automatic candidate colors
     */
    generateAutomaticCandidateColors() {
        console.log('[CandidateManager] Generating automatic candidate colors');
        
        this.colorIndex = 0;
        
        this.detectedCandidates.forEach(candidate => {
            if (this.isValidCandidateName(candidate)) {
                const normalizedName = this.normalizeCandidateName(candidate);
                
                if (!this.candidateColors[normalizedName]) {
                    this.candidateColors[normalizedName] = 
                        CANDIDATE_COLOR_PALETTE[this.colorIndex % CANDIDATE_COLOR_PALETTE.length];
                    this.colorIndex++;
                }
            }
        });
    }
    
    /**
     * Normalize candidate name for consistent lookup
     */
    normalizeCandidateName(name) {
        if (!name) return '';
        return name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
    }
    
    /**
     * Convert candidate name to display format
     */
    displayCandidateName(name) {
        if (!name) return '';
        return name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }
    
    /**
     * Get candidate color with fallback
     */
    getCandidateColor(candidateName, fallbackColor = '#cccccc') {
        if (!candidateName) return fallbackColor;
        
        const normalizedName = this.normalizeCandidateName(candidateName);
        
        // Try normalized name first
        if (this.candidateColors[normalizedName]) {
            return this.candidateColors[normalizedName];
        }
        
        // Try original name
        if (this.candidateColors[candidateName]) {
            return this.candidateColors[candidateName];
        }
        
        // Try global color schemes
        if (COLOR_SCHEMES.leading_candidate[normalizedName]) {
            return COLOR_SCHEMES.leading_candidate[normalizedName];
        }
        
        return fallbackColor;
    }
    
    /**
     * Get all candidate data for a specific candidate
     */
    getCandidateData(candidateName, properties) {
        const normalizedName = this.normalizeCandidateName(candidateName);
        
        return {
            name: candidateName,
            normalizedName,
            displayName: this.displayCandidateName(candidateName),
            color: this.getCandidateColor(candidateName),
            voteCount: properties[`votes_${candidateName}`] || 0,
            votePercentage: properties[`vote_pct_${candidateName}`] || 0,
            contribution: properties[`vote_pct_contribution_${candidateName}`] || 0
        };
    }
    
    /**
     * Get all candidates for a feature with their data
     */
    getFeatureCandidates(properties) {
        const candidates = [];
        
        this.detectedCandidates.forEach(candidateName => {
            const candidateData = this.getCandidateData(candidateName, properties);
            
            // Only include candidates with votes
            if (candidateData.voteCount > 0) {
                candidates.push(candidateData);
            }
        });
        
        // Sort by vote count (descending)
        candidates.sort((a, b) => b.voteCount - a.voteCount);
        
        return candidates;
    }
    
    /**
     * Create color gradient for candidate-specific fields
     */
    createCandidateGradient(candidateName, intensity) {
        const candidateColor = this.getCandidateColor(candidateName);
        
        if (candidateColor === '#cccccc') {
            // Fallback gradient
            return `hsl(220, 70%, ${90 - (intensity * 50)}%)`;
        }
        
        // Parse hex color
        const hex = candidateColor.replace('#', '');
        const r = parseInt(hex.substr(0, 2), 16);
        const g = parseInt(hex.substr(2, 2), 16);
        const b = parseInt(hex.substr(4, 2), 16);
        
        // Interpolate from white to candidate color
        const finalR = Math.round(255 + (r - 255) * intensity);
        const finalG = Math.round(255 + (g - 255) * intensity);
        const finalB = Math.round(255 + (b - 255) * intensity);
        
        return `rgb(${finalR}, ${finalG}, ${finalB})`;
    }
    
    /**
     * Get candidate statistics for the dataset
     */
    getCandidateStats(geoJsonData) {
        const stats = {
            totalCandidates: this.detectedCandidates.length,
            candidatesWithColors: 0,
            averageVotesPerCandidate: {},
            topPerformers: []
        };
        
        // Count candidates with assigned colors
        stats.candidatesWithColors = Object.keys(this.candidateColors).length - 3; // Exclude Tie, No Data, No Election Data
        
        // Calculate average votes per candidate
        if (geoJsonData.features.length > 0) {
            this.detectedCandidates.forEach(candidate => {
                const voteField = `votes_${candidate}`;
                const votes = geoJsonData.features
                    .map(f => f.properties[voteField] || 0)
                    .filter(v => v > 0);
                
                if (votes.length > 0) {
                    stats.averageVotesPerCandidate[candidate] = {
                        average: votes.reduce((a, b) => a + b, 0) / votes.length,
                        total: votes.reduce((a, b) => a + b, 0),
                        precincts: votes.length
                    };
                }
            });
            
            // Find top performers
            stats.topPerformers = Object.entries(stats.averageVotesPerCandidate)
                .sort((a, b) => b[1].total - a[1].total)
                .slice(0, 3)
                .map(([name, data]) => ({
                    name,
                    displayName: this.displayCandidateName(name),
                    totalVotes: data.total,
                    averageVotes: Math.round(data.average),
                    precincts: data.precincts
                }));
        }
        
        return stats;
    }
    
    /**
     * Clear candidate data
     */
    reset() {
        this.detectedCandidates = [];
        this.candidateColors = {};
        this.colorIndex = 0;
        
        console.log('[CandidateManager] Reset candidate data');
    }
    
    /**
     * Get current candidate information
     */
    getCandidateInfo() {
        return {
            detectedCandidates: [...this.detectedCandidates],
            candidateColors: { ...this.candidateColors },
            count: this.detectedCandidates.length
        };
    }
} 