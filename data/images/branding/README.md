# Branding Assets

This directory contains the logo and branding assets for the Statecraft.ai election mapping project.

## Recommended Assets

### Primary Logo

- `logo.svg` - Primary logo (SVG preferred for scalability)
- `logo.png` - Primary logo (PNG fallback, 512x512px recommended)
- `logo-white.svg` - White version for dark backgrounds
- `logo-dark.svg` - Dark version for light backgrounds

### Wordmark/Text Logo

- `wordmark.svg` - Text-only logo
- `wordmark-white.svg` - White text version
- `wordmark-dark.svg` - Dark text version

### Usage

- The watermark on the map uses `logo.svg` or `logo.png`
- Recommended minimum size: 32x32px for map watermark
- Maximum size: 64x64px for map watermark to avoid obstruction

## Current Implementation

The map watermark pulls from `logo.svg` (with PNG fallback) and displays "Statecraft.ai" text alongside it.
