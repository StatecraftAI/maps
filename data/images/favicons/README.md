# Favicon Assets

This directory contains all favicon and app icon assets following industry standard naming conventions.

## Required Favicon Files

### Core Favicons

- `favicon.ico` - Classic ICO format (16x16, 32x32, 48x48 multi-size)
- `favicon-16x16.png` - Small favicon for browser tabs
- `favicon-32x32.png` - Standard favicon for browser tabs

### Apple/iOS Icons

- `apple-touch-icon.png` - 180x180px for iOS home screen
- `apple-touch-icon-precomposed.png` - 180x180px (optional, for older iOS)

### Android/Chrome Icons

- `android-chrome-192x192.png` - 192x192px for Android home screen
- `android-chrome-512x512.png` - 512x512px for Android splash screen

### Windows/Microsoft Icons

- `mstile-150x150.png` - 150x150px for Windows Start Menu
- `mstile-310x310.png` - 310x310px large Windows tile (optional)

### Web App Manifest

- `site.webmanifest` - PWA manifest file

### Additional Formats

- `safari-pinned-tab.svg` - Monochrome SVG for Safari pinned tabs
- `browserconfig.xml` - Configuration for Windows tiles (optional)

## Current Implementation

The HTML already references:

- favicon.ico
- favicon-16x16.png
- favicon-32x32.png
- apple-touch-icon.png

Additional files can be added to enhance cross-platform support.
