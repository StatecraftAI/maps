/**
 * Supabase Configuration
 * 
 * Configuration for connecting to Supabase PostGIS backend.
 * Secrets should be injected via environment variables or build process.
 */

/**
 * Get Supabase configuration
 * @returns {Object|null} Supabase config or null if not available
 */
function getSupabaseConfig() {
  const supabaseUrl = 'https://kwkmhuzratvywntytfnv.supabase.co';
  const supabaseAnonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt3a21odXpyYXR2eXdudHl0Zm52Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDgyOTc3OTIsImV4cCI6MjA2Mzg3Mzc5Mn0._qQ_B6hXUEU7P0A7nmnBQ5ZMzu9qd2AhFVDINgpzZMg';
  if (!supabaseUrl || !supabaseAnonKey) {
    console.error('🔒 [Supabase] Supabase URL or Anon Key are not set.');
    return null;
  }

  return { url: supabaseUrl, anonKey: supabaseAnonKey };
}

/**
 * Initialize Supabase client
 * @returns {Object|null} Supabase client instance or null if not available
 */
export function initializeSupabaseClient() {
  try {
    // Check if Supabase is available
    if (typeof window === 'undefined' || !window.supabase) {
      console.warn('[Supabase] Supabase client library not loaded');
      return null;
    }

    // Get configuration
    const config = getSupabaseConfig();
    if (!config) {
      console.warn('[Supabase] No configuration available');
      return null;
    }

    // Create client
    const client = window.supabase.createClient(config.url, config.anonKey);
    
    console.log('✅ [Supabase] Client initialized successfully');
    return client;
  } catch (error) {
    console.error('[Supabase] Failed to initialize client:', error);
    return null;
  }
}

/**
 * Test Supabase connection by trying to query a known table
 * @param {Object} client - Supabase client instance
 * @returns {Promise<boolean>} True if connection is successful
 */
export async function testSupabaseConnection(client) {
  try {
    // Test with a simple query to a table we know exists
    const { data, error } = await client
      .from('election_results_zone1')
      .select('precinct')
      .limit(1);

    if (error) {
      // If zone1 doesn't exist, try other known tables
      const { data: data2, error: error2 } = await client
        .from('election_results_zone4')
        .select('precinct')
        .limit(1);
      
      if (error2) {
        console.warn('[Supabase] Connection test failed - no election tables found:', error2.message);
        return false;
      }
    }

    console.log('✅ [Supabase] Connection test successful');
    return true;
  } catch (error) {
    console.warn('[Supabase] Connection test error:', error);
    return false;
  }
}

/**
 * Get available configuration for debugging
 * @returns {Object} Configuration status
 */
export function getConfigStatus() {
  const config = getSupabaseConfig();
  return {
    hasConfig: !!config,
    hasUrl: !!(config?.url),
    hasAnonKey: !!(config?.anonKey),
    source: config ? (window.SUPABASE_CONFIG ? 'injected' : 'fallback') : 'none',
    environment: window.location.hostname
  };
} 