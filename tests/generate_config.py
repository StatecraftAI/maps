#!/usr/bin/env python3
"""
Generate config.js from environment variables.
This keeps secrets in .env and generates the config file for the HTML.
"""

import os
from pathlib import Path

def load_env_file(env_path):
    """Load environment variables from .env file."""
    env_vars = {}
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    return env_vars

def generate_config_js():
    """Generate config.js from environment variables."""
    # Look for .env file in current directory or parent
    env_path = Path('.env')
    if not env_path.exists():
        env_path = Path('../.env')
    
    if not env_path.exists():
        print("❌ No .env file found. Please create one from .env_template")
        return False
    
    env_vars = load_env_file(env_path)
    
    # Get required variables
    supabase_url = env_vars.get('SUPABASE_MAPS_URL', '')
    supabase_key = env_vars.get('SUPABASE_MAPS_ANON_KEY', '')
    
    if not supabase_url or not supabase_key:
        print("❌ Missing required environment variables:")
        print("   SUPABASE_MAPS_URL and SUPABASE_MAPS_ANON_KEY")
        return False
    
    # Handle 1Password references
    if supabase_url.startswith('op://'):
        print("⚠️  SUPABASE_MAPS_URL contains 1Password reference.")
        print("   Please run: op inject -i .env -o .env.local")
        print("   Then use .env.local instead")
        return False
        
    if supabase_key.startswith('op://'):
        print("⚠️  SUPABASE_MAPS_ANON_KEY contains 1Password reference.")
        print("   Please run: op inject -i .env -o .env.local")
        print("   Then use .env.local instead")
        return False
    
    # Generate config.js
    config_content = f"""// Generated from environment variables - DO NOT EDIT MANUALLY
// Run: python generate_config.py to regenerate this file
const SUPABASE_CONFIG = {{
    url: '{supabase_url}',
    anonKey: '{supabase_key}'
}};
"""
    
    config_path = Path('config.js')
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    print(f"✅ Generated {config_path}")
    print("   Config file created successfully!")
    return True

if __name__ == '__main__':
    generate_config_js() 