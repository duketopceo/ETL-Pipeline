"""
Configuration management for Google Cloud BigQuery integration
"""

import os
import json
from pathlib import Path

def load_bigquery_config(config_file='.bigquery_config.json'):
    """Load BigQuery configuration from file"""
    config_path = Path(config_file)
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return None
    return None

def save_bigquery_config(config, config_file='.bigquery_config.json'):
    """Save BigQuery configuration to file"""
    try:
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving config: {e}")
        return False

def setup_google_cloud_auth():
    """Check and guide user through Google Cloud authentication setup"""
    auth_methods = []
    
    # Check for service account key file
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        if os.path.exists(key_file):
            auth_methods.append(f"✅ Service account key file: {key_file}")
        else:
            auth_methods.append(f"❌ Service account key file not found: {key_file}")
    
    # Check for gcloud CLI authentication
    try:
        import subprocess
        result = subprocess.run(['gcloud', 'auth', 'list', '--format=json'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            accounts = json.loads(result.stdout)
            active_accounts = [acc for acc in accounts if acc.get('status') == 'ACTIVE']
            if active_accounts:
                auth_methods.append(f"✅ gcloud CLI authenticated as: {active_accounts[0].get('account')}")
            else:
                auth_methods.append("❌ gcloud CLI not authenticated")
        else:
            auth_methods.append("❌ gcloud CLI not available")
    except Exception:
        auth_methods.append("❌ gcloud CLI not available")
    
    # Check for Application Default Credentials
    try:
        from google.auth import default
        credentials, project = default()
        if credentials:
            auth_methods.append(f"✅ Application Default Credentials available (project: {project})")
    except Exception:
        auth_methods.append("❌ Application Default Credentials not available")
    
    return auth_methods

def get_setup_instructions():
    """Get setup instructions for Google Cloud integration"""
    return """
# Google Cloud BigQuery Setup Instructions

## Prerequisites
1. A Google Cloud Project with BigQuery API enabled
2. Proper authentication configured

## Authentication Setup (Choose one method)

### Method 1: Service Account Key File (Recommended for development)
1. Go to Google Cloud Console → IAM & Admin → Service Accounts
2. Create a new service account or select existing one
3. Add roles: BigQuery Admin, BigQuery Data Editor
4. Generate and download a JSON key file
5. Set environment variable: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"`

### Method 2: gcloud CLI Authentication
1. Install gcloud CLI: https://cloud.google.com/sdk/docs/install
2. Run: `gcloud auth login`
3. Run: `gcloud config set project YOUR_PROJECT_ID`

### Method 3: Application Default Credentials (for production)
1. On Google Cloud Platform (Compute Engine, Cloud Run, etc.)
2. Assign appropriate service account to the resource
3. No additional setup needed

## Required Permissions
Your service account needs these BigQuery permissions:
- `bigquery.datasets.create`
- `bigquery.datasets.get`
- `bigquery.tables.create`
- `bigquery.tables.updateData`
- `bigquery.jobs.create`

## Testing Connection
Run the authentication check in the notebook to verify setup.
"""