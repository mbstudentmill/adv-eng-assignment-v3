#!/usr/bin/env python3
"""
Secure Configuration Loader
Handles environment variables and API keys safely
"""

import os
from pathlib import Path
from typing import Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    """Secure configuration management for the application."""
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        self._load_env_file()
        self._validate_config()
    
    def _load_env_file(self):
        """Load environment variables from .env file if it exists."""
        env_file = Path(__file__).parent / '.env'
        if env_file.exists():
            logger.info("Loading configuration from .env file")
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
        else:
            logger.warning(".env file not found. Using system environment variables.")
    
    def _validate_config(self):
        """Validate that required configuration is present."""
        required_vars = ['NASA_API_KEY']
        missing_vars = []
        
        for var in required_vars:
            if not self.get(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            logger.error("Please set these variables in your .env file or system environment")
            raise ValueError(f"Missing required configuration: {missing_vars}")
    
    def get(self, key: str, default: Optional[str] = None) -> str:
        """Get configuration value safely."""
        return os.environ.get(key, default)
    
    @property
    def nasa_api_key(self) -> str:
        """Get NASA API key securely."""
        key = self.get('NASA_API_KEY')
        if not key:
            raise ValueError("NASA_API_KEY not found in environment variables")
        return key
    
    @property
    def nasa_api_base_url(self) -> str:
        """Get NASA API base URL."""
        return self.get('NASA_API_BASE_URL', 'https://api.nasa.gov/planetary')
    
    @property
    def debug(self) -> bool:
        """Get debug mode setting."""
        return self.get('DEBUG', 'False').lower() == 'true'
    
    @property
    def log_level(self) -> str:
        """Get log level setting."""
        return self.get('LOG_LEVEL', 'INFO')


# Global configuration instance
config = Config()


def get_config() -> Config:
    """Get the global configuration instance."""
    return config


# Security check - ensure we're not accidentally exposing keys
if __name__ == "__main__":
    # Test configuration loading
    try:
        cfg = get_config()
        print("✅ Configuration loaded successfully")
        print(f"NASA API Base URL: {cfg.nasa_api_base_url}")
        print(f"Debug Mode: {cfg.debug}")
        print(f"Log Level: {cfg.log_level}")
        # Don't print the actual API key for security
        print(f"NASA API Key: {'*' * 8}... (hidden for security)")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("Please check your .env file or environment variables")
