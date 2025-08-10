#!/usr/bin/env python3
"""
Setup Script for Advanced Data Engineering Assignment
Installs dependencies and configures the environment.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def install_python_packages():
    """Install required Python packages."""
    packages = [
        "pandas>=2.0.0",
        "pyspark>=3.5.0", 
        "pyarrow>=14.0.0",
        "google-cloud-storage>=2.10.0",
        "google-cloud-bigquery>=3.13.0",
        "google-auth>=2.23.0",
        "prefect>=2.14.0",
        "requests>=2.31.0",
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
        "python-dotenv>=1.0.0",
        "tqdm>=4.66.0"
    ]
    
    for package in packages:
        if not run_command(f"pip install {package}", f"Installing {package}"):
            return False
    return True

def create_directories():
    """Create necessary directories."""
    directories = [
        "data/bronze",
        "data/silver", 
        "data/gold",
        "logs",
        "temp",
        "viz/output"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"📁 Created directory: {directory}")
    
    return True

def generate_diagrams():
    """Generate required diagrams."""
    print("🎨 Generating assignment diagrams...")
    try:
        # Install matplotlib if not already installed
        subprocess.run("pip install matplotlib", shell=True, check=True)
        
        # Run diagram generation
        result = subprocess.run("python diagrams/generate_diagrams.py", shell=True, check=True)
        print("✅ Diagrams generated successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Diagram generation failed: {e}")
        return False

def setup_gcp():
    """Setup GCP configuration."""
    print("☁️  Setting up Google Cloud Platform...")
    
    # Check if gcloud is installed
    try:
        result = subprocess.run("gcloud --version", shell=True, capture_output=True, text=True)
        print("✅ Google Cloud CLI is installed")
    except FileNotFoundError:
        print("⚠️  Google Cloud CLI not found. Please install it manually:")
        print("   brew install google-cloud-sdk")
        print("   or visit: https://cloud.google.com/sdk/docs/install")
        return False
    
    # Check if authenticated
    try:
        result = subprocess.run("gcloud auth list", shell=True, capture_output=True, text=True)
        if "ACTIVE" in result.stdout:
            print("✅ GCP authentication active")
        else:
            print("⚠️  Please authenticate with GCP:")
            print("   gcloud auth login")
            print("   gcloud config set project ade-adveng-assign")
        return True
    except Exception as e:
        print(f"❌ GCP setup failed: {e}")
        return False

def create_env_file():
    """Create environment configuration file."""
    env_content = """# Advanced Data Engineering Assignment Environment Variables

# GCP Configuration
GCP_PROJECT_ID=ade-adveng-assign
GCP_REGION=us-central1

# NASA API Configuration
NASA_API_KEY=DEMO_KEY  # Replace with your actual API key

# Data Processing Configuration
IMDB_DOWNLOAD_DIR=data/raw
BATCH_SIZE=10000
MAX_WORKERS=4

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/pipeline.log
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print("📝 Created .env file with configuration")
    return True

def main():
    """Main setup function."""
    print("🚀 Advanced Data Engineering Assignment Setup")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required. Current version:", sys.version)
        return False
    
    print(f"✅ Python version: {sys.version}")
    
    # Create virtual environment if not exists
    if not os.path.exists("venv"):
        print("🔄 Creating virtual environment...")
        if not run_command("python3 -m venv venv", "Creating virtual environment"):
            return False
    
    # Activate virtual environment
    print("🔄 Activating virtual environment...")
    if os.name == 'nt':  # Windows
        activate_script = "venv\\Scripts\\activate"
    else:  # Unix/Linux/macOS
        activate_script = "source venv/bin/activate"
    
    print(f"📝 To activate virtual environment, run: {activate_script}")
    
    # Install packages
    if not install_python_packages():
        return False
    
    # Create directories
    if not create_directories():
        return False
    
    # Generate diagrams
    if not generate_diagrams():
        return False
    
    # Setup GCP
    setup_gcp()
    
    # Create environment file
    create_env_file()
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Activate virtual environment: source venv/bin/activate")
    print("2. Set your NASA API key in .env file")
    print("3. Configure GCP authentication: gcloud auth login")
    print("4. Run the pipeline: python orchestration/main.py")
    print("\n📚 For more information, see README.md")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)



