#!/usr/bin/env python3
"""
Google Colab Automated Runner for Dice Scraper Two-Tier Pipeline.
Automatically installs dependencies, sets up .env with Neon DB, CRM & active Webshare proxy,
and starts Round 1 followed by Round 2 with zero duplicate storage.
"""

import os
import sys
import subprocess

# 1. Ensure required dependencies are installed
required_packages = [
    "httpx",
    "beautifulsoup4",
    "pandas",
    "python-dotenv",
    "psycopg2-binary",
    "nest_asyncio"
]

print("📦 Checking and installing dependencies for Google Colab...")
for pkg in required_packages:
    module_name = "dotenv" if pkg == "python-dotenv" else (
        "bs4" if pkg == "beautifulsoup4" else (
            "psycopg2" if pkg == "psycopg2-binary" else pkg
        )
    )
    try:
        __import__(module_name)
    except ImportError:
        print(f"   Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

print("✅ Dependencies are ready!")

# 2. Automatically generate .env file with active credentials
_script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(_script_dir, ".env")

ENV_CONFIG = """# ==============================================================================
# Upstream CRM Configuration
# ==============================================================================
CRM_BACKEND_URL=https://api.applyus.org
INTERNAL_SERVICE_API_KEY=my_secure_applyus_secret_2026

# ==============================================================================
# Neon PostgreSQL Destination Database
# ==============================================================================
NEON_DATABASE_URL=postgresql://neondb_owner:npg_ND7pS0dReFCJ@ep-damp-waterfall-b5noll06-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require
NEON_TABLE=links

# ==============================================================================
# Scraper Settings
# ==============================================================================
MAX_JOBS=150
MAX_JOBS_PER_KEYWORD=150
MAX_MEMBERS=15
ONLY_LAST_24_HOURS=true
ROUND2_MAX_JOBS_PER_DOMAIN=50

# ==============================================================================
# Active Webshare Rotating Proxy Configuration
# ==============================================================================
WEBSHARE_PROXY=http://ltrabmxv-rotate:ayq2lqvdcey2@p.webshare.io:80
WEBSHARE_PROXIES=http://ltrabmxv-rotate:ayq2lqvdcey2@p.webshare.io:80
PROXY_URL=http://ltrabmxv-rotate:ayq2lqvdcey2@p.webshare.io:80
"""

if not os.path.exists(env_path):
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(ENV_CONFIG.strip())
    print("✅ Created .env configuration file automatically.")
else:
    # Ensure active proxy is written into .env
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(ENV_CONFIG.strip())
    print("✅ Verified and updated .env configuration.")

# 3. Setup environment & Event Loop for Colab
from dotenv import load_dotenv
load_dotenv(env_path, override=True)

import nest_asyncio
nest_asyncio.apply()

import asyncio
import main

if __name__ == "__main__":
    print("\n🚀 Starting Two-Tier Pipeline in Google Colab...")
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main.main())
    except RuntimeError as e:
        if "event loop is already running" in str(e).lower():
            asyncio.run(main.main())
        else:
            raise
