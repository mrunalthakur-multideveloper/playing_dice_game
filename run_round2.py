#!/usr/bin/env python3
"""
Standalone runner for Round 2 Scraping Pipeline.
Scrapes and verifies candidate jobs using CRM active-domains technical keywords
and the strict 5-Keyword Job Description Verification Rule.
"""
import sys
import asyncio
from round2_scraper import run_round2

if __name__ == "__main__":
    try:
        asyncio.run(run_round2())
    except RuntimeError as e:
        if "running event loop" in str(e).lower() or "event loop is already running" in str(e).lower():
            try:
                import nest_asyncio
                nest_asyncio.apply()
            except ImportError:
                pass
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run_round2())
        else:
            raise
