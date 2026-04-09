#!/usr/bin/env python3
"""
AQI.in Location Discovery Job.

This job parses AQI.in sitemaps to find all Vietnam air quality dashboard URLs.
It filters for URLs starting with https://www.aqi.in/vi/dashboard/vietnam/.

Usage:
    python jobs/aqiin/discover_locations.py
"""

import os
import sys
import logging
import requests
import xml.etree.ElementTree as ET
import json
from typing import List

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import JobLogger

def discover_vietnam_locations(sitemap_indices: List[str]) -> List[str]:
    """
    Parse sitemaps and return a list of Vietnam dashboard URLs.
    """
    logger = logging.getLogger(__name__)
    vietnam_urls = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    for sitemap_url in sitemap_indices:
        logger.info(f"Parsing sitemap: {sitemap_url}")
        try:
            response = requests.get(sitemap_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse XML
            root = ET.fromstring(response.content)
            
            # Sitemap namespace
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            for url_elem in root.findall('ns:url', ns):
                loc_elem = url_elem.find('ns:loc', ns)
                if loc_elem is not None:
                    url = loc_elem.text
                    if url and "aqi.in/vi/dashboard/vietnam/" in url:
                        slug = url.split('dashboard/')[-1].strip('/')
                        vietnam_urls.append(slug)
                        
        except Exception as e:
            logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
            
    return sorted(list(set(vietnam_urls)))

def main():
    """Main entry point."""
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    
    with JobLogger("discover_aqiin_locations", source="aqiin", level=log_level) as logger:
        logger.info("Starting AQI.in location discovery")
        
        index_url = "https://www.aqi.in/vi/sitemap-index.xml"
        logger.info(f"Fetching sitemap index: {index_url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            response = requests.get(index_url, headers=headers, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            aqi_sitemaps = []
            for sitemap_elem in root.findall('ns:sitemap', ns):
                loc_elem = sitemap_elem.find('ns:loc', ns)
                if loc_elem is not None:
                    loc = loc_elem.text
                    if "sitemap-aqi-" in loc:
                        aqi_sitemaps.append(loc)
            
            logger.info(f"Found {len(aqi_sitemaps)} AQI sitemaps")
            
            # Discover locations
            vietnam_slugs = discover_vietnam_locations(aqi_sitemaps)
            logger.info(f"Discovered {len(vietnam_slugs)} Vietnam locations")
            
            # Save to text file
            output_file = os.path.join(os.path.dirname(__file__), "vietnam_locations.txt")
            with open(output_file, 'w', encoding='utf-8') as f:
                for slug in vietnam_slugs:
                    f.write(f"{slug}\n")
            
            logger.info(f"Saved {len(vietnam_slugs)} locations to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to complete discovery: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
