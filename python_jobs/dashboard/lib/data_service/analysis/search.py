"""Search integration using DuckDuckGo Search API."""
import logging
from typing import List, Dict
from duckduckgo_search import DDGS

logger = logging.getLogger(__name__)

def execute_search_queries(queries: List[str], max_per_query: int = 2) -> List[Dict[str, str]]:
    """Execute search queries using DuckDuckGo to obtain relevant news/media references."""
    results = []
    try:
        with DDGS() as ddgs:
            for query in queries[:3]:  # Limit to top 3 queries to save time and bandwidth
                try:
                    hits = list(ddgs.text(query, max_results=max_per_query, region="vn-vi"))
                    for hit in hits:
                        results.append({
                            "query": query,
                            "title": hit.get("title", ""),
                            "snippet": hit.get("body", ""),
                            "url": hit.get("href", ""),
                        })
                except Exception as query_ex:
                    logger.warning(f"Failed query search '{query}': {query_ex}")
    except Exception as e:
        logger.warning(f"DuckDuckGo search initialization failed: {e}")
    return results[:8]  # Cap total results at 8 items
