"""RSS service for fetching and parsing GitLab RSS feeds."""

import re
from typing import Dict, List, Tuple

import aiohttp
import feedparser


class RSSService:
    """Handles RSS feed fetching and parsing operations."""
    
    @staticmethod
    async def fetch_feed_with_labels(url: str) -> Tuple[feedparser.FeedParserDict, Dict[str, List[str]]]:
        """Fetch feed and parse labels from raw XML.
        
        Args:
            url: The RSS feed URL to fetch
            
        Returns:
            Tuple of (parsed feed, labels_map dict mapping issue_id to labels list)
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                raw_xml = await response.text()
        
        # Parse with feedparser for entry metadata
        feed = feedparser.parse(raw_xml)
        
        # Parse raw XML to extract labels using regex (more reliable than namespace handling)
        labels_map = RSSService._extract_labels_from_xml(raw_xml)
        
        return feed, labels_map
    
    @staticmethod
    def _extract_labels_from_xml(raw_xml: str) -> Dict[str, List[str]]:
        """Extract labels from raw XML content.
        
        Args:
            raw_xml: Raw XML string from the feed
            
        Returns:
            Dictionary mapping issue IDs to their labels
        """
        labels_map: Dict[str, List[str]] = {}
        
        # Regex patterns for parsing
        entry_pattern = re.compile(r'<entry>(.*?)</entry>', re.DOTALL)
        id_pattern = re.compile(r'<id>([^<]+)</id>')
        labels_pattern = re.compile(r'<labels>(.*?)</labels>', re.DOTALL)
        label_pattern = re.compile(r'<label>([^<]+)</label>')
        
        for entry_match in entry_pattern.finditer(raw_xml):
            entry_xml = entry_match.group(1)
            
            # Extract issue ID
            id_match = id_pattern.search(entry_xml)
            if id_match:
                issue_id = id_match.group(1)
                labels = []
                
                # Extract labels container
                labels_match = labels_pattern.search(entry_xml)
                if labels_match:
                    labels_xml = labels_match.group(1)
                    labels = label_pattern.findall(labels_xml)
                
                labels_map[issue_id] = labels
        
        return labels_map
    
    @staticmethod
    async def fetch_raw_feed(url: str) -> str:
        """Fetch raw XML content from a feed URL.
        
        Args:
            url: The RSS feed URL to fetch
            
        Returns:
            Raw XML string
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()
    
    @staticmethod
    def validate_feed(rss_url: str) -> bool:
        """Validate that a URL is a valid RSS feed.
        
        Args:
            rss_url: URL to validate
            
        Returns:
            True if valid feed, False otherwise
        """
        try:
            feed = feedparser.parse(rss_url)
            return bool(feed.entries or feed.get('feed'))
        except Exception:
            return False
    
    @staticmethod
    def extract_labels_from_entry(entry) -> List[str]:
        """Extract labels from a feedparser entry.
        
        Args:
            entry: A feedparser entry object
            
        Returns:
            List of label strings
        """
        labels = []
        
        # GitLab RSS feeds include labels in tags
        if hasattr(entry, 'tags'):
            for tag in entry.tags:
                labels.append(tag.term)
        
        # GitLab work_items Atom feed has labels in a different format
        # Parse from the raw XML content if available
        if hasattr(entry, 'content'):
            for content in entry.content:
                content_value = content.get('value', '')
                # Look for label patterns in content
                label_matches = re.findall(r'<label>([^<]+)</label>', content_value)
                labels.extend(label_matches)
        
        # Check summary/description for labels
        summary = entry.get('summary', '') + entry.get('description', '')
        
        # Parse <label> tags from summary
        label_matches = re.findall(r'<label>([^<]+)</label>', summary)
        labels.extend(label_matches)
        
        # Parse labels formatted as ~label
        label_matches = re.findall(r'~([^\s~]+)', summary)
        labels.extend(label_matches)
        
        # Deduplicate
        return list(set(labels))

