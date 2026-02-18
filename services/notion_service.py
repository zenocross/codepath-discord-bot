"""Notion service for creating pages from GitLab issues."""

import re
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Optional, List

import aiohttp

from bot.config import Config


class NotionService:
    """Handles Notion API operations for GitLab issues."""
    
    @staticmethod
    def _get_headers() -> Dict[str, str]:
        """Get Notion API headers."""
        return {
            "Authorization": f"Bearer {Config.NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2025-09-03"
        }
    
    @staticmethod
    async def fetch_gitlab_issue_data(issue_url: str) -> Optional[Dict]:
        """Fetch issue data from GitLab API.
        
        Args:
            issue_url: Full GitLab issue URL (e.g., https://gitlab.com/group/project/-/issues/123)
            
        Returns:
            Issue data dict from GitLab API, or None if fetch fails
        """
        # Parse the issue URL to extract project path and issue IID
        match = re.match(r'https://gitlab\.com/(.+?)/-/(?:issues|work_items)/(\d+)', issue_url)
        if not match:
            return None
        
        project_path, issue_iid = match.groups()
        project_path_encoded = urllib.parse.quote(project_path, safe='')
        api_url = f"https://gitlab.com/api/v4/projects/{project_path_encoded}/issues/{issue_iid}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        return await response.json()
        except Exception as e:
            print(f"Error fetching GitLab issue data for {issue_url}: {e}")
        
        return None
    
    @staticmethod
    async def create_issue_page(issue_data: Dict) -> bool:
        """Create a Notion page from GitLab issue data.
        
        Args:
            issue_data: GitLab issue data dict with keys:
                - title: Issue title
                - web_url: Issue URL
                - author: Dict with 'username' key
                - labels: List of label dicts with 'name' key
                - state: 'opened' or 'closed'
        
        Returns:
            True if page was created successfully, False otherwise
        """
        if not Config.NOTION_ENABLED or not Config.NOTION_TOKEN or not Config.NOTION_DATABASE_ID:
            return False
        
        title = issue_data.get('title', 'Untitled')
        web_url = issue_data.get('web_url', '')
        author = issue_data.get('author', {})
        author_username = author.get('username', '') if isinstance(author, dict) else str(author)
        labels = issue_data.get('labels', [])
        state = issue_data.get('state', 'opened')
        
        # Extract label names
        label_names = []
        if labels:
            for label in labels:
                if isinstance(label, dict):
                    label_names.append(label.get('name', ''))
                elif isinstance(label, str):
                    label_names.append(label)
        
        # Build Notion page properties
        properties = {
            "Issue Title": {"title": [{"text": {"content": title[:2000]}}]},
            "Issue Link": {"url": web_url},
            "Date Added": {"date": {"start": datetime.now(timezone.utc).isoformat()}},
            "Maintainer Username": {"rich_text": [{"text": {"content": author_username}}]},
            "Status": {"status": {"name": "Open" if state == "opened" else "Closed"}}
        }
        
        # Add labels if present
        if label_names:
            # Limit to 10 labels and truncate each to 100 chars (Notion limit)
            properties["Labels"] = {
                "multi_select": [{"name": label[:100]} for label in label_names[:10]]
            }
        
        url = "https://api.notion.com/v1/pages"
        payload = {
            "parent": {"type": "database_id", "database_id": Config.NOTION_DATABASE_ID},
            "properties": properties
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=NotionService._get_headers(),
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_text = await response.text()
                        print(f"Notion API error: {response.status} - {error_text}")
                        return False
        except Exception as e:
            print(f"Error creating Notion page: {e}")
            return False
    
    @staticmethod
    async def create_issue_page_from_rss_entry(
        entry,
        labels: List[str],
        issue_url: Optional[str] = None
    ) -> bool:
        """Create a Notion page from RSS feed entry data.
        
        This method tries to fetch full issue data from GitLab API first,
        but falls back to using RSS data if the API call fails.
        
        Args:
            entry: Feedparser entry object
            labels: List of label strings
            issue_url: Optional issue URL (if not provided, uses entry.link)
        
        Returns:
            True if page was created successfully, False otherwise
        """
        if not Config.NOTION_ENABLED:
            return False
        
        issue_url = issue_url or entry.get('link', '')
        if not issue_url:
            return False
        
        # Try to fetch full issue data from GitLab API
        issue_data = await NotionService.fetch_gitlab_issue_data(issue_url)
        
        if issue_data:
            # Use full API data
            return await NotionService.create_issue_page(issue_data)
        else:
            # Fallback to RSS data
            title = entry.get('title', 'Untitled')
            author = entry.get('author', 'Unknown')
            
            # Build minimal issue data from RSS
            rss_issue_data = {
                'title': title,
                'web_url': issue_url,
                'author': {'username': author} if isinstance(author, str) else author,
                'labels': [{'name': label} for label in labels],
                'state': 'opened'  # RSS feeds typically only show open issues
            }
            
            return await NotionService.create_issue_page(rss_issue_data)

