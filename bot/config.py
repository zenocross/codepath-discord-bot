"""Configuration constants and environment variable loading."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration loaded from environment variables."""
    
    # Discord settings
    DISCORD_TOKEN: str = os.getenv('DISCORD_BOT_TOKEN', '')
    BOT_OWNER_ID: int = int(os.getenv('BOT_OWNER_ID', '0'))
    
    # Notion settings
    NOTION_TOKEN: str = os.getenv('NOTION_TOKEN', '')
    NOTION_DATABASE_ID: str = os.getenv('NOTION_DATABASE_ID', '')
    NOTION_ENABLED: bool = os.getenv('NOTION_ENABLED', 'false').lower() == 'true'
    
    # Timing intervals
    CHECK_INTERVAL_MINUTES: int = 5
    ANNOUNCEMENT_CHECK_INTERVAL_SECONDS: int = 60  # Check every minute, aligned to :00
    
    # Auto-subscription defaults
    AUTO_SUBSCRIBE_RSS_URL: str = (
        "https://gitlab.com/gitlab-org/gitlab/-/work_items.atom"
        "?sort=created_date&state=opened&first_page_size=100"
    )
    AUTO_SUBSCRIBE_CHANNEL_NAME: str = "issue-feed"
    AUTO_SUBSCRIBE_LABELS: set = {
        "backend",
        "frontend",
        "documentation",
        "type::bug",
        "type::feature",
        "type::maintenance",
        "quick-win",
        "quick-win::first-time-contributor",
        "community-bonus::100",
        "community-bonus::200",
        "community-bonus::300",
        "community-bonus::500",
        "co-create"
    }
    
    # File paths for persistence
    SUBSCRIPTIONS_FILE: str = 'subscriptions.json'
    CHANNEL_GROUPS_FILE: str = 'channel_groups.json'
    DM_GROUPS_FILE: str = 'dm_groups.json'
    SCHEDULED_MESSAGES_FILE: str = 'scheduled_messages.json'
    ALLOWED_USERS_FILE: str = 'allowed_users.json'
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that required configuration is present."""
        if not cls.DISCORD_TOKEN:
            print("Error: DISCORD_BOT_TOKEN environment variable not set")
            return False
        
        # Warn if Notion is enabled but not configured
        if cls.NOTION_ENABLED:
            if not cls.NOTION_TOKEN:
                print("Warning: NOTION_ENABLED is true but NOTION_TOKEN is not set")
            if not cls.NOTION_DATABASE_ID:
                print("Warning: NOTION_ENABLED is true but NOTION_DATABASE_ID is not set")
        
        return True

