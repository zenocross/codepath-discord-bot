"""
GitLab RSS Discord Bot

A Discord bot that monitors GitLab RSS feeds for new issues and supports
scheduled announcements to channel groups.

Usage:
    python app.py

Environment Variables:
    DISCORD_BOT_TOKEN: Your Discord bot token (required)
    BOT_OWNER_ID: Your Discord user ID for admin permissions (optional)
    
    NOTION_ENABLED: Set to 'true' to enable Notion integration (optional, default: false)
    NOTION_TOKEN: Your Notion integration token (required if NOTION_ENABLED=true)
    NOTION_DATABASE_ID: Your Notion database ID (required if NOTION_ENABLED=true)
"""

from bot.config import Config
from bot.client import GitLabRSSBot


def main() -> None:
    """Main entry point for the bot."""
    # Validate configuration
    if not Config.validate():
        exit(1)
    
    # Create and run the bot
    bot = GitLabRSSBot()
    bot.run(Config.DISCORD_TOKEN)


if __name__ == '__main__':
    main()
