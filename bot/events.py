"""Bot event handlers module."""

from datetime import datetime
from typing import TYPE_CHECKING

import discord
from discord.ext import commands

from bot.config import Config
from utils.embeds import EmbedBuilder

if TYPE_CHECKING:
    from bot.client import GitLabRSSBot


class EventsCog(commands.Cog, name="Events"):
    """Handles bot events like on_ready and help command."""
    
    def __init__(self, bot: 'GitLabRSSBot'):
        self.bot = bot
    
    @commands.Cog.listener()
    async def on_ready(self) -> None:
        """Called when the bot is ready and connected."""
        print(f'Logged in as {self.bot.user.name} ({self.bot.user.id})')
        print('------')
        
        # Only auto-subscribe to issue-feed if NO subscriptions exist at all
        # This ensures user-configured channels aren't overridden
        if not self.bot.subscriptions:
            for guild in self.bot.guilds:
                channel = discord.utils.get(guild.text_channels, name=Config.AUTO_SUBSCRIBE_CHANNEL_NAME)
                if channel:
                    self.bot.subscriptions[channel.id] = {
                        'url': Config.AUTO_SUBSCRIBE_RSS_URL,
                        'labels': Config.AUTO_SUBSCRIBE_LABELS.copy(),
                        'last_checked': datetime.now()
                    }
                    self.bot.seen_issues[channel.id] = set()
                    self.bot.save_subscriptions()
                    print(f'[GitLab] Auto-subscribed #{Config.AUTO_SUBSCRIBE_CHANNEL_NAME} in {guild.name}')
                    print(f'[GitLab] Filtering for labels: {", ".join(sorted(Config.AUTO_SUBSCRIBE_LABELS))}')
        else:
            print(f'[GitLab] Existing subscriptions found, skipping auto-subscribe to #{Config.AUTO_SUBSCRIBE_CHANNEL_NAME}')
        
        print(f'[GitLab] Monitoring {len(self.bot.subscriptions)} RSS feed(s)')
        print(f'[Announce] {len(self.bot.channel_groups)} channel group(s)')
        print(f'[Announce] {len(self.bot.scheduled_messages)} scheduled message(s)')
        print(f'[Announce] {len(self.bot.allowed_users)} allowed user(s)')
        if Config.BOT_OWNER_ID:
            print(f'[Announce] Bot owner ID: {Config.BOT_OWNER_ID}')
        else:
            print(f'[Announce] ⚠️ BOT_OWNER_ID not set in .env!')
        print('------')
    
    # ==================== Help Command ====================
    
    @commands.command(name='help')
    async def help_command(self, ctx: commands.Context) -> None:
        """Show help information - based on which prefix was used."""
        # Check which prefix was used to determine which help to show
        if ctx.prefix == '!announce ':
            embed = EmbedBuilder.announcement_help_embed()
        elif ctx.prefix == '!gitlab ':
            embed = EmbedBuilder.gitlab_help_embed()
        else:
            # Fallback: DMs default to announce, channels default to gitlab
            if isinstance(ctx.channel, discord.DMChannel):
                embed = EmbedBuilder.announcement_help_embed()
            else:
                embed = EmbedBuilder.gitlab_help_embed()
        
        await ctx.send(embed=embed)


async def setup(bot: 'GitLabRSSBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(EventsCog(bot))
