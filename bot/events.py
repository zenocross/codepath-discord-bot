"""Bot event handlers module."""

from typing import TYPE_CHECKING

import discord
from discord.ext import commands

from bot.config import Config
from utils.embeds import EmbedBuilder

if TYPE_CHECKING:
    from bot.client import DiscordBot


class EventsCog(commands.Cog, name="Events"):
    """Handles bot events like on_ready and help command."""
    
    def __init__(self, bot: 'DiscordBot'):
        self.bot = bot
    
    @commands.Cog.listener()
    async def on_ready(self) -> None:
        """Called when the bot is ready and connected."""
        print(f'Logged in as {self.bot.user.name} ({self.bot.user.id})')
        print('------')
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
            await ctx.send(embed=embed)
        elif ctx.prefix == '!tracker ':
            embed = EmbedBuilder.tracker_help_embed()
            await ctx.send(embed=embed)
        elif ctx.prefix == '!game ':
            embed = EmbedBuilder.game_help_embed()
            await ctx.send(embed=embed)
        elif ctx.prefix == '!app ':
            await ctx.send(embed=EmbedBuilder.app_help_embed())
        else:
            # Fallback: DMs default to announce, channels default to app overview
            if isinstance(ctx.channel, discord.DMChannel):
                embed = EmbedBuilder.announcement_help_embed()
            else:
                embed = EmbedBuilder.app_help_embed()
            await ctx.send(embed=embed)


async def setup(bot: 'DiscordBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(EventsCog(bot))
