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
    
    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError) -> None:
        """Global error handler - suppress expected errors."""
        if isinstance(error, commands.CommandNotFound):
            # Suppress CommandNotFound for !report <student_id> pattern
            # The on_message listener in report.py handles these
            if ctx.prefix == '!report ':
                content_after_prefix = ctx.message.content[8:].strip()
                if content_after_prefix:
                    first_arg = content_after_prefix.split()[0]
                    if first_arg.isdigit():
                        return
            # Let other CommandNotFound errors pass silently (or log if needed)
            return
        
        # Re-raise other errors so they get logged
        raise error
    
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
        elif ctx.prefix == '!checkin ':
            await ctx.send(embed=EmbedBuilder.checkin_help_embed())
        elif ctx.prefix == '!report ':
            await ctx.send(embed=EmbedBuilder.report_help_embed())
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
