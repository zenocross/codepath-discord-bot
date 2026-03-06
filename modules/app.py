"""App-level commands module (Cog)."""

from typing import TYPE_CHECKING

import discord
from discord.ext import commands

from bot.config import Config

if TYPE_CHECKING:
    from bot.client import DiscordBot


class AppCog(commands.Cog, name="App"):
    """App-level commands including user management."""
    
    def __init__(self, bot: 'DiscordBot'):
        self.bot = bot
    
    # ==================== User Management ====================
    
    @commands.command(name='users')
    async def manage_users(self, ctx: commands.Context, action: str = None, user_id: str = None) -> None:
        """Manage allowed users (owner only).
        
        Usage:
        !app users - List allowed users
        !app users add <user_id> - Add a user
        !app users remove <user_id> - Remove a user
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        # Only bot owner can manage users
        if ctx.author.id != Config.BOT_OWNER_ID:
            await ctx.send("❌ Only the bot owner can manage allowed users.")
            return
        
        if action is None:
            # List users
            if not self.bot.allowed_users:
                await ctx.send("📋 **Allowed Users:** None configured")
                return
            
            user_list = []
            for uid in self.bot.allowed_users:
                try:
                    user = await self.bot.fetch_user(uid)
                    user_list.append(f"• {user.name} (`{uid}`)")
                except:
                    user_list.append(f"• Unknown (`{uid}`)")
            
            await ctx.send(f"📋 **Allowed Users:**\n" + "\n".join(user_list))
        
        elif action == 'add' and user_id:
            try:
                uid = int(user_id)
                self.bot.allowed_users.add(uid)
                self.bot.save_allowed_users()
                await ctx.send(f"✅ Added user `{uid}` to allowed users.")
            except ValueError:
                await ctx.send("❌ Invalid user ID. Must be a number.")
        
        elif action == 'remove' and user_id:
            try:
                uid = int(user_id)
                if uid == Config.BOT_OWNER_ID:
                    await ctx.send("❌ Cannot remove the bot owner.")
                    return
                self.bot.allowed_users.discard(uid)
                self.bot.save_allowed_users()
                await ctx.send(f"✅ Removed user `{uid}` from allowed users.")
            except ValueError:
                await ctx.send("❌ Invalid user ID. Must be a number.")
        else:
            await ctx.send("Usage: `!app users [add|remove] [user_id]`")
    
    # ==================== DM Feed Management ====================
    
    @commands.command(name='set_feed')
    async def set_feed(self, ctx: commands.Context, channel_input: str = None) -> None:
        """Set the channel where DMs from non-allowed users are forwarded.
        
        Usage: 
        !app set_feed #channel-name (in server)
        !app set_feed <channel_id> (anywhere)
        """
        # Only bot owner can set feed
        if ctx.author.id != Config.BOT_OWNER_ID:
            await ctx.send("❌ Only the bot owner can set the DM feed channel.")
            return
        
        if channel_input is None:
            await ctx.send("Usage: `!app set_feed #channel-name` or `!app set_feed <channel_id>`")
            return
        
        # Try to resolve the channel
        channel = None
        
        # Check if it's a channel mention like <#123456789>
        import re
        mention_match = re.match(r'<#(\d+)>', channel_input)
        if mention_match:
            channel_id = int(mention_match.group(1))
            channel = self.bot.get_channel(channel_id)
        # Check if it's a raw channel ID
        elif channel_input.isdigit():
            channel_id = int(channel_input)
            channel = self.bot.get_channel(channel_id)
        # Try to find by name if in a guild context
        elif ctx.guild:
            channel = discord.utils.get(ctx.guild.text_channels, name=channel_input.lstrip('#'))
        
        if channel is None:
            await ctx.send(f"❌ Channel not found. Use a channel mention like `#channel-name` or a channel ID.")
            return
        
        if not isinstance(channel, discord.TextChannel):
            await ctx.send(f"❌ That's not a text channel.")
            return
        
        # Verify bot can send messages to the channel
        if not channel.permissions_for(channel.guild.me).send_messages:
            await ctx.send(f"❌ I don't have permission to send messages in {channel.mention}")
            return
        
        self.bot.dm_feed_channel_id = channel.id
        self.bot.save_dm_feed_channel()
        
        await ctx.send(f"✅ DM feed channel set to {channel.mention}\nDMs from users not in `!app users` will now be forwarded there.")
    
    @commands.command(name='clear_feed')
    async def clear_feed(self, ctx: commands.Context) -> None:
        """Remove the DM feed channel (stops forwarding DMs).
        
        Usage: !app clear_feed
        """
        # Only bot owner can clear feed
        if ctx.author.id != Config.BOT_OWNER_ID:
            await ctx.send("❌ Only the bot owner can clear the DM feed channel.")
            return
        
        if self.bot.dm_feed_channel_id is None:
            await ctx.send("ℹ️ No DM feed channel is currently set.")
            return
        
        self.bot.dm_feed_channel_id = None
        self.bot.save_dm_feed_channel()
        
        await ctx.send("✅ DM feed channel cleared. DMs from non-allowed users will no longer be forwarded.")
    
    @commands.command(name='feed')
    async def show_feed(self, ctx: commands.Context) -> None:
        """Show the current DM feed channel.
        
        Usage: !app feed
        """
        # Only bot owner can check feed
        if ctx.author.id != Config.BOT_OWNER_ID:
            await ctx.send("❌ Only the bot owner can check the DM feed channel.")
            return
        
        if self.bot.dm_feed_channel_id is None:
            await ctx.send("📭 **DM Feed:** Not configured\nUse `!app set_feed #channel` to set one.")
            return
        
        channel = self.bot.get_channel(self.bot.dm_feed_channel_id)
        if channel:
            await ctx.send(f"📬 **DM Feed:** {channel.mention} (`{channel.id}`)")
        else:
            await ctx.send(f"⚠️ **DM Feed:** Channel ID `{self.bot.dm_feed_channel_id}` is set but not accessible.\nUse `!app set_feed #channel` to update or `!app clear_feed` to remove.")


async def setup(bot: 'DiscordBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(AppCog(bot))






