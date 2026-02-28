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


async def setup(bot: 'DiscordBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(AppCog(bot))



