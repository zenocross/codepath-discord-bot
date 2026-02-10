"""Announcement commands module (Cog)."""

import uuid
from typing import TYPE_CHECKING

import discord
from discord.ext import commands

from bot.config import Config
from services.scheduler_service import SchedulerService
from utils.embeds import EmbedBuilder
from utils.time_utils import format_time_until, parse_time_string, parse_day_of_week

if TYPE_CHECKING:
    from bot.client import GitLabRSSBot


class AnnouncementsCog(commands.Cog, name="Announcements"):
    """Commands for managing announcements and scheduled messages."""
    
    def __init__(self, bot: 'GitLabRSSBot'):
        self.bot = bot
    
    def _check_dm_permission(self, ctx: commands.Context) -> bool:
        """Check if user is allowed to use announce commands."""
        return self.bot.is_user_allowed(ctx.author.id)
    
    # ==================== User Management ====================
    
    @commands.command(name='users')
    async def manage_users(self, ctx: commands.Context, action: str = None, user_id: str = None) -> None:
        """Manage allowed users (owner only).
        
        Usage:
        !announce users - List allowed users
        !announce users add <user_id> - Add a user
        !announce users remove <user_id> - Remove a user
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
            await ctx.send("Usage: `!announce users [add|remove] [user_id]`")
    
    # ==================== Channel Group Management ====================
    
    @commands.command(name='group')
    async def manage_group(self, ctx: commands.Context, action: str = None, group_name: str = None, channel_arg: str = None) -> None:
        """Manage channel groups.
        
        Usage:
        !announce group create <name> - Create a new group
        !announce group delete <name> - Delete a group
        !announce group add <name> <channel_id> - Add channel to group
        !announce group remove <name> <channel_id> - Remove channel from group
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if action == 'create' and group_name:
            if group_name in self.bot.channel_groups:
                await ctx.send(f"❌ Group `{group_name}` already exists.")
                return
            self.bot.channel_groups[group_name] = []
            self.bot.save_channel_groups()
            await ctx.send(f"✅ Created group `{group_name}`")
        
        elif action == 'delete' and group_name:
            if group_name not in self.bot.channel_groups:
                await ctx.send(f"❌ Group `{group_name}` doesn't exist.")
                return
            del self.bot.channel_groups[group_name]
            self.bot.save_channel_groups()
            await ctx.send(f"✅ Deleted group `{group_name}`")
        
        elif action == 'add' and group_name and channel_arg:
            if group_name not in self.bot.channel_groups:
                await ctx.send(f"❌ Group `{group_name}` doesn't exist. Create it first.")
                return
            try:
                channel_id = int(channel_arg.strip('<>#'))
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    await ctx.send(f"⚠️ Channel `{channel_id}` not found. Adding anyway (bot may not have access).")
                if channel_id not in self.bot.channel_groups[group_name]:
                    self.bot.channel_groups[group_name].append(channel_id)
                    self.bot.save_channel_groups()
                    channel_name = channel.name if channel else "unknown"
                    await ctx.send(f"✅ Added #{channel_name} (`{channel_id}`) to group `{group_name}`")
                else:
                    await ctx.send(f"ℹ️ Channel already in group `{group_name}`")
            except ValueError:
                await ctx.send("❌ Invalid channel ID.")
        
        elif action == 'remove' and group_name and channel_arg:
            if group_name not in self.bot.channel_groups:
                await ctx.send(f"❌ Group `{group_name}` doesn't exist.")
                return
            try:
                channel_id = int(channel_arg.strip('<>#'))
                if channel_id in self.bot.channel_groups[group_name]:
                    self.bot.channel_groups[group_name].remove(channel_id)
                    self.bot.save_channel_groups()
                    await ctx.send(f"✅ Removed channel `{channel_id}` from group `{group_name}`")
                else:
                    await ctx.send(f"ℹ️ Channel not in group `{group_name}`")
            except ValueError:
                await ctx.send("❌ Invalid channel ID.")
        else:
            await ctx.send("Usage: `!announce group <create|delete|add|remove> <name> [channel_id]`")
    
    @commands.command(name='groups')
    async def list_groups(self, ctx: commands.Context) -> None:
        """List all channel groups."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not self.bot.channel_groups:
            await ctx.send("📋 **Channel Groups:** None configured\n\nUse `!announce group create <name>` to create one.")
            return
        
        embed = EmbedBuilder.channel_groups_embed(
            self.bot.channel_groups,
            self.bot.get_channel
        )
        await ctx.send(embed=embed)
    
    # ==================== DM Group Management ====================
    
    @commands.command(name='dmgroup')
    async def manage_dmgroup(self, ctx: commands.Context, action: str = None, group_name: str = None, username: str = None) -> None:
        """Manage DM groups for sending direct messages.
        
        Usage:
        !announce dmgroup create <name> - Create a new DM group
        !announce dmgroup delete <name> - Delete a DM group
        !announce dmgroup add <name> <username> - Add user to group (by username or user ID)
        !announce dmgroup remove <name> <username> - Remove user from group
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if action == 'create' and group_name:
            if group_name in self.bot.dm_groups:
                await ctx.send(f"❌ DM group `{group_name}` already exists.")
                return
            self.bot.dm_groups[group_name] = []
            self.bot.save_dm_groups()
            await ctx.send(f"✅ Created DM group `{group_name}`")
        
        elif action == 'delete' and group_name:
            if group_name not in self.bot.dm_groups:
                await ctx.send(f"❌ DM group `{group_name}` doesn't exist.")
                return
            del self.bot.dm_groups[group_name]
            self.bot.save_dm_groups()
            await ctx.send(f"✅ Deleted DM group `{group_name}`")
        
        elif action == 'add' and group_name and username:
            if group_name not in self.bot.dm_groups:
                await ctx.send(f"❌ DM group `{group_name}` doesn't exist. Create it first.")
                return
            
            # Find the user by username
            await ctx.send(f"🔍 Searching for user `{username}`...")
            user = await self.bot.find_user_by_username(username)
            
            if not user:
                await ctx.send(
                    f"❌ User `{username}` not found in any server where the bot is installed.\n"
                    f"💡 Tip: The user must be in a server with the bot. You can also use their user ID directly."
                )
                return
            
            # Check if user already in group
            existing_ids = [u.get('user_id') for u in self.bot.dm_groups[group_name]]
            if user.id in existing_ids:
                await ctx.send(f"ℹ️ User `{user.name}` is already in DM group `{group_name}`")
                return
            
            self.bot.dm_groups[group_name].append({
                'user_id': user.id,
                'username': user.name
            })
            self.bot.save_dm_groups()
            await ctx.send(f"✅ Added **{user.name}** (`{user.id}`) to DM group `{group_name}`")
        
        elif action == 'remove' and group_name and username:
            if group_name not in self.bot.dm_groups:
                await ctx.send(f"❌ DM group `{group_name}` doesn't exist.")
                return
            
            # First try to find the user via Discord lookup (same as add)
            user = await self.bot.find_user_by_username(username)
            
            found_idx = None
            if user:
                # Found via Discord - search by user ID
                for idx, user_data in enumerate(self.bot.dm_groups[group_name]):
                    if user_data.get('user_id') == user.id:
                        found_idx = idx
                        break
            
            # Fallback: search by stored username or ID directly (for users no longer in any server)
            if found_idx is None:
                for idx, user_data in enumerate(self.bot.dm_groups[group_name]):
                    if (user_data.get('username', '').lower() == username.lower() or 
                        str(user_data.get('user_id')) == username):
                        found_idx = idx
                        break
            
            if found_idx is not None:
                removed = self.bot.dm_groups[group_name].pop(found_idx)
                self.bot.save_dm_groups()
                await ctx.send(f"✅ Removed **{removed.get('username')}** from DM group `{group_name}`")
            else:
                await ctx.send(f"ℹ️ User `{username}` not found in DM group `{group_name}`")
        
        else:
            await ctx.send(
                "Usage:\n"
                "`!announce dmgroup create <name>` - Create a DM group\n"
                "`!announce dmgroup delete <name>` - Delete a DM group\n"
                "`!announce dmgroup add <name> <username>` - Add user (by username or user ID)\n"
                "`!announce dmgroup remove <name> <username>` - Remove user"
            )
    
    @commands.command(name='dmgroups')
    async def list_dmgroups(self, ctx: commands.Context) -> None:
        """List all DM groups."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not self.bot.dm_groups:
            await ctx.send("📋 **DM Groups:** None configured\n\nUse `!announce dmgroup create <name>` to create one.")
            return
        
        embed = EmbedBuilder.dm_groups_embed(self.bot.dm_groups)
        await ctx.send(embed=embed)
    
    # ==================== Scheduling ====================
    
    def _resolve_group(self, group_name: str) -> tuple[str | None, str | None]:
        """Resolve a group name to its type.
        
        Returns:
            Tuple of (target_type, error_message)
            target_type is 'channel' or 'dm', error_message is set if group not found
        """
        in_channel = group_name in self.bot.channel_groups
        in_dm = group_name in self.bot.dm_groups
        
        if in_channel and in_dm:
            # Prefer channel group if name exists in both (edge case)
            return 'channel', None
        elif in_channel:
            return 'channel', None
        elif in_dm:
            return 'dm', None
        else:
            return None, f"Group `{group_name}` not found in channel groups or DM groups."
    
    def _parse_schedule_config(self, schedule_type: str, args: tuple) -> tuple[dict | None, str | None, str | None]:
        """Parse schedule configuration from arguments.
        
        Returns:
            Tuple of (config, message, error_message)
        """
        config = {}
        message = None
        
        if schedule_type == 'minutely':
            if not args:
                return None, None, "Please specify minutes: `!announce schedule <group> minutely <N> [message]`"
            try:
                minutes = int(args[0])
                if minutes < 1 or minutes > 1440:
                    return None, None, "Minutes must be between 1 and 1440 (24 hours)"
                config['minutes'] = minutes
                message = ' '.join(args[1:]) if len(args) > 1 else None
            except ValueError:
                return None, None, "Invalid minutes value"
        
        elif schedule_type == 'hourly':
            if not args:
                return None, None, "Please specify hours: `!announce schedule <group> hourly <N> [message]`"
            try:
                hours = int(args[0])
                if hours < 1 or hours > 168:
                    return None, None, "Hours must be between 1 and 168 (1 week)"
                config['hours'] = hours
                message = ' '.join(args[1:]) if len(args) > 1 else None
            except ValueError:
                return None, None, "Invalid hours value"
        
        elif schedule_type == 'daily':
            if not args:
                return None, None, "Please specify time: `!announce schedule <group> daily <HH:MM> [message]`"
            try:
                hour, minute = parse_time_string(args[0])
                config['hour'] = hour
                config['minute'] = minute
                message = ' '.join(args[1:]) if len(args) > 1 else None
            except (ValueError, IndexError):
                return None, None, "Invalid time format. Use HH:MM (e.g., 09:00)"
        
        elif schedule_type == 'weekly':
            if len(args) < 2:
                return None, None, "Please specify day and time: `!announce schedule <group> weekly <day> <HH:MM> [message]`"
            try:
                config['day'] = parse_day_of_week(args[0])
                hour, minute = parse_time_string(args[1])
                config['hour'] = hour
                config['minute'] = minute
                message = ' '.join(args[2:]) if len(args) > 2 else None
            except ValueError as e:
                return None, None, str(e)
        else:
            return None, None, "Invalid schedule type. Use: minutely, hourly, daily, or weekly"
        
        return config, message, None
    
    @commands.command(name='schedule')
    async def schedule_message(self, ctx: commands.Context, group_name: str = None, schedule_type: str = None, *args) -> None:
        """Schedule a recurring message to a channel group or DM group.
        
        Usage:
        !announce schedule <group> minutely <N> [message] - Every N minutes (1-1440)
        !announce schedule <group> hourly <N> [message] - Every N hours
        !announce schedule <group> daily <HH:MM> [message] - Daily at time (GMT)
        !announce schedule <group> weekly <day> <HH:MM> [message] - Weekly (day: mon/tue/wed/thu/fri/sat/sun)
        
        The group can be either a channel group or DM group - automatically detected.
        If message is not provided, you'll be prompted for it.
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not group_name or not schedule_type:
            await ctx.send(
                "Usage:\n"
                "`!announce schedule <group> minutely <N> [message]` - Every N minutes\n"
                "`!announce schedule <group> hourly <N> [message]` - Every N hours\n"
                "`!announce schedule <group> daily <HH:MM> [message]` - Daily at time (GMT)\n"
                "`!announce schedule <group> weekly <day> <HH:MM> [message]` - Weekly\n\n"
                "💡 Group can be a channel group or DM group (auto-detected)"
            )
            return
        
        # Auto-detect group type
        target_type, error = self._resolve_group(group_name)
        if error:
            await ctx.send(f"❌ {error}\n💡 Create it first with `!announce group create` or `!announce dmgroup create`")
            return
        
        # Parse schedule configuration
        config, message, parse_error = self._parse_schedule_config(schedule_type, args)
        if parse_error:
            await ctx.send(f"❌ {parse_error}")
            return
        
        # Generate schedule ID
        schedule_id = str(uuid.uuid4())[:8]
        
        # If no message provided, prompt for it
        if not message:
            self.bot.dm_conversations[ctx.author.id] = {
                'state': 'awaiting_schedule_message',
                'data': {
                    'schedule_id': schedule_id,
                    'group': group_name,
                    'type': schedule_type,
                    'config': config,
                    'target_type': target_type
                }
            }
            group_type_label = "DM group" if target_type == 'dm' else "channel group"
            await ctx.send(
                f"📝 Please send the message you want to schedule for {group_type_label} `{group_name}`:\n"
                f"(Just type your message and send it)"
            )
            return
        
        # Create the schedule
        next_run = SchedulerService.calculate_next_run(schedule_type, config)
        self.bot.scheduled_messages[schedule_id] = {
            'group': group_name,
            'type': schedule_type,
            'config': config,
            'message': message,
            'next_run': next_run,
            'active': True,
            'created_by': ctx.author.id,
            'target_type': target_type
        }
        self.bot.save_scheduled_messages()
        
        # Build confirmation message based on type
        if target_type == 'dm':
            user_count = len(self.bot.dm_groups.get(group_name, []))
            target_info = f"DM Group: `{group_name}` ({user_count} users)"
            icon = "📬"
        else:
            channel_count = len(self.bot.channel_groups.get(group_name, []))
            target_info = f"Channel Group: `{group_name}` ({channel_count} channels)"
            icon = "📢"
        
        time_until = format_time_until(next_run)
        await ctx.send(
            f"✅ **{icon} Schedule Created!**\n"
            f"• ID: `{schedule_id}`\n"
            f"• {target_info}\n"
            f"• Type: {schedule_type}\n"
            f"• Next send: {next_run.strftime('%Y-%m-%d %H:%M')} GMT ({time_until})\n"
            f"• Message preview: {message[:100]}{'...' if len(message) > 100 else ''}"
        )
    
    @commands.command(name='schedules')
    async def list_schedules(self, ctx: commands.Context) -> None:
        """List all scheduled messages."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not self.bot.scheduled_messages:
            await ctx.send("📋 **Scheduled Messages:** None configured\n\nUse `!announce schedule` to create one.")
            return
        
        embed = EmbedBuilder.schedules_list_embed(
            self.bot.scheduled_messages,
            SchedulerService.format_schedule_frequency_short
        )
        await ctx.send(embed=embed)
    
    @commands.command(name='preview')
    async def preview_schedule(self, ctx: commands.Context, schedule_id: str = None) -> None:
        """Preview a scheduled message and time until sent."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not schedule_id:
            await ctx.send("Usage: `!announce preview <schedule_id>`")
            return
        
        if schedule_id not in self.bot.scheduled_messages:
            await ctx.send(f"❌ Schedule `{schedule_id}` not found.")
            return
        
        sched = self.bot.scheduled_messages[schedule_id]
        group_name = sched.get('group', 'unknown')
        channel_count = len(self.bot.channel_groups.get(group_name, []))
        
        embed = EmbedBuilder.schedule_preview_embed(
            schedule_id,
            sched,
            channel_count,
            SchedulerService.format_schedule_frequency
        )
        await ctx.send(embed=embed)
    
    @commands.command(name='cancel')
    async def cancel_schedule(self, ctx: commands.Context, schedule_id: str = None) -> None:
        """Cancel a scheduled message."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not schedule_id:
            await ctx.send("Usage: `!announce cancel <schedule_id>`")
            return
        
        if schedule_id not in self.bot.scheduled_messages:
            await ctx.send(f"❌ Schedule `{schedule_id}` not found.")
            return
        
        del self.bot.scheduled_messages[schedule_id]
        self.bot.save_scheduled_messages()
        await ctx.send(f"✅ Cancelled schedule `{schedule_id}`")
    
    @commands.command(name='cancelall')
    async def cancel_all_schedules(self, ctx: commands.Context) -> None:
        """Cancel all scheduled messages."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not self.bot.scheduled_messages:
            await ctx.send("ℹ️ No scheduled messages to cancel.")
            return
        
        count = len(self.bot.scheduled_messages)
        self.bot.scheduled_messages.clear()
        self.bot.save_scheduled_messages()
        await ctx.send(f"✅ Cancelled all **{count}** scheduled message(s).")
    
    # ==================== Immediate Send ====================
    
    @commands.command(name='send')
    async def send_now(self, ctx: commands.Context, target: str = None, *, message: str = None) -> None:
        """Send an immediate message to a group (channel or DM), channel, or user.
        
        Usage:
        !announce send <group_name> <message> - Send to channel group or DM group (auto-detected)
        !announce send <channel_id> <message> - Send to a specific channel (18+ digit ID)
        !announce send <user_id> <message> - Send DM to a specific user (18+ digit ID, use dm: prefix)
        
        To disambiguate between channel and user IDs, use:
        !announce send dm:<user_id> <message> - Explicitly send as DM
        !announce send ch:<channel_id> <message> - Explicitly send to channel
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not target:
            await ctx.send(
                "Usage:\n"
                "`!announce send <group_name> <message>` - Send to group (channel or DM, auto-detected)\n"
                "`!announce send <channel_id> <message>` - Send to specific channel\n"
                "`!announce send dm:<user_id> <message>` - Send DM to specific user"
            )
            return
        
        # Check for explicit prefixes
        if target.startswith('dm:'):
            user_id_str = target[3:]
            if user_id_str.isdigit():
                await self._send_dm_to_user(ctx, int(user_id_str), message)
            else:
                await ctx.send("❌ Invalid user ID after dm: prefix")
            return
        
        if target.startswith('ch:'):
            channel_id_str = target[3:]
            if channel_id_str.isdigit():
                await self._send_to_channel(ctx, int(channel_id_str), message)
            else:
                await ctx.send("❌ Invalid channel ID after ch: prefix")
            return
        
        # Auto-detect: numeric = channel ID, otherwise check groups
        if target.isdigit():
            # Numeric - treat as channel ID
            await self._send_to_channel(ctx, int(target), message)
        else:
            # Group name - auto-detect type
            await self._send_to_group(ctx, target, message)
    
    async def _send_to_channel(self, ctx: commands.Context, channel_id: int, message: str = None) -> None:
        """Send message to a specific channel."""
        channel = self.bot.get_channel(channel_id)
        
        if not channel:
            await ctx.send(f"❌ Channel `{channel_id}` not found or bot doesn't have access.")
            return
        
        if not message:
            self.bot.dm_conversations[ctx.author.id] = {
                'state': 'awaiting_direct_message',
                'data': {'channel_id': channel_id, 'channel_name': channel.name}
            }
            await ctx.send(f"📝 Please send the message you want to send to #{channel.name}:")
            return
        
        try:
            await channel.send(message)
            await ctx.send(f"✅ Message sent to #{channel.name}!")
        except Exception as e:
            await ctx.send(f"❌ Failed to send: {e}")
    
    async def _send_dm_to_user(self, ctx: commands.Context, user_id: int, message: str = None) -> None:
        """Send DM to a specific user."""
        if not message:
            self.bot.dm_conversations[ctx.author.id] = {
                'state': 'awaiting_dm_user_message',
                'data': {'user_id': user_id}
            }
            await ctx.send(f"📝 Please send the message you want to DM to user `{user_id}`:")
            return
        
        success, error = await self.bot.send_dm_to_user(user_id, message)
        if success:
            await ctx.send(f"✅ DM sent to user `{user_id}`!")
        else:
            await ctx.send(f"❌ Failed to send DM: {error}")
    
    async def _send_to_group(self, ctx: commands.Context, group_name: str, message: str = None) -> None:
        """Send message to all targets in a group (channel or DM group)."""
        # Auto-detect group type
        target_type, error = self._resolve_group(group_name)
        
        if error:
            await ctx.send(f"❌ {error}")
            return
        
        if target_type == 'dm':
            await self._send_to_dm_group(ctx, group_name, message)
        else:
            await self._send_to_channel_group(ctx, group_name, message)
    
    async def _send_to_channel_group(self, ctx: commands.Context, group_name: str, message: str = None) -> None:
        """Send message to all channels in a channel group."""
        if not message:
            self.bot.dm_conversations[ctx.author.id] = {
                'state': 'awaiting_broadcast_message',
                'data': {'group': group_name, 'target_type': 'channel'}
            }
            await ctx.send(f"📝 Please send the message you want to broadcast to channel group `{group_name}`:")
            return
        
        channel_ids = self.bot.channel_groups[group_name]
        if not channel_ids:
            await ctx.send(f"❌ Channel group `{group_name}` has no channels.")
            return
        
        sent_count = 0
        failed_count = 0
        
        await ctx.send(f"📤 Broadcasting to {len(channel_ids)} channels...")
        
        for channel_id in channel_ids:
            try:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    await channel.send(message)
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"Error sending to channel {channel_id}: {e}")
                failed_count += 1
        
        await ctx.send(f"✅ **Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}")
    
    async def _send_to_dm_group(self, ctx: commands.Context, group_name: str, message: str = None) -> None:
        """Send DM to all users in a DM group."""
        if not message:
            self.bot.dm_conversations[ctx.author.id] = {
                'state': 'awaiting_dm_group_message',
                'data': {'group': group_name}
            }
            await ctx.send(f"📝 Please send the message you want to DM to group `{group_name}`:")
            return
        
        users = self.bot.dm_groups[group_name]
        if not users:
            await ctx.send(f"❌ DM group `{group_name}` has no users.")
            return
        
        sent_count = 0
        failed_count = 0
        failed_users = []
        
        await ctx.send(f"📤 Sending DMs to {len(users)} users...")
        
        for user_data in users:
            user_id = user_data.get('user_id')
            username = user_data.get('username', 'Unknown')
            if user_id:
                success, error = await self.bot.send_dm_to_user(user_id, message)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                    failed_users.append(f"{username}: {error}")
        
        result_msg = f"✅ **DM Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}"
        if failed_users and len(failed_users) <= 5:
            result_msg += f"\n\n**Failed:**\n" + "\n".join(f"• {u}" for u in failed_users)
        await ctx.send(result_msg)


async def setup(bot: 'GitLabRSSBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(AnnouncementsCog(bot))

