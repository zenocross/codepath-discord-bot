"""Announcement commands module (Cog)."""

import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Dict, List, Set

import discord
from discord.ext import commands

from bot.config import Config
from services.scheduler_service import SchedulerService
from services.file_processor import FileStorageService
from services.tracker_processor import TrackerDataProcessor
from utils.embeds import EmbedBuilder
from utils.time_utils import format_time_until, parse_time_string, parse_day_of_week

if TYPE_CHECKING:
    from bot.client import DiscordBot

# Valid intervention types for autogroup presets
VALID_INTERVENTION_TYPES = {
    'NO_SUBMISSIONS',
    'MISSING_ADMISSION_INFO',
    'MR_URL_MISMATCH',
    'COMMITS_NOT_OWNED',
    'README_NOT_OWNED',
    'README_NONEXISTENT',
    'README_LINK_MISSING',
    'MISSING_PREVIOUS_PHASE',
    'SKIPPED_PHASE',
    'UNEXPECTED_PHASE_CHANGE',
    'ISSUE_CHANGED',
    'INCORRECT_PHASE_URL',
    'MISSING_DELIVERABLES',
    'NO_ACTIVITY',
    'TIMELINE_COMPRESSED',
    'MEMBER_ID_MISMATCH',
    'INVALID_MEMBER_ID',
    'STALLED',
    'BLOCKED',
    'MISSING_SUNDAY',
    'MISSING_WEDNESDAY',
}

# Prefix for auto-generated DM groups
AUTO_GROUP_PREFIX = "auto_"


class AnnouncementsCog(commands.Cog, name="Announcements"):
    """Commands for managing announcements and scheduled messages."""
    
    def __init__(self, bot: 'DiscordBot'):
        self.bot = bot
        self.storage = FileStorageService()
        self.processor = TrackerDataProcessor()
    
    def _check_dm_permission(self, ctx: commands.Context) -> bool:
        """Check if user is allowed to use announce commands."""
        return self.bot.is_user_allowed(ctx.author.id)
    
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
        
        elif action == 'test' and group_name:
            # Test DM accessibility for all users in a group
            if group_name not in self.bot.dm_groups:
                await ctx.send(f"❌ DM group `{group_name}` doesn't exist.")
                return
            
            users = self.bot.dm_groups[group_name]
            if not users:
                await ctx.send(f"❌ DM group `{group_name}` has no users.")
                return
            
            await ctx.send(f"🔍 Testing DM accessibility for {len(users)} users in `{group_name}`...")
            
            can_dm = []
            cannot_dm = []
            
            for user_data in users:
                user_id = user_data.get('user_id')
                username = user_data.get('username', 'Unknown')
                name = user_data.get('name', '')
                display = f"{name} ({username})" if name else username
                
                if not user_id:
                    cannot_dm.append(f"{display} - no user ID")
                    continue
                
                try:
                    user = await self.bot.fetch_user(user_id)
                    if user:
                        # Try to create DM channel (doesn't send a message)
                        dm_channel = await user.create_dm()
                        if dm_channel:
                            can_dm.append(display)
                        else:
                            cannot_dm.append(f"{display} - couldn't create DM")
                    else:
                        cannot_dm.append(f"{display} - user not found")
                except discord.Forbidden:
                    cannot_dm.append(f"{display} - DMs disabled/blocked")
                except discord.NotFound:
                    cannot_dm.append(f"{display} - user not found")
                except Exception as e:
                    cannot_dm.append(f"{display} - error: {str(e)[:30]}")
            
            # Build response
            response = [f"**📬 DM Test Results for `{group_name}`**\n"]
            response.append(f"✅ **Can DM ({len(can_dm)}):**")
            if can_dm:
                for u in can_dm[:20]:
                    response.append(f"• {u}")
                if len(can_dm) > 20:
                    response.append(f"• ... and {len(can_dm) - 20} more")
            else:
                response.append("• None")
            
            response.append(f"\n❌ **Cannot DM ({len(cannot_dm)}):**")
            if cannot_dm:
                for u in cannot_dm[:20]:
                    response.append(f"• {u}")
                if len(cannot_dm) > 20:
                    response.append(f"• ... and {len(cannot_dm) - 20} more")
            else:
                response.append("• None")
            
            await ctx.send("\n".join(response))
        
        else:
            await ctx.send(
                "Usage:\n"
                "`!announce dmgroup create <name>` - Create a DM group\n"
                "`!announce dmgroup delete <name>` - Delete a DM group\n"
                "`!announce dmgroup add <name> <username>` - Add user (by username or user ID)\n"
                "`!announce dmgroup remove <name> <username>` - Remove user\n"
                "`!announce dmgroup test <name>` - Test DM accessibility"
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
    
    # ==================== Autogroup Commands ====================
    
    @commands.command(name='set_group')
    async def set_autogroup_preset(self, ctx: commands.Context, preset_name: str = None, *, intervention_types: str = None) -> None:
        """Create or update an autogroup preset based on intervention types.
        
        Usage: !announce set_group <name> <intervention_types>
        
        Intervention types (comma-separated):
        NO_SUBMISSIONS, MISSING_PREVIOUS_PHASE, SKIPPED_PHASE, STALLED, BLOCKED,
        MISSING_DELIVERABLES, NO_ACTIVITY, TIMELINE_COMPRESSED, INVALID_MEMBER_ID, etc.
        
        Example: !announce set_group critical NO_SUBMISSIONS,STALLED,BLOCKED
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not preset_name or not intervention_types:
            types_list = ", ".join(sorted(VALID_INTERVENTION_TYPES))
            await ctx.send(
                "**📋 Create Autogroup Preset**\n\n"
                "Usage: `!announce set_group <name> <intervention_types>`\n\n"
                "Example:\n"
                "• `!announce set_group critical NO_SUBMISSIONS,STALLED,BLOCKED`\n"
                "• `!announce set_group phase_issues MISSING_PREVIOUS_PHASE,SKIPPED_PHASE`\n\n"
                f"**Valid Intervention Types:**\n```\n{types_list}\n```"
            )
            return
        
        # Parse and validate intervention types
        types_list = [t.strip().upper() for t in intervention_types.split(',')]
        invalid_types = [t for t in types_list if t not in VALID_INTERVENTION_TYPES]
        
        if invalid_types:
            await ctx.send(
                f"❌ Invalid intervention types: `{', '.join(invalid_types)}`\n\n"
                f"Valid types: `{', '.join(sorted(VALID_INTERVENTION_TYPES))}`"
            )
            return
        
        # Save the preset
        created_by = f"{ctx.author.name}#{ctx.author.discriminator}" if ctx.author.discriminator != "0" else ctx.author.name
        self.storage.set_autogroup_preset(preset_name, types_list, created_by)
        
        await ctx.send(
            f"✅ **Autogroup Preset Created/Updated**\n"
            f"• Name: `{preset_name}`\n"
            f"• Intervention Types: `{', '.join(types_list)}`\n"
            f"• Created by: {created_by}\n\n"
            f"Run `!announce autogroup` to create DM groups from presets."
        )
    
    @commands.command(name='delete_preset')
    async def delete_autogroup_preset(self, ctx: commands.Context, preset_name: str = None) -> None:
        """Delete an autogroup preset.
        
        Usage: !announce delete_preset <name>
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        if not preset_name:
            await ctx.send("Usage: `!announce delete_preset <name>`")
            return
        
        if self.storage.delete_autogroup_preset(preset_name):
            await ctx.send(f"✅ Deleted preset `{preset_name}`")
        else:
            await ctx.send(f"❌ Preset `{preset_name}` not found.")
    
    @commands.command(name='presets')
    async def list_autogroup_presets(self, ctx: commands.Context) -> None:
        """List all autogroup presets."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        presets = self.storage.get_all_autogroup_presets()
        
        if not presets:
            await ctx.send(
                "📋 **Autogroup Presets:** None configured\n\n"
                "Use `!announce set_group <name> <intervention_types>` to create one."
            )
            return
        
        lines = ["**📋 Autogroup Presets**\n"]
        for name, data in sorted(presets.items()):
            types = data.get('intervention_types', [])
            created_by = data.get('created_by', 'Unknown')
            lines.append(f"• **{name}**: `{', '.join(types)}`\n  └ by {created_by}")
        
        await ctx.send("\n".join(lines))
    
    @commands.command(name='autogroup')
    async def autogroup(self, ctx: commands.Context) -> None:
        """Create DM groups automatically based on presets and current tracker data.
        
        This will:
        1. Clear all existing auto-generated DM groups (prefixed with 'auto_')
        2. Create phase-based groups (auto_phase_1, auto_phase_2, etc.)
        3. Create intervention-based groups from presets
        
        Only students with unresolved issues (not bypassed) are included.
        """
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        await ctx.send("🔄 Processing tracker data and creating autogroups...")
        
        # Load tracker data
        typeform_file = self.storage.get_file("typeform")
        if not typeform_file:
            await ctx.send("❌ No typeform data uploaded. Use `!tracker upload typeform` first.")
            return
        
        typeform_data = self.storage.read_file(typeform_file)
        master_file = self.storage.get_file("master")
        master_data = self.storage.read_file(master_file) if master_file else None
        
        # Get start_date and target_date (same as submissions_download)
        start_date = self.storage.get_start_date()
        target_date = self.storage.get_last_submissions_date()
        
        if not start_date:
            await ctx.send(
                "❌ **No start date set.**\n\n"
                "Set the program start date first using `!tracker start_date MM/DD/YYYY`."
            )
            return
        
        if not target_date:
            await ctx.send(
                "❌ **No submissions date set.**\n\n"
                "Run `!tracker submissions <DATE>` first to set the date filter."
            )
            return
        
        # Calculate current week (same logic as tracker)
        days_since_start = (target_date - start_date).days
        current_week = max(1, (days_since_start // 7) + 1)
        
        # Get phase completions and bypasses
        phase_completions = self.storage.get_all_phase_completions()
        bypasses = self.storage.get_all_bypasses()
        
        # Get app data for phone numbers
        app_file = self.storage.get_file("app")
        app_data = self.storage.read_file(app_file) if app_file else None
        
        # Process the data with full options (same as submissions_download)
        result = self.processor.process(
            typeform_data,
            options={
                'master_data': master_data,
                'app_data': app_data,
                'start_date': start_date,
                'target_date': target_date,
                'current_week': current_week,
                'filter_by_date': True,
                'phase_completions': phase_completions,
                'bypasses': bypasses
            }
        )
        
        if not result.success:
            await ctx.send(f"❌ Processing failed: {result.error_message}")
            return
        
        students = result.students
        if not students:
            await ctx.send("❌ No student data found in processing result.")
            return
        
        # Clear existing auto-generated groups
        auto_groups_removed = []
        groups_to_remove = [name for name in self.bot.dm_groups.keys() if name.startswith(AUTO_GROUP_PREFIX)]
        for group_name in groups_to_remove:
            del self.bot.dm_groups[group_name]
            auto_groups_removed.append(group_name)
        
        # Build lookup: discord_username -> user object
        discord_user_cache: Dict[str, discord.User] = {}
        
        async def find_user(discord_username: str) -> discord.User | None:
            """Find a user by Discord username."""
            if not discord_username:
                return None
            if discord_username in discord_user_cache:
                return discord_user_cache[discord_username]
            user = await self.bot.find_user_by_username(discord_username)
            if user:
                discord_user_cache[discord_username] = user
            return user
        
        # Build student data with user lookups
        # Group by member_id to get latest data per student
        student_data: Dict[str, dict] = {}
        for s in students:
            key = s.member_id or s.name
            if key not in student_data or s.week > student_data[key].get('week', 0):
                # Check if this submission is bypassed
                bypass_key = f"{s.member_id}:{s.submission_num}"
                is_bypassed = bypass_key in bypasses and bypasses[bypass_key].get('bypassed', False)
                
                student_data[key] = {
                    'name': s.name,
                    'member_id': s.member_id,
                    'discord_username': s.discord_username,
                    'phase': s.current_phase,
                    'intervention_type': s.intervention_type,
                    'grade_status': s.grade_status,
                    'week': s.week,
                    'bypassed': is_bypassed or s.intervention_type == 'BYPASSED'
                }
                
                # Debug: print specific student (search by member_id, name, or discord)
                search_term = 'mgadepalli'
                if (search_term in str(key).lower() or 
                    search_term in str(s.name).lower() or 
                    search_term in str(s.member_id).lower() or
                    search_term in str(s.discord_username).lower()):
                    print(f"[Autogroup Debug] {search_term}: member_id={s.member_id}, name={s.name}, week={s.week}, intervention={s.intervention_type}, phase={s.current_phase}, bypassed={is_bypassed}, discord={s.discord_username}")
        
        # Create phase-based groups
        phase_groups: Dict[str, List[dict]] = {
            f"{AUTO_GROUP_PREFIX}phase_1": [],
            f"{AUTO_GROUP_PREFIX}phase_2": [],
            f"{AUTO_GROUP_PREFIX}phase_3": [],
            f"{AUTO_GROUP_PREFIX}phase_4": [],
        }
        
        for key, data in student_data.items():
            phase = data.get('phase', '')
            if '1' in phase:
                phase_groups[f"{AUTO_GROUP_PREFIX}phase_1"].append(data)
            elif '2' in phase:
                phase_groups[f"{AUTO_GROUP_PREFIX}phase_2"].append(data)
            elif '3' in phase:
                phase_groups[f"{AUTO_GROUP_PREFIX}phase_3"].append(data)
            elif '4' in phase:
                phase_groups[f"{AUTO_GROUP_PREFIX}phase_4"].append(data)
        
        # Create intervention-based groups from presets
        presets = self.storage.get_all_autogroup_presets()
        preset_groups: Dict[str, List[dict]] = {}
        
        # Debug: collect all unique intervention types found
        all_intervention_types: Set[str] = set()
        for key, data in student_data.items():
            student_interventions = data.get('intervention_type', '')
            if student_interventions:
                student_types = set(student_interventions.replace('\n', ',').split(','))
                student_types = {t.strip().split(':')[0] for t in student_types if t.strip()}
                all_intervention_types.update(student_types)
        
        print(f"[Autogroup] Found intervention types: {all_intervention_types}")
        
        for preset_name, preset_data in presets.items():
            group_name = f"{AUTO_GROUP_PREFIX}{preset_name}"
            preset_groups[group_name] = []
            intervention_types = set(preset_data.get('intervention_types', []))
            
            for key, data in student_data.items():
                # Skip bypassed students
                if data.get('bypassed'):
                    continue
                
                # Check if student has any of the intervention types
                student_interventions = data.get('intervention_type', '')
                if student_interventions:
                    # Split by newline since multiple interventions can be combined
                    student_types = set(student_interventions.replace('\n', ',').split(','))
                    student_types = {t.strip().split(':')[0] for t in student_types if t.strip()}
                    
                    if intervention_types & student_types:
                        preset_groups[group_name].append(data)
                        print(f"[Autogroup] Matched {key} to {preset_name}: {student_types}")
        
        # Resolve Discord users and create DM groups
        groups_created = []
        users_added = 0
        users_not_found = 0
        users_not_found_list = []  # Track which users weren't found
        
        all_groups = {**phase_groups, **preset_groups}
        
        for group_name, members in all_groups.items():
            if not members:
                continue
            
            self.bot.dm_groups[group_name] = []
            
            for member in members:
                discord_username = member.get('discord_username', '')
                member_name = member.get('name', 'Unknown')
                member_id = member.get('member_id', '?')
                
                if not discord_username:
                    users_not_found += 1
                    not_found_msg = f"{member_name} ({member_id}) - no Discord username"
                    if not_found_msg not in users_not_found_list:
                        users_not_found_list.append(not_found_msg)
                    print(f"[Autogroup] No Discord username for {member_name} ({member_id}) in {group_name}")
                    continue
                
                user = await find_user(discord_username)
                if user:
                    self.bot.dm_groups[group_name].append({
                        'user_id': user.id,
                        'username': user.name,
                        'member_id': member_id,
                        'name': member_name
                    })
                    users_added += 1
                else:
                    users_not_found += 1
                    not_found_msg = f"{member_name} ({member_id}) - `{discord_username}`"
                    if not_found_msg not in users_not_found_list:
                        users_not_found_list.append(not_found_msg)
                    print(f"[Autogroup] Discord user not found: '{discord_username}' for {member_name} ({member_id}) in {group_name}")
            
            if self.bot.dm_groups[group_name]:
                groups_created.append(f"{group_name} ({len(self.bot.dm_groups[group_name])} users)")
            else:
                del self.bot.dm_groups[group_name]
        
        # Save DM groups
        self.bot.save_dm_groups()
        
        # Build response
        response = ["✅ **Autogroup Complete**\n"]
        
        if auto_groups_removed:
            response.append(f"🗑️ Cleared {len(auto_groups_removed)} auto-generated group(s)")
        
        if groups_created:
            response.append(f"\n**📋 Groups Created:**")
            for group in groups_created:
                response.append(f"• {group}")
        else:
            response.append("\n⚠️ No groups created (no matching students found)")
        
        response.append(f"\n**Stats:**")
        response.append(f"• Students processed: {len(student_data)}")
        response.append(f"• Users added to groups: {users_added}")
        if users_not_found:
            response.append(f"• Users not found: {users_not_found}")
        
        # Show which users weren't found (deduplicated)
        if users_not_found_list:
            response.append(f"\n**⚠️ Users Not Found:**")
            for user_info in users_not_found_list[:15]:  # Limit to 15 to avoid message too long
                response.append(f"• {user_info}")
            if len(users_not_found_list) > 15:
                response.append(f"• ... and {len(users_not_found_list) - 15} more")
        
        await ctx.send("\n".join(response))
    
    @commands.command(name='clear_autogroups')
    async def clear_autogroups(self, ctx: commands.Context) -> None:
        """Clear all auto-generated DM groups (those starting with 'auto_')."""
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("⚠️ This command only works in DMs for security.")
            return
        
        if not self._check_dm_permission(ctx):
            await ctx.send("❌ You don't have permission to use announce commands.")
            return
        
        groups_to_remove = [name for name in self.bot.dm_groups.keys() if name.startswith(AUTO_GROUP_PREFIX)]
        
        if not groups_to_remove:
            await ctx.send("ℹ️ No auto-generated groups to clear.")
            return
        
        for group_name in groups_to_remove:
            del self.bot.dm_groups[group_name]
        
        self.bot.save_dm_groups()
        await ctx.send(f"✅ Cleared {len(groups_to_remove)} auto-generated group(s):\n• " + "\n• ".join(groups_to_remove))


async def setup(bot: 'DiscordBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(AnnouncementsCog(bot))

