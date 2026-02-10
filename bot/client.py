"""Main Discord bot client with state management."""

from datetime import datetime, timezone
from typing import Dict, Set, List

import discord
from discord.ext import commands, tasks

from bot.config import Config
from services.persistence import PersistenceService
from services.rss_service import RSSService
from services.scheduler_service import SchedulerService
from utils.embeds import EmbedBuilder


class GitLabRSSBot(commands.Bot):
    """Main bot class handling GitLab RSS feeds and announcements."""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.dm_messages = True
        intents.members = True  # Required for username lookup across servers
        
        super().__init__(
            command_prefix=['!gitlab ', '!announce '],
            intents=intents,
            help_command=None
        )
        
        # GitLab RSS subscriptions: {channel_id: {'url': str, 'labels': set, 'last_checked': datetime}}
        self.subscriptions: Dict[int, Dict] = {}
        self.seen_issues: Dict[int, Set[str]] = {}
        
        # Announcement system
        self.channel_groups: Dict[str, List[int]] = {}  # {group_name: [channel_ids]}
        self.dm_groups: Dict[str, List[Dict]] = {}  # {group_name: [{user_id: int, username: str}]}
        self.scheduled_messages: Dict[str, Dict] = {}  # {schedule_id: {message, group, type, config, next_run, target_type}}
        self.allowed_users: Set[int] = set()  # User IDs allowed to use announce commands
        self.dm_conversations: Dict[int, Dict] = {}  # {user_id: {state, data}} for multi-step DM commands
        
        # Load all data from files
        self._load_all_data()
    
    def _load_all_data(self) -> None:
        """Load all persistent data from JSON files."""
        self.subscriptions, self.seen_issues = PersistenceService.load_subscriptions()
        self.channel_groups = PersistenceService.load_channel_groups()
        self.dm_groups = PersistenceService.load_dm_groups()
        self.scheduled_messages = PersistenceService.load_scheduled_messages()
        self.allowed_users = PersistenceService.load_allowed_users()
    
    # ==================== Persistence Helpers ====================
    
    def save_subscriptions(self) -> None:
        """Save subscriptions to JSON file."""
        PersistenceService.save_subscriptions(self.subscriptions, self.seen_issues)
    
    def save_channel_groups(self) -> None:
        """Save channel groups to JSON file."""
        PersistenceService.save_channel_groups(self.channel_groups)
    
    def save_dm_groups(self) -> None:
        """Save DM groups to JSON file."""
        PersistenceService.save_dm_groups(self.dm_groups)
    
    def save_scheduled_messages(self) -> None:
        """Save scheduled messages to JSON file."""
        PersistenceService.save_scheduled_messages(self.scheduled_messages)
    
    def save_allowed_users(self) -> None:
        """Save allowed users to JSON file."""
        PersistenceService.save_allowed_users(self.allowed_users)
    
    # ==================== Permission Checks ====================
    
    def is_user_allowed(self, user_id: int) -> bool:
        """Check if a user is allowed to use announce commands."""
        return user_id in self.allowed_users or user_id == Config.BOT_OWNER_ID
    
    # ==================== User Lookup Helpers ====================
    
    async def find_user_by_username(self, username: str) -> discord.User | None:
        """Find a user across all guilds by username.
        
        Args:
            username: Username to search for. Can be 'username' or 'username#1234'
            
        Returns:
            discord.User if found, None otherwise
        """
        # Check if it's a user ID (all digits)
        if username.isdigit():
            try:
                return await self.fetch_user(int(username))
            except discord.NotFound:
                return None
        
        # Check for discriminator format (legacy username#1234)
        discriminator = None
        search_name = username
        if '#' in username:
            parts = username.rsplit('#', 1)
            search_name = parts[0]
            discriminator = parts[1] if len(parts) > 1 and parts[1].isdigit() else None
        
        # Search across all guilds the bot is in
        for guild in self.guilds:
            # Fetch all members if not cached (requires members intent)
            try:
                # This fetches members from Discord API if not cached
                members = guild.members
                if len(members) < guild.member_count:
                    # Members aren't fully cached, fetch them
                    members = [member async for member in guild.fetch_members(limit=None)]
            except discord.Forbidden:
                # No permission to fetch members, use cached only
                members = guild.members
            except Exception as e:
                print(f"Error fetching members from {guild.name}: {e}")
                members = guild.members
            
            for member in members:
                # Match by username (case-insensitive)
                if member.name.lower() == search_name.lower():
                    # If discriminator provided, verify it matches
                    if discriminator:
                        if str(member.discriminator) == discriminator:
                            return member
                    else:
                        return member
                
                # Also check display name (nickname)
                if member.display_name.lower() == search_name.lower():
                    return member
        
        return None
    
    async def send_dm_to_user(self, user_id: int, message: str) -> tuple[bool, str]:
        """Send a DM to a user by ID.
        
        Args:
            user_id: Discord user ID
            message: Message content to send
            
        Returns:
            Tuple of (success: bool, error_message: str)
        """
        try:
            user = await self.fetch_user(user_id)
            if user:
                await user.send(message)
                return True, ""
            return False, "User not found"
        except discord.Forbidden:
            return False, "Cannot DM user (DMs disabled or blocked)"
        except discord.HTTPException as e:
            return False, f"HTTP error: {e}"
        except Exception as e:
            return False, str(e)
    
    # ==================== Bot Lifecycle ====================
    
    async def setup_hook(self) -> None:
        """Called when the bot is starting up - load cogs and start tasks."""
        # Load modules (cogs)
        await self.load_extension('modules.gitlab_rss')
        await self.load_extension('modules.announcements')
        await self.load_extension('bot.events')
        
        # Start background tasks
        self.check_feeds.start()
        self.check_scheduled_messages.start()
    
    # ==================== Background Tasks ====================
    
    @tasks.loop(minutes=Config.CHECK_INTERVAL_MINUTES)
    async def check_feeds(self) -> None:
        """Periodically check RSS feeds for new issues."""
        for channel_id, sub_data in list(self.subscriptions.items()):
            try:
                channel = self.get_channel(channel_id)
                if not channel:
                    continue
                
                feed, labels_map = await RSSService.fetch_feed_with_labels(sub_data['url'])
                
                if channel_id not in self.seen_issues:
                    self.seen_issues[channel_id] = set()
                
                new_issues = []
                for entry in feed.entries:
                    issue_id = entry.get('id', entry.get('link', ''))
                    
                    # Skip if we've seen this issue before
                    if issue_id in self.seen_issues[channel_id]:
                        continue
                    
                    # Get labels from our parsed map
                    issue_labels = labels_map.get(issue_id, [])
                    
                    # Filter by labels if configured
                    if sub_data['labels']:
                        if not any(label in sub_data['labels'] for label in issue_labels):
                            self.seen_issues[channel_id].add(issue_id)
                            continue
                    
                    new_issues.append((entry, issue_labels, issue_id))
                
                # Post new issues
                for entry, issue_labels, issue_id in new_issues:
                    await self._post_issue(channel, entry, issue_labels)
                    self.seen_issues[channel_id].add(issue_id)
                
                if new_issues:
                    sub_data['last_checked'] = datetime.now()
                    self.save_subscriptions()
                    
            except Exception as e:
                print(f"Error checking feed for channel {channel_id}: {e}")
    
    @check_feeds.before_loop
    async def before_check_feeds(self) -> None:
        """Wait until the bot is ready before starting the loop."""
        await self.wait_until_ready()
    
    @tasks.loop(count=1)  # Run once, then we manage our own loop
    async def check_scheduled_messages(self) -> None:
        """Check and send scheduled announcements - runs exactly at :00 of each minute."""
        import asyncio
        
        while True:
            # Wait until the next :00 second mark
            now = datetime.now(timezone.utc)
            seconds_until_next_minute = 60 - now.second - (now.microsecond / 1_000_000)
            if seconds_until_next_minute > 0 and seconds_until_next_minute <= 60:
                await asyncio.sleep(seconds_until_next_minute)
            
            # Now we're at :00 - check schedules
            now = datetime.now(timezone.utc)
            
            for schedule_id, sched in list(self.scheduled_messages.items()):
                if not sched.get('active', True):
                    continue
                
                next_run = sched.get('next_run')
                if next_run is None:
                    continue
                
                # Ensure timezone aware
                if next_run.tzinfo is None:
                    next_run = next_run.replace(tzinfo=timezone.utc)
                
                if now >= next_run:
                    # Check if we recently sent (within last 30 seconds) to prevent duplicate sends
                    if SchedulerService.is_recently_sent(sched.get('last_sent')):
                        continue
                    
                    # Time to send!
                    await self._send_scheduled_announcement(schedule_id, sched)
                    
                    # Track when we sent
                    sched['last_sent'] = now
                    
                    # Calculate next run time - ensure it's in the future
                    next_run_candidate = SchedulerService.calculate_next_run(
                        sched['type'], 
                        sched.get('config', {})
                    )
                    
                    # If somehow still in the past (clock drift/long operation), keep adding intervals
                    while next_run_candidate <= now:
                        next_run_candidate = next_run_candidate + SchedulerService.get_interval_delta(
                            sched['type'], 
                            sched.get('config', {})
                        )
                    
                    sched['next_run'] = next_run_candidate
                    self.save_scheduled_messages()
    
    @check_scheduled_messages.before_loop
    async def before_check_scheduled_messages(self) -> None:
        """Wait until the bot is ready."""
        await self.wait_until_ready()
    
    # ==================== Internal Helpers ====================
    
    async def _post_issue(self, channel, entry, labels: List[str]) -> None:
        """Post a new issue to Discord."""
        title = entry.get('title', 'No title')
        link = entry.get('link', '')
        author = entry.get('author', 'Unknown')
        published = entry.get('published', '')
        
        embed = EmbedBuilder.issue_embed(title, link, author, labels, published)
        await channel.send(embed=embed)
    
    async def _send_scheduled_announcement(self, schedule_id: str, sched: Dict) -> None:
        """Send a scheduled announcement to channels or DMs based on target_type."""
        target_type = sched.get('target_type', 'channel')  # Default to channel for backwards compatibility
        
        if target_type == 'dm':
            await self._send_scheduled_dm_announcement(schedule_id, sched)
        else:
            await self._send_scheduled_channel_announcement(schedule_id, sched)
    
    async def _send_scheduled_channel_announcement(self, schedule_id: str, sched: Dict) -> None:
        """Send a scheduled announcement to all channels in the group."""
        group_name = sched.get('group')
        message = sched.get('message', '')
        
        if not group_name or group_name not in self.channel_groups:
            print(f"Schedule {schedule_id}: Channel group '{group_name}' not found")
            return
        
        channel_ids = self.channel_groups[group_name]
        sent_count = 0
        
        for channel_id in channel_ids:
            try:
                channel = self.get_channel(channel_id)
                if channel:
                    await channel.send(message)
                    sent_count += 1
            except Exception as e:
                print(f"Error sending to channel {channel_id}: {e}")
        
        print(f"Schedule {schedule_id}: Sent to {sent_count}/{len(channel_ids)} channels")
    
    async def _send_scheduled_dm_announcement(self, schedule_id: str, sched: Dict) -> None:
        """Send a scheduled DM announcement to all users in the DM group."""
        group_name = sched.get('group')
        message = sched.get('message', '')
        
        if not group_name or group_name not in self.dm_groups:
            print(f"Schedule {schedule_id}: DM group '{group_name}' not found")
            return
        
        users = self.dm_groups[group_name]
        sent_count = 0
        failed_count = 0
        
        for user_data in users:
            user_id = user_data.get('user_id')
            if user_id:
                success, error = await self.send_dm_to_user(user_id, message)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                    print(f"Error sending DM to user {user_id}: {error}")
        
        print(f"Schedule {schedule_id}: DM sent to {sent_count}/{len(users)} users, {failed_count} failed")
    
    # ==================== Message Event Override ====================
    
    async def on_message(self, message) -> None:
        """Handle DM conversations for multi-step commands.
        
        This overrides the default on_message to handle multi-step DM flows
        BEFORE command processing occurs.
        """
        # Ignore bot's own messages
        if message.author == self.user:
            return
        
        # Check if this is a DM and user has an active conversation
        if isinstance(message.channel, discord.DMChannel):
            user_id = message.author.id
            
            # Only handle if user is in a conversation AND message is NOT a command
            if user_id in self.dm_conversations:
                # Check if this looks like a command - if so, let it process normally
                is_command = any(message.content.startswith(prefix) for prefix in self.command_prefix)
                
                if not is_command:
                    await self._handle_dm_conversation(message, user_id)
                    return
        
        # Process commands as normal
        await self.process_commands(message)
    
    async def _handle_dm_conversation(self, message, user_id: int) -> None:
        """Handle an ongoing DM conversation."""
        conv = self.dm_conversations[user_id]
        state = conv.get('state')
        data = conv.get('data', {})
        
        # Unified schedule creation (handles both channel and DM groups)
        if state in ('awaiting_message', 'awaiting_schedule_message'):
            await self._complete_schedule_creation(message, user_id, data)
        
        elif state == 'awaiting_broadcast_message':
            await self._send_broadcast(message, user_id, data)
        
        elif state == 'awaiting_direct_message':
            await self._send_direct_channel_message(message, user_id, data)
        
        elif state == 'awaiting_dm_user_message':
            await self._complete_dm_to_user(message, user_id, data)
        
        elif state == 'awaiting_dm_group_message':
            await self._complete_dm_to_group(message, user_id, data)
    
    async def _complete_schedule_creation(self, message, user_id: int, data: dict) -> None:
        """Complete the schedule creation from a DM conversation (unified for channel and DM groups)."""
        from utils.time_utils import format_time_until
        
        schedule_id = data['schedule_id']
        target_type = data.get('target_type', 'channel')  # Default to channel for backwards compatibility
        next_run = SchedulerService.calculate_next_run(data['type'], data['config'])
        
        self.scheduled_messages[schedule_id] = {
            'group': data['group'],
            'type': data['type'],
            'config': data['config'],
            'message': message.content,
            'next_run': next_run,
            'active': True,
            'created_by': user_id,
            'target_type': target_type
        }
        self.save_scheduled_messages()
        
        # Build confirmation message based on target type
        time_until = format_time_until(next_run)
        group_name = data['group']
        
        if target_type == 'dm':
            user_count = len(self.dm_groups.get(group_name, []))
            target_info = f"DM Group: `{group_name}` ({user_count} users)"
            icon = "📬"
        else:
            channel_count = len(self.channel_groups.get(group_name, []))
            target_info = f"Channel Group: `{group_name}` ({channel_count} channels)"
            icon = "📢"
        
        await message.channel.send(
            f"✅ **{icon} Schedule Created!**\n"
            f"• ID: `{schedule_id}`\n"
            f"• {target_info}\n"
            f"• Type: {data['type']}\n"
            f"• Next send: {next_run.strftime('%Y-%m-%d %H:%M')} GMT ({time_until})\n"
            f"• Message preview: {message.content[:100]}{'...' if len(message.content) > 100 else ''}"
        )
        
        del self.dm_conversations[user_id]
    
    async def _send_broadcast(self, message, user_id: int, data: dict) -> None:
        """Send a broadcast message from a DM conversation."""
        group_name = data['group']
        channel_ids = self.channel_groups.get(group_name, [])
        
        sent_count = 0
        failed_count = 0
        
        await message.channel.send(f"📤 Broadcasting to {len(channel_ids)} channels...")
        
        for channel_id in channel_ids:
            try:
                channel = self.get_channel(channel_id)
                if channel:
                    await channel.send(message.content)
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"Error sending to channel {channel_id}: {e}")
                failed_count += 1
        
        await message.channel.send(f"✅ **Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}")
        
        del self.dm_conversations[user_id]
    
    async def _send_direct_channel_message(self, message, user_id: int, data: dict) -> None:
        """Send a direct message to a channel from a DM conversation."""
        channel_id = data['channel_id']
        channel_name = data.get('channel_name', 'unknown')
        
        try:
            channel = self.get_channel(channel_id)
            if channel:
                await channel.send(message.content)
                await message.channel.send(f"✅ Message sent to #{channel_name}!")
            else:
                await message.channel.send(f"❌ Channel not found or bot doesn't have access.")
        except Exception as e:
            await message.channel.send(f"❌ Failed to send: {e}")
        
        del self.dm_conversations[user_id]
    
    async def _complete_dm_to_user(self, message, user_id: int, data: dict) -> None:
        """Send a DM to a specific user from a conversation."""
        target_user_id = data['user_id']
        
        success, error = await self.send_dm_to_user(target_user_id, message.content)
        if success:
            await message.channel.send(f"✅ DM sent to user `{target_user_id}`!")
        else:
            await message.channel.send(f"❌ Failed to send DM: {error}")
        
        del self.dm_conversations[user_id]
    
    async def _complete_dm_to_group(self, message, user_id: int, data: dict) -> None:
        """Send a DM to all users in a group from a conversation."""
        group_name = data['group']
        users = self.dm_groups.get(group_name, [])
        
        sent_count = 0
        failed_count = 0
        failed_users = []
        
        await message.channel.send(f"📤 Sending DMs to {len(users)} users...")
        
        for user_data in users:
            target_id = user_data.get('user_id')
            username = user_data.get('username', 'Unknown')
            if target_id:
                success, error = await self.send_dm_to_user(target_id, message.content)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                    failed_users.append(f"{username}: {error}")
        
        result_msg = f"✅ **DM Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}"
        if failed_users and len(failed_users) <= 5:
            result_msg += f"\n\n**Failed:**\n" + "\n".join(f"• {u}" for u in failed_users)
        await message.channel.send(result_msg)
        
        del self.dm_conversations[user_id]

