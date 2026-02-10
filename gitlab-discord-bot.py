import discord
from discord.ext import commands, tasks
import feedparser
import aiohttp
import json
import os
import re
import xml.etree.ElementTree as ET
import uuid
from datetime import datetime, timedelta, timezone
from typing import Set, Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
CHECK_INTERVAL_MINUTES = 5
ANNOUNCEMENT_CHECK_INTERVAL_SECONDS = 60  # Check schedules every minute

# Bot creator ID (set this to your Discord user ID to bootstrap permissions)
BOT_OWNER_ID = int(os.getenv('BOT_OWNER_ID', '0'))

# Auto-subscription configuration
AUTO_SUBSCRIBE_RSS_URL = "https://gitlab.com/gitlab-org/gitlab/-/work_items.atom?sort=created_date&state=opened&first_page_size=100"
AUTO_SUBSCRIBE_CHANNEL_NAME = "issue-feed"
AUTO_SUBSCRIBE_LABELS = {
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

class GitLabRSSBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.dm_messages = True
        super().__init__(command_prefix=['!gitlab ', '!announce '], intents=intents, help_command=None)
        
        # GitLab RSS subscriptions: {channel_id: {'url': str, 'labels': set, 'last_checked': datetime}}
        self.subscriptions: Dict[int, Dict] = {}
        self.seen_issues: Dict[int, Set[str]] = {}
        
        # Announcement system
        self.channel_groups: Dict[str, List[int]] = {}  # {group_name: [channel_ids]}
        self.scheduled_messages: Dict[str, Dict] = {}  # {schedule_id: {message, group, type, config, next_run}}
        self.allowed_users: Set[int] = set()  # User IDs allowed to use announce commands
        self.dm_conversations: Dict[int, Dict] = {}  # {user_id: {state, data}} for multi-step DM commands
        
        # Load all data from files
        self.load_subscriptions()
        self.load_announcement_data()
        
    def load_subscriptions(self):
        """Load subscriptions from JSON file"""
        try:
            if os.path.exists('subscriptions.json'):
                with open('subscriptions.json', 'r') as f:
                    data = json.load(f)
                    for channel_id_str, sub_data in data.items():
                        channel_id = int(channel_id_str)
                        self.subscriptions[channel_id] = {
                            'url': sub_data['url'],
                            'labels': set(sub_data.get('labels', [])),
                            'last_checked': datetime.fromisoformat(sub_data.get('last_checked', datetime.now().isoformat()))
                        }
                        self.seen_issues[channel_id] = set(sub_data.get('seen_issues', []))
        except Exception as e:
            print(f"Error loading subscriptions: {e}")
    
    def save_subscriptions(self):
        """Save subscriptions to JSON file"""
        try:
            data = {}
            for channel_id, sub_data in self.subscriptions.items():
                data[str(channel_id)] = {
                    'url': sub_data['url'],
                    'labels': list(sub_data['labels']),
                    'last_checked': sub_data['last_checked'].isoformat(),
                    'seen_issues': list(self.seen_issues.get(channel_id, []))
                }
            with open('subscriptions.json', 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving subscriptions: {e}")
    
    def load_announcement_data(self):
        """Load channel groups, scheduled messages, and allowed users"""
        # Load channel groups
        try:
            if os.path.exists('channel_groups.json'):
                with open('channel_groups.json', 'r') as f:
                    self.channel_groups = json.load(f)
        except Exception as e:
            print(f"Error loading channel groups: {e}")
        
        # Load scheduled messages
        try:
            if os.path.exists('scheduled_messages.json'):
                with open('scheduled_messages.json', 'r') as f:
                    data = json.load(f)
                    for schedule_id, sched in data.items():
                        sched['next_run'] = datetime.fromisoformat(sched['next_run']) if sched.get('next_run') else None
                        self.scheduled_messages[schedule_id] = sched
        except Exception as e:
            print(f"Error loading scheduled messages: {e}")
        
        # Load allowed users
        try:
            if os.path.exists('allowed_users.json'):
                with open('allowed_users.json', 'r') as f:
                    self.allowed_users = set(json.load(f))
            # Always include bot owner
            if BOT_OWNER_ID:
                self.allowed_users.add(BOT_OWNER_ID)
        except Exception as e:
            print(f"Error loading allowed users: {e}")
    
    def save_channel_groups(self):
        """Save channel groups to JSON file"""
        try:
            with open('channel_groups.json', 'w') as f:
                json.dump(self.channel_groups, f, indent=2)
        except Exception as e:
            print(f"Error saving channel groups: {e}")
    
    def save_scheduled_messages(self):
        """Save scheduled messages to JSON file"""
        try:
            data = {}
            for schedule_id, sched in self.scheduled_messages.items():
                data[schedule_id] = {
                    **sched,
                    'next_run': sched['next_run'].isoformat() if sched.get('next_run') else None
                }
            with open('scheduled_messages.json', 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving scheduled messages: {e}")
    
    def save_allowed_users(self):
        """Save allowed users to JSON file"""
        try:
            with open('allowed_users.json', 'w') as f:
                json.dump(list(self.allowed_users), f, indent=2)
        except Exception as e:
            print(f"Error saving allowed users: {e}")
    
    def is_user_allowed(self, user_id: int) -> bool:
        """Check if a user is allowed to use announce commands"""
        return user_id in self.allowed_users or user_id == BOT_OWNER_ID
    
    def calculate_next_run(self, schedule_type: str, config: Dict) -> datetime:
        """Calculate the next run time for a scheduled message"""
        now = datetime.now(timezone.utc)
        
        if schedule_type == 'hourly':
            hours = config.get('hours', 1)
            return now + timedelta(hours=hours)
        
        elif schedule_type == 'daily':
            target_hour = config.get('hour', 9)
            target_minute = config.get('minute', 0)
            next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            return next_run
        
        elif schedule_type == 'weekly':
            target_day = config.get('day', 0)  # 0 = Monday
            target_hour = config.get('hour', 9)
            target_minute = config.get('minute', 0)
            days_ahead = target_day - now.weekday()
            if days_ahead < 0 or (days_ahead == 0 and now.hour * 60 + now.minute >= target_hour * 60 + target_minute):
                days_ahead += 7
            next_run = now + timedelta(days=days_ahead)
            next_run = next_run.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            return next_run
        
        return now + timedelta(hours=1)  # Default fallback
    
    def format_time_until(self, target: datetime) -> str:
        """Format time remaining until a datetime"""
        now = datetime.now(timezone.utc)
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        delta = target - now
        
        if delta.total_seconds() < 0:
            return "overdue"
        
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes = remainder // 60
        
        if hours > 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}d {hours}h {minutes}m"
        return f"{hours}h {minutes}m"
    
    async def setup_hook(self):
        """Called when the bot is starting up"""
        self.check_feeds.start()
        self.check_scheduled_messages.start()
    
    async def fetch_feed_with_labels(self, url: str) -> tuple:
        """Fetch feed and parse labels from raw XML"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                raw_xml = await response.text()
        
        # Parse with feedparser for entry metadata
        feed = feedparser.parse(raw_xml)
        
        # Parse raw XML to extract labels using regex (more reliable than namespace handling)
        labels_map = {}
        
        # Split by <entry> to process each entry
        entry_pattern = re.compile(r'<entry>(.*?)</entry>', re.DOTALL)
        id_pattern = re.compile(r'<id>([^<]+)</id>')
        labels_pattern = re.compile(r'<labels>(.*?)</labels>', re.DOTALL)
        label_pattern = re.compile(r'<label>([^<]+)</label>')
        
        for entry_match in entry_pattern.finditer(raw_xml):
            entry_xml = entry_match.group(1)
            
            # Extract issue ID
            id_match = id_pattern.search(entry_xml)
            if id_match:
                issue_id = id_match.group(1)
                labels = []
                
                # Extract labels container
                labels_match = labels_pattern.search(entry_xml)
                if labels_match:
                    labels_xml = labels_match.group(1)
                    labels = label_pattern.findall(labels_xml)
                
                labels_map[issue_id] = labels
        
        return feed, labels_map
    
    @tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
    async def check_feeds(self):
        """Periodically check RSS feeds for new issues"""
        for channel_id, sub_data in list(self.subscriptions.items()):
            try:
                channel = self.get_channel(channel_id)
                if not channel:
                    continue
                
                feed, labels_map = await self.fetch_feed_with_labels(sub_data['url'])
                
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
                    await self.post_issue(channel, entry, issue_labels)
                    self.seen_issues[channel_id].add(issue_id)
                
                if new_issues:
                    sub_data['last_checked'] = datetime.now()
                    self.save_subscriptions()
                    
            except Exception as e:
                print(f"Error checking feed for channel {channel_id}: {e}")
    
    @check_feeds.before_loop
    async def before_check_feeds(self):
        """Wait until the bot is ready before starting the loop"""
        await self.wait_until_ready()
    
    @tasks.loop(seconds=ANNOUNCEMENT_CHECK_INTERVAL_SECONDS)
    async def check_scheduled_messages(self):
        """Check and send scheduled announcements"""
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
                # Time to send!
                await self.send_scheduled_announcement(schedule_id, sched)
                
                # Calculate next run time
                sched['next_run'] = self.calculate_next_run(sched['type'], sched.get('config', {}))
                self.save_scheduled_messages()
    
    @check_scheduled_messages.before_loop
    async def before_check_scheduled_messages(self):
        """Wait until the bot is ready before starting the loop"""
        await self.wait_until_ready()
    
    async def send_scheduled_announcement(self, schedule_id: str, sched: Dict):
        """Send a scheduled announcement to all channels in the group"""
        group_name = sched.get('group')
        message = sched.get('message', '')
        
        if not group_name or group_name not in self.channel_groups:
            print(f"Schedule {schedule_id}: Group '{group_name}' not found")
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
    
    def extract_labels(self, entry) -> List[str]:
        """Extract labels from RSS entry"""
        import re
        labels = []
        
        # GitLab RSS feeds include labels in tags
        if hasattr(entry, 'tags'):
            for tag in entry.tags:
                labels.append(tag.term)
        
        # GitLab work_items Atom feed has labels in a different format
        # Parse from the raw XML content if available
        if hasattr(entry, 'content'):
            for content in entry.content:
                content_value = content.get('value', '')
                # Look for label patterns in content
                label_matches = re.findall(r'<label>([^<]+)</label>', content_value)
                labels.extend(label_matches)
        
        # Check summary/description for labels
        summary = entry.get('summary', '') + entry.get('description', '')
        
        # Parse <label> tags from summary
        label_matches = re.findall(r'<label>([^<]+)</label>', summary)
        labels.extend(label_matches)
        
        # Parse labels formatted as ~label
        label_matches = re.findall(r'~([^\s~]+)', summary)
        labels.extend(label_matches)
        
        # Deduplicate
        return list(set(labels))
    
    async def post_issue(self, channel, entry, labels: List[str]):
        """Post a new issue to Discord"""
        title = entry.get('title', 'No title')
        link = entry.get('link', '')
        author = entry.get('author', 'Unknown')
        published = entry.get('published', '')
        
        embed = discord.Embed(
            title=title,
            url=link,
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        embed.add_field(name="Author", value=author, inline=True)
        
        if labels:
            # Color code based on priority labels
            if any('bug' in label.lower() for label in labels):
                embed.color = discord.Color.red()
            elif any('feature' in label.lower() for label in labels):
                embed.color = discord.Color.green()
            
            # Format labels nicely
            label_text = ', '.join([f"`{label}`" for label in labels])
            embed.add_field(name="Labels", value=label_text, inline=False)
        
        embed.set_footer(text="GitLab Issue")
        
        await channel.send(embed=embed)

# Initialize bot
bot = GitLabRSSBot()

@bot.command(name='subscribe')
async def subscribe(ctx, rss_url: str):
    """Subscribe this channel to a GitLab RSS feed
    
    Example: !gitlab subscribe https://gitlab.com/group/project/-/issues.atom
    """
    channel_id = ctx.channel.id
    
    # Test the RSS feed
    try:
        feed = feedparser.parse(rss_url)
        if not feed.entries and not feed.get('feed'):
            await ctx.send("❌ Invalid RSS feed URL. Please check the URL and try again.")
            return
    except Exception as e:
        await ctx.send(f"❌ Error accessing RSS feed: {e}")
        return
    
    bot.subscriptions[channel_id] = {
        'url': rss_url,
        'labels': set(),
        'last_checked': datetime.now()
    }
    
    bot.seen_issues[channel_id] = set()
    bot.save_subscriptions()
    
    await ctx.send(f"✅ Subscribed to GitLab RSS feed!\n"
                   f"This channel will receive all new issues.\n"
                   f"Use `!gitlab filter` to customize which labels to track.")

@bot.command(name='unsubscribe')
async def unsubscribe(ctx):
    """Unsubscribe this channel from the RSS feed"""
    channel_id = ctx.channel.id
    
    if channel_id in bot.subscriptions:
        del bot.subscriptions[channel_id]
        if channel_id in bot.seen_issues:
            del bot.seen_issues[channel_id]
        bot.save_subscriptions()
        await ctx.send("✅ Unsubscribed from GitLab RSS feed.")
    else:
        await ctx.send("❌ This channel is not subscribed to any feed.")

@bot.command(name='filter')
async def filter_labels(ctx, *labels: str):
    """Set label filters for this channel
    
    Examples:
    !gitlab filter backend frontend type::bug
    !gitlab filter quick-win community-bonus::100
    !gitlab filter (clears all filters)
    """
    channel_id = ctx.channel.id
    
    if channel_id not in bot.subscriptions:
        await ctx.send("❌ This channel is not subscribed to any feed. Use `!gitlab subscribe` first.")
        return
    
    if not labels:
        bot.subscriptions[channel_id]['labels'] = set()
        bot.save_subscriptions()
        await ctx.send("✅ Cleared all label filters. This channel will receive all issues.")
        return
    
    # Normalize labels (replace spaces with hyphens)
    normalized_labels = {label.replace(' ', '-') for label in labels}
    
    bot.subscriptions[channel_id]['labels'] = normalized_labels
    bot.save_subscriptions()
    
    label_list = '\n'.join([f"• `{label}`" for label in sorted(normalized_labels)])
    await ctx.send(f"✅ Label filters updated! This channel will only receive issues with these labels:\n{label_list}")

@bot.command(name='status')
async def status(ctx):
    """Show subscription status for this channel"""
    channel_id = ctx.channel.id
    
    if channel_id not in bot.subscriptions:
        await ctx.send("❌ This channel is not subscribed to any feed.")
        return
    
    sub = bot.subscriptions[channel_id]
    
    embed = discord.Embed(
        title="GitLab RSS Subscription Status",
        color=discord.Color.green()
    )
    
    embed.add_field(name="RSS URL", value=sub['url'], inline=False)
    embed.add_field(
        name="Last Checked",
        value=sub['last_checked'].strftime("%Y-%m-%d %H:%M:%S"),
        inline=True
    )
    embed.add_field(
        name="Issues Tracked",
        value=str(len(bot.seen_issues.get(channel_id, []))),
        inline=True
    )
    
    if sub['labels']:
        label_list = '\n'.join([f"• `{label}`" for label in sorted(sub['labels'])])
        embed.add_field(name="Active Label Filters", value=label_list, inline=False)
    else:
        embed.add_field(name="Active Label Filters", value="None (tracking all issues)", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='check')
async def check_now(ctx):
    """Manually check the feed and show debug info"""
    channel_id = ctx.channel.id
    
    if channel_id not in bot.subscriptions:
        await ctx.send("❌ This channel is not subscribed to any feed.")
        return
    
    sub = bot.subscriptions[channel_id]
    await ctx.send("🔍 Checking feed...")
    
    try:
        feed, labels_map = await bot.fetch_feed_with_labels(sub['url'])
        total_entries = len(feed.entries)
        
        if channel_id not in bot.seen_issues:
            bot.seen_issues[channel_id] = set()
        
        new_count = 0
        matching_count = 0
        sample_labels = []
        
        for entry in feed.entries[:10]:  # Check first 10 for debug
            issue_id = entry.get('id', entry.get('link', ''))
            is_new = issue_id not in bot.seen_issues[channel_id]
            issue_labels = labels_map.get(issue_id, [])
            
            if is_new:
                new_count += 1
            
            # Check if labels match
            matches_filter = True
            if sub['labels']:
                matches_filter = any(label in sub['labels'] for label in issue_labels)
            
            if matches_filter and is_new:
                matching_count += 1
            
            # Collect sample labels for debugging
            if len(sample_labels) < 5:  # Always collect samples for debugging
                sample_labels.append({
                    'title': entry.get('title', 'No title')[:50],
                    'labels': issue_labels[:5],
                    'is_new': is_new,
                    'matches': matches_filter
                })
        
        # Build debug message
        embed = discord.Embed(
            title="Feed Check Results",
            color=discord.Color.blue()
        )
        embed.add_field(name="Total in feed", value=str(total_entries), inline=True)
        embed.add_field(name="Already seen", value=str(len(bot.seen_issues[channel_id])), inline=True)
        embed.add_field(name="New & matching", value=str(matching_count), inline=True)
        embed.add_field(name="Labels parsed", value=str(len([v for v in labels_map.values() if v])), inline=True)
        
        if sample_labels:
            sample_text = ""
            for s in sample_labels:
                status = "✅" if s['matches'] and s['is_new'] else "❌"
                labels_str = ", ".join(s['labels'][:3]) if s['labels'] else "(no labels)"
                sample_text += f"{status} **{s['title']}...**\n└ Labels: `{labels_str}`\n"
            embed.add_field(name="Sample Issues", value=sample_text[:1024], inline=False)
        else:
            embed.add_field(name="Sample Issues", value="No labels found in entries", inline=False)
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        await ctx.send(f"❌ Error checking feed: {e}")

@bot.command(name='debug')
async def debug_feed(ctx):
    """Show raw feed data for debugging"""
    channel_id = ctx.channel.id
    
    if channel_id not in bot.subscriptions:
        await ctx.send("❌ This channel is not subscribed to any feed.")
        return
    
    sub = bot.subscriptions[channel_id]
    await ctx.send("🔍 Fetching raw feed for debug...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(sub['url']) as response:
                raw_xml = await response.text()
        
        # Check if <labels> exists anywhere in the feed
        has_labels_tag = '<labels>' in raw_xml
        labels_count = raw_xml.count('<label>')
        
        # Get first entry snippet
        entry_start = raw_xml.find('<entry>')
        entry_end = raw_xml.find('</entry>') + 8
        first_entry = raw_xml[entry_start:entry_end] if entry_start != -1 else "No entry found"
        
        # Truncate for Discord
        first_entry_preview = first_entry[:1500] + "..." if len(first_entry) > 1500 else first_entry
        
        await ctx.send(f"**Feed Debug Info:**\n"
                       f"• Has `<labels>` tag: {has_labels_tag}\n"
                       f"• Total `<label>` tags: {labels_count}\n"
                       f"• Feed size: {len(raw_xml)} chars\n\n"
                       f"**First entry preview:**\n```xml\n{first_entry_preview}\n```")
        
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command(name='labels')
async def show_labels(ctx):
    """Show available label options"""
    labels = [
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
    ]
    
    embed = discord.Embed(
        title="Available GitLab Labels",
        description="Use these labels with the `!gitlab filter` command",
        color=discord.Color.blue()
    )
    
    categories = {
        "Component": ["backend", "frontend", "documentation"],
        "Type": ["type::bug", "type::feature", "type::maintenance"],
        "Difficulty": ["quick-win", "quick-win::first-time-contributor"],
        "Community Bonus": ["community-bonus::100", "community-bonus::200", "community-bonus::300", "community-bonus::500"],
        "Other": ["co-create"]
    }
    
    for category, category_labels in categories.items():
        label_text = '\n'.join([f"`{label}`" for label in category_labels])
        embed.add_field(name=category, value=label_text, inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='help')
async def help_command(ctx):
    """Show help information"""
    embed = discord.Embed(
        title="GitLab RSS Bot - Help",
        description="Monitor GitLab issues and filter by labels",
        color=discord.Color.purple()
    )
    
    commands_info = {
        "subscribe <rss_url>": "Subscribe this channel to a GitLab RSS feed",
        "unsubscribe": "Unsubscribe this channel from the RSS feed",
        "filter <labels...>": "Set label filters (space-separated)",
        "status": "Show current subscription status",
        "check": "Manually check feed and show debug info",
        "labels": "Show available label options",
        "help": "Show this help message"
    }
    
    for cmd, desc in commands_info.items():
        embed.add_field(name=f"!gitlab {cmd}", value=desc, inline=False)
    
    embed.add_field(
        name="Examples",
        value=(
            "```\n"
            "!gitlab subscribe https://gitlab.com/group/project/-/issues.atom\n"
            "!gitlab filter backend type::bug quick-win\n"
            "!gitlab status\n"
            "```"
        ),
        inline=False
    )
    
    await ctx.send(embed=embed)

# ============================================================
# ANNOUNCEMENT BOT COMMANDS (DM-based)
# ============================================================

def check_dm_permission(ctx) -> bool:
    """Check if user is allowed to use announce commands"""
    return bot.is_user_allowed(ctx.author.id)

@bot.command(name='users')
async def manage_users(ctx, action: str = None, user_id: str = None):
    """Manage allowed users (owner only)
    
    Usage:
    !announce users - List allowed users
    !announce users add <user_id> - Add a user
    !announce users remove <user_id> - Remove a user
    """
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    # Only bot owner can manage users
    if ctx.author.id != BOT_OWNER_ID:
        await ctx.send("❌ Only the bot owner can manage allowed users.")
        return
    
    if action is None:
        # List users
        if not bot.allowed_users:
            await ctx.send("📋 **Allowed Users:** None configured")
            return
        
        user_list = []
        for uid in bot.allowed_users:
            try:
                user = await bot.fetch_user(uid)
                user_list.append(f"• {user.name} (`{uid}`)")
            except:
                user_list.append(f"• Unknown (`{uid}`)")
        
        await ctx.send(f"📋 **Allowed Users:**\n" + "\n".join(user_list))
    
    elif action == 'add' and user_id:
        try:
            uid = int(user_id)
            bot.allowed_users.add(uid)
            bot.save_allowed_users()
            await ctx.send(f"✅ Added user `{uid}` to allowed users.")
        except ValueError:
            await ctx.send("❌ Invalid user ID. Must be a number.")
    
    elif action == 'remove' and user_id:
        try:
            uid = int(user_id)
            if uid == BOT_OWNER_ID:
                await ctx.send("❌ Cannot remove the bot owner.")
                return
            bot.allowed_users.discard(uid)
            bot.save_allowed_users()
            await ctx.send(f"✅ Removed user `{uid}` from allowed users.")
        except ValueError:
            await ctx.send("❌ Invalid user ID. Must be a number.")
    else:
        await ctx.send("Usage: `!announce users [add|remove] [user_id]`")

@bot.command(name='group')
async def manage_group(ctx, action: str = None, group_name: str = None, channel_arg: str = None):
    """Manage channel groups
    
    Usage:
    !announce group create <name> - Create a new group
    !announce group delete <name> - Delete a group
    !announce group add <name> <channel_id> - Add channel to group
    !announce group remove <name> <channel_id> - Remove channel from group
    """
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if action == 'create' and group_name:
        if group_name in bot.channel_groups:
            await ctx.send(f"❌ Group `{group_name}` already exists.")
            return
        bot.channel_groups[group_name] = []
        bot.save_channel_groups()
        await ctx.send(f"✅ Created group `{group_name}`")
    
    elif action == 'delete' and group_name:
        if group_name not in bot.channel_groups:
            await ctx.send(f"❌ Group `{group_name}` doesn't exist.")
            return
        del bot.channel_groups[group_name]
        bot.save_channel_groups()
        await ctx.send(f"✅ Deleted group `{group_name}`")
    
    elif action == 'add' and group_name and channel_arg:
        if group_name not in bot.channel_groups:
            await ctx.send(f"❌ Group `{group_name}` doesn't exist. Create it first.")
            return
        try:
            channel_id = int(channel_arg.strip('<>#'))
            channel = bot.get_channel(channel_id)
            if not channel:
                await ctx.send(f"⚠️ Channel `{channel_id}` not found. Adding anyway (bot may not have access).")
            if channel_id not in bot.channel_groups[group_name]:
                bot.channel_groups[group_name].append(channel_id)
                bot.save_channel_groups()
                channel_name = channel.name if channel else "unknown"
                await ctx.send(f"✅ Added #{channel_name} (`{channel_id}`) to group `{group_name}`")
            else:
                await ctx.send(f"ℹ️ Channel already in group `{group_name}`")
        except ValueError:
            await ctx.send("❌ Invalid channel ID.")
    
    elif action == 'remove' and group_name and channel_arg:
        if group_name not in bot.channel_groups:
            await ctx.send(f"❌ Group `{group_name}` doesn't exist.")
            return
        try:
            channel_id = int(channel_arg.strip('<>#'))
            if channel_id in bot.channel_groups[group_name]:
                bot.channel_groups[group_name].remove(channel_id)
                bot.save_channel_groups()
                await ctx.send(f"✅ Removed channel `{channel_id}` from group `{group_name}`")
            else:
                await ctx.send(f"ℹ️ Channel not in group `{group_name}`")
        except ValueError:
            await ctx.send("❌ Invalid channel ID.")
    else:
        await ctx.send("Usage: `!announce group <create|delete|add|remove> <name> [channel_id]`")

@bot.command(name='groups')
async def list_groups(ctx):
    """List all channel groups"""
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not bot.channel_groups:
        await ctx.send("📋 **Channel Groups:** None configured\n\nUse `!announce group create <name>` to create one.")
        return
    
    embed = discord.Embed(title="📋 Channel Groups", color=discord.Color.blue())
    
    for group_name, channel_ids in bot.channel_groups.items():
        if channel_ids:
            channel_list = []
            for cid in channel_ids:
                channel = bot.get_channel(cid)
                if channel:
                    channel_list.append(f"• #{channel.name} (`{cid}`)")
                else:
                    channel_list.append(f"• Unknown (`{cid}`)")
            embed.add_field(name=f"**{group_name}** ({len(channel_ids)} channels)", 
                           value="\n".join(channel_list)[:1024], inline=False)
        else:
            embed.add_field(name=f"**{group_name}** (0 channels)", 
                           value="No channels yet", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='schedule')
async def schedule_message(ctx, group_name: str = None, schedule_type: str = None, *args):
    """Schedule a recurring message
    
    Usage:
    !announce schedule <group> hourly <N> [message] - Every N hours
    !announce schedule <group> daily <HH:MM> [message] - Daily at time (GMT)
    !announce schedule <group> weekly <day> <HH:MM> [message] - Weekly (day: mon/tue/wed/thu/fri/sat/sun)
    
    If message is not provided, you'll be prompted for it.
    """
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not group_name or not schedule_type:
        await ctx.send("Usage:\n"
                       "`!announce schedule <group> hourly <N> [message]`\n"
                       "`!announce schedule <group> daily <HH:MM> [message]`\n"
                       "`!announce schedule <group> weekly <day> <HH:MM> [message]`")
        return
    
    if group_name not in bot.channel_groups:
        await ctx.send(f"❌ Group `{group_name}` doesn't exist. Create it first with `!announce group create {group_name}`")
        return
    
    config = {}
    message = None
    
    if schedule_type == 'hourly':
        if not args:
            await ctx.send("❌ Please specify hours: `!announce schedule <group> hourly <N> [message]`")
            return
        try:
            hours = int(args[0])
            if hours < 1 or hours > 168:
                await ctx.send("❌ Hours must be between 1 and 168 (1 week)")
                return
            config['hours'] = hours
            message = ' '.join(args[1:]) if len(args) > 1 else None
        except ValueError:
            await ctx.send("❌ Invalid hours value")
            return
    
    elif schedule_type == 'daily':
        if not args:
            await ctx.send("❌ Please specify time: `!announce schedule <group> daily <HH:MM> [message]`")
            return
        try:
            time_parts = args[0].split(':')
            config['hour'] = int(time_parts[0])
            config['minute'] = int(time_parts[1]) if len(time_parts) > 1 else 0
            if not (0 <= config['hour'] <= 23 and 0 <= config['minute'] <= 59):
                raise ValueError()
            message = ' '.join(args[1:]) if len(args) > 1 else None
        except (ValueError, IndexError):
            await ctx.send("❌ Invalid time format. Use HH:MM (e.g., 09:00)")
            return
    
    elif schedule_type == 'weekly':
        if len(args) < 2:
            await ctx.send("❌ Please specify day and time: `!announce schedule <group> weekly <day> <HH:MM> [message]`")
            return
        days = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
        day_str = args[0].lower()[:3]
        if day_str not in days:
            await ctx.send("❌ Invalid day. Use: mon, tue, wed, thu, fri, sat, sun")
            return
        config['day'] = days[day_str]
        try:
            time_parts = args[1].split(':')
            config['hour'] = int(time_parts[0])
            config['minute'] = int(time_parts[1]) if len(time_parts) > 1 else 0
            if not (0 <= config['hour'] <= 23 and 0 <= config['minute'] <= 59):
                raise ValueError()
            message = ' '.join(args[2:]) if len(args) > 2 else None
        except (ValueError, IndexError):
            await ctx.send("❌ Invalid time format. Use HH:MM (e.g., 09:00)")
            return
    else:
        await ctx.send("❌ Invalid schedule type. Use: hourly, daily, or weekly")
        return
    
    # Generate schedule ID
    schedule_id = str(uuid.uuid4())[:8]
    
    # If no message provided, prompt for it
    if not message:
        bot.dm_conversations[ctx.author.id] = {
            'state': 'awaiting_message',
            'data': {
                'schedule_id': schedule_id,
                'group': group_name,
                'type': schedule_type,
                'config': config
            }
        }
        await ctx.send(f"📝 Please send the message you want to schedule for group `{group_name}`:\n"
                       f"(Just type your message and send it)")
        return
    
    # Create the schedule
    next_run = bot.calculate_next_run(schedule_type, config)
    bot.scheduled_messages[schedule_id] = {
        'group': group_name,
        'type': schedule_type,
        'config': config,
        'message': message,
        'next_run': next_run,
        'active': True,
        'created_by': ctx.author.id
    }
    bot.save_scheduled_messages()
    
    time_until = bot.format_time_until(next_run)
    await ctx.send(f"✅ **Schedule Created!**\n"
                   f"• ID: `{schedule_id}`\n"
                   f"• Group: `{group_name}`\n"
                   f"• Type: {schedule_type}\n"
                   f"• Next send: {next_run.strftime('%Y-%m-%d %H:%M')} GMT ({time_until})\n"
                   f"• Message preview: {message[:100]}{'...' if len(message) > 100 else ''}")

@bot.command(name='schedules')
async def list_schedules(ctx):
    """List all scheduled messages"""
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not bot.scheduled_messages:
        await ctx.send("📋 **Scheduled Messages:** None configured\n\nUse `!announce schedule` to create one.")
        return
    
    embed = discord.Embed(title="📋 Scheduled Messages", color=discord.Color.green())
    
    for schedule_id, sched in bot.scheduled_messages.items():
        status = "🟢 Active" if sched.get('active', True) else "🔴 Paused"
        next_run = sched.get('next_run')
        time_until = bot.format_time_until(next_run) if next_run else "N/A"
        next_run_str = next_run.strftime('%Y-%m-%d %H:%M GMT') if next_run else "N/A"
        
        # Format schedule type
        sched_type = sched.get('type', 'unknown')
        config = sched.get('config', {})
        if sched_type == 'hourly':
            freq = f"Every {config.get('hours', 1)}h"
        elif sched_type == 'daily':
            freq = f"Daily at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        elif sched_type == 'weekly':
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            freq = f"Weekly on {days[config.get('day', 0)]} at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        else:
            freq = sched_type
        
        message_preview = sched.get('message', '')[:50] + ('...' if len(sched.get('message', '')) > 50 else '')
        
        embed.add_field(
            name=f"`{schedule_id}` → {sched.get('group', 'unknown')} {status}",
            value=f"**{freq}**\nNext: {next_run_str} ({time_until})\nMsg: {message_preview}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name='preview')
async def preview_schedule(ctx, schedule_id: str = None):
    """Preview a scheduled message and time until sent"""
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not schedule_id:
        await ctx.send("Usage: `!announce preview <schedule_id>`")
        return
    
    if schedule_id not in bot.scheduled_messages:
        await ctx.send(f"❌ Schedule `{schedule_id}` not found.")
        return
    
    sched = bot.scheduled_messages[schedule_id]
    next_run = sched.get('next_run')
    time_until = bot.format_time_until(next_run) if next_run else "N/A"
    
    # Format schedule info
    sched_type = sched.get('type', 'unknown')
    config = sched.get('config', {})
    if sched_type == 'hourly':
        freq = f"Every {config.get('hours', 1)} hours"
    elif sched_type == 'daily':
        freq = f"Daily at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
    elif sched_type == 'weekly':
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        freq = f"Every {days[config.get('day', 0)]} at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
    else:
        freq = sched_type
    
    group_name = sched.get('group', 'unknown')
    channel_count = len(bot.channel_groups.get(group_name, []))
    
    embed = discord.Embed(title=f"📋 Schedule Preview: `{schedule_id}`", color=discord.Color.blue())
    embed.add_field(name="Group", value=f"`{group_name}` ({channel_count} channels)", inline=True)
    embed.add_field(name="Frequency", value=freq, inline=True)
    embed.add_field(name="Status", value="🟢 Active" if sched.get('active', True) else "🔴 Paused", inline=True)
    embed.add_field(name="Next Send", value=f"{next_run.strftime('%Y-%m-%d %H:%M GMT') if next_run else 'N/A'}", inline=True)
    embed.add_field(name="⏰ Time Until", value=f"**{time_until}**", inline=True)
    embed.add_field(name="Message", value=sched.get('message', 'No message')[:1024], inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='cancel')
async def cancel_schedule(ctx, schedule_id: str = None):
    """Cancel a scheduled message"""
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not schedule_id:
        await ctx.send("Usage: `!announce cancel <schedule_id>`")
        return
    
    if schedule_id not in bot.scheduled_messages:
        await ctx.send(f"❌ Schedule `{schedule_id}` not found.")
        return
    
    del bot.scheduled_messages[schedule_id]
    bot.save_scheduled_messages()
    await ctx.send(f"✅ Cancelled schedule `{schedule_id}`")

@bot.command(name='send')
async def send_now(ctx, group_name: str = None, *, message: str = None):
    """Send an immediate broadcast to a group
    
    Usage: !announce send <group> <message>
    """
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ This command only works in DMs for security.")
        return
    
    if not check_dm_permission(ctx):
        await ctx.send("❌ You don't have permission to use announce commands.")
        return
    
    if not group_name:
        await ctx.send("Usage: `!announce send <group> <message>`")
        return
    
    if group_name not in bot.channel_groups:
        await ctx.send(f"❌ Group `{group_name}` doesn't exist.")
        return
    
    if not message:
        # Prompt for message
        bot.dm_conversations[ctx.author.id] = {
            'state': 'awaiting_broadcast_message',
            'data': {'group': group_name}
        }
        await ctx.send(f"📝 Please send the message you want to broadcast to group `{group_name}`:")
        return
    
    channel_ids = bot.channel_groups[group_name]
    if not channel_ids:
        await ctx.send(f"❌ Group `{group_name}` has no channels.")
        return
    
    sent_count = 0
    failed_count = 0
    
    await ctx.send(f"📤 Broadcasting to {len(channel_ids)} channels...")
    
    for channel_id in channel_ids:
        try:
            channel = bot.get_channel(channel_id)
            if channel:
                await channel.send(message)
                sent_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"Error sending to channel {channel_id}: {e}")
            failed_count += 1
    
    await ctx.send(f"✅ **Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}")

@bot.command(name='ahelp')
async def announce_help(ctx):
    """Show announcement bot help"""
    if not isinstance(ctx.channel, discord.DMChannel):
        await ctx.send("⚠️ Announcement commands only work in DMs. Please DM me!")
        return
    
    embed = discord.Embed(
        title="📢 Announcement Bot - Help",
        description="Manage channel groups and schedule announcements",
        color=discord.Color.purple()
    )
    
    embed.add_field(
        name="👥 User Management (Owner only)",
        value="`!announce users` - List allowed users\n"
              "`!announce users add <id>` - Add user\n"
              "`!announce users remove <id>` - Remove user",
        inline=False
    )
    
    embed.add_field(
        name="📁 Channel Groups",
        value="`!announce groups` - List all groups\n"
              "`!announce group create <name>` - Create group\n"
              "`!announce group delete <name>` - Delete group\n"
              "`!announce group add <name> <channel_id>` - Add channel\n"
              "`!announce group remove <name> <channel_id>` - Remove channel",
        inline=False
    )
    
    embed.add_field(
        name="⏰ Scheduling",
        value="`!announce schedule <group> hourly <N> [msg]` - Every N hours\n"
              "`!announce schedule <group> daily <HH:MM> [msg]` - Daily at time (GMT)\n"
              "`!announce schedule <group> weekly <day> <HH:MM> [msg]` - Weekly\n"
              "`!announce schedules` - List all schedules\n"
              "`!announce preview <id>` - Preview schedule + time until sent\n"
              "`!announce cancel <id>` - Cancel a schedule",
        inline=False
    )
    
    embed.add_field(
        name="📤 Immediate Broadcast",
        value="`!announce send <group> <message>` - Send now",
        inline=False
    )
    
    embed.set_footer(text="All times are in GMT/UTC")
    
    await ctx.send(embed=embed)

@bot.event
async def on_message(message):
    """Handle DM conversations for multi-step commands"""
    # Ignore bot's own messages
    if message.author == bot.user:
        return
    
    # Check if this is a DM and user has an active conversation
    if isinstance(message.channel, discord.DMChannel):
        user_id = message.author.id
        
        if user_id in bot.dm_conversations:
            conv = bot.dm_conversations[user_id]
            state = conv.get('state')
            data = conv.get('data', {})
            
            if state == 'awaiting_message':
                # Complete the schedule creation
                schedule_id = data['schedule_id']
                next_run = bot.calculate_next_run(data['type'], data['config'])
                
                bot.scheduled_messages[schedule_id] = {
                    'group': data['group'],
                    'type': data['type'],
                    'config': data['config'],
                    'message': message.content,
                    'next_run': next_run,
                    'active': True,
                    'created_by': user_id
                }
                bot.save_scheduled_messages()
                
                time_until = bot.format_time_until(next_run)
                await message.channel.send(
                    f"✅ **Schedule Created!**\n"
                    f"• ID: `{schedule_id}`\n"
                    f"• Group: `{data['group']}`\n"
                    f"• Type: {data['type']}\n"
                    f"• Next send: {next_run.strftime('%Y-%m-%d %H:%M')} GMT ({time_until})\n"
                    f"• Message preview: {message.content[:100]}{'...' if len(message.content) > 100 else ''}"
                )
                
                del bot.dm_conversations[user_id]
                return
            
            elif state == 'awaiting_broadcast_message':
                # Send the broadcast
                group_name = data['group']
                channel_ids = bot.channel_groups.get(group_name, [])
                
                sent_count = 0
                failed_count = 0
                
                await message.channel.send(f"📤 Broadcasting to {len(channel_ids)} channels...")
                
                for channel_id in channel_ids:
                    try:
                        channel = bot.get_channel(channel_id)
                        if channel:
                            await channel.send(message.content)
                            sent_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        print(f"Error sending to channel {channel_id}: {e}")
                        failed_count += 1
                
                await message.channel.send(f"✅ **Broadcast Complete!**\n• Sent: {sent_count}\n• Failed: {failed_count}")
                
                del bot.dm_conversations[user_id]
                return
    
    # Process commands as normal
    await bot.process_commands(message)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name} ({bot.user.id})')
    print('------')
    
    # Auto-subscribe to the configured channel if not already subscribed
    for guild in bot.guilds:
        channel = discord.utils.get(guild.text_channels, name=AUTO_SUBSCRIBE_CHANNEL_NAME)
        if channel and channel.id not in bot.subscriptions:
            bot.subscriptions[channel.id] = {
                'url': AUTO_SUBSCRIBE_RSS_URL,
                'labels': AUTO_SUBSCRIBE_LABELS.copy(),
                'last_checked': datetime.now()
            }
            bot.seen_issues[channel.id] = set()
            bot.save_subscriptions()
            print(f'[GitLab] Auto-subscribed #{AUTO_SUBSCRIBE_CHANNEL_NAME} in {guild.name}')
            print(f'[GitLab] Filtering for labels: {", ".join(sorted(AUTO_SUBSCRIBE_LABELS))}')
    
    print(f'[GitLab] Monitoring {len(bot.subscriptions)} RSS feed(s)')
    print(f'[Announce] {len(bot.channel_groups)} channel group(s)')
    print(f'[Announce] {len(bot.scheduled_messages)} scheduled message(s)')
    print(f'[Announce] {len(bot.allowed_users)} allowed user(s)')
    if BOT_OWNER_ID:
        print(f'[Announce] Bot owner ID: {BOT_OWNER_ID}')
    else:
        print(f'[Announce] ⚠️ BOT_OWNER_ID not set in .env!')
    print('------')

# Run the bot
if __name__ == '__main__':
    if not DISCORD_TOKEN:
        print("Error: DISCORD_BOT_TOKEN environment variable not set")
        exit(1)
    
    bot.run(DISCORD_TOKEN)