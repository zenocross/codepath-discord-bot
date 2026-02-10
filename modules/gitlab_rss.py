"""GitLab RSS commands module (Cog)."""

from datetime import datetime
from typing import TYPE_CHECKING

import aiohttp
import discord
import feedparser
from discord.ext import commands

from bot.config import Config
from services.rss_service import RSSService
from utils.embeds import EmbedBuilder

if TYPE_CHECKING:
    from bot.client import GitLabRSSBot


class GitLabRSSCog(commands.Cog, name="GitLab RSS"):
    """Commands for managing GitLab RSS feed subscriptions."""
    
    def __init__(self, bot: 'GitLabRSSBot'):
        self.bot = bot
    
    @commands.command(name='subscribe')
    async def subscribe(self, ctx: commands.Context, rss_url: str) -> None:
        """Subscribe this channel to a GitLab RSS feed.
        
        Example: !gitlab subscribe https://gitlab.com/gitlab-org/gitlab/-/work_items.atom
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
        
        self.bot.subscriptions[channel_id] = {
            'url': rss_url,
            'labels': set(),
            'last_checked': datetime.now()
        }
        
        self.bot.seen_issues[channel_id] = set()
        self.bot.save_subscriptions()
        
        await ctx.send(
            f"✅ Subscribed to GitLab RSS feed!\n"
            f"This channel will receive all new issues.\n"
            f"Use `!gitlab filter` to customize which labels to track."
        )
    
    @commands.command(name='unsubscribe')
    async def unsubscribe(self, ctx: commands.Context) -> None:
        """Unsubscribe this channel from the RSS feed."""
        channel_id = ctx.channel.id
        
        if channel_id in self.bot.subscriptions:
            del self.bot.subscriptions[channel_id]
            if channel_id in self.bot.seen_issues:
                del self.bot.seen_issues[channel_id]
            self.bot.save_subscriptions()
            await ctx.send("✅ Unsubscribed from GitLab RSS feed.")
        else:
            await ctx.send("❌ This channel is not subscribed to any feed.")
    
    @commands.command(name='filter')
    async def filter_labels(self, ctx: commands.Context, *labels: str) -> None:
        """Set label filters for this channel.
        
        Examples:
        !gitlab filter backend frontend type::bug
        !gitlab filter quick-win community-bonus::100
        !gitlab filter (clears all filters)
        """
        channel_id = ctx.channel.id
        
        if channel_id not in self.bot.subscriptions:
            await ctx.send("❌ This channel is not subscribed to any feed. Use `!gitlab subscribe` first.")
            return
        
        if not labels:
            self.bot.subscriptions[channel_id]['labels'] = set()
            self.bot.save_subscriptions()
            await ctx.send("✅ Cleared all label filters. This channel will receive all issues.")
            return
        
        # Normalize labels (replace spaces with hyphens)
        normalized_labels = {label.replace(' ', '-') for label in labels}
        
        self.bot.subscriptions[channel_id]['labels'] = normalized_labels
        self.bot.save_subscriptions()
        
        label_list = '\n'.join([f"• `{label}`" for label in sorted(normalized_labels)])
        await ctx.send(f"✅ Label filters updated! This channel will only receive issues with these labels:\n{label_list}")
    
    @commands.command(name='status')
    async def status(self, ctx: commands.Context) -> None:
        """Show subscription status for this channel."""
        channel_id = ctx.channel.id
        
        if channel_id not in self.bot.subscriptions:
            await ctx.send("❌ This channel is not subscribed to any feed.")
            return
        
        sub = self.bot.subscriptions[channel_id]
        
        embed = EmbedBuilder.subscription_status_embed(
            url=sub['url'],
            last_checked=sub['last_checked'],
            issues_tracked=len(self.bot.seen_issues.get(channel_id, [])),
            labels=sub['labels']
        )
        
        await ctx.send(embed=embed)
    
    @commands.command(name='check')
    async def check_now(self, ctx: commands.Context) -> None:
        """Manually check the feed and show debug info."""
        channel_id = ctx.channel.id
        
        if channel_id not in self.bot.subscriptions:
            await ctx.send("❌ This channel is not subscribed to any feed.")
            return
        
        sub = self.bot.subscriptions[channel_id]
        await ctx.send("🔍 Checking feed...")
        
        try:
            feed, labels_map = await RSSService.fetch_feed_with_labels(sub['url'])
            total_entries = len(feed.entries)
            
            if channel_id not in self.bot.seen_issues:
                self.bot.seen_issues[channel_id] = set()
            
            new_count = 0
            matching_count = 0
            sample_labels = []
            
            for entry in feed.entries[:10]:  # Check first 10 for debug
                issue_id = entry.get('id', entry.get('link', ''))
                is_new = issue_id not in self.bot.seen_issues[channel_id]
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
            
            embed = EmbedBuilder.feed_check_results_embed(
                total_entries=total_entries,
                already_seen=len(self.bot.seen_issues[channel_id]),
                new_matching=matching_count,
                labels_parsed=len([v for v in labels_map.values() if v]),
                sample_issues=sample_labels
            )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error checking feed: {e}")
    
    @commands.command(name='debug')
    async def debug_feed(self, ctx: commands.Context) -> None:
        """Show raw feed data for debugging."""
        channel_id = ctx.channel.id
        
        if channel_id not in self.bot.subscriptions:
            await ctx.send("❌ This channel is not subscribed to any feed.")
            return
        
        sub = self.bot.subscriptions[channel_id]
        await ctx.send("🔍 Fetching raw feed for debug...")
        
        try:
            raw_xml = await RSSService.fetch_raw_feed(sub['url'])
            
            # Check if <labels> exists anywhere in the feed
            has_labels_tag = '<labels>' in raw_xml
            labels_count = raw_xml.count('<label>')
            
            # Get first entry snippet
            entry_start = raw_xml.find('<entry>')
            entry_end = raw_xml.find('</entry>') + 8
            first_entry = raw_xml[entry_start:entry_end] if entry_start != -1 else "No entry found"
            
            # Truncate for Discord
            first_entry_preview = first_entry[:1500] + "..." if len(first_entry) > 1500 else first_entry
            
            await ctx.send(
                f"**Feed Debug Info:**\n"
                f"• Has `<labels>` tag: {has_labels_tag}\n"
                f"• Total `<label>` tags: {labels_count}\n"
                f"• Feed size: {len(raw_xml)} chars\n\n"
                f"**First entry preview:**\n```xml\n{first_entry_preview}\n```"
            )
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    @commands.command(name='labels')
    async def show_labels(self, ctx: commands.Context) -> None:
        """Show available label options."""
        embed = EmbedBuilder.available_labels_embed()
        await ctx.send(embed=embed)
    
    # ==================== Channel Management ====================
    
    @commands.command(name='channels')
    async def list_channels(self, ctx: commands.Context) -> None:
        """List all channels receiving GitLab feed updates."""
        if not self.bot.subscriptions:
            await ctx.send("📺 **Feed Channels:** None configured\n\nUse `!gitlab addchannel <channel_id>` to add one.")
            return
        
        embed = discord.Embed(
            title="📺 GitLab Feed Channels",
            color=discord.Color.orange()
        )
        
        channel_list = []
        for channel_id, sub_data in self.bot.subscriptions.items():
            channel = self.bot.get_channel(channel_id)
            if channel:
                labels_count = len(sub_data.get('labels', set()))
                labels_info = f"({labels_count} label filters)" if labels_count else "(all issues)"
                channel_list.append(f"• #{channel.name} (`{channel_id}`) {labels_info}")
            else:
                channel_list.append(f"• Unknown (`{channel_id}`)")
        
        embed.add_field(
            name=f"**{len(self.bot.subscriptions)} channel(s)**",
            value="\n".join(channel_list)[:1024] if channel_list else "No channels",
            inline=False
        )
        
        await ctx.send(embed=embed)
    
    @commands.command(name='addchannel')
    async def add_channel(self, ctx: commands.Context, channel_arg: str = None) -> None:
        """Add a channel to receive GitLab feed updates.
        
        Usage: !gitlab addchannel <channel_id>
        
        The channel will be subscribed to the default GitLab RSS feed with default label filters.
        """
        if not channel_arg:
            await ctx.send("Usage: `!gitlab addchannel <channel_id>`\n\nExample: `!gitlab addchannel 1234567890`")
            return
        
        try:
            channel_id = int(channel_arg.strip('<>#'))
        except ValueError:
            await ctx.send("❌ Invalid channel ID. Must be a number.")
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send(f"⚠️ Channel `{channel_id}` not found. Adding anyway (bot may not have access).")
        
        if channel_id in self.bot.subscriptions:
            channel_name = channel.name if channel else "unknown"
            await ctx.send(f"ℹ️ Channel #{channel_name} (`{channel_id}`) is already subscribed.")
            return
        
        # Subscribe with default settings
        self.bot.subscriptions[channel_id] = {
            'url': Config.AUTO_SUBSCRIBE_RSS_URL,
            'labels': Config.AUTO_SUBSCRIBE_LABELS.copy(),
            'last_checked': datetime.now()
        }
        self.bot.seen_issues[channel_id] = set()
        self.bot.save_subscriptions()
        
        channel_name = channel.name if channel else "unknown"
        labels_list = ', '.join(sorted(list(Config.AUTO_SUBSCRIBE_LABELS)[:5])) + "..."
        await ctx.send(
            f"✅ Added #{channel_name} (`{channel_id}`) to GitLab feed!\n"
            f"• Feed: Default GitLab work items\n"
            f"• Labels: {labels_list}\n\n"
            f"Use `!gitlab filter` in that channel to customize labels."
        )
    
    @commands.command(name='removechannel')
    async def remove_channel(self, ctx: commands.Context, channel_arg: str = None) -> None:
        """Remove a channel from receiving GitLab feed updates.
        
        Usage: !gitlab removechannel <channel_id>
        """
        if not channel_arg:
            await ctx.send("Usage: `!gitlab removechannel <channel_id>`\n\nExample: `!gitlab removechannel 1234567890`")
            return
        
        try:
            channel_id = int(channel_arg.strip('<>#'))
        except ValueError:
            await ctx.send("❌ Invalid channel ID. Must be a number.")
            return
        
        if channel_id not in self.bot.subscriptions:
            await ctx.send(f"ℹ️ Channel `{channel_id}` is not subscribed to any feed.")
            return
        
        channel = self.bot.get_channel(channel_id)
        channel_name = channel.name if channel else "unknown"
        
        del self.bot.subscriptions[channel_id]
        if channel_id in self.bot.seen_issues:
            del self.bot.seen_issues[channel_id]
        self.bot.save_subscriptions()
        
        await ctx.send(f"✅ Removed #{channel_name} (`{channel_id}`) from GitLab feed.")


async def setup(bot: 'GitLabRSSBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(GitLabRSSCog(bot))

