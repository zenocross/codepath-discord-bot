"""Discord embed builder utilities."""

from datetime import datetime
from typing import List, Dict, Optional, Any

import discord

from utils.time_utils import format_time_until, format_datetime_gmt


class EmbedBuilder:
    """Factory class for creating Discord embeds."""
    
    # ==================== GitLab RSS Embeds ====================
    
    @staticmethod
    def issue_embed(
        title: str,
        link: str,
        author: str,
        labels: List[str],
        published: str = ''
    ) -> discord.Embed:
        """Create an embed for a GitLab issue notification.
        
        Args:
            title: Issue title
            link: Issue URL
            author: Issue author name
            labels: List of issue labels
            published: Published date string
            
        Returns:
            Configured Discord embed
        """
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
        
        return embed
    
    @staticmethod
    def subscription_status_embed(
        url: str,
        last_checked: datetime,
        issues_tracked: int,
        labels: set
    ) -> discord.Embed:
        """Create an embed showing subscription status.
        
        Args:
            url: RSS feed URL
            last_checked: Last check datetime
            issues_tracked: Number of tracked issues
            labels: Set of filter labels
            
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(
            title="GitLab RSS Subscription Status",
            color=discord.Color.green()
        )
        
        embed.add_field(name="RSS URL", value=url, inline=False)
        embed.add_field(
            name="Last Checked",
            value=last_checked.strftime("%Y-%m-%d %H:%M:%S"),
            inline=True
        )
        embed.add_field(
            name="Issues Tracked",
            value=str(issues_tracked),
            inline=True
        )
        
        if labels:
            label_list = '\n'.join([f"• `{label}`" for label in sorted(labels)])
            embed.add_field(name="Active Label Filters", value=label_list, inline=False)
        else:
            embed.add_field(name="Active Label Filters", value="None (tracking all issues)", inline=False)
        
        return embed
    
    @staticmethod
    def feed_check_results_embed(
        total_entries: int,
        already_seen: int,
        new_matching: int,
        labels_parsed: int,
        sample_issues: List[Dict]
    ) -> discord.Embed:
        """Create an embed showing feed check results.
        
        Args:
            total_entries: Total entries in feed
            already_seen: Number already seen
            new_matching: New entries matching filters
            labels_parsed: Number of entries with labels parsed
            sample_issues: List of sample issue dicts
            
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(
            title="Feed Check Results",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Total in feed", value=str(total_entries), inline=True)
        embed.add_field(name="Already seen", value=str(already_seen), inline=True)
        embed.add_field(name="New & matching", value=str(new_matching), inline=True)
        embed.add_field(name="Labels parsed", value=str(labels_parsed), inline=True)
        
        if sample_issues:
            sample_text = ""
            for s in sample_issues:
                status = "✅" if s['matches'] and s['is_new'] else "❌"
                labels_str = ", ".join(s['labels'][:3]) if s['labels'] else "(no labels)"
                sample_text += f"{status} **{s['title']}...**\n└ Labels: `{labels_str}`\n"
            embed.add_field(name="Sample Issues", value=sample_text[:1024], inline=False)
        else:
            embed.add_field(name="Sample Issues", value="No labels found in entries", inline=False)
        
        return embed
    
    @staticmethod
    def available_labels_embed() -> discord.Embed:
        """Create an embed showing available GitLab labels.
        
        Returns:
            Configured Discord embed
        """
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
        
        return embed
    
    # ==================== Announcement Embeds ====================
    
    @staticmethod
    def channel_groups_embed(
        channel_groups: Dict[str, List[int]],
        get_channel_func
    ) -> discord.Embed:
        """Create an embed showing channel groups.
        
        Args:
            channel_groups: Dict mapping group names to channel ID lists
            get_channel_func: Function to get channel by ID
            
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(title="📋 Channel Groups", color=discord.Color.blue())
        
        for group_name, channel_ids in channel_groups.items():
            if channel_ids:
                channel_list = []
                for cid in channel_ids:
                    channel = get_channel_func(cid)
                    if channel:
                        channel_list.append(f"• #{channel.name} (`{cid}`)")
                    else:
                        channel_list.append(f"• Unknown (`{cid}`)")
                embed.add_field(
                    name=f"**{group_name}** ({len(channel_ids)} channels)",
                    value="\n".join(channel_list)[:1024],
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"**{group_name}** (0 channels)",
                    value="No channels yet",
                    inline=False
                )
        
        return embed
    
    @staticmethod
    def dm_groups_embed(dm_groups: Dict[str, List[Dict]]) -> discord.Embed:
        """Create an embed showing DM groups.
        
        Args:
            dm_groups: Dict mapping group names to lists of user dicts
            
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(title="📬 DM Groups", color=discord.Color.purple())
        
        for group_name, users in dm_groups.items():
            if users:
                user_list = []
                for user_data in users:
                    username = user_data.get('username', 'Unknown')
                    user_id = user_data.get('user_id', '?')
                    user_list.append(f"• {username} (`{user_id}`)")
                embed.add_field(
                    name=f"**{group_name}** ({len(users)} users)",
                    value="\n".join(user_list)[:1024],
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"**{group_name}** (0 users)",
                    value="No users yet",
                    inline=False
                )
        
        return embed
    
    @staticmethod
    def schedules_list_embed(
        scheduled_messages: Dict[str, Dict],
        format_frequency_func
    ) -> discord.Embed:
        """Create an embed listing all scheduled messages.
        
        Args:
            scheduled_messages: Dict of schedule data
            format_frequency_func: Function to format schedule frequency
            
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(title="📋 Scheduled Messages", color=discord.Color.green())
        
        for schedule_id, sched in scheduled_messages.items():
            status = "🟢 Active" if sched.get('active', True) else "🔴 Paused"
            target_type = sched.get('target_type', 'channel')
            target_icon = "📬" if target_type == 'dm' else "📢"
            next_run = sched.get('next_run')
            time_until = format_time_until(next_run)
            next_run_str = format_datetime_gmt(next_run)
            
            freq = format_frequency_func(sched.get('type', 'unknown'), sched.get('config', {}))
            message_preview = sched.get('message', '')[:50] + ('...' if len(sched.get('message', '')) > 50 else '')
            
            embed.add_field(
                name=f"{target_icon} `{schedule_id}` → {sched.get('group', 'unknown')} {status}",
                value=f"**{freq}**\nNext: {next_run_str} ({time_until})\nMsg: {message_preview}",
                inline=False
            )
        
        return embed
    
    @staticmethod
    def schedule_preview_embed(
        schedule_id: str,
        sched: Dict,
        channel_count: int,
        format_frequency_func
    ) -> discord.Embed:
        """Create an embed previewing a scheduled message.
        
        Args:
            schedule_id: Schedule ID
            sched: Schedule data dict
            channel_count: Number of channels in the group
            format_frequency_func: Function to format schedule frequency
            
        Returns:
            Configured Discord embed
        """
        next_run = sched.get('next_run')
        time_until = format_time_until(next_run)
        freq = format_frequency_func(sched.get('type', 'unknown'), sched.get('config', {}))
        group_name = sched.get('group', 'unknown')
        
        embed = discord.Embed(
            title=f"📋 Schedule Preview: `{schedule_id}`",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Group", value=f"`{group_name}` ({channel_count} channels)", inline=True)
        embed.add_field(name="Frequency", value=freq, inline=True)
        embed.add_field(name="Status", value="🟢 Active" if sched.get('active', True) else "🔴 Paused", inline=True)
        embed.add_field(name="Next Send", value=format_datetime_gmt(next_run), inline=True)
        embed.add_field(name="⏰ Time Until", value=f"**{time_until}**", inline=True)
        embed.add_field(name="Message", value=sched.get('message', 'No message')[:1024], inline=False)
        
        return embed
    
    # ==================== Help Embeds ====================
    
    @staticmethod
    def gitlab_help_embed() -> discord.Embed:
        """Create help embed for GitLab RSS commands.
        
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(
            title="🦊 GitLab RSS Bot - Help",
            description="Monitor GitLab issues and filter by labels",
            color=discord.Color.purple()
        )
        
        embed.add_field(
            name="📥 Subscription",
            value="`!gitlab subscribe <rss_url>` - Subscribe this channel to a GitLab RSS feed\n"
                  "`!gitlab unsubscribe` - Unsubscribe this channel from the RSS feed",
            inline=False
        )
        
        embed.add_field(
            name="📺 Channel Management",
            value="`!gitlab channels` - List all channels receiving the feed\n"
                  "`!gitlab addchannel <channel_id>` - Add a channel to receive the feed\n"
                  "`!gitlab removechannel <channel_id>` - Remove a channel from the feed",
            inline=False
        )
        
        embed.add_field(
            name="🏷️ Filtering",
            value="`!gitlab filter <labels...>` - Set label filters (space-separated)\n"
                  "`!gitlab filter` - Clear all filters (receive all issues)\n"
                  "`!gitlab labels` - Show available label options",
            inline=False
        )
        
        embed.add_field(
            name="📊 Status & Debug",
            value="`!gitlab status` - Show current subscription status\n"
                  "`!gitlab check` - Manually check feed and show debug info\n"
                  "`!gitlab debug` - Show raw feed data for debugging",
            inline=False
        )
        
        embed.add_field(
            name="💡 Examples",
            value=(
                "```\n"
                "!gitlab subscribe https://gitlab.com/gitlab-org/gitlab/-/work_items.atom\n"
                "!gitlab addchannel 1234567890\n"
                "!gitlab filter backend type::bug quick-win\n"
                "```"
            ),
            inline=False
        )
        
        embed.set_footer(text="For announcement help, use !announce help")
        
        return embed
    
    @staticmethod
    def announcement_help_embed() -> discord.Embed:
        """Create help embed for announcement commands.
        
        Returns:
            Configured Discord embed
        """
        embed = discord.Embed(
            title="📢 Announcement Bot - Help",
            description="Manage channel groups, DM groups, and schedule announcements",
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
            value="`!announce groups` - List all channel groups\n"
                  "`!announce group create <name>` - Create group\n"
                  "`!announce group delete <name>` - Delete group\n"
                  "`!announce group add <name> <channel_id>` - Add channel\n"
                  "`!announce group remove <name> <channel_id>` - Remove channel",
            inline=False
        )
        
        embed.add_field(
            name="📬 DM Groups",
            value="`!announce dmgroups` - List all DM groups\n"
                  "`!announce dmgroup create <name>` - Create DM group\n"
                  "`!announce dmgroup delete <name>` - Delete DM group\n"
                  "`!announce dmgroup add <name> <username>` - Add user by username/ID\n"
                  "`!announce dmgroup remove <name> <username>` - Remove user",
            inline=False
        )
        
        embed.add_field(
            name="⏰ Scheduling (auto-detects channel vs DM group)",
            value="`!announce schedule <group> minutely <N> [msg]` - Every N minutes\n"
                  "`!announce schedule <group> hourly <N> [msg]` - Every N hours\n"
                  "`!announce schedule <group> daily <HH:MM> [msg]` - Daily (GMT)\n"
                  "`!announce schedule <group> weekly <day> <HH:MM> [msg]` - Weekly",
            inline=False
        )
        
        embed.add_field(
            name="📋 Schedule Management",
            value="`!announce schedules` - List all schedules\n"
                  "`!announce preview <id>` - Preview schedule + time until sent\n"
                  "`!announce cancel <id>` - Cancel a schedule\n"
                  "`!announce cancelall` - Cancel all schedules",
            inline=False
        )
        
        embed.add_field(
            name="📤 Immediate Send (auto-detects target type)",
            value="`!announce send <group> <message>` - Send to group (channel or DM)\n"
                  "`!announce send <channel_id> <message>` - Send to specific channel\n"
                  "`!announce send dm:<user_id> <message>` - Send DM to specific user",
            inline=False
        )
        
        embed.set_footer(text="All times are in GMT/UTC | For GitLab help, use !gitlab help in a channel")
        
        return embed

