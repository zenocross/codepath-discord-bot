"""Weekly check-in module for student questionnaires.

Commands:
    !checkin                 - Start the weekly check-in questionnaire (DM only)
    !checkin status          - View current week's check-in status
    !checkin modify          - Modify current week's check-in responses
    !checkin report          - View check-in report (Admin only)
    !checkin report_download - Download check-ins as CSV (Admin only)
    !checkin post [channel]  - Post check-in prompt with reaction (Admin only)
    !checkin weekly          - Manage scheduled weekly check-in posts (Admin only)
    !checkin help            - Show help for check-in commands
"""

import csv
import io
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import discord
from discord.ext import commands
from discord import ui


# ==================== Constants ====================

PHASES = {
    "1": "Phase 1: Issue Selection",
    "2": "Phase 2: Reproduction & Planning",
    "3": "Phase 3: Implementation",
    "4": "Phase 4: Submission & Iteration"
}

SUPPORT_OPTIONS = [
    ("office_hours", "Office hours guidance"),
    ("technical_resource", "Technical resource/tutorial"),
    ("mentor_pairing", "Mentor pairing"),
    ("one_on_one", "1:1 session with Cam")
]

CHECKIN_DATA_FILE = os.path.join('data', 'checkins.json')
CHECKIN_SETTINGS_FILE = os.path.join('data', '_checkin_settings.json')

# Reaction emoji for check-in
CHECKIN_EMOJI = "📋"


# ==================== Data Management ====================

def load_checkin_data() -> Dict[str, Any]:
    """Load check-in data from JSON file."""
    if os.path.exists(CHECKIN_DATA_FILE):
        try:
            with open(CHECKIN_DATA_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_checkin_data(data: Dict[str, Any]) -> None:
    """Save check-in data to JSON file."""
    os.makedirs(os.path.dirname(CHECKIN_DATA_FILE), exist_ok=True)
    with open(CHECKIN_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def load_checkin_settings() -> Dict[str, Any]:
    """Load check-in settings (reaction message ID, etc.)."""
    if os.path.exists(CHECKIN_SETTINGS_FILE):
        try:
            with open(CHECKIN_SETTINGS_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_checkin_settings(settings: Dict[str, Any]) -> None:
    """Save check-in settings."""
    os.makedirs(os.path.dirname(CHECKIN_SETTINGS_FILE), exist_ok=True)
    with open(CHECKIN_SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=2)


def get_current_week(start_date: Optional[datetime] = None) -> int:
    """Calculate the current week number based on program start date.
    
    Uses the same start_date as the tracker module from _tracker_settings.json.
    Week transitions occur on Wednesday at 5PM UTC (12PM EST / 9AM PST).
    """
    from utils.time_utils import get_program_week
    
    if start_date is None:
        # Use the tracker's settings file (same as !tracker start_date)
        settings_file = os.path.join('data', 'uploads', '_tracker_settings.json')
        if os.path.exists(settings_file):
            try:
                with open(settings_file, 'r') as f:
                    data = json.load(f)
                    date_str = data.get('start_date', '')
                    # Handle both formats: "2026-02-23" and "2026-02-23T00:00:00"
                    if 'T' in date_str:
                        start_date = datetime.fromisoformat(date_str)
                    else:
                        start_date = datetime.strptime(date_str, '%Y-%m-%d')
            except:
                start_date = datetime.now()
        else:
            start_date = datetime.now()
    
    return get_program_week(start_date)


def get_user_checkin(user_id: int, week: int) -> Optional[Dict[str, Any]]:
    """Get a user's check-in data for a specific week."""
    data = load_checkin_data()
    user_key = str(user_id)
    week_key = f"week_{week}"
    
    if user_key in data and week_key in data[user_key]:
        return data[user_key][week_key]
    return None


def save_user_checkin(user_id: int, week: int, checkin_data: Dict[str, Any]) -> None:
    """Save a user's check-in data for a specific week."""
    data = load_checkin_data()
    user_key = str(user_id)
    week_key = f"week_{week}"
    
    if user_key not in data:
        data[user_key] = {}
    
    data[user_key][week_key] = {
        **checkin_data,
        'submitted_at': datetime.now().isoformat(),
        'week': week
    }
    
    save_checkin_data(data)


# ==================== UI Components ====================

class PhaseSelect(ui.Select):
    """Select menu for choosing current phase."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label=f"Phase {num}",
                description=desc.split(": ")[1] if ": " in desc else desc,
                value=num
            )
            for num, desc in PHASES.items()
        ]
        super().__init__(
            placeholder="Select your current phase...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.phase = self.values[0]
        self.view.phase_label = PHASES[self.values[0]]
        
        # Update the view to show block status question
        await self.view.show_block_question(interaction)


class BlockStatusSelect(ui.Select):
    """Select menu for blocked status."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Yes, I'm blocked/stuck",
                description="I need some help or support",
                value="true",
                emoji="🚧"
            ),
            discord.SelectOption(
                label="No, I'm good",
                description="Making progress without issues",
                value="false",
                emoji="✅"
            )
        ]
        super().__init__(
            placeholder="Are you blocked or stuck on something?",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.blocked = self.values[0] == "true"
        
        if self.view.blocked:
            # Show support options
            await self.view.show_support_options(interaction)
        else:
            # Finish check-in
            self.view.support_needed = []
            await self.view.finish_checkin(interaction)


class SupportSelect(ui.Select):
    """Multi-select for support options."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label=label,
                value=value
            )
            for value, label in SUPPORT_OPTIONS
        ]
        super().__init__(
            placeholder="What kind of support would help? (Select all that apply)",
            min_values=1,
            max_values=len(SUPPORT_OPTIONS),
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.support_needed = self.values
        await self.view.finish_checkin(interaction)


class CheckinView(ui.View):
    """Main view for the check-in questionnaire."""
    
    def __init__(self, user_id: int, week: int, is_modify: bool = False, bot=None, discord_user=None):
        super().__init__(timeout=300)  # 5 minute timeout
        self.user_id = user_id
        self.week = week
        self.is_modify = is_modify
        self.bot = bot
        self.discord_user = discord_user  # Store the discord.User object
        
        # Response data
        self.phase: Optional[str] = None
        self.phase_label: Optional[str] = None
        self.blocked: Optional[bool] = None
        self.support_needed: List[str] = []
        
        # Add initial phase select
        self.add_item(PhaseSelect())
    
    async def show_block_question(self, interaction: discord.Interaction):
        """Show the block status question."""
        # Clear current items and add block status select
        self.clear_items()
        self.add_item(BlockStatusSelect())
        
        embed = discord.Embed(
            title="📋 Weekly Check-in (Step 2/3)",
            description=f"**Phase selected:** {self.phase_label}\n\n**Are you blocked or stuck on something?**",
            color=discord.Color.blue()
        )
        
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def show_support_options(self, interaction: discord.Interaction):
        """Show support options for blocked students."""
        self.clear_items()
        self.add_item(SupportSelect())
        
        embed = discord.Embed(
            title="📋 Weekly Check-in (Step 3/3)",
            description=(
                f"**Phase:** {self.phase_label}\n"
                f"**Blocked:** Yes\n\n"
                "**What kind of support would help you most right now?**\n"
                "Select all that apply:"
            ),
            color=discord.Color.orange()
        )
        
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def finish_checkin(self, interaction: discord.Interaction):
        """Complete the check-in and save data."""
        # Build support labels
        support_labels = []
        for value in self.support_needed:
            for opt_value, opt_label in SUPPORT_OPTIONS:
                if opt_value == value:
                    support_labels.append(opt_label)
                    break
        
        # Get discord username
        discord_name = "Unknown"
        if self.discord_user:
            discord_name = self.discord_user.name
        elif self.bot:
            try:
                user = await self.bot.fetch_user(self.user_id)
                discord_name = user.name
            except:
                pass
        
        # Look up full name from master CSV
        full_name = self._lookup_student_name(discord_name)
        
        # Save the check-in data (including full name)
        checkin_data = {
            'phase': self.phase,
            'phase_label': self.phase_label,
            'blocked': self.blocked,
            'support_needed': self.support_needed,
            'support_labels': support_labels,
            'full_name': full_name,
            'discord_name': discord_name
        }
        
        save_user_checkin(self.user_id, self.week, checkin_data)
        
        # Award community points for new check-ins (not modifies)
        points_awarded = 0
        if not self.is_modify and self.bot:
            try:
                game_cog = self.bot.get_cog('Game')
                if game_cog:
                    # Check if this checkin was already processed
                    checkin_key = f"{self.user_id}_week_{self.week}"
                    processed = set(game_cog.community_state.get('processed_checkins', []))
                    
                    if checkin_key not in processed:
                        if game_cog.award_checkin_points(discord_name, self.week):
                            points_awarded = game_cog.get_checkin_points()
                            # Mark as processed
                            processed.add(checkin_key)
                            game_cog.community_state['processed_checkins'] = list(processed)
                            game_cog._save_community_state()
            except Exception as e:
                print(f"[Checkin] Error awarding community points: {e}")
        
        # Send notification to bot feed
        await self._notify_bot_feed(support_labels, full_name, discord_name)
        
        # Build confirmation message
        self.clear_items()
        
        # Build points message if awarded
        points_msg = ""
        if points_awarded > 0:
            points_msg = f"\n\n🏆 **+{points_awarded} community points** awarded!"
        
        if self.blocked:
            status_text = "🚧 **Blocked** - Support requested"
            support_text = "\n".join([f"  • {label}" for label in support_labels])
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** {status_text}\n\n"
                f"**Support needed:**\n{support_text}\n\n"
                "Your responses have been recorded. A team member will reach out to help!"
                f"{points_msg}"
            )
            color = discord.Color.orange()
        else:
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** ✅ **Good to go!**\n\n"
                "Great! Keep up the good work. Your check-in has been recorded."
                f"{points_msg}"
            )
            color = discord.Color.green()
        
        action_text = "modified" if self.is_modify else "submitted"
        embed = discord.Embed(
            title=f"✅ Week {self.week} Check-in {action_text.title()}!",
            description=description,
            color=color
        )
        embed.set_footer(text="Use !checkin status to view | !checkin modify to change")
        
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()
    
    def _lookup_student_name(self, discord_name: str) -> str:
        """Look up student's full name from master CSV by Discord username.
        
        Returns the full name if found, otherwise 'N/A'.
        """
        if not self.bot:
            return "N/A"
        
        try:
            master_data = self.bot.file_storage.read_file_by_category("master")
            if master_data:
                master_text = master_data.decode('utf-8')
                
                # Preprocess: find the actual header row (containing "Member ID")
                lines = master_text.splitlines()
                header_row_idx = None
                for idx, line in enumerate(lines):
                    if "Member ID" in line or "member_id" in line.lower():
                        header_row_idx = idx
                        break
                
                if header_row_idx is None:
                    print("[Checkin] Could not find header row in master CSV")
                    return "N/A"
                
                # Get lines from header onwards
                data_lines = lines[header_row_idx:]
                
                # Strip leading empty column if present
                if data_lines and data_lines[0].startswith(','):
                    data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
                
                cleaned_csv = '\n'.join(data_lines)
                reader = csv.DictReader(io.StringIO(cleaned_csv))
                
                for row in reader:
                    # Check various possible column names for discord username
                    row_discord = (row.get('Discord Username') or 
                                   row.get('discord_username') or 
                                   row.get('Discord') or '').strip().lower()
                    # Remove @ prefix if present
                    row_discord = row_discord.lstrip('@')
                    compare_name = discord_name.lower().lstrip('@')
                    
                    if row_discord == compare_name:
                        full_name = (row.get('Full Name') or 
                                    row.get('Name') or 
                                    row.get('name') or 
                                    row.get('Student Name') or '').strip()
                        if full_name:
                            return full_name
                        break
        except Exception as e:
            print(f"[Checkin] Error looking up student name: {e}")
        
        return "N/A"
    
    async def _notify_bot_feed(self, support_labels: list, full_name: str, discord_name: str):
        """Send check-in notification to bot feed channel."""
        if not self.bot or not self.bot.dm_feed_channel_id:
            return
        
        feed_channel = self.bot.get_channel(self.bot.dm_feed_channel_id)
        if not feed_channel:
            return
        
        # Build display name
        if full_name and full_name != "N/A":
            display_name = f"**{full_name}** ({discord_name})"
        else:
            display_name = f"**{discord_name}**"
        
        # Build summary
        if self.blocked:
            if support_labels:
                support_text = ", ".join(support_labels)
                summary = f"🚧 Needs help: {support_text}"
            else:
                summary = "🚧 Blocked (no specific support selected)"
            color = discord.Color.orange()
        else:
            summary = "✅ All good!"
            color = discord.Color.green()
        
        # Build embed
        action_text = "modified" if self.is_modify else "submitted"
        embed = discord.Embed(
            title=f"📋 Week {self.week} Check-in {action_text.title()}",
            color=color
        )
        embed.add_field(name="Student", value=display_name, inline=True)
        embed.add_field(name="Phase", value=self.phase_label, inline=True)
        embed.add_field(name="Status", value=summary, inline=False)
        embed.set_footer(text=f"User ID: {self.user_id}")
        
        try:
            await feed_channel.send(embed=embed)
        except Exception as e:
            print(f"[Checkin] Error sending feed notification: {e}")
    
    async def on_timeout(self):
        """Handle view timeout."""
        pass


# ==================== Cog ====================

class CheckinCog(commands.Cog, name="Checkin"):
    """Cog for weekly student check-ins."""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    async def _post_checkin_to_channel(self, channel: discord.TextChannel) -> discord.Message:
        """Post a check-in prompt to a specific channel. Returns the message."""
        current_week = get_current_week()
        
        embed = discord.Embed(
            title=f"📋 Week {current_week} Check-in",
            description=(
                "**Time for your weekly check-in!**\n\n"
                f"React with {CHECKIN_EMOJI} below to start your check-in.\n"
                "I'll DM you a quick questionnaire about your progress.\n\n"
                "*Already completed? Send `!checkin status` to the bot to view your response or `!checkin modify` to change your answers.*"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Check-ins help us track progress and provide support when needed.")
        
        # Send message with @everyone and add reaction
        message = await channel.send(content="@everyone", embed=embed)
        await message.add_reaction(CHECKIN_EMOJI)
        
        # Save message ID and channel ID for reaction tracking
        settings = load_checkin_settings()
        settings['reaction_messages'] = settings.get('reaction_messages', [])
        
        # Add this message to tracked messages
        settings['reaction_messages'].append({
            'message_id': message.id,
            'channel_id': channel.id,
            'guild_id': channel.guild.id if channel.guild else None,
            'week': current_week,
            'posted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        save_checkin_settings(settings)
        return message
    
    def _is_dm(self, ctx: commands.Context) -> bool:
        """Check if the command was sent in DMs."""
        return isinstance(ctx.channel, discord.DMChannel)
    
    async def _do_checkin(self, ctx: commands.Context, is_modify: bool = False):
        """Core check-in logic shared by start and modify commands."""
        if not self._is_dm(ctx):
            await ctx.send("📬 **Please use this command in DMs!**\n\nSend me a direct message with `!checkin` to begin your weekly check-in.")
            return
        
        week = get_current_week()
        existing = get_user_checkin(ctx.author.id, week)
        
        if existing and not is_modify:
            # Already checked in this week
            embed = discord.Embed(
                title="⚠️ Already Checked In",
                description=(
                    f"You've already submitted your Week {week} check-in.\n\n"
                    f"**Phase:** {existing.get('phase_label', 'N/A')}\n"
                    f"**Blocked:** {'Yes' if existing.get('blocked') else 'No'}\n\n"
                    "Use `!checkin modify` to change your responses.\n"
                    "Use `!checkin status` to view full details."
                ),
                color=discord.Color.yellow()
            )
            await ctx.send(embed=embed)
            return
        
        if not existing and is_modify:
            embed = discord.Embed(
                title="📋 No Check-in to Modify",
                description=(
                    f"You haven't submitted a check-in for Week {week} yet.\n\n"
                    "Use `!checkin` to begin your weekly check-in."
                ),
                color=discord.Color.greyple()
            )
            await ctx.send(embed=embed)
            return
        
        # Start the questionnaire
        title = "📋 Modify Week Check-in (Step 1/3)" if is_modify else "📋 Weekly Check-in (Step 1/3)"
        subtitle = "Modify Mode" if is_modify else "Check-in"
        embed = discord.Embed(
            title=title,
            description=(
                f"**Week {week} {subtitle}**\n\n"
                "Let's get your weekly status update!\n\n"
                "**What phase are you currently in?**"
            ),
            color=discord.Color.blue()
        )
        
        view = CheckinView(ctx.author.id, week, is_modify=is_modify, bot=self.bot, discord_user=ctx.author)
        await ctx.send(embed=embed, view=view)
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for !checkin alone (without subcommand)."""
        if message.author.bot:
            return
        
        content = message.content.strip()
        
        # Check for exactly "!checkin" (no subcommand)
        if content.lower() == '!checkin':
            # Create a fake context to reuse the checkin logic
            ctx = await self.bot.get_context(message)
            ctx.prefix = '!checkin'
            await self._do_checkin(ctx)
    
    @commands.command(name='status')
    async def checkin_status(self, ctx: commands.Context):
        """View your current week's check-in status.
        
        Usage: !checkin status
        """
        week = get_current_week()
        checkin = get_user_checkin(ctx.author.id, week)
        
        if not checkin:
            embed = discord.Embed(
                title="📋 No Check-in Found",
                description=(
                    f"You haven't submitted a check-in for Week {week} yet.\n\n"
                    "Use `!checkin` to begin your weekly check-in."
                ),
                color=discord.Color.greyple()
            )
            await ctx.send(embed=embed)
            return
        
        # Build status display
        if checkin.get('blocked'):
            status_text = "🚧 Blocked - Support requested"
            support_labels = checkin.get('support_labels', [])
            support_text = "\n".join([f"  • {label}" for label in support_labels]) if support_labels else "  • None specified"
            
            description = (
                f"**Phase:** {checkin.get('phase_label', 'N/A')}\n"
                f"**Status:** {status_text}\n\n"
                f"**Support needed:**\n{support_text}"
            )
            color = discord.Color.orange()
        else:
            description = (
                f"**Phase:** {checkin.get('phase_label', 'N/A')}\n"
                f"**Status:** ✅ Good to go!"
            )
            color = discord.Color.green()
        
        submitted_at = checkin.get('submitted_at', '')
        if submitted_at:
            try:
                dt = datetime.fromisoformat(submitted_at)
                submitted_str = dt.strftime("%m/%d/%Y %I:%M %p")
            except:
                submitted_str = submitted_at
        else:
            submitted_str = "Unknown"
        
        embed = discord.Embed(
            title=f"📋 Week {week} Check-in Status",
            description=description,
            color=color
        )
        embed.set_footer(text=f"Submitted: {submitted_str} | Use !checkin modify to change")
        
        await ctx.send(embed=embed)
    
    @commands.command(name='modify')
    async def modify_checkin(self, ctx: commands.Context):
        """Modify your current week's check-in responses.
        
        Usage: !checkin modify
        Note: This command only works in DMs.
        """
        await self._do_checkin(ctx, is_modify=True)
    
    @commands.command(name='report')
    async def checkin_report(self, ctx: commands.Context):
        """View check-in report for all students (Admin only).
        
        Usage: !checkin report
        Shows current week's check-ins and per-week breakdown.
        """
        # Check if user is admin (allowed user or bot owner)
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to view check-in reports.")
            return
        
        data = load_checkin_data()
        current_week = get_current_week()
        
        if not data:
            await ctx.send("📋 **No check-ins recorded yet.**")
            return
        
        # Collect stats per week
        week_stats: Dict[int, Dict[str, Any]] = {}  # week_num -> {total, blocked, phases, users}
        
        for user_id, weeks_data in data.items():
            for week_key, checkin in weeks_data.items():
                if not week_key.startswith('week_'):
                    continue
                
                week_num = int(week_key.replace('week_', ''))
                
                if week_num not in week_stats:
                    week_stats[week_num] = {
                        'total': 0,
                        'blocked': 0,
                        'phases': {'1': 0, '2': 0, '3': 0, '4': 0},
                        'users': []
                    }
                
                week_stats[week_num]['total'] += 1
                if checkin.get('blocked'):
                    week_stats[week_num]['blocked'] += 1
                
                phase = checkin.get('phase', '0')
                if phase in week_stats[week_num]['phases']:
                    week_stats[week_num]['phases'][phase] += 1
                
                # Store user info for current week details
                week_stats[week_num]['users'].append({
                    'user_id': user_id,
                    'phase': checkin.get('phase_label', 'Unknown'),
                    'blocked': checkin.get('blocked', False),
                    'support': checkin.get('support_labels', []),
                    'submitted_at': checkin.get('submitted_at', '')
                })
        
        # Build report
        embed = discord.Embed(
            title="📋 Check-in Report",
            description=f"**Current Week:** {current_week}",
            color=discord.Color.blue()
        )
        
        # Current week details
        if current_week in week_stats:
            cw = week_stats[current_week]
            blocked_pct = (cw['blocked'] / cw['total'] * 100) if cw['total'] > 0 else 0
            
            # Phase breakdown
            phase_lines = []
            for p_num, p_count in cw['phases'].items():
                if p_count > 0:
                    phase_lines.append(f"Phase {p_num}: {p_count}")
            phase_str = " | ".join(phase_lines) if phase_lines else "None"
            
            embed.add_field(
                name=f"📊 Week {current_week} Summary",
                value=(
                    f"**Total Check-ins:** {cw['total']}\n"
                    f"**Blocked:** {cw['blocked']} ({blocked_pct:.0f}%)\n"
                    f"**By Phase:** {phase_str}"
                ),
                inline=False
            )
            
            # List all users who checked in this week
            all_users_lines = []
            for u in cw['users'][:15]:  # Limit to 15
                status_icon = "🚧" if u['blocked'] else "✅"
                # Extract short phase (e.g., "Phase 2" from "Phase 2: Reproduction & Planning")
                phase_short = u['phase'].split(':')[0] if ':' in u['phase'] else u['phase']
                all_users_lines.append(f"{status_icon} <@{u['user_id']}> - {phase_short}")
            
            if len(cw['users']) > 15:
                all_users_lines.append(f"... and {len(cw['users']) - 15} more")
            
            embed.add_field(
                name=f"👥 Week {current_week} Check-ins",
                value="\n".join(all_users_lines) if all_users_lines else "None",
                inline=False
            )
            
            # List blocked students with support details
            blocked_users = [u for u in cw['users'] if u['blocked']]
            if blocked_users:
                blocked_lines = []
                for u in blocked_users[:10]:  # Limit to 10
                    support_str = ", ".join(u['support'][:2]) if u['support'] else "No support specified"
                    blocked_lines.append(f"• <@{u['user_id']}> - {support_str}")
                
                if len(blocked_users) > 10:
                    blocked_lines.append(f"... and {len(blocked_users) - 10} more")
                
                embed.add_field(
                    name="🚧 Blocked - Support Needed",
                    value="\n".join(blocked_lines),
                    inline=False
                )
        else:
            embed.add_field(
                name=f"📊 Week {current_week}",
                value="No check-ins yet this week.",
                inline=False
            )
        
        # Per-week breakdown (all weeks)
        if week_stats:
            breakdown_lines = []
            for week_num in sorted(week_stats.keys(), reverse=True):
                ws = week_stats[week_num]
                marker = "← current" if week_num == current_week else ""
                breakdown_lines.append(
                    f"**Week {week_num}:** {ws['total']} check-ins, {ws['blocked']} blocked {marker}"
                )
            
            embed.add_field(
                name="📈 All Weeks Breakdown",
                value="\n".join(breakdown_lines[:10]) if breakdown_lines else "No data",
                inline=False
            )
        
        # Total unique users
        total_unique = len(data)
        embed.set_footer(text=f"Total unique users who checked in: {total_unique}")
        
        await ctx.send(embed=embed)
    
    @commands.command(name='report_download')
    async def checkin_report_download(self, ctx: commands.Context):
        """Download check-in data as CSV (Admin only).
        
        Usage: !checkin report_download
        """
        import csv
        import io
        
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to download check-in reports.")
            return
        
        data = load_checkin_data()
        
        if not data:
            await ctx.send("📋 **No check-ins recorded yet.**")
            return
        
        # Build CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow(['User ID', 'Week', 'Phase', 'Phase Label', 'Blocked', 'Support Needed', 'Submitted At'])
        
        # Data rows
        for user_id, weeks_data in data.items():
            for week_key, checkin in weeks_data.items():
                if not week_key.startswith('week_'):
                    continue
                
                week_num = week_key.replace('week_', '')
                support_str = "; ".join(checkin.get('support_labels', []))
                
                writer.writerow([
                    user_id,
                    week_num,
                    checkin.get('phase', ''),
                    checkin.get('phase_label', ''),
                    'Yes' if checkin.get('blocked') else 'No',
                    support_str,
                    checkin.get('submitted_at', '')
                ])
        
        # Send as file
        csv_bytes = output.getvalue().encode('utf-8')
        file = discord.File(fp=io.BytesIO(csv_bytes), filename='checkin_report.csv')
        
        current_week = get_current_week()
        await ctx.send(
            f"📋 **Check-in Report Download**\n"
            f"Current Week: {current_week}\n"
            f"Total Records: {sum(len(w) for w in data.values())}",
            file=file
        )
    
    @commands.command(name='post')
    async def checkin_post(self, ctx: commands.Context, channel_id: Optional[str] = None):
        """Post a check-in prompt message with reaction (Admin only).
        
        Usage: 
            !checkin post              - Post in current channel
            !checkin post <channel_id> - Post in specified channel
        
        Users can react to start their weekly check-in via DM.
        """
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to post check-in prompts.")
            return
        
        # Determine target channel
        if channel_id:
            # Remove <# and > if user used channel mention format
            channel_id_clean = channel_id.strip('<#>').strip()
            try:
                target_channel = self.bot.get_channel(int(channel_id_clean))
                if target_channel is None:
                    target_channel = await self.bot.fetch_channel(int(channel_id_clean))
            except (ValueError, discord.NotFound, discord.Forbidden):
                await ctx.send(f"❌ Could not find channel with ID `{channel_id}`. Make sure the bot has access.")
                return
        else:
            target_channel = ctx.channel
        
        try:
            await self._post_checkin_to_channel(target_channel)
            
            if channel_id:
                await ctx.send(f"✅ Check-in prompt posted in <#{target_channel.id}>! Users can react with {CHECKIN_EMOJI} to start.", delete_after=10)
            else:
                await ctx.send(f"✅ Check-in prompt posted! Users can react with {CHECKIN_EMOJI} to start.", delete_after=5)
        except discord.Forbidden:
            await ctx.send(f"❌ I don't have permission to post in <#{target_channel.id}>.")
        except Exception as e:
            await ctx.send(f"❌ Failed to post check-in: {e}")
    
    @commands.command(name='weekly')
    async def checkin_weekly(self, ctx: commands.Context, action: Optional[str] = None, *args):
        """Schedule automatic weekly check-in posts (Admin only).
        
        Usage:
            !checkin weekly                     - View current schedule
            !checkin weekly set <channel_id> <day> <HH:MM>
                                                - Set schedule (day: mon/tue/wed/thu/fri/sat/sun)
            !checkin weekly off                 - Disable scheduled posts
            !checkin weekly reset               - Reset "already posted" flag (for testing)
        
        Examples:
            !checkin weekly set 123456789 wed 09:00
            !checkin weekly set #general wed 14:30
        """
        from services.scheduler_service import SchedulerService
        from utils.time_utils import format_time_until
        
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to manage check-in schedules.")
            return
        
        CHECKIN_SCHEDULE_ID = "checkin_weekly"
        
        # View current schedule
        if action is None:
            if CHECKIN_SCHEDULE_ID not in self.bot.scheduled_messages:
                await ctx.send("📅 **Check-in Schedule:** Not configured\n\nUse `!checkin weekly set <channel_id> <day> <HH:MM>` to set up.")
                return
            
            sched = self.bot.scheduled_messages[CHECKIN_SCHEDULE_ID]
            if not sched.get('active', True):
                await ctx.send("📅 **Check-in Schedule:** Disabled\n\nUse `!checkin weekly set ...` to re-enable.")
                return
            
            config = sched.get('config', {})
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_name = day_names[config.get('day', 0)]
            time_str = f"{config.get('hour', 9):02d}:{config.get('minute', 0):02d}"
            channel_id = sched.get('channel_id')
            next_run = sched.get('next_run')
            
            embed = discord.Embed(
                title="📅 Check-in Schedule",
                color=discord.Color.green()
            )
            embed.add_field(name="Status", value="✅ Enabled", inline=True)
            embed.add_field(name="Channel", value=f"<#{channel_id}>", inline=True)
            embed.add_field(name="Day & Time", value=f"{day_name} at {time_str} UTC", inline=True)
            
            if next_run:
                time_until = format_time_until(next_run)
                embed.add_field(name="Next Post", value=time_until, inline=True)
            
            await ctx.send(embed=embed)
            return
        
        # Disable schedule
        if action.lower() == 'off':
            if CHECKIN_SCHEDULE_ID in self.bot.scheduled_messages:
                del self.bot.scheduled_messages[CHECKIN_SCHEDULE_ID]
                self.bot.save_scheduled_messages()
            await ctx.send("✅ Check-in schedule disabled.")
            return
        
        # Set schedule
        if action.lower() == 'set':
            if len(args) < 3:
                await ctx.send("❌ Usage: `!checkin weekly set <channel_id> <day> <HH:MM>`\n"
                              "Example: `!checkin weekly set 123456789 wed 09:00`")
                return
            
            channel_arg, day_arg, time_arg = args[0], args[1], args[2]
            
            # Parse channel
            channel_id_clean = channel_arg.strip('<#>').strip()
            try:
                channel = self.bot.get_channel(int(channel_id_clean))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(channel_id_clean))
                channel_id = channel.id
            except (ValueError, discord.NotFound, discord.Forbidden):
                await ctx.send(f"❌ Could not find channel `{channel_arg}`. Make sure the bot has access.")
                return
            
            # Parse day
            day_map = {
                'mon': 0, 'monday': 0,
                'tue': 1, 'tuesday': 1,
                'wed': 2, 'wednesday': 2,
                'thu': 3, 'thursday': 3,
                'fri': 4, 'friday': 4,
                'sat': 5, 'saturday': 5,
                'sun': 6, 'sunday': 6
            }
            day_lower = day_arg.lower()
            if day_lower not in day_map:
                await ctx.send(f"❌ Invalid day `{day_arg}`. Use: mon, tue, wed, thu, fri, sat, sun")
                return
            day_num = day_map[day_lower]
            
            # Parse time
            try:
                time_parts = time_arg.split(':')
                hour = int(time_parts[0])
                minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                if not (0 <= hour <= 23 and 0 <= minute <= 59):
                    raise ValueError("Invalid time range")
            except (ValueError, IndexError):
                await ctx.send(f"❌ Invalid time `{time_arg}`. Use format: HH:MM (e.g., 09:00, 14:30)")
                return
            
            # Create schedule config
            config = {
                'day': day_num,
                'hour': hour,
                'minute': minute
            }
            
            # Calculate next run time
            next_run = SchedulerService.calculate_next_run('weekly', config)
            
            # Save to bot's scheduled_messages (uses existing scheduler system)
            self.bot.scheduled_messages[CHECKIN_SCHEDULE_ID] = {
                'type': 'weekly',
                'target_type': 'checkin',
                'channel_id': channel_id,
                'config': config,
                'next_run': next_run,
                'active': True,
                'created_by': ctx.author.id
            }
            self.bot.save_scheduled_messages()
            
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            time_until = format_time_until(next_run)
            await ctx.send(
                f"✅ **Check-in schedule set!**\n"
                f"📍 Channel: <#{channel_id}>\n"
                f"📅 Every **{day_names[day_num]}** at **{hour:02d}:{minute:02d} UTC**\n"
                f"⏰ Next post: {time_until}"
            )
            return
        
        # Reset the "already posted" flag for testing
        if action.lower() == 'reset':
            settings = load_checkin_settings()
            settings['last_scheduled_week'] = 0
            save_checkin_settings(settings)
            await ctx.send("✅ Check-in schedule reset. The next scheduled time will post even if already posted this week.")
            return
        
        await ctx.send("❌ Unknown action. Use `!checkin weekly`, `!checkin weekly set ...`, `!checkin weekly off`, or `!checkin weekly reset`")
    
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Handle reactions to check-in prompt messages."""
        # Ignore bot's own reactions
        if payload.user_id == self.bot.user.id:
            return
        
        # Check if this is the check-in emoji
        if str(payload.emoji) != CHECKIN_EMOJI:
            return
        
        # Check if this message is a tracked check-in prompt
        settings = load_checkin_settings()
        tracked_messages = settings.get('reaction_messages', [])
        
        message_ids = [m['message_id'] for m in tracked_messages]
        if payload.message_id not in message_ids:
            return
        
        # Get the user
        user = self.bot.get_user(payload.user_id)
        if user is None:
            try:
                user = await self.bot.fetch_user(payload.user_id)
            except discord.NotFound:
                return
        
        # Don't process for bots
        if user.bot:
            return
        
        # Check if user already has a check-in this week
        current_week = get_current_week()
        existing = get_user_checkin(str(payload.user_id), current_week)
        
        try:
            if existing:
                # Already has check-in, send reminder
                await user.send(
                    f"📋 You've already completed your Week {current_week} check-in!\n"
                    f"Use `!checkin status` to view it or `!checkin modify` to make changes."
                )
            else:
                # Start the check-in process via DM
                await user.send(
                    f"📋 **Week {current_week} Check-in**\n"
                    f"Let's get your weekly check-in started!"
                )
                
                # Create and send the check-in view
                view = CheckinView(user.id, current_week, bot=self.bot, discord_user=user)
                embed = discord.Embed(
                    title=f"Week {current_week} Check-in",
                    description="**What phase are you currently in?**",
                    color=discord.Color.blue()
                )
                view.message = await user.send(embed=embed, view=view)
        except discord.Forbidden:
            # Can't DM user - try to notify in channel
            try:
                channel = self.bot.get_channel(payload.channel_id)
                if channel:
                    await channel.send(
                        f"<@{payload.user_id}> I couldn't DM you! Please enable DMs from server members, "
                        f"or use `!checkin` in my DMs directly.",
                        delete_after=15
                    )
            except:
                pass


async def setup(bot: commands.Bot):
    """Set up the Checkin cog."""
    await bot.add_cog(CheckinCog(bot))
