"""Weekly check-in module for student questionnaires.

Commands:
    !checkin                 - Start the weekly check-in questionnaire (DM only)
    !checkin status          - View current week's check-in status
    !checkin modify          - Modify current week's check-in responses
    !checkin report          - View check-in report (Admin only)
    !checkin report_download - Download check-ins as CSV (Admin only)
    !checkin help            - Show help for check-in commands
"""

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


def get_current_week(start_date: Optional[datetime] = None) -> int:
    """Calculate the current week number based on program start date.
    
    Uses the same start_date as the tracker module from _tracker_settings.json.
    """
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
    
    days_since_start = (datetime.now() - start_date).days
    return max(1, (days_since_start // 7) + 1)


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
    
    def __init__(self, user_id: int, week: int, is_modify: bool = False):
        super().__init__(timeout=300)  # 5 minute timeout
        self.user_id = user_id
        self.week = week
        self.is_modify = is_modify
        
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
        
        # Save the check-in data
        checkin_data = {
            'phase': self.phase,
            'phase_label': self.phase_label,
            'blocked': self.blocked,
            'support_needed': self.support_needed,
            'support_labels': support_labels
        }
        
        save_user_checkin(self.user_id, self.week, checkin_data)
        
        # Build confirmation message
        self.clear_items()
        
        if self.blocked:
            status_text = "🚧 **Blocked** - Support requested"
            support_text = "\n".join([f"  • {label}" for label in support_labels])
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** {status_text}\n\n"
                f"**Support needed:**\n{support_text}\n\n"
                "Your responses have been recorded. A team member will reach out to help!"
            )
            color = discord.Color.orange()
        else:
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** ✅ **Good to go!**\n\n"
                "Great! Keep up the good work. Your check-in has been recorded."
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
    
    async def on_timeout(self):
        """Handle view timeout."""
        pass


# ==================== Cog ====================

class CheckinCog(commands.Cog, name="Checkin"):
    """Cog for weekly student check-ins."""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
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
        
        view = CheckinView(ctx.author.id, week, is_modify=is_modify)
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


async def setup(bot: commands.Bot):
    """Set up the Checkin cog."""
    await bot.add_cog(CheckinCog(bot))
