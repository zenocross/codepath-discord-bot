"""Weekly check-in module for student questionnaires.

Commands:
    !checkin                 - Start the weekly check-in questionnaire (DM only)
    !checkin status          - View current week's check-in status
    !checkin modify          - Modify current week's check-in responses
    !checkin report          - View check-in report (Admin only)
    !checkin report_download - Download check-ins as CSV (Admin only)
    !checkin nps_download    - Download NPS survey report as CSV (Admin only)
    !checkin preview_post    - Preview check-in post with timing info (Admin only)
    !checkin post <channel> [utc_timestamp] - Post check-in prompt (Admin only)
    !checkin weekly          - Manage scheduled weekly check-in posts (Admin only)
    !checkin help            - Show help for check-in commands

Notes:
    - Most weeks: 3-step questionnaire (phase, block status, support options)
    - Week 5 only: 7-step questionnaire with additional survey questions (NPS, confidence, proficiency)
    - The post command supports mocked UTC time for testing: !checkin post #channel 2026-03-26T18:00:00
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

# NPS Scale (0-10)
NPS_SCALE = [(str(i), str(i)) for i in range(11)]

# Confidence/Proficiency Scales (1-5)
CONFIDENCE_LABELS = {
    "5": "Very Confident",
    "4": "Confident",
    "3": "Moderately confident",
    "2": "Somewhat confident",
    "1": "Not confident"
}

PROFICIENCY_LABELS = {
    "5": "Very proficient",
    "4": "Proficient",
    "3": "Moderately proficient",
    "2": "Somewhat proficient",
    "1": "Not proficient"
}

# Weeks where extended survey questions are shown (specific weeks only)
SURVEY_WEEKS = {5}  # Only week 5 has the NPS survey

# Weeks where event/office hours questions are shown
EVENT_SURVEY_WEEKS = {6}  # Only week 6 has the event questions

# Reasons for not attending (shared between event and office hours questions)
NOT_ATTENDING_REASONS = [
    ("schedule_conflict", "Schedule conflict"),
    ("didnt_know", "Didn't know about it"),
    ("not_valuable", "Don't find it valuable right now"),
    ("other", "Other")
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
            self.view.support_needed = []
            # Continue to survey questions based on week
            if self.view.week in SURVEY_WEEKS:
                await self.view.show_nps_question(interaction)
            elif self.view.week in EVENT_SURVEY_WEEKS:
                await self.view.show_midprogram_question(interaction)
            else:
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
        # Continue to survey questions based on week
        if self.view.week in SURVEY_WEEKS:
            await self.view.show_nps_question(interaction)
        elif self.view.week in EVENT_SURVEY_WEEKS:
            await self.view.show_midprogram_question(interaction)
        else:
            await self.view.finish_checkin(interaction)


class NPSSelect(ui.Select):
    """Select menu for NPS score (0-10)."""
    
    def __init__(self):
        # 10 to 0, no descriptions
        options = [
            discord.SelectOption(label=str(i), value=str(i))
            for i in range(10, -1, -1)
        ]
        super().__init__(
            placeholder="Select a score from 0 to 10...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.nps_score = int(self.values[0])
        await self.view.show_nps_reason_prompt(interaction)


class NPSReasonModal(ui.Modal, title="Reason for Your Score"):
    """Modal for collecting the reason behind NPS score."""
    
    reason = ui.TextInput(
        label="What is the primary reason for your score?",
        style=discord.TextStyle.paragraph,
        placeholder="Please share your thoughts...",
        required=True,
        min_length=1,
        max_length=1000
    )
    
    def __init__(self, view: 'CheckinView'):
        super().__init__()
        self.checkin_view = view
    
    async def on_submit(self, interaction: discord.Interaction):
        self.checkin_view.nps_reason = self.reason.value.strip() if self.reason.value else "N/A"
        await self.checkin_view.show_opensource_confidence(interaction)


class NPSReasonView(ui.View):
    """View with buttons to enter reason or skip."""
    
    def __init__(self, checkin_view: 'CheckinView'):
        super().__init__(timeout=300)
        self.checkin_view = checkin_view
    
    @ui.button(label="Enter Reason", style=discord.ButtonStyle.primary, emoji="✏️")
    async def enter_reason(self, interaction: discord.Interaction, button: ui.Button):
        """Open the modal to enter reason."""
        modal = NPSReasonModal(self.checkin_view)
        await interaction.response.send_modal(modal)
        # Don't stop the view - user can click again if they cancel the modal
    
    @ui.button(label="Skip (N/A)", style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip_reason(self, interaction: discord.Interaction, button: ui.Button):
        """Skip entering a reason."""
        self.checkin_view.nps_reason = "N/A"
        await self.checkin_view.show_opensource_confidence(interaction)
        self.stop()


class OpenSourceConfidenceSelect(ui.Select):
    """Select menu for open source contribution confidence."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label=f"{value} = {label}",
                value=value
            )
            for value, label in sorted(CONFIDENCE_LABELS.items(), key=lambda x: int(x[0]), reverse=True)
        ]
        super().__init__(
            placeholder="Select your confidence level...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.opensource_confidence = int(self.values[0])
        await self.view.show_gitlab_proficiency(interaction)


class GitLabProficiencySelect(ui.Select):
    """Select menu for GitLab platform proficiency."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label=f"{value} = {label}",
                value=value
            )
            for value, label in sorted(PROFICIENCY_LABELS.items(), key=lambda x: int(x[0]), reverse=True)
        ]
        super().__init__(
            placeholder="Select your proficiency level...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.gitlab_proficiency = int(self.values[0])
        await self.view.show_ai_confidence(interaction)


class AIConfidenceSelect(ui.Select):
    """Select menu for AI tools confidence."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label=f"{value} = {label}",
                value=value
            )
            for value, label in sorted(CONFIDENCE_LABELS.items(), key=lambda x: int(x[0]), reverse=True)
        ]
        super().__init__(
            placeholder="Select your confidence level...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.ai_confidence = int(self.values[0])
        await self.view.finish_checkin(interaction)


# ==================== Week 6 Event Survey Components ====================

class MidprogramEventSelect(ui.Select):
    """Select menu for midprogram event attendance."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Yes, I'm attending",
                value="yes",
                emoji="✅"
            ),
            discord.SelectOption(
                label="No, I can't attend",
                value="no",
                emoji="❌"
            )
        ]
        super().__init__(
            placeholder="Select your answer...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.attending_midprogram = self.values[0] == "yes"
        if self.view.attending_midprogram:
            # Skip reason, go to office hours question
            self.view.midprogram_reason = None
            await self.view.show_office_hours_question(interaction)
        else:
            # Show reason question
            await self.view.show_midprogram_reason(interaction)


class MidprogramReasonSelect(ui.Select):
    """Select menu for reason not attending midprogram event."""
    
    def __init__(self):
        options = [
            discord.SelectOption(label=label, value=value)
            for value, label in NOT_ATTENDING_REASONS
        ]
        super().__init__(
            placeholder="Select the main reason...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.midprogram_reason = self.values[0]
        await self.view.show_office_hours_question(interaction)


class OfficeHoursSelect(ui.Select):
    """Select menu for office hours attendance."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Yes, I have attended",
                value="yes",
                emoji="✅"
            ),
            discord.SelectOption(
                label="No, I haven't attended",
                value="no",
                emoji="❌"
            )
        ]
        super().__init__(
            placeholder="Select your answer...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.attended_office_hours = self.values[0] == "yes"
        if self.view.attended_office_hours:
            # Skip reason, finish checkin
            self.view.office_hours_reason = None
            await self.view.finish_checkin(interaction)
        else:
            # Show reason question
            await self.view.show_office_hours_reason(interaction)


class OfficeHoursReasonSelect(ui.Select):
    """Select menu for reason not attending office hours."""
    
    def __init__(self):
        options = [
            discord.SelectOption(label=label, value=value)
            for value, label in NOT_ATTENDING_REASONS
        ]
        super().__init__(
            placeholder="Select the main reason...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        self.view.office_hours_reason = self.values[0]
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
        
        # Response data - Core questions
        self.phase: Optional[str] = None
        self.phase_label: Optional[str] = None
        self.blocked: Optional[bool] = None
        self.support_needed: List[str] = []
        
        # Response data - Survey questions (only for week 5)
        self.nps_score: Optional[int] = None
        self.nps_reason: str = "N/A"
        self.opensource_confidence: Optional[int] = None
        self.gitlab_proficiency: Optional[int] = None
        self.ai_confidence: Optional[int] = None
        
        # Response data - Event survey questions (only for week 6)
        self.attending_midprogram: Optional[bool] = None
        self.midprogram_reason: Optional[str] = None
        self.attended_office_hours: Optional[bool] = None
        self.office_hours_reason: Optional[str] = None
        
        # Add initial phase select
        self.add_item(PhaseSelect())
    
    @property
    def has_survey(self) -> bool:
        """Check if this week includes the NPS survey questions (week 5)."""
        return self.week in SURVEY_WEEKS
    
    @property
    def has_event_survey(self) -> bool:
        """Check if this week includes the event survey questions (week 6)."""
        return self.week in EVENT_SURVEY_WEEKS
    
    @property
    def total_steps(self) -> int:
        """Get total number of steps based on week."""
        if self.has_survey:
            return 7  # Week 5: 3 core + 4 NPS survey
        elif self.has_event_survey:
            return 5  # Week 6: 3 core + 2 event questions (reasons are sub-questions)
        return 3  # Other weeks: 3 core questions
    
    async def _safe_edit_message(self, interaction: discord.Interaction, embed: discord.Embed, view):
        """Safely edit message with fallback for expired interactions."""
        try:
            if interaction.response.is_done():
                # Interaction already responded, use followup
                await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
            else:
                await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            # Interaction expired, try to edit via the message directly
            try:
                if hasattr(self, 'message') and self.message:
                    await self.message.edit(embed=embed, view=view)
                elif interaction.message:
                    await interaction.message.edit(embed=embed, view=view)
            except Exception as e:
                print(f"[Checkin] Failed to edit message after interaction expired: {e}")
    
    async def show_block_question(self, interaction: discord.Interaction):
        """Show the block status question."""
        # Clear current items and add block status select
        self.clear_items()
        self.add_item(BlockStatusSelect())
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 2/{self.total_steps})",
            description=f"**Phase selected:** {self.phase_label}\n\n**Are you blocked or stuck on something?**",
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_support_options(self, interaction: discord.Interaction):
        """Show support options for blocked students."""
        self.clear_items()
        self.add_item(SupportSelect())
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 3/{self.total_steps})",
            description=(
                f"**Phase:** {self.phase_label}\n"
                f"**Blocked:** Yes\n\n"
                "**What kind of support would help you most right now?**\n"
                "Select all that apply:"
            ),
            color=discord.Color.orange()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_nps_question(self, interaction: discord.Interaction):
        """Show the NPS question (only for week 5+)."""
        self.clear_items()
        self.add_item(NPSSelect())
        
        blocked_str = "🚧 Yes" if self.blocked else "✅ No"
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 4/{self.total_steps})",
            description=(
                f"**Phase:** {self.phase_label}\n"
                f"**Blocked:** {blocked_str}\n\n"
                "**I would recommend the CodePath AI Corps x GitLab program to a friend or colleague.**\n"
                "*(0 = Not at all likely, 10 = Extremely likely)*"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_nps_reason_prompt(self, interaction: discord.Interaction):
        """Show the prompt to enter NPS reason."""
        reason_view = NPSReasonView(self)
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 4/{self.total_steps} - Reason)",
            description=(
                f"**NPS Score:** {self.nps_score}/10\n\n"
                "**What is the primary reason for your score?**\n\n"
                "Click **Enter Reason** to type your response, or **Skip** to continue."
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, reason_view)
    
    async def show_opensource_confidence(self, interaction: discord.Interaction):
        """Show the open source confidence question (only for week 5+)."""
        self.clear_items()
        self.add_item(OpenSourceConfidenceSelect())
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 5/{self.total_steps})",
            description=(
                f"**NPS Score:** {self.nps_score}/10\n"
                f"**Reason:** {self.nps_reason[:50]}{'...' if len(self.nps_reason) > 50 else ''}\n\n"
                "**How confident do you feel contributing to open source projects?**"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_gitlab_proficiency(self, interaction: discord.Interaction):
        """Show the GitLab proficiency question (only for week 5+)."""
        self.clear_items()
        self.add_item(GitLabProficiencySelect())
        
        conf_label = CONFIDENCE_LABELS.get(str(self.opensource_confidence), "Unknown")
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 6/{self.total_steps})",
            description=(
                f"**Open Source Confidence:** {self.opensource_confidence}/5 ({conf_label})\n\n"
                "**How proficient are you at navigating the GitLab platform?**"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_ai_confidence(self, interaction: discord.Interaction):
        """Show the AI tools confidence question (only for week 5+)."""
        self.clear_items()
        self.add_item(AIConfidenceSelect())
        
        prof_label = PROFICIENCY_LABELS.get(str(self.gitlab_proficiency), "Unknown")
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 7/{self.total_steps})",
            description=(
                f"**GitLab Proficiency:** {self.gitlab_proficiency}/5 ({prof_label})\n\n"
                "**How confident are you using AI tools (like Claude Code, ChatGPT, CoPilot, etc.) "
                "to support your coding and technical work?**"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    # ==================== Week 6 Event Survey Methods ====================
    
    async def show_midprogram_question(self, interaction: discord.Interaction):
        """Show the midprogram event attendance question (only for week 6)."""
        self.clear_items()
        self.add_item(MidprogramEventSelect())
        
        blocked_str = "🚧 Yes" if self.blocked else "✅ No"
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 4/{self.total_steps})",
            description=(
                f"**Phase:** {self.phase_label}\n"
                f"**Blocked:** {blocked_str}\n\n"
                "**Are you attending the midprogram event on Thursday?**"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_midprogram_reason(self, interaction: discord.Interaction):
        """Show the reason for not attending midprogram event."""
        self.clear_items()
        self.add_item(MidprogramReasonSelect())
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 4/{self.total_steps} - follow-up)",
            description=(
                "**Attending midprogram event:** ❌ No\n\n"
                "**Why can't you come?**"
            ),
            color=discord.Color.orange()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_office_hours_question(self, interaction: discord.Interaction):
        """Show the office hours attendance question (only for week 6)."""
        self.clear_items()
        self.add_item(OfficeHoursSelect())
        
        midprogram_str = "✅ Yes" if self.attending_midprogram else "❌ No"
        reason_str = ""
        if not self.attending_midprogram and self.midprogram_reason:
            reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == self.midprogram_reason), self.midprogram_reason)
            reason_str = f" ({reason_label})"
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 5/{self.total_steps})",
            description=(
                f"**Attending midprogram event:** {midprogram_str}{reason_str}\n\n"
                "**Have you attended an office hours session with GitLab mentors?**"
            ),
            color=discord.Color.blue()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
    async def show_office_hours_reason(self, interaction: discord.Interaction):
        """Show the reason for not attending office hours."""
        self.clear_items()
        self.add_item(OfficeHoursReasonSelect())
        
        embed = discord.Embed(
            title=f"📋 Weekly Check-in (Step 5/{self.total_steps} - follow-up)",
            description=(
                "**Attended office hours:** ❌ No\n\n"
                "**Why not?**"
            ),
            color=discord.Color.orange()
        )
        
        await self._safe_edit_message(interaction, embed, self)
    
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
        
        # Save the check-in data (including full name and survey responses)
        checkin_data = {
            'phase': self.phase,
            'phase_label': self.phase_label,
            'blocked': self.blocked,
            'support_needed': self.support_needed,
            'support_labels': support_labels,
            'full_name': full_name,
            'discord_name': discord_name,
            # NPS Survey questions (week 5)
            'nps_score': self.nps_score,
            'nps_reason': self.nps_reason,
            'opensource_confidence': self.opensource_confidence,
            'gitlab_proficiency': self.gitlab_proficiency,
            'ai_confidence': self.ai_confidence,
            # Event survey questions (week 6)
            'attending_midprogram': self.attending_midprogram,
            'midprogram_reason': self.midprogram_reason,
            'attended_office_hours': self.attended_office_hours,
            'office_hours_reason': self.office_hours_reason
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
        
        # Build survey summary based on week
        survey_summary = ""
        if self.has_survey:
            os_conf_label = CONFIDENCE_LABELS.get(str(self.opensource_confidence), "N/A")
            gl_prof_label = PROFICIENCY_LABELS.get(str(self.gitlab_proficiency), "N/A")
            ai_conf_label = CONFIDENCE_LABELS.get(str(self.ai_confidence), "N/A")
            
            survey_summary = (
                f"\n\n**📊 Survey Responses:**\n"
                f"• NPS Score: {self.nps_score}/10\n"
                f"• Open Source Confidence: {self.opensource_confidence}/5 ({os_conf_label})\n"
                f"• GitLab Proficiency: {self.gitlab_proficiency}/5 ({gl_prof_label})\n"
                f"• AI Tools Confidence: {self.ai_confidence}/5 ({ai_conf_label})"
            )
        elif self.has_event_survey:
            midprogram_str = "✅ Yes" if self.attending_midprogram else "❌ No"
            if not self.attending_midprogram and self.midprogram_reason:
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == self.midprogram_reason), self.midprogram_reason)
                midprogram_str += f" ({reason_label})"
            
            office_hours_str = "✅ Yes" if self.attended_office_hours else "❌ No"
            if not self.attended_office_hours and self.office_hours_reason:
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == self.office_hours_reason), self.office_hours_reason)
                office_hours_str += f" ({reason_label})"
            
            survey_summary = (
                f"\n\n**📊 Event Survey:**\n"
                f"• Attending midprogram event: {midprogram_str}\n"
                f"• Attended office hours: {office_hours_str}"
            )
        
        if self.blocked:
            status_text = "🚧 **Blocked** - Support requested"
            support_text = "\n".join([f"  • {label}" for label in support_labels])
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** {status_text}\n\n"
                f"**Support needed:**\n{support_text}"
                f"{survey_summary}\n\n"
                "Your responses have been recorded. A team member will reach out to help!"
                f"{points_msg}"
            )
            color = discord.Color.orange()
        else:
            description = (
                f"**Phase:** {self.phase_label}\n"
                f"**Status:** ✅ **Good to go!**"
                f"{survey_summary}\n\n"
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
        
        await self._safe_edit_message(interaction, embed, self)
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
        
        # Add survey responses for week 5 (NPS survey)
        if self.has_survey and self.nps_score is not None:
            survey_text = (
                f"**Recommendation Score (0-10):** {self.nps_score}\n"
                f"**Recommendation Reason:** {self.nps_reason[:100]}{'...' if len(self.nps_reason) > 100 else ''}\n"
                f"**Open Source Confidence (1-5):** {self.opensource_confidence}\n"
                f"**GitLab Proficiency (1-5):** {self.gitlab_proficiency}\n"
                f"**AI Confidence (1-5):** {self.ai_confidence}"
            )
            embed.add_field(name="📊 Survey Responses", value=survey_text, inline=False)
        # Add event survey responses for week 6
        elif self.has_event_survey and self.attending_midprogram is not None:
            midprogram_str = "Yes" if self.attending_midprogram else "No"
            if not self.attending_midprogram and self.midprogram_reason:
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == self.midprogram_reason), self.midprogram_reason)
                midprogram_str += f" ({reason_label})"
            
            office_hours_str = "Yes" if self.attended_office_hours else "No"
            if not self.attended_office_hours and self.office_hours_reason:
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == self.office_hours_reason), self.office_hours_reason)
                office_hours_str += f" ({reason_label})"
            
            survey_text = (
                f"**Attending Midprogram Event:** {midprogram_str}\n"
                f"**Attended Office Hours:** {office_hours_str}"
            )
            embed.add_field(name="📅 Event Survey", value=survey_text, inline=False)
        
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
    
    async def _post_checkin_to_channel(
        self, 
        channel: discord.TextChannel, 
        mocked_utc: Optional[datetime] = None,
        skip_mentions: bool = False
    ) -> discord.Message:
        """Post a check-in prompt to a specific channel. Returns the message.
        
        Args:
            channel: Target channel to post in
            mocked_utc: Optional mocked UTC datetime for testing week calculation
            skip_mentions: If True, don't mention any role (useful for testing)
        """
        from utils.time_utils import get_program_week
        
        # Calculate week using mocked time if provided
        if mocked_utc:
            settings_file = os.path.join('data', 'uploads', '_tracker_settings.json')
            start_date = datetime.now()
            if os.path.exists(settings_file):
                try:
                    with open(settings_file, 'r') as f:
                        data = json.load(f)
                        date_str = data.get('start_date', '')
                        if 'T' in date_str:
                            start_date = datetime.fromisoformat(date_str)
                        else:
                            start_date = datetime.strptime(date_str, '%Y-%m-%d')
                except:
                    pass
            current_week = get_program_week(start_date, mocked_utc)
        else:
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
        
        # Add mocked time info if testing
        if mocked_utc:
            embed.add_field(
                name="🧪 Test Mode",
                value=f"Mocked UTC: `{mocked_utc.strftime('%Y-%m-%d %H:%M:%S')}`",
                inline=False
            )
        
        # Send message with role mention (SP26-Students) or no mention if testing
        content = None
        if not skip_mentions and hasattr(channel, 'guild') and channel.guild:
            # Look up the SP26-Students role (case-insensitive)
            role = discord.utils.find(
                lambda r: r.name.lower() == "sp26-students",
                channel.guild.roles
            )
            if role:
                content = role.mention
            else:
                # Fallback to @everyone if role not found
                content = "@everyone"
        message = await channel.send(content=content, embed=embed)
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
            'posted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mocked_utc': mocked_utc.isoformat() if mocked_utc else None
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
        
        # Start the questionnaire (3 steps normally, 7 steps for survey weeks)
        total_steps = 7 if week in SURVEY_WEEKS else 3
        title = f"📋 Modify Week Check-in (Step 1/{total_steps})" if is_modify else f"📋 Weekly Check-in (Step 1/{total_steps})"
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
        
        # Build survey summary if available
        survey_summary = ""
        if checkin.get('nps_score') is not None:
            os_conf = checkin.get('opensource_confidence')
            gl_prof = checkin.get('gitlab_proficiency')
            ai_conf = checkin.get('ai_confidence')
            
            os_conf_label = CONFIDENCE_LABELS.get(str(os_conf), "N/A") if os_conf else "N/A"
            gl_prof_label = PROFICIENCY_LABELS.get(str(gl_prof), "N/A") if gl_prof else "N/A"
            ai_conf_label = CONFIDENCE_LABELS.get(str(ai_conf), "N/A") if ai_conf else "N/A"
            
            survey_summary = (
                f"\n\n**📊 Survey Responses:**\n"
                f"• NPS Score: {checkin.get('nps_score', 'N/A')}/10\n"
                f"• Open Source Confidence: {os_conf}/5 ({os_conf_label})\n"
                f"• GitLab Proficiency: {gl_prof}/5 ({gl_prof_label})\n"
                f"• AI Tools Confidence: {ai_conf}/5 ({ai_conf_label})"
            )
        elif checkin.get('attending_midprogram') is not None:
            midprogram_str = "✅ Yes" if checkin.get('attending_midprogram') else "❌ No"
            if not checkin.get('attending_midprogram') and checkin.get('midprogram_reason'):
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == checkin.get('midprogram_reason')), checkin.get('midprogram_reason'))
                midprogram_str += f" ({reason_label})"
            
            office_hours_str = "✅ Yes" if checkin.get('attended_office_hours') else "❌ No"
            if not checkin.get('attended_office_hours') and checkin.get('office_hours_reason'):
                reason_label = next((label for val, label in NOT_ATTENDING_REASONS if val == checkin.get('office_hours_reason')), checkin.get('office_hours_reason'))
                office_hours_str += f" ({reason_label})"
            
            survey_summary = (
                f"\n\n**📅 Event Survey:**\n"
                f"• Attending midprogram event: {midprogram_str}\n"
                f"• Attended office hours: {office_hours_str}"
            )
        
        # Build status display
        if checkin.get('blocked'):
            status_text = "🚧 Blocked - Support requested"
            support_labels = checkin.get('support_labels', [])
            support_text = "\n".join([f"  • {label}" for label in support_labels]) if support_labels else "  • None specified"
            
            description = (
                f"**Phase:** {checkin.get('phase_label', 'N/A')}\n"
                f"**Status:** {status_text}\n\n"
                f"**Support needed:**\n{support_text}"
                f"{survey_summary}"
            )
            color = discord.Color.orange()
        else:
            description = (
                f"**Phase:** {checkin.get('phase_label', 'N/A')}\n"
                f"**Status:** ✅ Good to go!"
                f"{survey_summary}"
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
        
        # Header (including all survey columns)
        writer.writerow([
            'User ID', 'Week', 'Phase', 'Phase Label', 'Blocked', 'Support Needed',
            'NPS Score', 'NPS Reason', 'Open Source Confidence', 'GitLab Proficiency', 
            'AI Tools Confidence',
            'Attending Midprogram', 'Midprogram Reason', 'Attended Office Hours', 'Office Hours Reason',
            'Submitted At'
        ])
        
        # Data rows
        for user_id, weeks_data in data.items():
            for week_key, checkin in weeks_data.items():
                if not week_key.startswith('week_'):
                    continue
                
                week_num = week_key.replace('week_', '')
                support_str = "; ".join(checkin.get('support_labels', []))
                
                # Format event survey fields
                attending_midprogram = checkin.get('attending_midprogram')
                attending_midprogram_str = '' if attending_midprogram is None else ('Yes' if attending_midprogram else 'No')
                
                attended_office_hours = checkin.get('attended_office_hours')
                attended_office_hours_str = '' if attended_office_hours is None else ('Yes' if attended_office_hours else 'No')
                
                writer.writerow([
                    user_id,
                    week_num,
                    checkin.get('phase', ''),
                    checkin.get('phase_label', ''),
                    'Yes' if checkin.get('blocked') else 'No',
                    support_str,
                    checkin.get('nps_score', ''),
                    checkin.get('nps_reason', ''),
                    checkin.get('opensource_confidence', ''),
                    checkin.get('gitlab_proficiency', ''),
                    checkin.get('ai_confidence', ''),
                    attending_midprogram_str,
                    checkin.get('midprogram_reason', ''),
                    attended_office_hours_str,
                    checkin.get('office_hours_reason', ''),
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
    
    @commands.command(name='nps_download')
    async def checkin_nps_download(self, ctx: commands.Context):
        """Download extended survey responses as CSV with NPS analysis (Admin only).
        
        Exports all check-ins that include survey responses (NPS, confidence, proficiency).
        Includes averages at the bottom using the proper NPS formula.
        
        NPS Formula: % Promoters (9-10) - % Detractors (0-6)
        
        Usage: !checkin nps_download
        """
        import csv
        import io
        
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to download NPS reports.")
            return
        
        data = load_checkin_data()
        
        if not data:
            await ctx.send("📋 **No check-ins recorded yet.**")
            return
        
        # Collect all survey responses
        survey_rows = []
        nps_scores = []
        opensource_scores = []
        gitlab_scores = []
        ai_scores = []
        
        for user_id, weeks_data in data.items():
            for week_key, checkin in weeks_data.items():
                if not week_key.startswith('week_'):
                    continue
                
                # Only include check-ins with survey data
                if checkin.get('nps_score') is None:
                    continue
                
                week_num = week_key.replace('week_', '')
                
                nps = checkin.get('nps_score')
                opensource = checkin.get('opensource_confidence')
                gitlab = checkin.get('gitlab_proficiency')
                ai_conf = checkin.get('ai_confidence')
                
                survey_rows.append({
                    'user_id': user_id,
                    'week': week_num,
                    'discord_name': checkin.get('discord_name', ''),
                    'full_name': checkin.get('full_name', ''),
                    'nps_score': nps,
                    'nps_reason': checkin.get('nps_reason', ''),
                    'opensource_confidence': opensource,
                    'gitlab_proficiency': gitlab,
                    'ai_confidence': ai_conf,
                    'submitted_at': checkin.get('submitted_at', '')
                })
                
                # Collect scores for averaging
                if nps is not None:
                    nps_scores.append(nps)
                if opensource is not None:
                    opensource_scores.append(opensource)
                if gitlab is not None:
                    gitlab_scores.append(gitlab)
                if ai_conf is not None:
                    ai_scores.append(ai_conf)
        
        if not survey_rows:
            await ctx.send("📋 **No survey responses found.** Survey questions only appear in specific weeks (e.g., Week 5).")
            return
        
        # Calculate NPS using proper formula
        # Promoters: 9-10, Passives: 7-8, Detractors: 0-6
        if nps_scores:
            promoters = sum(1 for s in nps_scores if s >= 9)
            detractors = sum(1 for s in nps_scores if s <= 6)
            total = len(nps_scores)
            nps_result = ((promoters / total) - (detractors / total)) * 100
            
            promoter_pct = (promoters / total) * 100
            passive_pct = ((total - promoters - detractors) / total) * 100
            detractor_pct = (detractors / total) * 100
        else:
            nps_result = 0
            promoter_pct = passive_pct = detractor_pct = 0
        
        # Calculate simple averages for other scores
        avg_opensource = sum(opensource_scores) / len(opensource_scores) if opensource_scores else 0
        avg_gitlab = sum(gitlab_scores) / len(gitlab_scores) if gitlab_scores else 0
        avg_ai = sum(ai_scores) / len(ai_scores) if ai_scores else 0
        
        # Build CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            'User ID', 'Week', 'Discord Name', 'Full Name',
            'NPS Score (0-10)', 'NPS Reason',
            'Open Source Confidence (1-5)', 'GitLab Proficiency (1-5)', 'AI Confidence (1-5)',
            'Submitted At'
        ])
        
        # Data rows
        for row in survey_rows:
            writer.writerow([
                row['user_id'],
                row['week'],
                row['discord_name'],
                row['full_name'],
                row['nps_score'],
                row['nps_reason'],
                row['opensource_confidence'],
                row['gitlab_proficiency'],
                row['ai_confidence'],
                row['submitted_at']
            ])
        
        # Empty row before summary
        writer.writerow([])
        writer.writerow(['=== SUMMARY ==='])
        writer.writerow([])
        
        # NPS Analysis
        writer.writerow(['NPS Analysis'])
        writer.writerow(['Total Responses', len(nps_scores)])
        writer.writerow(['Promoters (9-10)', f"{promoters} ({promoter_pct:.1f}%)"])
        writer.writerow(['Passives (7-8)', f"{total - promoters - detractors} ({passive_pct:.1f}%)"])
        writer.writerow(['Detractors (0-6)', f"{detractors} ({detractor_pct:.1f}%)"])
        writer.writerow(['NPS Score', f"{nps_result:.1f}"])
        writer.writerow([])
        
        # Other Averages
        writer.writerow(['Score Averages'])
        writer.writerow(['Open Source Confidence (1-5)', f"{avg_opensource:.2f}"])
        writer.writerow(['GitLab Proficiency (1-5)', f"{avg_gitlab:.2f}"])
        writer.writerow(['AI Confidence (1-5)', f"{avg_ai:.2f}"])
        
        # Send as file
        csv_bytes = output.getvalue().encode('utf-8')
        file = discord.File(fp=io.BytesIO(csv_bytes), filename='nps_survey_report.csv')
        
        current_week = get_current_week()
        await ctx.send(
            f"📊 **NPS Survey Report Download**\n"
            f"Current Week: {current_week}\n"
            f"Survey Responses: {len(survey_rows)}\n"
            f"**NPS Score: {nps_result:.1f}** (Promoters: {promoter_pct:.0f}% | Passives: {passive_pct:.0f}% | Detractors: {detractor_pct:.0f}%)",
            file=file
        )
    
    @commands.command(name='preview_post')
    async def checkin_preview(self, ctx: commands.Context):
        """Preview what the check-in post would look like (Admin only).
        
        Shows the current week number, the embed that would be posted,
        and timing information about the next cutoff.
        
        Usage: !checkin preview_post
        """
        from datetime import timezone
        from utils.time_utils import WEEK_CUTOFF_DAY, WEEK_CUTOFF_HOUR_UTC
        
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to preview check-in posts.")
            return
        
        current_week = get_current_week()
        now = datetime.now(timezone.utc)
        
        # Calculate next cutoff (Wednesday 5PM UTC)
        days_until_cutoff = (WEEK_CUTOFF_DAY - now.weekday()) % 7
        next_cutoff = now.replace(hour=WEEK_CUTOFF_HOUR_UTC, minute=0, second=0, microsecond=0)
        next_cutoff = next_cutoff + timedelta(days=days_until_cutoff)
        
        # If we're past this week's cutoff, it's next week
        if now >= next_cutoff:
            next_cutoff = next_cutoff + timedelta(days=7)
        
        # Calculate time until cutoff
        time_until = next_cutoff - now
        hours_until = time_until.total_seconds() / 3600
        
        if hours_until >= 24:
            days = int(hours_until // 24)
            hours = hours_until % 24
            time_str = f"{days}d {hours:.1f}h"
        else:
            time_str = f"{hours_until:.1f}h"
        
        # Show timing info
        timing_embed = discord.Embed(
            title="⏱️ Check-in Preview - Timing Info",
            description=(
                f"**Current Week:** {current_week}\n"
                f"**Current Time (UTC):** {now.strftime('%Y-%m-%d %H:%M')}\n"
                f"**Next Cutoff:** {next_cutoff.strftime('%Y-%m-%d %H:%M')} UTC (Wed 5PM)\n"
                f"**Time Until Cutoff:** {time_str}\n\n"
                f"After the cutoff, the check-in will show **Week {current_week + 1}**."
            ),
            color=discord.Color.orange()
        )
        await ctx.send(embed=timing_embed)
        
        # Show preview of what the check-in post would look like
        preview_embed = discord.Embed(
            title=f"📋 Week {current_week} Check-in",
            description=(
                "**Time for your weekly check-in!**\n\n"
                f"React with {CHECKIN_EMOJI} below to start your check-in.\n"
                "I'll DM you a quick questionnaire about your progress.\n\n"
                "*Already completed? Send `!checkin status` to the bot to view your response or `!checkin modify` to change your answers.*"
            ),
            color=discord.Color.blue()
        )
        preview_embed.set_footer(text="Check-ins help us track progress and provide support when needed.")
        
        await ctx.send("**📝 Preview of check-in post:**", embed=preview_embed)
    
    @commands.command(name='post')
    async def checkin_post(self, ctx: commands.Context, channel_id: str = None, utc_timestamp: Optional[str] = None):
        """Post a check-in prompt message with reaction (Admin only).
        
        Usage: 
            !checkin post <channel_id>                  - Post in specified channel
            !checkin post <channel_id> <utc_timestamp>  - Post with mocked UTC time (for testing)
        
        UTC timestamp format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
        Example: !checkin post #general 2026-03-26T18:00:00
        
        When using mocked time, role mentions are skipped to prevent pings during testing.
        Users can react to start their weekly check-in via DM.
        """
        from datetime import timezone
        
        # Check if user is admin
        if not self.bot.is_user_allowed(ctx.author.id):
            await ctx.send("❌ **Admin only.** You don't have permission to post check-in prompts.")
            return
        
        # Require channel_id
        if not channel_id:
            await ctx.send(
                "❌ **Channel ID required.**\n"
                "Usage: `!checkin post <channel_id> [utc_timestamp]`\n"
                "Example: `!checkin post #general` or `!checkin post #general 2026-03-26T18:00:00`"
            )
            return
        
        # Parse mocked UTC time if provided
        mocked_utc = None
        if utc_timestamp:
            try:
                # Support both date-only and datetime formats
                if 'T' in utc_timestamp:
                    mocked_utc = datetime.fromisoformat(utc_timestamp).replace(tzinfo=timezone.utc)
                else:
                    mocked_utc = datetime.strptime(utc_timestamp, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            except ValueError:
                await ctx.send(
                    "❌ **Invalid UTC timestamp format.**\n"
                    "Use `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`\n"
                    "Example: `2026-03-26` or `2026-03-26T18:00:00`"
                )
                return
        
        # Determine target channel (remove <# and > if user used channel mention format)
        channel_id_clean = channel_id.strip('<#>').strip()
        try:
            target_channel = self.bot.get_channel(int(channel_id_clean))
            if target_channel is None:
                target_channel = await self.bot.fetch_channel(int(channel_id_clean))
        except (ValueError, discord.NotFound, discord.Forbidden):
            await ctx.send(f"❌ Could not find channel with ID `{channel_id}`. Make sure the bot has access.")
            return
        
        try:
            # Skip role mentions when testing with mocked time
            await self._post_checkin_to_channel(target_channel, mocked_utc=mocked_utc, skip_mentions=mocked_utc is not None)
            
            test_info = f" (mocked UTC: `{utc_timestamp}`)" if mocked_utc else ""
            await ctx.send(f"✅ Check-in prompt posted in <#{target_channel.id}>!{test_info} Users can react with {CHECKIN_EMOJI} to start.", delete_after=10)
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
                                                - Set schedule (time in UTC)
                                                - day: mon/tue/wed/thu/fri/sat/sun
            !checkin weekly off                 - Disable scheduled posts
            !checkin weekly reset               - Reset "already posted" flag (for testing)
        
        Examples:
            !checkin weekly set #general wed 17:00  (Wed 5PM UTC)
            !checkin weekly set #general mon 14:30  (Mon 2:30PM UTC)
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
            embed.add_field(name="Schedule", value=f"Every {day_name} at {time_str} UTC", inline=True)
            
            if next_run:
                from datetime import datetime
                try:
                    next_dt = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
                    next_date_str = next_dt.strftime('%b %d, %Y at %H:%M UTC')
                except:
                    next_date_str = next_run
                time_until = format_time_until(next_run)
                embed.add_field(name="Next Post", value=f"{next_date_str}\n({time_until})", inline=True)
            
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
                await ctx.send("❌ Usage: `!checkin weekly set <channel> <day> <HH:MM>` (UTC)\n"
                              "Example: `!checkin weekly set #general wed 17:00` (Wed 5PM UTC)")
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
        
        # Find the tracked message to get its stored week
        tracked_msg = None
        for m in tracked_messages:
            if m['message_id'] == payload.message_id:
                tracked_msg = m
                break
        
        if tracked_msg is None:
            return
        
        # Use the week from the tracked message (supports mocked time testing)
        checkin_week = tracked_msg.get('week', get_current_week())
        
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
        existing = get_user_checkin(str(payload.user_id), checkin_week)
        
        try:
            if existing:
                # Already has check-in, send reminder
                await user.send(
                    f"📋 You've already completed your Week {checkin_week} check-in!\n"
                    f"Use `!checkin status` to view it or `!checkin modify` to make changes."
                )
            else:
                # Start the check-in process via DM
                if checkin_week in SURVEY_WEEKS:
                    total_steps = 7
                elif checkin_week in EVENT_SURVEY_WEEKS:
                    total_steps = 5
                else:
                    total_steps = 3
                await user.send(
                    f"📋 **Week {checkin_week} Check-in**\n"
                    f"Let's get your weekly check-in started!"
                )
                
                # Create and send the check-in view
                view = CheckinView(user.id, checkin_week, bot=self.bot, discord_user=user)
                embed = discord.Embed(
                    title=f"📋 Weekly Check-in (Step 1/{total_steps})",
                    description=(
                        f"**Week {checkin_week} Check-in**\n\n"
                        "Let's get your weekly status update!\n\n"
                        "**What phase are you currently in?**"
                    ),
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
