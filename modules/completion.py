"""Completion module for student self-service phase tracking.

Allows students to directly message the bot to set their completed phase.

Commands:
    !completion set_phase_complete <phase> - Set your completed phase (DM only)
    !completion status - Check your current phase completion status (DM only)
"""

import io
from typing import Optional, TYPE_CHECKING

import discord
from discord.ext import commands

if TYPE_CHECKING:
    from bot.client import DiscordBot


class CompletionCog(commands.Cog, name="Completion"):
    """Cog for student self-service completion tracking.
    
    Students can DM the bot to set their own phase completion status.
    Their Discord username is used to look up their member ID.
    """
    
    def __init__(self, bot: 'DiscordBot'):
        self.bot = bot
        self.storage = bot.file_storage
    
    def _preprocess_master_csv(self, master_text: str) -> str:
        """Preprocess master CSV to find actual header row and strip metadata."""
        lines = master_text.splitlines()
        header_row_idx = None
        
        for idx, line in enumerate(lines):
            if "Member ID" in line or "member_id" in line.lower():
                header_row_idx = idx
                break
        
        if header_row_idx is None:
            return master_text
        
        data_lines = lines[header_row_idx:]
        if data_lines and data_lines[0].startswith(','):
            data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
        
        return '\n'.join(data_lines)
    
    def _lookup_member_id_by_discord(self, discord_username: str) -> Optional[tuple]:
        """Look up member ID by Discord username.
        
        Searches multiple columns and handles various Discord name formats.
        
        Returns:
            Tuple of (member_id, name, discord_username) or None if not found
        """
        master_file = self.storage.get_file("master")
        if not master_file:
            return None
        
        try:
            import csv
            master_data = self.storage.read_file(master_file)
            text_data = master_data.decode('utf-8-sig')
            
            # Preprocess to find actual header row
            text_data = self._preprocess_master_csv(text_data)
            
            reader = csv.DictReader(io.StringIO(text_data))
            rows = list(reader)
            
            if not rows:
                return None
            
            headers = list(rows[0].keys())
            headers_lower = {h.lower(): h for h in headers}
            
            # Helper to find column (case-insensitive)
            def find_col(possible_names):
                for name in possible_names:
                    if name in headers:
                        return name
                    if name.lower() in headers_lower:
                        return headers_lower[name.lower()]
                return None
            
            # Find columns
            member_id_col = find_col(["Member ID", "member_id", "MemberID"])
            discord_col = find_col(["Discord Username", "Discord", "discord_username", "Discord Handle"])
            name_col = find_col(["Full Name", "Name", "full_name", "Student Name"])
            
            if not member_id_col:
                return None
            
            # Clean the search input
            discord_lower = discord_username.lower().strip()
            if discord_lower.startswith('@'):
                discord_lower = discord_lower[1:]
            if '#' in discord_lower:
                discord_lower = discord_lower.split('#')[0]
            
            # Search Discord column (if found)
            if discord_col:
                for row in rows:
                    roster_discord = str(row.get(discord_col, "")).strip()
                    discord_clean = roster_discord.lower()
                    if discord_clean.startswith('@'):
                        discord_clean = discord_clean[1:]
                    if '#' in discord_clean:
                        discord_clean = discord_clean.split('#')[0]
                    
                    if discord_clean == discord_lower:
                        member_id = str(row.get(member_id_col, "")).strip()
                        name = str(row.get(name_col, "")).strip() if name_col else ""
                        return (member_id, name, roster_discord)
                
                # Partial match
                for row in rows:
                    roster_discord = str(row.get(discord_col, "")).strip()
                    if discord_lower in roster_discord.lower():
                        member_id = str(row.get(member_id_col, "")).strip()
                        name = str(row.get(name_col, "")).strip() if name_col else ""
                        return (member_id, name, roster_discord)
            
            # Fallback: Search by Name
            if name_col:
                for row in rows:
                    name = str(row.get(name_col, "")).strip()
                    if discord_lower in name.lower():
                        member_id = str(row.get(member_id_col, "")).strip()
                        roster_discord = str(row.get(discord_col, "")).strip() if discord_col else ""
                        return (member_id, name, roster_discord)
            
            return None
        except Exception as e:
            print(f"[Completion] Error looking up member ID: {e}")
            return None
    
    @commands.group(name='completion', invoke_without_command=True)
    async def completion_group(self, ctx: commands.Context):
        """Student self-service completion tracking commands."""
        await ctx.send(
            "**📝 Completion Commands**\n\n"
            "Use these commands in a **DM to the bot** to track your progress:\n\n"
            "• `!completion set_phase_complete <phase>` - Set your completed phase (1-4)\n"
            "• `!completion status` - Check your current completion status\n\n"
            "Example: `!completion set_phase_complete 2`"
        )
    
    @completion_group.command(name='set_phase_complete')
    async def set_phase_complete(self, ctx: commands.Context, phase: int = None):
        """Set your completed phase.
        
        Usage: !completion set_phase_complete <phase>
        
        This command works best in a DM to the bot.
        Your Discord username is used to look up your member ID.
        """
        if phase is None:
            await ctx.send(
                "**📝 Set Phase Complete**\n\n"
                "Usage: `!completion set_phase_complete <phase>`\n\n"
                "Example: `!completion set_phase_complete 2`\n\n"
                "Phase must be between 1 and 4."
            )
            return
        
        if phase < 1 or phase > 4:
            await ctx.send("❌ Phase must be between 1 and 4.")
            return
        
        # Look up the user's member ID by their Discord username
        discord_username = ctx.author.name
        result = self._lookup_member_id_by_discord(discord_username)
        
        if not result:
            # Try with display name as fallback
            if hasattr(ctx.author, 'display_name') and ctx.author.display_name != discord_username:
                result = self._lookup_member_id_by_discord(ctx.author.display_name)
        
        if not result:
            await ctx.send(
                f"❌ **Not Found**\n\n"
                f"Your Discord username (`{discord_username}`) was not found in the master roster.\n\n"
                f"Please make sure your Discord username matches what's registered in the program, "
                f"or contact a program administrator for help."
            )
            return
        
        member_id, name, roster_discord = result
        
        # Set the phase completion (single phase as list)
        updated_by = f"self:{ctx.author.id}"
        self.storage.set_phase_complete(member_id, [phase], updated_by, name)
        
        await ctx.send(
            f"✅ **Phase Complete Updated**\n\n"
            f"• Name: {name}\n"
            f"• Member ID: `{member_id}`\n"
            f"• Completed Phase: **Phase {phase}**\n\n"
            f"Great progress! Keep up the good work! 🎉"
        )
    
    @completion_group.command(name='status')
    async def completion_status(self, ctx: commands.Context):
        """Check your current phase completion status."""
        discord_username = ctx.author.name
        result = self._lookup_member_id_by_discord(discord_username)
        
        if not result:
            if hasattr(ctx.author, 'display_name') and ctx.author.display_name != discord_username:
                result = self._lookup_member_id_by_discord(ctx.author.display_name)
        
        if not result:
            await ctx.send(
                f"❌ **Not Found**\n\n"
                f"Your Discord username (`{discord_username}`) was not found in the master roster.\n\n"
                f"Please make sure your Discord username matches what's registered in the program."
            )
            return
        
        member_id, name, roster_discord = result
        
        # Get current phase completion (returns list)
        completed_phases = self.storage.get_phase_complete(member_id)
        
        if completed_phases:
            phase_names = {
                1: "Phase 1: Issue Selection",
                2: "Phase 2: Reproduction",
                3: "Phase 3: Implementation",
                4: "Phase 4: Submission"
            }
            
            # Format completed phases
            phases_str = ", ".join([phase_names.get(p, f"Phase {p}") for p in sorted(completed_phases)])
            
            await ctx.send(
                f"📊 **Your Completion Status**\n\n"
                f"• Name: {name}\n"
                f"• Member ID: `{member_id}`\n"
                f"• Phases Complete: **{phases_str}**\n\n"
                f"To update, use `!completion set_phase_complete <phase>`"
            )
        else:
            await ctx.send(
                f"📊 **Your Completion Status**\n\n"
                f"• Name: {name}\n"
                f"• Member ID: `{member_id}`\n"
                f"• Current Phase Complete: *Not set*\n\n"
                f"Use `!completion set_phase_complete <phase>` to record your progress."
            )


async def setup(bot: 'DiscordBot') -> None:
    """Load the Completion cog."""
    await bot.add_cog(CompletionCog(bot))



