"""Game/Points tracking module (Cog)."""

import asyncio
import csv
import io
import random
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import discord
from discord.ext import commands, tasks

from services.persistence import PersistenceService

if TYPE_CHECKING:
    from bot.client import DiscordBot


class GameCog(commands.Cog, name="Game"):
    """Commands for managing game points and standings."""
    
    def __init__(self, bot: 'DiscordBot'):
        self.bot = bot
        self.file_storage = bot.file_storage  # Use shared instance
        self.trivia_questions = PersistenceService.load_trivia_questions()
        self.trivia_points = PersistenceService.get_trivia_points()
        self.current_timeout_task = None
        
        # Lock to prevent race conditions in timeout handling
        self._timeout_lock = asyncio.Lock()
        
        # Community points tracking
        self.community_state = PersistenceService.load_community_state()
        
        # Resume trivia if channel is configured
        if self.bot.trivia_state.get('channel_id'):
            current_q = self.bot.trivia_state.get('current_question')
            if current_q and not self.bot.trivia_state.get('answered_by'):
                # Resume timeout for existing question using aligned boundaries
                interval = self.bot.trivia_state.get('interval_minutes', 5)
                if interval > 0:
                    seconds_until = self._get_seconds_until_next_boundary(interval)
                    # Cancel any existing task before creating new one
                    if self.current_timeout_task and not self.current_timeout_task.done():
                        self.current_timeout_task.cancel()
                    self.current_timeout_task = self.bot.loop.create_task(
                        self._check_question_timeout_seconds(current_q['id'], seconds_until)
                    )
                    self.bot.loop.create_task(self._announce_trivia_resume(interval, seconds_until))
            else:
                # No active question, post a new one
                self._start_trivia_loop()
    
    def cog_unload(self):
        """Clean up when cog is unloaded."""
        self.trivia_loop.cancel()
    
    def _start_trivia_loop(self):
        """Start trivia by posting the first question."""
        if self.trivia_loop.is_running():
            self.trivia_loop.cancel()
        self.trivia_loop.start()
    
    async def _announce_trivia_resume(self, interval: int, seconds_until: float):
        """Announce trivia resumption with timing info."""
        await self.bot.wait_until_ready()
        
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        
        current_q = self.bot.trivia_state.get('current_question')
        if current_q:
            question_number = self.bot.trivia_state.get('question_number', 1)
            now = datetime.now(timezone.utc)
            timeout_time = now + timedelta(seconds=seconds_until)
            try:
                await channel.send(
                    f"🔄 **Trivia Resumed!**\n"
                    f"Question #{question_number} is still active.\n"
                    f"⏱️ Will timeout at **{timeout_time.hour:02d}:{timeout_time.minute:02d} UTC** (~{int(seconds_until/60)} min)"
                )
            except Exception as e:
                print(f"[Trivia] Failed to announce resume: {e}")
    
    def _check_permission(self, ctx: commands.Context) -> bool:
        """Check if user is allowed to use game admin commands."""
        return self.bot.is_user_allowed(ctx.author.id)
    
    def _get_seconds_until_next_boundary(self, interval_minutes: int) -> float:
        """Calculate seconds until the next clock-aligned timeout boundary.
        
        For example, with 2 min interval at 1:01:30, next boundary is 1:02:00 (30 seconds).
        With 5 min interval at 1:03:00, next boundary is 1:05:00 (2 minutes).
        With 60 min interval at 1:15:00, next boundary is 2:00:00 (45 minutes).
        """
        now = datetime.now(timezone.utc)
        total_minutes = now.hour * 60 + now.minute
        
        # Find minutes until next aligned boundary
        remainder = total_minutes % interval_minutes
        if remainder == 0 and now.second == 0:
            # Exactly on boundary, next one is interval_minutes away
            minutes_until = interval_minutes
        else:
            minutes_until = interval_minutes - remainder
        
        # Calculate total seconds, accounting for current seconds
        seconds_until = (minutes_until * 60) - now.second - (now.microsecond / 1_000_000)
        
        # Ensure at least 1 second
        return max(1, seconds_until)
    
    def _display_name(self, username: str) -> str:
        """Get display name without discriminator.
        
        Args:
            username: The full username (may include #xxxx)
            
        Returns:
            Username without discriminator
        """
        if '#' in username:
            return username.rsplit('#', 1)[0]
        return username
    
    def _normalize_name(self, name: str) -> str:
        """Normalize a name for matching by removing special chars and lowercasing.
        
        Args:
            name: The name to normalize
            
        Returns:
            Normalized name (lowercase, no underscores, no trailing numbers)
        """
        import re
        # Remove discriminator if present
        if '#' in name:
            name = name.rsplit('#', 1)[0]
        # Lowercase
        name = name.lower().strip()
        # Remove underscores
        name = name.replace('_', '')
        # Remove trailing numbers
        name = re.sub(r'\d+$', '', name)
        return name
    
    def _find_matching_user(self, search_name: str, ctx: commands.Context = None) -> Optional[str]:
        """Find a matching username in the roster, handling various name formats.
        
        Matches:
        - Exact match (case-insensitive)
        - Base name without discriminator
        - Normalized match (ignoring underscores, trailing numbers)
        - Discord nickname to roster name (if ctx provided)
        
        Args:
            search_name: The username to search for
            ctx: Optional command context for nickname lookup
            
        Returns:
            The matching roster username, or None if not found
        """
        search_lower = search_name.lower().strip()
        search_normalized = self._normalize_name(search_name)
        
        for roster_name in self.bot.game_points.keys():
            roster_lower = roster_name.lower()
            roster_base = roster_name.rsplit('#', 1)[0].lower() if '#' in roster_name else roster_lower
            roster_normalized = self._normalize_name(roster_name)
            
            # Exact match
            if roster_lower == search_lower:
                return roster_name
            
            # Match base name (before #) against search
            if roster_base == search_lower:
                return roster_name
            
            # Normalized match (e.g., "rom__21" matches "Rom#1293" via "rom")
            if roster_normalized == search_normalized:
                return roster_name
            
            # Match search (if it has #) against roster base
            if '#' in search_name:
                search_base = search_name.rsplit('#', 1)[0].lower()
                if roster_lower == search_base or roster_base == search_base:
                    return roster_name
        
        # If ctx provided, check if the user has a nickname that matches a roster entry
        if ctx and ctx.guild:
            member = ctx.guild.get_member_named(search_name)
            if not member:
                # Try to find by display name
                for m in ctx.guild.members:
                    if m.name.lower() == search_lower or m.display_name.lower() == search_lower:
                        member = m
                        break
            
            if member:
                # Check member's username and nickname against roster (normalized)
                member_name_norm = self._normalize_name(member.name)
                member_display_norm = self._normalize_name(member.display_name)
                
                for roster_name in self.bot.game_points.keys():
                    roster_normalized = self._normalize_name(roster_name)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        return roster_name
        
        return None
    
    def _preprocess_master_csv(self, master_text: str) -> str:
        """Preprocess master CSV to find actual header row and strip metadata.
        
        The master CSV may have metadata rows at the top before the actual header.
        This function finds the header row (containing "Member ID") and returns
        the CSV starting from that row.
        """
        lines = master_text.splitlines()
        header_row_idx = None
        
        for idx, line in enumerate(lines):
            if "Member ID" in line or "member_id" in line.lower():
                header_row_idx = idx
                break
        
        if header_row_idx is None:
            return master_text
        
        data_lines = lines[header_row_idx:]
        
        # Strip leading empty column if present
        if data_lines and data_lines[0].startswith(','):
            data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
        
        return '\n'.join(data_lines)
    
    def _get_master_discord_usernames(self) -> List[str]:
        """Get all Discord usernames from the master roster CSV.
        
        Returns:
            List of discord usernames found in master CSV
        """
        master_data = self.file_storage.read_file_by_category("master")
        if not master_data:
            return []
        
        try:
            content = master_data.decode("utf-8-sig")
            content = self._preprocess_master_csv(content)
            reader = csv.DictReader(io.StringIO(content))
            headers = reader.fieldnames or []
            
            discord_col = None
            discord_variants = ["Discord Username", "Discord", "discord_username"]
            for variant in discord_variants:
                if variant in headers:
                    discord_col = variant
                    break
            
            if not discord_col:
                return []
            
            usernames = []
            for row in reader:
                username = str(row.get(discord_col, "")).strip()
                if username:
                    usernames.append(username)
            
            return usernames
        except Exception as e:
            print(f"[Game] Error reading master CSV: {e}")
            return []
    
    def _get_master_discord_to_name_map(self) -> Dict[str, str]:
        """Get mapping of Discord username to Full Name from master roster.
        
        Returns:
            Dict mapping discord_username -> full_name
        """
        master_data = self.file_storage.read_file_by_category("master")
        if not master_data:
            return {}
        
        try:
            content = master_data.decode("utf-8-sig")
            content = self._preprocess_master_csv(content)
            reader = csv.DictReader(io.StringIO(content))
            headers = reader.fieldnames or []
            
            discord_col = None
            discord_variants = ["Discord Username", "Discord", "discord_username"]
            for variant in discord_variants:
                if variant in headers:
                    discord_col = variant
                    break
            
            name_col = None
            name_variants = ["Full Name", "Name", "full_name", "name", "Student Name"]
            for variant in name_variants:
                if variant in headers:
                    name_col = variant
                    break
            
            if not discord_col:
                return {}
            
            mapping = {}
            for row in reader:
                username = str(row.get(discord_col, "")).strip()
                name = str(row.get(name_col, "")).strip() if name_col else ""
                if username:
                    mapping[username] = name
            
            return mapping
        except Exception as e:
            print(f"[Game] Error reading master CSV for name mapping: {e}")
            return {}
    
    def _sync_points_with_master(self) -> Dict[str, int]:
        """Sync points dictionary with master roster.
        
        Ensures all users from master are in points dict (with 0 if new),
        and removes users no longer in master.
        
        Returns:
            Updated points dictionary
        """
        master_users = set(self._get_master_discord_usernames())
        current_points = self.bot.game_points.copy()
        
        synced_points: Dict[str, int] = {}
        for username in master_users:
            synced_points[username] = current_points.get(username, 0)
        
        if synced_points != self.bot.game_points:
            self.bot.game_points = synced_points
            self.bot.save_game_points()
        
        return synced_points
    
    @commands.command(name='standing')
    async def standing(self, ctx: commands.Context) -> None:
        """Display the points leaderboard for all members.
        
        Usage: !game standing
        """
        master_file = self.file_storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        points = self._sync_points_with_master()
        
        if not points:
            await ctx.send("📊 No members found in the master roster.")
            return
        
        sorted_standings = sorted(points.items(), key=lambda x: (-x[1], x[0].lower()))
        
        embed = discord.Embed(
            title="🏆 Points Leaderboard",
            color=discord.Color.gold()
        )
        
        lines = []
        for rank, (username, pts) in enumerate(sorted_standings, 1):
            if rank <= 3:
                medal = ["🥇", "🥈", "🥉"][rank - 1]
            else:
                medal = f"`{rank}.`"
            
            # Try to find actual Discord member by checking normalized names
            display = self._display_name(username)
            roster_normalized = self._normalize_name(username)
            found = False
            
            # Search across all guilds the bot is in
            for guild in self.bot.guilds:
                if found:
                    break
                for member in guild.members:
                    member_name_norm = self._normalize_name(member.name)
                    member_display_norm = self._normalize_name(member.display_name)
                    # Check if member's username or nickname matches roster entry (normalized)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        # Show their display name (nickname if set, otherwise username)
                        display = member.display_name
                        found = True
                        break
            
            lines.append(f"{medal} **{display}** — {pts} pts")
        
        if len(lines) > 25:
            chunks = [lines[i:i+25] for i in range(0, len(lines), 25)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    embed.description = "\n".join(chunk)
                    await ctx.send(embed=embed)
                else:
                    cont_embed = discord.Embed(
                        title=f"🏆 Points Leaderboard (continued)",
                        description="\n".join(chunk),
                        color=discord.Color.gold()
                    )
                    await ctx.send(embed=cont_embed)
        else:
            embed.description = "\n".join(lines) if lines else "No members yet."
            embed.set_footer(text=f"Total members: {len(points)}")
            await ctx.send(embed=embed)
    
    @commands.command(name='grant_points')
    async def grant_points(self, ctx: commands.Context, user: str = None, points: int = None) -> None:
        """Grant points to a specific user.
        
        Usage: !game grant_points <discord_username> <points>
        
        Examples:
            !game grant_points john_doe 10
            !game grant_points "jane doe" 5
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to grant points.")
            return
        
        if not user or points is None:
            await ctx.send("Usage: `!game grant_points <discord_username> <points>`\n"
                          "Example: `!game grant_points john_doe 10`")
            return
        
        master_file = self.file_storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        self._sync_points_with_master()
        
        target_user = user.strip()
        matching_user = self._find_matching_user(target_user, ctx)
        
        if not matching_user:
            await ctx.send(f"❌ User `{target_user}` not found in the master roster.\n"
                          f"💡 Use `!game standing` to see all registered members.")
            return
        
        old_points = self.bot.game_points[matching_user]
        self.bot.game_points[matching_user] = old_points + points
        self.bot.save_game_points()
        
        new_points = self.bot.game_points[matching_user]
        
        if points >= 0:
            await ctx.send(f"✅ Granted **{points}** points to **{target_user}**!\n"
                          f"📊 {old_points} → {new_points} pts")
        else:
            await ctx.send(f"✅ Deducted **{abs(points)}** points from **{target_user}**!\n"
                          f"📊 {old_points} → {new_points} pts")
    
    @commands.command(name='reset')
    async def reset(self, ctx: commands.Context, confirm: str = None) -> None:
        """Reset all points to zero.
        
        Usage: !game reset confirm
        
        Requires 'confirm' to prevent accidental resets.
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to reset points.")
            return
        
        if confirm != "confirm":
            await ctx.send("⚠️ This will reset ALL points to zero!\n"
                          "To confirm, use: `!game reset confirm`")
            return
        
        master_file = self.file_storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded.")
            return
        
        master_users = self._get_master_discord_usernames()
        self.bot.game_points = {username: 0 for username in master_users}
        self.bot.save_game_points()
        
        await ctx.send(f"🔄 Points reset! All **{len(master_users)}** members now have 0 points.")
    
    @commands.command(name='points')
    async def check_points(self, ctx: commands.Context, user: str = None) -> None:
        """Check points for a specific user or yourself.
        
        Usage: 
            !game points - Check your own points
            !game points <username> - Check another user's points
        """
        master_file = self.file_storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded.")
            return
        
        self._sync_points_with_master()
        
        if not user:
            author_name = ctx.author.name
            matching_user = self._find_matching_user(author_name, ctx)
            
            if not matching_user:
                # Also try with display_name (nickname)
                if ctx.guild and ctx.author in ctx.guild.members:
                    matching_user = self._find_matching_user(ctx.author.display_name, ctx)
            
            if not matching_user:
                await ctx.send(f"❌ Your Discord username `{author_name}` wasn't found in the roster.\n"
                              f"💡 Make sure your Discord username matches what's in the master roster.")
                return
            
            pts = self.bot.game_points[matching_user]
            await ctx.send(f"🏆 **{author_name}** has **{pts}** points")
        else:
            target_user = user.strip()
            matching_user = self._find_matching_user(target_user, ctx)
            
            if not matching_user:
                await ctx.send(f"❌ User `{target_user}` not found in the roster.")
                return
            
            pts = self.bot.game_points[matching_user]
            await ctx.send(f"🏆 **{target_user}** has **{pts}** points")
    
    # ==================== Trivia System ====================
    
    @commands.command(name='trivia')
    async def trivia(self, ctx: commands.Context, channel_id: str = None) -> None:
        """Set or view the trivia channel.
        
        Usage:
            !game trivia <channel_id> - Set trivia channel
            !game trivia stop - Stop trivia
            !game trivia - View current trivia status
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure trivia.")
            return
        
        if channel_id is None:
            current_channel = self.bot.trivia_state.get('channel_id')
            if current_channel:
                channel = self.bot.get_channel(current_channel)
                channel_name = channel.name if channel else "unknown"
                current_q = self.bot.trivia_state.get('current_question')
                used = len(self.bot.trivia_state.get('used_questions', []))
                total = len(self.trivia_questions)
                interval = self.bot.trivia_state.get('interval_minutes', 5)
                
                status = f"📺 Channel: #{channel_name} (`{current_channel}`)\n"
                status += f"📝 Questions used: {used}/{total}\n"
                status += f"⏱️ Duration: {interval} minutes\n"
                if current_q:
                    status += f"❓ Current question active (ID: {current_q.get('id')})"
                else:
                    status += "💤 No active question"
                
                await ctx.send(f"🎯 **Trivia Status**\n{status}")
            else:
                await ctx.send("🎯 Trivia is not configured.\n"
                              "Use `!game trivia <channel_id>` to set it up.")
            return
        
        if channel_id.lower() == 'stop':
            self.bot.trivia_state['channel_id'] = None
            self.bot.trivia_state['current_question'] = None
            self.bot.trivia_state['answered_by'] = None
            self.bot.save_trivia_state()
            self.trivia_loop.cancel()
            await ctx.send("⏹️ Trivia stopped.")
            return
        
        try:
            cid = int(channel_id.strip('<>#'))
            channel = self.bot.get_channel(cid)
            
            if not channel:
                await ctx.send(f"❌ Channel `{cid}` not found or bot doesn't have access.")
                return
            
            self.bot.trivia_state['channel_id'] = cid
            self.bot.trivia_state['current_question'] = None
            self.bot.trivia_state['answered_by'] = None
            self.bot.save_trivia_state()
            
            self._start_trivia_loop()
            
            interval = self.bot.trivia_state.get('interval_minutes', 5)
            await ctx.send(f"✅ Trivia channel set to #{channel.name}!\n"
                          f"⏱️ Question duration: {interval} minutes.\n"
                          f"📝 {len(self.trivia_questions)} questions loaded.")
            
        except ValueError:
            await ctx.send("❌ Invalid channel ID. Provide a numeric channel ID.")
    
    @commands.command(name='trivia_reset')
    async def trivia_reset(self, ctx: commands.Context) -> None:
        """Reset trivia progress (re-enable all questions).
        
        Usage: !game trivia_reset
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to reset trivia.")
            return
        
        self.bot.trivia_state['used_questions'] = []
        self.bot.trivia_state['current_question'] = None
        self.bot.trivia_state['answered_by'] = None
        self.bot.trivia_state['question_number'] = 0
        self.bot.save_trivia_state()
        
        await ctx.send(f"🔄 Trivia reset! All {len(self.trivia_questions)} questions are available again.")
    
    @commands.command(name='trivia_set_minutes')
    async def trivia_set_minutes(self, ctx: commands.Context, minutes: int = None) -> None:
        """Set how long each question stays active before timing out.
        
        Usage: !game trivia_set_minutes <minutes>
        
        Example: !game trivia_set_minutes 10
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure trivia.")
            return
        
        if minutes is None:
            current = self.bot.trivia_state.get('interval_minutes', 5)
            msg = f"⏱️ Question duration: **{current}** minutes"
            
            # Show time remaining on current question if active
            current_q = self.bot.trivia_state.get('current_question')
            if current_q and not self.bot.trivia_state.get('answered_by'):
                posted_at = self.bot.trivia_state.get('question_posted_at')
                if posted_at:
                    posted_time = datetime.fromisoformat(posted_at)
                    now = datetime.now(timezone.utc)
                    elapsed = (now - posted_time).total_seconds() / 60
                    remaining = current - elapsed
                    if remaining > 0:
                        msg += f"\n⏳ Current question has ~{int(remaining)} min remaining"
                    else:
                        msg += f"\n⏳ Current question should timeout soon"
            
            msg += f"\nUse `!game trivia_set_minutes <minutes>` to change it."
            await ctx.send(msg)
            return
        
        if minutes < 1:
            await ctx.send("❌ Duration must be at least 1 minute.")
            return
        
        if minutes > 1440:
            await ctx.send("❌ Duration cannot exceed 1440 minutes (24 hours).")
            return
        
        old_interval = self.bot.trivia_state.get('interval_minutes', 5)
        self.bot.trivia_state['interval_minutes'] = minutes
        self.bot.save_trivia_state()
        
        msg = f"✅ Question duration changed: {old_interval} → **{minutes}** minutes"
        
        # Apply immediately to current question if active
        current_q = self.bot.trivia_state.get('current_question')
        if current_q and not self.bot.trivia_state.get('answered_by'):
            # Cancel existing timeout
            if self.current_timeout_task and not self.current_timeout_task.done():
                self.current_timeout_task.cancel()
            
            # Calculate next aligned timeout boundary
            seconds_until_boundary = self._get_seconds_until_next_boundary(minutes)
            minutes_until = seconds_until_boundary / 60
            
            # Calculate what time the timeout will be
            now = datetime.now(timezone.utc)
            timeout_minute = (now.minute + int(minutes_until) + 1) % 60
            timeout_hour = (now.hour + (now.minute + int(minutes_until) + 1) // 60) % 24
            
            self.current_timeout_task = self.bot.loop.create_task(
                self._check_question_timeout_seconds(current_q['id'], seconds_until_boundary)
            )
            msg += f"\n⏱️ Question will timeout at **{timeout_hour:02d}:{timeout_minute:02d} UTC** (~{int(minutes_until)} min)"
        
        await ctx.send(msg)
    
    @commands.command(name='trivia_scores')
    async def trivia_scores(self, ctx: commands.Context) -> None:
        """Show trivia-only leaderboard.
        
        Usage: !game trivia_scores
        """
        trivia_points = self.bot.trivia_state.get('trivia_points', {})
        
        if not trivia_points:
            await ctx.send("📊 No trivia scores yet. Answer questions correctly to earn points!")
            return
        
        sorted_scores = sorted(trivia_points.items(), key=lambda x: (-x[1], x[0].lower()))
        
        embed = discord.Embed(
            title="🎯 Trivia Leaderboard",
            color=discord.Color.blue()
        )
        
        lines = []
        for rank, (username, pts) in enumerate(sorted_scores, 1):
            if pts == 0:
                continue
            if rank <= 3:
                medal = ["🥇", "🥈", "🥉"][rank - 1]
            else:
                medal = f"`{rank}.`"
            
            # Find display name
            display = self._display_name(username)
            roster_normalized = self._normalize_name(username)
            for guild in self.bot.guilds:
                found = False
                for member in guild.members:
                    member_name_norm = self._normalize_name(member.name)
                    member_display_norm = self._normalize_name(member.display_name)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        display = member.display_name
                        found = True
                        break
                if found:
                    break
            
            lines.append(f"{medal} **{display}** — {pts} pts")
        
        if lines:
            embed.description = "\n".join(lines)
        else:
            embed.description = "No scores yet!"
        
        await ctx.send(embed=embed)
    
    @commands.command(name='trivia_next')
    async def trivia_next(self, ctx: commands.Context) -> None:
        """Manually trigger the next trivia question.
        
        Usage: !game trivia_next
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to trigger trivia.")
            return
        
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id:
            await ctx.send("❌ Trivia channel not configured. Use `!game trivia <channel_id>` first.")
            return
        
        await self._post_trivia_question()
        await ctx.send("✅ Trivia question posted!")
    
    # ==================== Community Points System ====================
    
    def _save_community_state(self) -> None:
        """Save community state to JSON file."""
        PersistenceService.save_community_state(self.community_state)
    
    def _find_matching_user_in_master(self, search_name: str, master_users: set) -> Optional[str]:
        """Find a matching username in the master roster set.
        
        Similar to _find_matching_user but checks against master CSV users only.
        """
        search_lower = search_name.lower().strip()
        search_normalized = self._normalize_name(search_name)
        
        for roster_name in master_users:
            roster_lower = roster_name.lower()
            roster_base = roster_name.rsplit('#', 1)[0].lower() if '#' in roster_name else roster_lower
            roster_normalized = self._normalize_name(roster_name)
            
            if roster_lower == search_lower:
                return roster_name
            if roster_base == search_lower:
                return roster_name
            if roster_normalized == search_normalized:
                return roster_name
            if '#' in search_name:
                search_base = search_name.rsplit('#', 1)[0].lower()
                if roster_lower == search_base or roster_base == search_base:
                    return roster_name
        
        return None
    
    async def _score_community_message(self, message: discord.Message) -> None:
        """Score a message in real-time if it's in a tracked community channel.
        
        Called automatically for every message in tracked channels.
        Also handles messages in threads within tracked forum channels.
        """
        # Check if this channel is being tracked
        cid_str = str(message.channel.id)
        tracked_channels = self.community_state.get('channels', {})
        
        # For thread messages, check if parent forum is tracked
        parent_id = None
        if isinstance(message.channel, discord.Thread) and message.channel.parent:
            parent_id = message.channel.parent_id
            if str(parent_id) in tracked_channels:
                cid_str = str(parent_id)  # Use parent forum's config
        
        if cid_str not in tracked_channels:
            return
        
        # Skip bot messages
        if message.author.bot:
            return
        
        # Skip command messages
        if message.content.startswith('!'):
            return
        
        # Get master roster users
        master_users = set(self._get_master_discord_usernames())
        if not master_users:
            return
        
        # Find matching user in master roster (not game_points)
        author_name = message.author.name
        matching_user = self._find_matching_user_in_master(author_name, master_users)
        
        if not matching_user and hasattr(message.author, 'display_name'):
            matching_user = self._find_matching_user_in_master(message.author.display_name, master_users)
        
        if not matching_user:
            # User not in master roster, skip scoring
            return
        
        points_config = self._get_channel_points(message.channel.id)
        points = 0
        
        # Initialize first_responders for this channel if needed
        if 'first_responders' not in self.community_state:
            self.community_state['first_responders'] = {}
        if cid_str not in self.community_state['first_responders']:
            self.community_state['first_responders'][cid_str] = {}
        
        first_responders = self.community_state['first_responders'][cid_str]
        
        # Handle forum thread messages differently
        if isinstance(message.channel, discord.Thread) and parent_id:
            thread_id = str(message.channel.id)
            
            # Check if this is the thread starter message
            if message.channel.starter_message and message.id == message.channel.starter_message.id:
                points = points_config['first_post']
            else:
                # Reply within a thread - use thread_id as the "original post" key
                if thread_id not in first_responders:
                    # First reply in this thread (not by thread owner)
                    if message.channel.owner_id != message.author.id:
                        first_responders[thread_id] = message.author.id
                        points = points_config['first_response']
                    else:
                        # Thread owner replying to their own thread - subsequent
                        points = points_config['subsequent_response']
                elif first_responders.get(thread_id) == message.author.id:
                    # Same person who was first responder
                    points = points_config['subsequent_response']
                else:
                    # Someone else replying after first response
                    points = points_config['subsequent_response']
        elif message.reference and message.reference.message_id:
            # This is a reply in a regular text channel
            ref_msg_id = str(message.reference.message_id)
            try:
                ref_msg = await message.channel.fetch_message(message.reference.message_id)
                
                # Skip self-replies
                if ref_msg.author.id == message.author.id:
                    return
                
                if ref_msg_id not in first_responders:
                    # First response to this message
                    first_responders[ref_msg_id] = message.author.id
                    points = points_config['first_response']
                else:
                    # Subsequent response
                    points = points_config['subsequent_response']
            except Exception:
                # If we can't fetch referenced message, treat as subsequent
                points = points_config['subsequent_response']
        else:
            # This is a new post (not a reply)
            points = points_config['first_post']
        
        if points > 0:
            if 'community_points' not in self.community_state:
                self.community_state['community_points'] = {}
            
            old_pts = self.community_state['community_points'].get(matching_user, 0)
            self.community_state['community_points'][matching_user] = old_pts + points
            self._save_community_state()
    
    def _get_channel_points(self, channel_id: int) -> Dict[str, int]:
        """Get point configuration for a channel.
        
        Returns channel-specific points if configured, otherwise default points.
        """
        channel_config = self.community_state.get('channels', {}).get(str(channel_id), {})
        saved_defaults = self.community_state.get('default_points', {})
        return {
            'first_post': channel_config.get('first_post', saved_defaults.get('first_post', 5)),
            'first_response': channel_config.get('first_response', saved_defaults.get('first_response', 8)),
            'subsequent_response': channel_config.get('subsequent_response', saved_defaults.get('subsequent_response', 2)),
            'emoji_reaction': channel_config.get('emoji_reaction', saved_defaults.get('emoji_reaction', 1))
        }
    
    async def _score_community_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        """Score an emoji reaction in real-time if it's in a tracked community channel.
        
        Called automatically when someone adds a reaction in a tracked channel.
        """
        # Get channel - check if it's tracked or if parent forum is tracked
        channel = self.bot.get_channel(payload.channel_id)
        if not channel:
            return
        
        tracked_channels = self.community_state.get('channels', {})
        cid_str = str(payload.channel_id)
        
        # For thread messages, check if parent forum is tracked
        if isinstance(channel, discord.Thread) and channel.parent:
            parent_id = channel.parent_id
            if str(parent_id) in tracked_channels:
                cid_str = str(parent_id)
        
        if cid_str not in tracked_channels:
            return
        
        # Get the message that was reacted to
        try:
            message = await channel.fetch_message(payload.message_id)
        except Exception:
            return
        
        # Skip self-reactions (reacting to your own message)
        if message.author.id == payload.user_id:
            return
        
        # Skip bot messages
        if message.author.bot:
            return
        
        # Get the user who reacted
        user = self.bot.get_user(payload.user_id)
        if not user or user.bot:
            return
        
        # Get master roster users
        master_users = set(self._get_master_discord_usernames())
        if not master_users:
            return
        
        # Find matching user in master roster
        author_name = user.name
        matching_user = self._find_matching_user_in_master(author_name, master_users)
        
        if not matching_user and hasattr(user, 'display_name'):
            matching_user = self._find_matching_user_in_master(user.display_name, master_users)
        
        if not matching_user:
            return
        
        # Check if this user already scored for reacting to this message
        # Only count once per user per message
        if 'reaction_scores' not in self.community_state:
            self.community_state['reaction_scores'] = {}
        
        msg_id_str = str(payload.message_id)
        user_id_str = str(payload.user_id)
        
        if msg_id_str not in self.community_state['reaction_scores']:
            self.community_state['reaction_scores'][msg_id_str] = []
        
        if user_id_str in self.community_state['reaction_scores'][msg_id_str]:
            # Already scored for this message
            return
        
        # Award points for the reaction
        points_config = self._get_channel_points(int(cid_str))
        points = points_config['emoji_reaction']
        
        if points > 0:
            if 'community_points' not in self.community_state:
                self.community_state['community_points'] = {}
            
            # Mark as scored for this message
            self.community_state['reaction_scores'][msg_id_str].append(user_id_str)
            
            old_pts = self.community_state['community_points'].get(matching_user, 0)
            self.community_state['community_points'][matching_user] = old_pts + points
            self._save_community_state()
    
    async def _process_message_reactions_batch(
        self, 
        message: discord.Message, 
        points_config: Dict[str, int],
        master_users: set,
        skipped_users: set
    ) -> int:
        """Process all reactions on a message for batch scoring.
        
        Returns total points awarded for reactions on this message.
        Only counts once per user per message (multiple emojis = 1 point).
        """
        total_reaction_points = 0
        reaction_points = points_config.get('emoji_reaction', 1)
        
        if reaction_points <= 0:
            return 0
        
        # Track users who have already been scored for this message
        scored_users: set = set()
        
        for reaction in message.reactions:
            try:
                async for user in reaction.users():
                    # Skip bots
                    if user.bot:
                        continue
                    
                    # Skip self-reactions
                    if user.id == message.author.id:
                        continue
                    
                    # Skip if already scored for this message
                    if user.id in scored_users:
                        continue
                    
                    # Find matching user in master roster
                    matching_user = self._find_matching_user_in_master(user.name, master_users)
                    
                    if not matching_user and hasattr(user, 'display_name'):
                        matching_user = self._find_matching_user_in_master(user.display_name, master_users)
                    
                    if not matching_user:
                        skipped_users.add(user.name)
                        continue
                    
                    # Award points and mark as scored
                    scored_users.add(user.id)
                    old_pts = self.community_state['community_points'].get(matching_user, 0)
                    self.community_state['community_points'][matching_user] = old_pts + reaction_points
                    total_reaction_points += reaction_points
            except Exception:
                continue
        
        return total_reaction_points
    
    @commands.group(name='community', invoke_without_command=True)
    async def community_group(self, ctx: commands.Context) -> None:
        """Community points tracking commands.
        
        Usage: !game community <subcommand>
        
        Subcommands:
            add_channel <channel_id> - Add a channel to track
            remove_channel <channel_id> - Remove a channel from tracking
            clear_all_channels - Remove all tracked channels
            process_scores - Process all messages and calculate scores
            leaderboard - Show community points leaderboard
            status - Show current community tracking status
        """
        if ctx.invoked_subcommand is None:
            await self._show_community_status(ctx)
    
    async def _show_community_status(self, ctx: commands.Context) -> None:
        """Show current community tracking status."""
        channels = self.community_state.get('channels', {})
        points = self.community_state.get('community_points', {})
        defaults = self.community_state.get('default_points', {})
        
        embed = discord.Embed(
            title="📊 Community Points Status",
            color=discord.Color.green()
        )
        
        if channels:
            channel_list = []
            for cid in channels.keys():
                channel = self.bot.get_channel(int(cid))
                name = f"#{channel.name}" if channel else f"Unknown ({cid})"
                channel_list.append(name)
            embed.add_field(
                name="📺 Tracked Channels",
                value="\n".join(channel_list) if channel_list else "None",
                inline=False
            )
        else:
            embed.add_field(name="📺 Tracked Channels", value="None configured", inline=False)
        
        embed.add_field(
            name="⚙️ Default Points",
            value=f"First Post: {defaults.get('first_post', 5)}\n"
                  f"First Response: {defaults.get('first_response', 8)}\n"
                  f"Subsequent Response: {defaults.get('subsequent_response', 2)}\n"
                  f"Emoji Reaction: {defaults.get('emoji_reaction', 1)}",
            inline=True
        )
        
        embed.add_field(
            name="👥 Participants",
            value=f"{len([p for p, pts in points.items() if pts > 0])} with points",
            inline=True
        )
        
        embed.set_footer(text="Use !game community <subcommand> for more options")
        await ctx.send(embed=embed)
    
    @community_group.command(name='add_channel')
    async def community_add_channel(self, ctx: commands.Context, channel_id: str = None) -> None:
        """Add a channel to community tracking.
        
        Usage: !game community add_channel <channel_id>
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure community tracking.")
            return
        
        if not channel_id:
            await ctx.send("Usage: `!game community add_channel <channel_id>`\n"
                          "Example: `!game community add_channel 123456789`")
            return
        
        try:
            cid = int(channel_id.strip('<>#'))
            channel = self.bot.get_channel(cid)
            
            if not channel:
                await ctx.send(f"❌ Channel `{cid}` not found or bot doesn't have access.")
                return
            
            if 'channels' not in self.community_state:
                self.community_state['channels'] = {}
            
            if str(cid) in self.community_state['channels']:
                await ctx.send(f"⚠️ Channel #{channel.name} is already being tracked.")
                return
            
            self.community_state['channels'][str(cid)] = {
                'added_at': datetime.now(timezone.utc).isoformat(),
                'last_processed_id': None
            }
            self._save_community_state()
            
            await ctx.send(f"✅ Added #{channel.name} to community tracking.\n"
                          f"📝 Run `!game community process_scores` to process existing messages.")
            
        except ValueError:
            await ctx.send("❌ Invalid channel ID. Provide a numeric channel ID.")
    
    @community_group.command(name='remove_channel')
    async def community_remove_channel(self, ctx: commands.Context, channel_id: str = None) -> None:
        """Remove a channel from community tracking.
        
        Usage: !game community remove_channel <channel_id>
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure community tracking.")
            return
        
        if not channel_id:
            await ctx.send("Usage: `!game community remove_channel <channel_id>`")
            return
        
        try:
            cid = int(channel_id.strip('<>#'))
            cid_str = str(cid)
            
            if cid_str not in self.community_state.get('channels', {}):
                await ctx.send(f"❌ Channel `{cid}` is not being tracked.")
                return
            
            channel = self.bot.get_channel(cid)
            channel_name = f"#{channel.name}" if channel else f"ID {cid}"
            
            del self.community_state['channels'][cid_str]
            self._save_community_state()
            
            await ctx.send(f"✅ Removed {channel_name} from community tracking.")
            
        except ValueError:
            await ctx.send("❌ Invalid channel ID.")
    
    @community_group.command(name='clear_all_channels')
    async def community_clear_channels(self, ctx: commands.Context, confirm: str = None) -> None:
        """Remove all channels from community tracking.
        
        Usage: !game community clear_all_channels confirm
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure community tracking.")
            return
        
        if confirm != "confirm":
            await ctx.send("⚠️ This will remove ALL tracked channels!\n"
                          "To confirm, use: `!game community clear_all_channels confirm`")
            return
        
        count = len(self.community_state.get('channels', {}))
        self.community_state['channels'] = {}
        self._save_community_state()
        
        await ctx.send(f"✅ Cleared {count} channel(s) from community tracking.")
    
    @community_group.command(name='process_scores')
    async def community_process_scores(self, ctx: commands.Context) -> None:
        """Process all messages and calculate community points from scratch.
        
        Usage: !game community process_scores
        
        This scans all messages in tracked channels and recalculates all scores.
        New messages are scored in real-time automatically once channels are set up.
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to process community scores.")
            return
        
        channels = self.community_state.get('channels', {})
        if not channels:
            await ctx.send("❌ No channels configured for tracking.\n"
                          "Use `!game community add_channel <channel_id>` first.")
            return
        
        master_users = set(self._get_master_discord_usernames())
        if not master_users:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        # Always do full reprocess - clear existing scores
        self.community_state['community_points'] = {}
        self.community_state['first_responders'] = {}
        self.community_state['reaction_scores'] = {}
        
        status_msg = await ctx.send("⏳ Processing community messages... This may take a while.")
        
        total_messages = 0
        total_points_awarded = 0
        skipped_users = set()
        
        for cid_str in channels.keys():
            cid = int(cid_str)
            channel = self.bot.get_channel(cid)
            
            if not channel:
                continue
            
            try:
                await status_msg.edit(content=f"⏳ Processing #{channel.name}...")
                
                # Track first responders per original message/thread for this channel
                thread_first_responders: Dict[int, int] = {}
                channel_messages = 0
                points_config = self._get_channel_points(cid)
                
                # Check if this is a ForumChannel
                if isinstance(channel, discord.ForumChannel):
                    # Process forum threads (both active and archived)
                    threads_processed = 0
                    
                    # Get active threads
                    active_threads = channel.threads
                    
                    # Get archived threads
                    archived_threads = []
                    async for thread in channel.archived_threads(limit=None):
                        archived_threads.append(thread)
                    
                    all_threads = list(active_threads) + archived_threads
                    
                    for thread in all_threads:
                        threads_processed += 1
                        thread_id = thread.id
                        
                        # Process thread starter message as first_post
                        try:
                            starter_msg = thread.starter_message
                            if not starter_msg:
                                starter_msg = await thread.fetch_message(thread.id)
                            
                            if starter_msg and not starter_msg.author.bot:
                                if not starter_msg.content.startswith('!'):
                                    channel_messages += 1
                                    total_messages += 1
                                    
                                    author_name = starter_msg.author.name
                                    matching_user = self._find_matching_user_in_master(author_name, master_users)
                                    
                                    if not matching_user and hasattr(starter_msg.author, 'display_name'):
                                        matching_user = self._find_matching_user_in_master(
                                            starter_msg.author.display_name, master_users
                                        )
                                    
                                    if matching_user:
                                        old_pts = self.community_state['community_points'].get(matching_user, 0)
                                        self.community_state['community_points'][matching_user] = (
                                            old_pts + points_config['first_post']
                                        )
                                        total_points_awarded += points_config['first_post']
                                    else:
                                        skipped_users.add(author_name)
                                    
                                    # Process reactions on starter message
                                    reaction_pts = await self._process_message_reactions_batch(
                                        starter_msg, points_config, master_users, skipped_users
                                    )
                                    total_points_awarded += reaction_pts
                        except Exception:
                            pass
                        
                        # Process replies in the thread
                        thread_owner_id = thread.owner_id
                        first_reply_recorded = False
                        
                        async for message in thread.history(limit=None, oldest_first=True):
                            # Skip the starter message (already processed)
                            if message.id == thread.id:
                                continue
                            
                            if message.author.bot:
                                continue
                            
                            if message.content.startswith('!'):
                                continue
                            
                            channel_messages += 1
                            total_messages += 1
                            
                            author_name = message.author.name
                            matching_user = self._find_matching_user_in_master(author_name, master_users)
                            
                            if not matching_user and hasattr(message.author, 'display_name'):
                                matching_user = self._find_matching_user_in_master(
                                    message.author.display_name, master_users
                                )
                            
                            if not matching_user:
                                skipped_users.add(author_name)
                                continue
                            
                            points = 0
                            
                            # First non-owner reply gets first_response points
                            if not first_reply_recorded and message.author.id != thread_owner_id:
                                thread_first_responders[thread_id] = message.author.id
                                points = points_config['first_response']
                                first_reply_recorded = True
                            else:
                                points = points_config['subsequent_response']
                            
                            if points > 0:
                                old_pts = self.community_state['community_points'].get(matching_user, 0)
                                self.community_state['community_points'][matching_user] = old_pts + points
                                total_points_awarded += points
                            
                            # Process reactions on this message
                            reaction_pts = await self._process_message_reactions_batch(
                                message, points_config, master_users, skipped_users
                            )
                            total_points_awarded += reaction_pts
                        
                        if threads_processed % 10 == 0:
                            await status_msg.edit(
                                content=f"⏳ Processing #{channel.name}... {threads_processed} threads, {channel_messages} messages"
                            )
                else:
                    # Regular text channel - process messages directly
                    async for message in channel.history(limit=None, oldest_first=True):
                        if message.author.bot:
                            continue
                        
                        if message.content.startswith('!'):
                            continue
                        
                        channel_messages += 1
                        total_messages += 1
                        
                        author_name = message.author.name
                        matching_user = self._find_matching_user_in_master(author_name, master_users)
                        
                        if not matching_user and hasattr(message.author, 'display_name'):
                            matching_user = self._find_matching_user_in_master(message.author.display_name, master_users)
                        
                        if not matching_user:
                            skipped_users.add(author_name)
                            continue
                        
                        points = 0
                        
                        if message.reference and message.reference.message_id:
                            ref_msg_id = message.reference.message_id
                            try:
                                ref_msg = await channel.fetch_message(ref_msg_id)
                                if ref_msg.author.id == message.author.id:
                                    continue
                                
                                if ref_msg_id not in thread_first_responders:
                                    thread_first_responders[ref_msg_id] = message.author.id
                                    points = points_config['first_response']
                                else:
                                    points = points_config['subsequent_response']
                            except Exception:
                                points = points_config['subsequent_response']
                        else:
                            points = points_config['first_post']
                        
                        if points > 0:
                            old_pts = self.community_state['community_points'].get(matching_user, 0)
                            self.community_state['community_points'][matching_user] = old_pts + points
                            total_points_awarded += points
                        
                        # Process reactions on this message
                        reaction_pts = await self._process_message_reactions_batch(
                            message, points_config, master_users, skipped_users
                        )
                        total_points_awarded += reaction_pts
                        
                        if channel_messages % 100 == 0:
                            await status_msg.edit(
                                content=f"⏳ Processing #{channel.name}... {channel_messages} messages"
                            )
                
                # Store first responders for real-time scoring
                if 'first_responders' not in self.community_state:
                    self.community_state['first_responders'] = {}
                self.community_state['first_responders'][cid_str] = {
                    str(k): v for k, v in thread_first_responders.items()
                }
                
            except discord.Forbidden:
                await ctx.send(f"⚠️ Missing permissions to read #{channel.name if channel else cid}")
            except Exception as e:
                await ctx.send(f"⚠️ Error processing channel {cid}: {e}")
        
        self._save_community_state()
        
        embed = discord.Embed(
            title="✅ Community Points Processed",
            color=discord.Color.green()
        )
        embed.add_field(name="📨 Messages Processed", value=str(total_messages), inline=True)
        embed.add_field(name="🏆 Points Awarded", value=str(total_points_awarded), inline=True)
        embed.add_field(
            name="👥 Participants",
            value=str(len([p for p, pts in self.community_state.get('community_points', {}).items() if pts > 0])),
            inline=True
        )
        
        if skipped_users:
            skipped_sample = list(skipped_users)[:5]
            skipped_text = ", ".join(skipped_sample)
            if len(skipped_users) > 5:
                skipped_text += f" +{len(skipped_users) - 5} more"
            embed.add_field(
                name="⏭️ Skipped (not in roster)",
                value=skipped_text,
                inline=False
            )
        
        embed.set_footer(text="New messages will be scored in real-time automatically.")
        await status_msg.edit(content=None, embed=embed)
    
    @community_group.command(name='leaderboard')
    async def community_leaderboard(self, ctx: commands.Context) -> None:
        """Show community points leaderboard.
        
        Usage: !game community leaderboard
        """
        # Get all users from master roster
        master_users = self._get_master_discord_usernames()
        if not master_users:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        community_points = self.community_state.get('community_points', {})
        
        # Build full list with all master users (0 points if not scored)
        all_scores = {}
        for username in master_users:
            all_scores[username] = community_points.get(username, 0)
        
        sorted_scores = sorted(all_scores.items(), key=lambda x: (-x[1], x[0].lower()))
        
        embed = discord.Embed(
            title="🏘️ Community Points Leaderboard",
            color=discord.Color.purple()
        )
        
        lines = []
        for rank, (username, pts) in enumerate(sorted_scores, 1):
            if rank <= 3 and pts > 0:
                medal = ["🥇", "🥈", "🥉"][rank - 1]
            elif pts > 0:
                medal = f"`{rank}.`"
            else:
                medal = "`-`"
            
            display = self._display_name(username)
            roster_normalized = self._normalize_name(username)
            for guild in self.bot.guilds:
                found = False
                for member in guild.members:
                    member_name_norm = self._normalize_name(member.name)
                    member_display_norm = self._normalize_name(member.display_name)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        display = member.display_name
                        found = True
                        break
                if found:
                    break
            
            lines.append(f"{medal} **{display}** — {pts} pts")
            
            if rank >= 25:
                remaining = len(sorted_scores) - 25
                if remaining > 0:
                    lines.append(f"... and {remaining} more")
                break
        
        if lines:
            embed.description = "\n".join(lines)
        else:
            embed.description = "No users in roster!"
        
        total_points = sum(pts for pts in community_points.values())
        embed.set_footer(text=f"Total members: {len(master_users)} | Total points: {total_points}")
        
        await ctx.send(embed=embed)
    
    @community_group.command(name='download')
    async def community_download(self, ctx: commands.Context) -> None:
        """Download community points leaderboard as CSV.
        
        Usage: !game community download
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to download community scores.")
            return
        
        # Get all users from master roster
        master_users = self._get_master_discord_usernames()
        if not master_users:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        # Get Discord username to Full Name mapping
        discord_to_name = self._get_master_discord_to_name_map()
        
        community_points = self.community_state.get('community_points', {})
        
        # Build full list with all master users (0 points if not scored)
        all_scores = {}
        for username in master_users:
            all_scores[username] = community_points.get(username, 0)
        
        sorted_scores = sorted(all_scores.items(), key=lambda x: (-x[1], x[0].lower()))
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Rank', 'Full Name', 'Discord Username', 'Points'])
        
        current_rank = 0
        prev_points = None
        for idx, (username, pts) in enumerate(sorted_scores, 1):
            # Handle ties - same points = same rank
            if pts != prev_points:
                current_rank = idx
            prev_points = pts
            
            rank_display = current_rank if pts > 0 else "-"
            full_name = discord_to_name.get(username, "")
            writer.writerow([rank_display, full_name, username, pts])
        
        output.seek(0)
        
        # Create file and send
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"community_leaderboard_{timestamp}.csv"
        
        file = discord.File(io.BytesIO(output.getvalue().encode('utf-8')), filename=filename)
        
        total_points = sum(pts for pts in community_points.values())
        await ctx.send(
            f"📥 **Community Leaderboard Export**\n"
            f"• Total members: {len(master_users)}\n"
            f"• Total points: {total_points}",
            file=file
        )
    
    @community_group.command(name='reset_scores')
    async def community_reset_scores(self, ctx: commands.Context, confirm: str = None) -> None:
        """Reset all community points to zero.
        
        Usage: !game community reset_scores confirm
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to reset community scores.")
            return
        
        if confirm != "confirm":
            await ctx.send("⚠️ This will reset ALL community points to zero!\n"
                          "To confirm, use: `!game community reset_scores confirm`")
            return
        
        self.community_state['community_points'] = {}
        self.community_state['processed_messages'] = {}
        self.community_state['reaction_scores'] = {}
        for cid in self.community_state.get('channels', {}):
            self.community_state['channels'][cid]['last_processed_id'] = None
        self._save_community_state()
        
        await ctx.send("🔄 Community points reset! All scores cleared and channels marked for reprocessing.")
    
    @community_group.command(name='set_points')
    async def community_set_points(self, ctx: commands.Context, point_type: str = None, 
                                   value: int = None, channel_id: str = None) -> None:
        """Set point values for community actions.
        
        Usage: 
            !game community set_points <type> <value> - Set default points
            !game community set_points <type> <value> <channel_id> - Set channel-specific points
        
        Point types: first_post, first_response, subsequent_response, emoji_reaction
        """
        if not self._check_permission(ctx):
            await ctx.send("❌ You don't have permission to configure community points.")
            return
        
        valid_types = ['first_post', 'first_response', 'subsequent_response', 'emoji_reaction']
        
        if not point_type or point_type not in valid_types or value is None:
            await ctx.send(
                "Usage: `!game community set_points <type> <value> [channel_id]`\n"
                f"Valid types: {', '.join(valid_types)}\n"
                "Example: `!game community set_points first_response 10`"
            )
            return
        
        if value < 0:
            await ctx.send("❌ Point value cannot be negative.")
            return
        
        if channel_id:
            cid = int(channel_id.strip('<>#'))
            cid_str = str(cid)
            
            if cid_str not in self.community_state.get('channels', {}):
                await ctx.send(f"❌ Channel `{cid}` is not being tracked.")
                return
            
            self.community_state['channels'][cid_str][point_type] = value
            channel = self.bot.get_channel(cid)
            channel_name = f"#{channel.name}" if channel else f"ID {cid}"
            await ctx.send(f"✅ Set `{point_type}` to **{value}** for {channel_name}")
        else:
            if 'default_points' not in self.community_state:
                self.community_state['default_points'] = {}
            self.community_state['default_points'][point_type] = value
            await ctx.send(f"✅ Set default `{point_type}` to **{value}**")
        
        self._save_community_state()
    
    @tasks.loop(count=1)
    async def trivia_loop(self):
        """Post the first trivia question when trivia starts."""
        await self._post_trivia_question()
    
    @trivia_loop.before_loop
    async def before_trivia_loop(self):
        """Wait until the bot is ready."""
        await self.bot.wait_until_ready()
    
    async def _post_trivia_question(self):
        """Post a random trivia question to the configured channel."""
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        
        used_questions = self.bot.trivia_state.get('used_questions', [])
        available = [q for q in self.trivia_questions if q['id'] not in used_questions]
        
        if not available:
            used_questions.clear()
            self.bot.trivia_state['used_questions'] = []
            self.bot.trivia_state['question_number'] = 0
            available = self.trivia_questions.copy()
            try:
                await channel.send("🔄 All questions exhausted! Starting fresh round...")
            except Exception as e:
                print(f"[Trivia] Failed to send reset message: {e}")
        
        if not available:
            return
        
        question = random.choice(available)
        
        # Increment sequential question number
        question_number = self.bot.trivia_state.get('question_number', 0) + 1
        self.bot.trivia_state['question_number'] = question_number
        
        self.bot.trivia_state['current_question'] = question
        self.bot.trivia_state['answered_by'] = None
        self.bot.trivia_state['used_questions'].append(question['id'])
        self.bot.trivia_state['question_posted_at'] = discord.utils.utcnow().isoformat()
        self.bot.save_trivia_state()
        
        trivia_pts = PersistenceService.get_trivia_points()
        embed = discord.Embed(
            title="❓ Trivia Question",
            description=question['question'],
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"First correct answer wins {trivia_pts} points! ? Question #{question_number}")
        
        try:
            await channel.send(embed=embed)
        except Exception as e:
            print(f"[Trivia] Failed to send question: {e}")
            return
        
        # Start timeout aligned to clock boundaries
        interval = self.bot.trivia_state.get('interval_minutes', 5)
        if interval > 0:
            # Cancel any existing timeout task before creating new one
            if self.current_timeout_task and not self.current_timeout_task.done():
                self.current_timeout_task.cancel()
            seconds_until_timeout = self._get_seconds_until_next_boundary(interval)
            self.current_timeout_task = self.bot.loop.create_task(
                self._check_question_timeout_seconds(question['id'], seconds_until_timeout)
            )
    
    async def _check_question_timeout(self, question_id: str, timeout_minutes: int):
        """Check if question timed out without correct answer."""
        await asyncio.sleep(timeout_minutes * 60)
        
        current_q = self.bot.trivia_state.get('current_question')
        if not current_q:
            return
        
        if current_q['id'] != question_id:
            return
        
        if self.bot.trivia_state.get('answered_by'):
            return
        
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        
        self.bot.trivia_state['current_question'] = None
        self.bot.save_trivia_state()
        
        try:
            await channel.send(f"⏱️ Time's up! The correct answer was: **{current_q['answer']}**")
        except Exception as e:
            print(f"[Trivia] Failed to send timeout message: {e}")
        
        # Post the next question after a short delay
        await asyncio.sleep(3)
        try:
            await self._post_trivia_question()
        except Exception as e:
            print(f"[Trivia] Failed to post next question: {e}")
    
    async def _check_question_timeout_seconds(self, question_id: str, timeout_seconds: float):
        """Check if question timed out (seconds version for precise timing)."""
        await asyncio.sleep(timeout_seconds)
        
        # Use lock to prevent race condition with duplicate timeout tasks
        async with self._timeout_lock:
            current_q = self.bot.trivia_state.get('current_question')
            if not current_q or current_q['id'] != question_id:
                return
            
            if self.bot.trivia_state.get('answered_by'):
                return
            
            try:
                await self._timeout_current_question()
            except Exception as e:
                print(f"[Trivia] Error during timeout: {e}")
    
    async def _timeout_current_question(self):
        """Immediately timeout the current question."""
        current_q = self.bot.trivia_state.get('current_question')
        if not current_q:
            return
        
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        
        self.bot.trivia_state['current_question'] = None
        self.bot.save_trivia_state()
        
        try:
            await channel.send(f"⏱️ Time's up! The correct answer was: **{current_q['answer']}**")
        except Exception as e:
            print(f"[Trivia] Failed to send timeout message: {e}")
        
        # Post the next question after a short delay
        await asyncio.sleep(3)
        try:
            await self._post_trivia_question()
        except Exception as e:
            print(f"[Trivia] Failed to post next question: {e}")
    
    async def _handle_trivia_points(self, message: discord.Message, cmd: str):
        """Handle !trivia points command."""
        trivia_points = self.bot.trivia_state.get('trivia_points', {})
        
        # Check if looking up another user
        parts = cmd.split(maxsplit=1)
        if len(parts) > 1:
            username = parts[1].strip()
            matching_user = self._find_matching_user(username)
            if matching_user:
                pts = trivia_points.get(matching_user, 0)
                display = self._display_name(matching_user)
                await message.channel.send(f"🎯 **{display}** has **{pts}** trivia points.")
            else:
                await message.channel.send(f"❌ User `{username}` not found in trivia scores.")
            return
        
        # Look up own points
        author_name = message.author.name
        matching_user = self._find_matching_user(author_name)
        
        if not matching_user and hasattr(message.author, 'display_name'):
            matching_user = self._find_matching_user(message.author.display_name)
        
        if not matching_user:
            author_norm = self._normalize_name(author_name)
            display_norm = self._normalize_name(message.author.display_name) if hasattr(message.author, 'display_name') else ""
            for roster_name in trivia_points.keys():
                roster_norm = self._normalize_name(roster_name)
                if roster_norm == author_norm or roster_norm == display_norm:
                    matching_user = roster_name
                    break
        
        if matching_user:
            pts = trivia_points.get(matching_user, 0)
            await message.channel.send(f"🎯 You have **{pts}** trivia points, {message.author.mention}!")
        else:
            await message.channel.send(f"❌ You haven't earned any trivia points yet.")
    
    async def _handle_trivia_leaderboard(self, message: discord.Message):
        """Handle !trivia leaderboard command."""
        trivia_points = self.bot.trivia_state.get('trivia_points', {})
        
        if not trivia_points:
            await message.channel.send("📊 No trivia scores yet. Answer questions correctly to earn points!")
            return
        
        sorted_scores = sorted(trivia_points.items(), key=lambda x: (-x[1], x[0].lower()))
        
        embed = discord.Embed(
            title="🎯 Trivia Leaderboard",
            color=discord.Color.blue()
        )
        
        lines = []
        for rank, (username, pts) in enumerate(sorted_scores, 1):
            if pts == 0:
                continue
            if rank <= 3:
                medal = ["🥇", "🥈", "🥉"][rank - 1]
            else:
                medal = f"`{rank}.`"
            
            display = self._display_name(username)
            roster_normalized = self._normalize_name(username)
            for guild in self.bot.guilds:
                found = False
                for member in guild.members:
                    member_name_norm = self._normalize_name(member.name)
                    member_display_norm = self._normalize_name(member.display_name)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        display = member.display_name
                        found = True
                        break
                if found:
                    break
            
            lines.append(f"{medal} **{display}** — {pts} pts")
        
        if lines:
            embed.description = "\n".join(lines)
        else:
            embed.description = "No scores yet!"
        
        await message.channel.send(embed=embed)
    
    async def _handle_community_points(self, message: discord.Message, cmd: str):
        """Handle !community points command."""
        community_points = self.community_state.get('community_points', {})
        
        parts = cmd.split(maxsplit=1)
        if len(parts) > 1:
            username = parts[1].strip()
            matching_user = self._find_matching_user(username)
            if matching_user:
                pts = community_points.get(matching_user, 0)
                display = self._display_name(matching_user)
                await message.channel.send(f"🏘️ **{display}** has **{pts}** community points.")
            else:
                await message.channel.send(f"❌ User `{username}` not found in community scores.")
            return
        
        author_name = message.author.name
        matching_user = self._find_matching_user(author_name)
        
        if not matching_user and hasattr(message.author, 'display_name'):
            matching_user = self._find_matching_user(message.author.display_name)
        
        if not matching_user:
            author_norm = self._normalize_name(author_name)
            display_norm = self._normalize_name(message.author.display_name) if hasattr(message.author, 'display_name') else ""
            for roster_name in community_points.keys():
                roster_norm = self._normalize_name(roster_name)
                if roster_norm == author_norm or roster_norm == display_norm:
                    matching_user = roster_name
                    break
        
        if matching_user:
            pts = community_points.get(matching_user, 0)
            await message.channel.send(f"🏘️ You have **{pts}** community points, {message.author.mention}!")
        else:
            await message.channel.send(f"❌ You haven't earned any community points yet, or your Discord username isn't in the roster.")
    
    async def _handle_community_leaderboard(self, message: discord.Message):
        """Handle !community leaderboard command."""
        # Get all users from master roster
        master_users = self._get_master_discord_usernames()
        if not master_users:
            await message.channel.send("❌ No master roster uploaded.")
            return
        
        community_points = self.community_state.get('community_points', {})
        
        # Build full list with all master users (0 points if not scored)
        all_scores = {}
        for username in master_users:
            all_scores[username] = community_points.get(username, 0)
        
        sorted_scores = sorted(all_scores.items(), key=lambda x: (-x[1], x[0].lower()))
        
        embed = discord.Embed(
            title="🏘️ Community Points Leaderboard",
            color=discord.Color.purple()
        )
        
        lines = []
        for rank, (username, pts) in enumerate(sorted_scores, 1):
            if rank <= 3 and pts > 0:
                medal = ["🥇", "🥈", "🥉"][rank - 1]
            elif pts > 0:
                medal = f"`{rank}.`"
            else:
                medal = "`-`"
            
            display = self._display_name(username)
            roster_normalized = self._normalize_name(username)
            for guild in self.bot.guilds:
                found = False
                for member in guild.members:
                    member_name_norm = self._normalize_name(member.name)
                    member_display_norm = self._normalize_name(member.display_name)
                    if roster_normalized == member_name_norm or roster_normalized == member_display_norm:
                        display = member.display_name
                        found = True
                        break
                if found:
                    break
            
            lines.append(f"{medal} **{display}** — {pts} pts")
            
            if rank >= 15:
                remaining = len(sorted_scores) - 15
                if remaining > 0:
                    lines.append(f"... and {remaining} more")
                break
        
        if lines:
            embed.description = "\n".join(lines)
        else:
            embed.description = "No users in roster!"
        
        total_points = sum(pts for pts in community_points.values())
        embed.set_footer(text=f"Total members: {len(master_users)} | Total points: {total_points}")
        
        await message.channel.send(embed=embed)
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for trivia answers, !trivia commands, and !community commands."""
        if message.author.bot:
            return
        
        content = message.content.strip()
        
        # Handle !community commands (works anywhere)
        if content.lower().startswith('!community '):
            cmd = content[11:].strip().lower()
            
            if cmd == 'points' or cmd.startswith('points '):
                await self._handle_community_points(message, cmd)
                return
            elif cmd == 'leaderboard':
                await self._handle_community_leaderboard(message)
                return
            elif cmd == 'help':
                await message.channel.send(
                    "🏘️ **Community Commands**:\n"
                    "`!community points` - Check your community points\n"
                    "`!community points <user>` - Check another user's points\n"
                    "`!community leaderboard` - View community leaderboard"
                )
                return
        
        # Real-time community points scoring
        await self._score_community_message(message)
        
        # Trivia functionality only works in trivia channel
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id or message.channel.id != channel_id:
            return
        
        # Handle !trivia commands in trivia channel
        if content.lower().startswith('!trivia '):
            cmd = content[8:].strip().lower()
            
            if cmd == 'points' or cmd.startswith('points '):
                await self._handle_trivia_points(message, cmd)
                return
            elif cmd == 'leaderboard':
                await self._handle_trivia_leaderboard(message)
                return
            elif cmd == 'help':
                await message.channel.send(
                    "🎯 **Trivia Commands** (only work in trivia channel):\n"
                    "`!trivia points` - Check your trivia points\n"
                    "`!trivia points <user>` - Check another user's trivia points\n"
                    "`!trivia leaderboard` - View trivia-only leaderboard"
                )
                return
        
        # Use lock to prevent race condition with timeout
        async with self._timeout_lock:
            current_q = self.bot.trivia_state.get('current_question')
            if not current_q:
                return
            
            if self.bot.trivia_state.get('answered_by'):
                return
            
            user_answer = content.lower()
            correct_answer = current_q['answer'].strip().lower()
            
            if user_answer != correct_answer:
                return
            
            # Mark as answered immediately to prevent race with timeout
            self.bot.trivia_state['answered_by'] = message.author.id
            self.bot.trivia_state['current_question'] = None
            self.bot.save_trivia_state()
            
            # Cancel any pending timeout task
            if self.current_timeout_task and not self.current_timeout_task.done():
                self.current_timeout_task.cancel()
        
        # Rest of processing can happen outside the lock
        self._sync_points_with_master()
        
        author_name = message.author.name
        matching_user = self._find_matching_user(author_name)
        
        # Also try display_name (nickname) if no match
        if not matching_user and hasattr(message.author, 'display_name'):
            matching_user = self._find_matching_user(message.author.display_name)
        
        # Try normalized matching against all roster entries
        if not matching_user:
            author_norm = self._normalize_name(author_name)
            display_norm = self._normalize_name(message.author.display_name) if hasattr(message.author, 'display_name') else ""
            for roster_name in self.bot.game_points.keys():
                roster_norm = self._normalize_name(roster_name)
                if roster_norm == author_norm or roster_norm == display_norm:
                    matching_user = roster_name
                    break
        
        trivia_pts = PersistenceService.get_trivia_points()
        
        if matching_user:
            # Track trivia-specific points first (for display)
            if 'trivia_points' not in self.bot.trivia_state:
                self.bot.trivia_state['trivia_points'] = {}
            old_trivia_pts = self.bot.trivia_state['trivia_points'].get(matching_user, 0)
            new_trivia_pts = old_trivia_pts + trivia_pts
            self.bot.trivia_state['trivia_points'][matching_user] = new_trivia_pts
            self.bot.save_trivia_state()
            
            # Also add to overall game points
            old_game_pts = self.bot.game_points[matching_user]
            self.bot.game_points[matching_user] = old_game_pts + trivia_pts
            self.bot.save_game_points()
            
            await message.channel.send(
                f"🎉 **Correct!** {message.author.mention} got it!\n"
                f"✅ Answer: `{current_q['answer']}`\n"
                f"🏆 +{trivia_pts} trivia points ({old_trivia_pts} → {new_trivia_pts})"
            )
        else:
            await message.channel.send(
                f"🎉 **Correct!** {message.author.mention} got it!\n"
                f"✅ Answer: `{current_q['answer']}`\n"
                f"⚠️ But your Discord username isn't in the roster, no points awarded."
            )
        
        # Post the next question after a short delay
        await asyncio.sleep(5)
        await self._post_trivia_question()
    
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Listen for emoji reactions in tracked community channels."""
        # Skip bot reactions
        if payload.user_id == self.bot.user.id:
            return
        
        # Score the reaction for community points
        await self._score_community_reaction(payload)


async def setup(bot: 'DiscordBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(GameCog(bot))





