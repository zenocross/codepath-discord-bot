"""Game/Points tracking module (Cog)."""

import asyncio
import csv
import io
import random
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import discord
from discord.ext import commands, tasks

from services.file_processor import FileStorageService
from services.persistence import PersistenceService

if TYPE_CHECKING:
    from bot.client import GitLabRSSBot


class GameCog(commands.Cog, name="Game"):
    """Commands for managing game points and standings."""
    
    def __init__(self, bot: 'GitLabRSSBot'):
        self.bot = bot
        self.file_storage = FileStorageService()
        self.trivia_questions = PersistenceService.load_trivia_questions()
        self.trivia_points = PersistenceService.get_trivia_points()
        self.current_timeout_task = None
        
        # Resume trivia if channel is configured
        if self.bot.trivia_state.get('channel_id'):
            current_q = self.bot.trivia_state.get('current_question')
            if current_q and not self.bot.trivia_state.get('answered_by'):
                # Resume timeout for existing question using aligned boundaries
                interval = self.bot.trivia_state.get('interval_minutes', 5)
                if interval > 0:
                    seconds_until = self._get_seconds_until_next_boundary(interval)
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
            await channel.send(
                f"🔄 **Trivia Resumed!**\n"
                f"Question #{question_number} is still active.\n"
                f"⏱️ Will timeout at **{timeout_time.hour:02d}:{timeout_time.minute:02d} UTC** (~{int(seconds_until/60)} min)"
            )
    
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
            await channel.send("🔄 All questions exhausted! Starting fresh round...")
        
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
        
        await channel.send(embed=embed)
        
        # Start timeout aligned to clock boundaries
        interval = self.bot.trivia_state.get('interval_minutes', 5)
        if interval > 0:
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
        
        await channel.send(f"⏱️ Time's up! The correct answer was: **{current_q['answer']}**")
        
        # Post the next question after a short delay
        await asyncio.sleep(3)
        await self._post_trivia_question()
    
    async def _check_question_timeout_seconds(self, question_id: str, timeout_seconds: float):
        """Check if question timed out (seconds version for precise timing)."""
        await asyncio.sleep(timeout_seconds)
        
        current_q = self.bot.trivia_state.get('current_question')
        if not current_q or current_q['id'] != question_id:
            return
        
        if self.bot.trivia_state.get('answered_by'):
            return
        
        await self._timeout_current_question()
    
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
        
        await channel.send(f"⏱️ Time's up! The correct answer was: **{current_q['answer']}**")
        
        # Post the next question after a short delay
        await asyncio.sleep(3)
        await self._post_trivia_question()
    
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
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for trivia answers and !trivia commands."""
        if message.author.bot:
            return
        
        channel_id = self.bot.trivia_state.get('channel_id')
        if not channel_id or message.channel.id != channel_id:
            return
        
        content = message.content.strip()
        
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
        
        current_q = self.bot.trivia_state.get('current_question')
        if not current_q:
            return
        
        if self.bot.trivia_state.get('answered_by'):
            return
        
        user_answer = content.lower()
        correct_answer = current_q['answer'].strip().lower()
        
        if user_answer == correct_answer:
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
            
            self.bot.trivia_state['answered_by'] = message.author.id
            self.bot.trivia_state['current_question'] = None
            self.bot.save_trivia_state()
            
            trivia_pts = PersistenceService.get_trivia_points()
            
            if matching_user:
                old_game_pts = self.bot.game_points[matching_user]
                self.bot.game_points[matching_user] = old_game_pts + trivia_pts
                self.bot.save_game_points()
                new_game_pts = self.bot.game_points[matching_user]
                
                # Also track trivia-specific points
                if 'trivia_points' not in self.bot.trivia_state:
                    self.bot.trivia_state['trivia_points'] = {}
                old_trivia_pts = self.bot.trivia_state['trivia_points'].get(matching_user, 0)
                self.bot.trivia_state['trivia_points'][matching_user] = old_trivia_pts + trivia_pts
                self.bot.save_trivia_state()
                
                await message.channel.send(
                    f"🎉 **Correct!** {message.author.mention} got it!\n"
                    f"✅ Answer: `{current_q['answer']}`\n"
                    f"🏆 +{trivia_pts} points ({old_game_pts} → {new_game_pts})"
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


async def setup(bot: 'GitLabRSSBot') -> None:
    """Setup function for loading the cog."""
    await bot.add_cog(GameCog(bot))
