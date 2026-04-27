"""Report module for student activity analysis.

Commands:
    !report <student_id>          - Show student info, commits/MRs breakdown, per-week analysis
    !report <student_id> validate - Validate commits match the student's GitLab username
    !report all                   - Download all student data as CSV
    !report all validate          - Download all with ownership validation
    !report help                  - Show this help message
"""

import asyncio
import io
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands

from services.report_service import ReportService, StudentReport
from utils.embeds import EmbedBuilder


class ReportCog(commands.Cog, name="Report"):
    """Cog for generating student activity reports.
    
    Analyzes commits and MRs from student READMEs across all typeform submissions.
    
    Note: Bot uses '!report ' as prefix, so commands are direct (not subcommands).
    """
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.report_service = ReportService(
            storage=bot.file_storage
        )
    
    def _create_report_embed(self, report: StudentReport, validate: bool = False) -> discord.Embed:
        """Create a Discord embed from a StudentReport."""
        embed = discord.Embed(
            title=f"📊 Report: {report.name or 'Unknown'}",
            color=discord.Color.blue() if not validate else (
                discord.Color.green() if not report.validation_errors else discord.Color.red()
            )
        )
        
        embed.add_field(
            name="Student Info",
            value=(
                f"**Member ID:** `{report.member_id}`\n"
                f"**GitLab:** `{report.gitlab_username or 'Unknown'}`\n"
                f"**READMEs:** {len(report.readme_urls)}"
            ),
            inline=False
        )
        
        # Git activity summary
        embed.add_field(
            name="📈 Git Activity",
            value=(
                f"**Total Commits:** {report.total_commits}\n"
                f"**Total MRs:** {report.total_mrs}"
            ),
            inline=True
        )
        
        # MR Status Breakdown
        embed.add_field(
            name="🔀 MR Status",
            value=(
                f"✅ Merged: {report.merged_mrs}\n"
                f"📝 Open: {report.open_mrs}\n"
                f"❌ Closed: {report.closed_mrs}"
            ),
            inline=True
        )
        
        # Submissions tracking
        total_expected = report.total_expected_weeks
        wed_pct = (report.total_wed_submissions / total_expected * 100) if total_expected > 0 else 0
        sun_pct = (report.total_sun_submissions / total_expected * 100) if total_expected > 0 else 0
        
        embed.add_field(
            name="📋 Submissions",
            value=(
                f"**Wed:** {report.total_wed_submissions}/{total_expected} ({wed_pct:.0f}%)\n"
                f"**Sun:** {report.total_sun_submissions}/{total_expected} ({sun_pct:.0f}%)"
            ),
            inline=True
        )
        
        if validate:
            embed.add_field(
                name="✅ Ownership Validation",
                value=(
                    f"**Owned Commits:** {report.owned_commits}/{report.total_commits}\n"
                    f"**Owned MRs:** {report.owned_mrs}/{report.total_mrs}"
                ),
                inline=True
            )
        
        # Per-week activity breakdown
        week_lines = []
        all_weeks = sorted(set(report.weeks_with_commits) | set(report.weeks_with_mrs) | 
                          set(report.weeks_with_wed_submissions) | set(report.weeks_with_sun_submissions))
        
        for week in range(1, min(total_expected + 1, 11)):
            commits = len(report.commits_by_week.get(week, []))
            mrs = len(report.mrs_by_week.get(week, []))
            wed = "✓" if report.wed_submissions_by_week.get(week) else "✗"
            sun = "✓" if report.sun_submissions_by_week.get(week) else "✗"
            
            # Only show weeks with some activity or expected submissions
            if commits > 0 or mrs > 0 or week <= total_expected:
                week_lines.append(f"**W{week}:** {commits}c/{mrs}mr | Wed:{wed} Sun:{sun}")
        
        if week_lines:
            # Split into two columns if too many weeks
            if len(week_lines) > 5:
                col1 = week_lines[:5]
                col2 = week_lines[5:10]
                embed.add_field(
                    name="📅 Weekly Breakdown (W1-5)",
                    value='\n'.join(col1),
                    inline=True
                )
                if col2:
                    embed.add_field(
                        name="📅 Weekly Breakdown (W6-10)",
                        value='\n'.join(col2),
                        inline=True
                    )
            else:
                embed.add_field(
                    name="📅 Weekly Breakdown",
                    value='\n'.join(week_lines),
                    inline=False
                )
        
        # MR details
        mr_lines = []
        for mr in report.merge_requests[:5]:
            if mr.is_merged:
                status_emoji = "✅"
            elif mr.state == 'opened':
                status_emoji = "📝"
            else:
                status_emoji = "❌"
            title_short = mr.title[:35] + "..." if len(mr.title) > 35 else mr.title
            owner_marker = " 👤" if validate and mr.is_owned else ""
            mr_lines.append(f"{status_emoji} [{title_short}]({mr.url}){owner_marker}")
        
        if mr_lines:
            if len(report.merge_requests) > 5:
                mr_lines.append(f"_...and {len(report.merge_requests) - 5} more MRs_")
            embed.add_field(
                name="🔗 MR Details",
                value='\n'.join(mr_lines),
                inline=False
            )
        
        if validate and report.validation_errors:
            error_lines = report.validation_errors[:5]
            if len(report.validation_errors) > 5:
                error_lines.append(f"_...and {len(report.validation_errors) - 5} more issues_")
            embed.add_field(
                name="⚠️ Validation Issues",
                value='\n'.join(f"• {e}" for e in error_lines),
                inline=False
            )
        
        if report.readme_urls:
            readme_lines = []
            for url in report.readme_urls[:3]:
                short_url = url[:50] + "..." if len(url) > 50 else url
                readme_lines.append(f"• [{short_url}]({url})")
            if len(report.readme_urls) > 3:
                readme_lines.append(f"_...and {len(report.readme_urls) - 3} more_")
            embed.add_field(
                name="📝 README URLs",
                value='\n'.join(readme_lines),
                inline=False
            )
        
        embed.set_footer(text=f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return embed
    
    async def _generate_student_report(self, ctx: commands.Context, student_id: str, validate: bool = False):
        """Internal method to generate a student report."""
        async with ctx.typing():
            status_msg = await ctx.send(
                f"🔍 {'Validating' if validate else 'Generating'} report for student `{student_id}`..."
            )
            
            try:
                report = await asyncio.to_thread(
                    self.report_service.generate_student_report,
                    student_id,
                    validate_ownership=validate
                )
                
                if not report.readme_urls:
                    await status_msg.edit(content=(
                        f"⚠️ No data found for student ID `{student_id}`.\n"
                        f"Make sure the student has submitted typeform responses with README links."
                    ))
                    return
                
                embed = self._create_report_embed(report, validate=validate)
                await status_msg.edit(content=None, embed=embed)
                
            except Exception as e:
                await status_msg.edit(content=f"❌ Error generating report: {str(e)}")
    
    @commands.command(name='get')
    async def report_student(self, ctx: commands.Context, student_id: str, action: str = None):
        """Generate a report for a specific student using explicit command.
        
        Usage:
            !report get <student_id>          - Show student activity report
            !report get <student_id> validate - Validate commit/MR ownership
        """
        validate = action and action.lower() == 'validate'
        await self._generate_student_report(ctx, student_id, validate)
    
    @commands.command(name='all')
    async def report_all(self, ctx: commands.Context, action: str = None):
        """Download reports for all students as a CSV file.
        
        Usage:
            !report all          - Generate reports for all students
            !report all validate - Generate reports with ownership validation
        """
        validate = action and action.lower() == 'validate'
        
        async with ctx.typing():
            mode_str = "with validation" if validate else ""
            status_msg = await ctx.send(f"📊 Collecting student data from all typeform submissions {mode_str}...")
            
            try:
                students = await asyncio.to_thread(
                    self.report_service.collect_all_students_data
                )
                
                total = len(students)
                if total == 0:
                    await status_msg.edit(content="⚠️ No student data found in typeform CSVs.")
                    return
                
                await status_msg.edit(content=f"📊 Found {total} students. Generating reports {mode_str} (this may take a while)...")
                
                reports = []
                last_update = datetime.now()
                validation_issues_count = 0
                
                for i, (member_id, data) in enumerate(students.items()):
                    report = await asyncio.to_thread(
                        self.report_service.generate_student_report,
                        member_id,
                        validate
                    )
                    reports.append(report)
                    
                    if validate and report.validation_errors:
                        validation_issues_count += 1
                    
                    now = datetime.now()
                    if (now - last_update).seconds >= 5:
                        await status_msg.edit(
                            content=f"📊 Processing {mode_str}... {i+1}/{total} students ({(i+1)*100//total}%)"
                        )
                        last_update = now
                
                await status_msg.edit(content=f"📊 Generated {len(reports)} reports. Creating CSV...")
                
                csv_bytes = self.report_service.export_reports_csv(reports)
                
                filename = f"student_reports{'_validated' if validate else ''}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                file = discord.File(io.BytesIO(csv_bytes), filename=filename)
                
                # Calculate summary stats
                total_commits = sum(r.total_commits for r in reports)
                total_mrs = sum(r.total_mrs for r in reports)
                merged_mrs = sum(r.merged_mrs for r in reports)
                open_mrs = sum(r.open_mrs for r in reports)
                closed_mrs = sum(r.closed_mrs for r in reports)
                total_wed = sum(r.total_wed_submissions for r in reports)
                total_sun = sum(r.total_sun_submissions for r in reports)
                expected_per_student = 10
                total_expected = len(reports) * expected_per_student
                
                # Count students with at least 1 merged MR
                students_with_merged = sum(1 for r in reports if r.merged_mrs > 0)
                
                summary_lines = [
                    f"✅ **Student Reports Generated{'  (Validated)' if validate else ''}**",
                    f"",
                    f"**Students:** {len(reports)}",
                    f"**Total Commits:** {total_commits}",
                    f"",
                    f"**MR Breakdown:**",
                    f"• Total MRs: {total_mrs}",
                    f"• ✅ Merged (total): {merged_mrs}",
                    f"• 👥 Students with 1+ merged: {students_with_merged}",
                    f"• 📝 Open: {open_mrs}",
                    f"• ❌ Closed: {closed_mrs}",
                    f"",
                    f"**Submissions:**",
                    f"• Wed: {total_wed}/{total_expected} ({total_wed*100//total_expected if total_expected else 0}%)",
                    f"• Sun: {total_sun}/{total_expected} ({total_sun*100//total_expected if total_expected else 0}%)",
                ]
                
                if validate:
                    owned_commits = sum(r.owned_commits for r in reports)
                    owned_mrs = sum(r.owned_mrs for r in reports)
                    summary_lines.extend([
                        f"",
                        f"**Validation:**",
                        f"• Owned Commits: {owned_commits}/{total_commits}",
                        f"• Owned MRs: {owned_mrs}/{total_mrs}",
                        f"• Students with issues: {validation_issues_count}",
                    ])
                
                await status_msg.edit(content='\n'.join(summary_lines))
                await ctx.send(file=file)
                
            except Exception as e:
                await status_msg.edit(content=f"❌ Error generating reports: {str(e)}")
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle !report <student_id> pattern directly (without 'get' keyword)."""
        if message.author.bot:
            return
        
        content = message.content.strip()
        
        if not content.startswith('!report '):
            return
        
        args = content[8:].strip().split()
        
        if not args:
            return
        
        first_arg = args[0].lower()
        # Skip if it's a known command
        if first_arg in ('help', 'all', 'get'):
            return
        
        # Handle numeric student IDs directly
        if first_arg.isdigit():
            ctx = await self.bot.get_context(message)
            student_id = args[0]
            validate = len(args) > 1 and args[1].lower() == 'validate'
            await self._generate_student_report(ctx, student_id, validate)


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(ReportCog(bot))
