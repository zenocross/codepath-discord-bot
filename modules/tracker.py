"""Tracker module for processing CSV files to Excel.

Commands:
    !tracker upload           - Interactive upload wizard
    !tracker upload master    - Upload master roster CSV
    !tracker upload typeform  - Upload typeform responses CSV
    !tracker upload zoom      - Upload zoom attendance CSV
    !tracker upload app       - Upload app data CSV (phone numbers)
    !tracker download         - Generate Excel report from uploaded CSVs
    !tracker files            - Show status of uploaded CSV files
    !tracker clear <type>     - Clear specific CSV file
    !tracker clearall         - Clear all uploaded CSV files
    !tracker start_date       - Set or view program start date
    !tracker submissions      - Real-time submission checking
    !tracker submissions_download [options] - Download report filtered by submissions date
        Options: nofilter, validate_commits, validate_all
    !tracker set_phase_complete <phase> <member_id> - Set a student's completed phase
    !tracker get_member_id <discord_info> - Look up member ID from Discord username/ID
    !tracker no_issues        - Show issue status from validated data (requires validate first)
    !tracker no_issues quick  - Quick list of students without issue_url (no validation)
    !tracker no_issues validate - Crawl READMEs to find/validate issue URLs
    !tracker search_issues_title <term> - Search issue titles (use NOT:<term> to exclude)
    !tracker search_dl_issues_title <term> - Search + download CSV (use NOT:<term> to exclude)
    !tracker help             - Show help (handled by bot/events.py)
"""

import asyncio
import io
from datetime import datetime, timedelta
from typing import Optional

import discord
from discord.ext import commands

from services.file_processor import VALID_FILE_CATEGORIES
from services.tracker_processor import TrackerDataProcessor
from services.gitlab_service import GitLabService


# File category descriptions
FILE_DESCRIPTIONS = {
    "master": "Master Roster (student list with enrollment data)",
    "typeform": "Typeform Responses (weekly progress submissions)",
    "zoom": "Zoom Attendance (lecture/office hours attendance)",
    "app": "App Data (phone numbers and additional contact info)"
}


class TrackerCog(commands.Cog, name="Tracker"):
    """Cog for processing tracker CSV files.
    
    Supports uploading 3 separate CSV files (master, typeform, zoom)
    and generating comprehensive Excel reports.
    
    Note: Bot uses '!tracker ' as prefix, so commands are direct (not subcommands).
    """
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.storage = bot.file_storage  # Use shared instance
        self.processor = TrackerDataProcessor()
        self.gitlab = GitLabService()
        # Track users in upload wizard to prevent conflicts
        self._upload_sessions: dict[int, str] = {}
    
    async def _wait_for_csv(self, ctx: commands.Context, 
                           category: str, timeout: float = 120.0) -> Optional[bytes]:
        """Wait for a CSV file upload from the user.
        
        Returns the file bytes if successful, None if cancelled or timed out.
        """
        def check(message: discord.Message) -> bool:
            # Same user, same channel
            if message.author.id != ctx.author.id or message.channel.id != ctx.channel.id:
                return False
            
            # Check for cancel command
            if message.content.lower() in ['cancel', '!cancel']:
                return True
            
            # Check for CSV attachment
            for attachment in message.attachments:
                if attachment.filename.lower().endswith('.csv'):
                    return True
            
            return False
        
        try:
            message = await self.bot.wait_for('message', check=check, timeout=timeout)
            
            # Check if cancelled
            if message.content.lower() in ['cancel', '!cancel']:
                return None
            
            # Get CSV attachment
            for attachment in message.attachments:
                if attachment.filename.lower().endswith('.csv'):
                    file_data = await attachment.read()
                    
                    # Store the file
                    stored_file = self.storage.store_file(
                        filename=attachment.filename,
                        data=file_data,
                        user_id=ctx.author.id,
                        category=category
                    )
                    
                    # Format file size
                    size_kb = len(file_data) / 1024
                    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
                    
                    await ctx.send(
                        f"✅ **{category.title()} CSV Stored!**\n"
                        f"• File: `{attachment.filename}`\n"
                        f"• Size: {size_str}"
                    )
                    
                    return file_data
            
            return None
            
        except asyncio.TimeoutError:
            await ctx.send(f"⏱️ Upload timed out for {category} CSV.")
            return None
    
    @commands.command(name='files')
    async def files(self, ctx: commands.Context):
        """Show status of all uploaded CSV files."""
        files = self.storage.get_all_files()
        
        status_lines = ["**📁 Tracker CSV Status**\n"]
        
        for category in VALID_FILE_CATEGORIES:
            stored = files.get(category)
            desc = FILE_DESCRIPTIONS.get(category, category)
            
            if stored:
                # Format upload time
                upload_time = stored.uploaded_at.strftime("%Y-%m-%d %H:%M")
                status_lines.append(
                    f"✅ **{category.title()}** ({desc})\n"
                    f"   └─ `{stored.filename}` (uploaded {upload_time})"
                )
            else:
                status_lines.append(
                    f"❌ **{category.title()}** ({desc})\n"
                    f"   └─ Not uploaded"
                )
        
        await ctx.send("\n".join(status_lines))
    
    @commands.group(name='upload', invoke_without_command=True)
    async def upload(self, ctx: commands.Context):
        """Interactive upload wizard - prompts for each CSV file."""
        # Check if user already in upload session
        if ctx.author.id in self._upload_sessions:
            await ctx.send("⚠️ You already have an upload session in progress.")
            return
        
        self._upload_sessions[ctx.author.id] = "wizard"
        
        try:
            await ctx.send(
                "**📤 Tracker Upload Wizard**\n\n"
                "I'll guide you through uploading each CSV file.\n"
                "For each file, you can:\n"
                "• Upload a CSV file\n"
                "• Type `skip` to skip that file\n"
                "• Type `cancel` to abort the wizard\n"
                "─────────────────────────────"
            )
            
            for category in ["master", "typeform", "zoom"]:
                desc = FILE_DESCRIPTIONS.get(category, category)
                existing = self.storage.get_file(category)
                
                existing_info = ""
                if existing:
                    existing_info = f"\n   └─ Current: `{existing.filename}`"
                
                await ctx.send(
                    f"\n**{category.upper()}** - {desc}{existing_info}\n"
                    f"Upload the {category} CSV file, type `skip`, or type `cancel`:"
                )
                
                # Wait for response
                def check(message: discord.Message) -> bool:
                    if message.author.id != ctx.author.id or message.channel.id != ctx.channel.id:
                        return False
                    
                    content = message.content.lower().strip()
                    if content in ['skip', 'cancel', '!cancel']:
                        return True
                    
                    for attachment in message.attachments:
                        if attachment.filename.lower().endswith('.csv'):
                            return True
                    
                    return False
                
                try:
                    message = await self.bot.wait_for('message', check=check, timeout=120.0)
                    content = message.content.lower().strip()
                    
                    if content in ['cancel', '!cancel']:
                        await ctx.send("❌ Upload wizard cancelled.")
                        return
                    
                    if content == 'skip':
                        await ctx.send(f"⏭️ Skipped {category} CSV.")
                        continue
                    
                    # Process CSV upload
                    for attachment in message.attachments:
                        if attachment.filename.lower().endswith('.csv'):
                            file_data = await attachment.read()
                            
                            self.storage.store_file(
                                filename=attachment.filename,
                                data=file_data,
                                user_id=ctx.author.id,
                                category=category
                            )
                            
                            size_kb = len(file_data) / 1024
                            size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
                            
                            await ctx.send(
                                f"✅ **{category.title()} CSV Stored!**\n"
                                f"   • File: `{attachment.filename}`\n"
                                f"   • Size: {size_str}"
                            )
                            break
                    
                except asyncio.TimeoutError:
                    await ctx.send(f"⏱️ Timed out waiting for {category} CSV. Wizard ended.")
                    return
            
            # Wizard complete
            await ctx.send(
                "─────────────────────────────\n"
                "**✅ Upload Wizard Complete!**\n\n"
                "Run `!tracker files` to see all uploaded files.\n"
                "Run `!tracker download` to generate the report."
            )
            
        finally:
            # Clean up session
            self._upload_sessions.pop(ctx.author.id, None)
    
    @upload.command(name='master')
    async def upload_master(self, ctx: commands.Context):
        """Upload master roster CSV file."""
        existing = self.storage.get_file("master")
        existing_info = f"\n   └─ Current: `{existing.filename}`" if existing else ""
        
        await ctx.send(
            f"**📤 Upload Master Roster CSV**{existing_info}\n\n"
            f"Please upload the master roster CSV file, or type `cancel` to abort:"
        )
        
        await self._wait_for_csv(ctx, "master")
    
    @upload.command(name='typeform')
    async def upload_typeform(self, ctx: commands.Context):
        """Upload typeform responses CSV file."""
        existing = self.storage.get_file("typeform")
        existing_info = f"\n   └─ Current: `{existing.filename}`" if existing else ""
        
        await ctx.send(
            f"**📤 Upload Typeform Responses CSV**{existing_info}\n\n"
            f"Please upload the typeform responses CSV file, or type `cancel` to abort:"
        )
        
        await self._wait_for_csv(ctx, "typeform")
    
    @upload.command(name='zoom')
    async def upload_zoom(self, ctx: commands.Context):
        """Upload zoom attendance CSV file."""
        existing = self.storage.get_file("zoom")
        existing_info = f"\n   └─ Current: `{existing.filename}`" if existing else ""
        
        await ctx.send(
            f"**📤 Upload Zoom Attendance CSV**{existing_info}\n\n"
            f"Please upload the zoom attendance CSV file, or type `cancel` to abort:"
        )
        
        await self._wait_for_csv(ctx, "zoom")
    
    @upload.command(name='app')
    async def upload_app(self, ctx: commands.Context):
        """Upload app/phone data CSV file."""
        existing = self.storage.get_file("app")
        existing_info = f"\n   └─ Current: `{existing.filename}`" if existing else ""
        
        await ctx.send(
            f"**📤 Upload App Data CSV**{existing_info}\n\n"
            f"This CSV should contain Member ID and Phone Number columns.\n"
            f"Please upload the app data CSV file, or type `cancel` to abort:"
        )
        
        await self._wait_for_csv(ctx, "app")
    
    @commands.group(name='clear', invoke_without_command=True)
    async def clear(self, ctx: commands.Context):
        """Clear uploaded CSV files. Use subcommands to specify which file."""
        await ctx.send(
            "**🗑️ Clear CSV Files**\n\n"
            "Use one of the following commands:\n"
            "• `!tracker clear master` - Remove master roster CSV\n"
            "• `!tracker clear typeform` - Remove typeform responses CSV\n"
            "• `!tracker clear zoom` - Remove zoom attendance CSV\n"
            "• `!tracker clear app` - Remove app data CSV\n"
            "• `!tracker clearall` - Remove all CSV files"
        )
    
    @clear.command(name='master')
    async def clear_master(self, ctx: commands.Context):
        """Clear the master roster CSV file."""
        if self.storage.delete_file("master"):
            await ctx.send("✅ **Master CSV cleared!**")
        else:
            await ctx.send("ℹ️ No master CSV file to clear.")
    
    @clear.command(name='typeform')
    async def clear_typeform(self, ctx: commands.Context):
        """Clear the typeform responses CSV file."""
        if self.storage.delete_file("typeform"):
            await ctx.send("✅ **Typeform CSV cleared!**")
        else:
            await ctx.send("ℹ️ No typeform CSV file to clear.")
    
    @clear.command(name='zoom')
    async def clear_zoom(self, ctx: commands.Context):
        """Clear the zoom attendance CSV file."""
        if self.storage.delete_file("zoom"):
            await ctx.send("✅ **Zoom CSV cleared!**")
        else:
            await ctx.send("ℹ️ No zoom CSV file to clear.")
    
    @clear.command(name='app')
    async def clear_app(self, ctx: commands.Context):
        """Clear the app data CSV file."""
        if self.storage.delete_file("app"):
            await ctx.send("✅ **App data CSV cleared!**")
        else:
            await ctx.send("ℹ️ No app data CSV file to clear.")
    
    @commands.command(name='clearall')
    async def clearall(self, ctx: commands.Context):
        """Clear all uploaded CSV files."""
        deleted = self.storage.delete_all_files()
        if deleted > 0:
            await ctx.send(f"✅ **All CSV files cleared!** ({deleted} file(s) removed)")
        else:
            await ctx.send("ℹ️ No CSV files to clear.")
    
    @commands.command(name='start_date')
    async def start_date(self, ctx: commands.Context, date_str: Optional[str] = None):
        """Set or view the program start date for week calculations.
        
        Usage:
            !tracker start_date           - View current start date
            !tracker start_date MM/DD/YYYY - Set start date
        """
        if date_str is None:
            # View current start date
            current = self.storage.get_start_date()
            if current:
                await ctx.send(
                    f"📅 **Program Start Date:** {current.strftime('%m/%d/%Y')}\n"
                    f"Week 1 began on this date."
                )
            else:
                await ctx.send(
                    "📅 **No start date set.**\n\n"
                    "Set it using `!tracker start_date MM/DD/YYYY`"
                )
            return
        
        # Parse and set the date
        try:
            parsed_date = datetime.strptime(date_str, "%m/%d/%Y")
            self.storage.set_start_date(parsed_date)
            await ctx.send(
                f"✅ **Start date set!**\n"
                f"• Date: {parsed_date.strftime('%m/%d/%Y')} ({parsed_date.strftime('%A')})\n"
                f"• Week 1 begins on this date."
            )
        except ValueError:
            await ctx.send(
                "❌ **Invalid date format.**\n\n"
                "Use MM/DD/YYYY format, e.g., `!tracker start_date 01/15/2026`"
            )
    
    @commands.command(name='submissions')
    async def submissions(self, ctx: commands.Context, date_str: Optional[str] = None):
        """Real-time submission checking up to a specific date.
        
        Usage:
            !tracker submissions          - Check submissions up to today
            !tracker submissions MM/DD/YYYY - Check submissions up to specified date
        """
        # Check for required files
        typeform_file = self.storage.get_file("typeform")
        master_file = self.storage.get_file("master")
        
        if not typeform_file:
            await ctx.send(
                "❌ **No typeform CSV uploaded.**\n\n"
                "Upload it using `!tracker upload typeform`."
            )
            return
        
        if not master_file:
            await ctx.send(
                "❌ **No master CSV uploaded.**\n\n"
                "The master CSV (enrollee list) is required for submission checking.\n"
                "Upload it using `!tracker upload master`."
            )
            return
        
        # Check for start date
        start_date = self.storage.get_start_date()
        if not start_date:
            await ctx.send(
                "❌ **No start date set.**\n\n"
                "Set the program start date first using `!tracker start_date MM/DD/YYYY`."
            )
            return
        
        # Parse the target date
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                await ctx.send(
                    "❌ **Invalid date format.**\n\n"
                    "Use MM/DD/YYYY format, e.g., `!tracker submissions 02/15/2026`"
                )
                return
        else:
            target_date = datetime.now()
        
        # Calculate current week
        days_since_start = (target_date - start_date).days
        current_week = max(1, (days_since_start // 7) + 1)
        
        # Store the last submissions date for downloads
        self.storage.set_last_submissions_date(target_date)
        
        await ctx.send(
            f"📊 **Checking Submissions**\n"
            f"• Start Date: {start_date.strftime('%m/%d/%Y')}\n"
            f"• Target Date: {target_date.strftime('%m/%d/%Y')}\n"
            f"• Current Week: {current_week}\n\n"
            f"⏳ Analyzing submissions..."
        )
        
        try:
            # Read files
            typeform_data = self.storage.read_file(typeform_file)
            master_data = self.storage.read_file(master_file)
            app_data = self.storage.read_file_by_category("app")
            
            # Process with date filter
            result = self.processor.process_submissions(
                typeform_data,
                master_data=master_data,
                start_date=start_date,
                target_date=target_date,
                current_week=current_week,
                app_data=app_data
            )
            
            if not result.success:
                await ctx.send(f"❌ Processing failed: {result.error_message}")
                return
            
            # Send the summary embed
            await ctx.send(embed=result.summary_embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error processing submissions: {e}")
    
    @commands.command(name='submissions_download')
    async def submissions_download(self, ctx: commands.Context, *, options: str = ""):
        """Download tracker report filtered by the last used submissions date.
        
        Uses the date from the most recent !tracker submissions command.
        
        GitLab Options (space-separated):
            nofilter         - Fetch GitLab data for all commit/MR links in README
            validate_commits - Validate commits found in student READMEs
            validate_all     - Validate commits AND MRs found in student READMEs
        
        Examples:
            !tracker submissions_download                  - Basic report (no GitLab)
            !tracker submissions_download nofilter         - GitLab data, all links
            !tracker submissions_download validate_commits - Validate commits only
            !tracker submissions_download validate_all     - Validate commits + MRs
        """
        # Check for required files
        typeform_file = self.storage.get_file("typeform")
        master_file = self.storage.get_file("master")
        
        if not typeform_file:
            await ctx.send(
                "❌ **No typeform CSV uploaded.**\n\n"
                "Upload it using `!tracker upload typeform`."
            )
            return
        
        # Parse options
        opts = options.lower().split()
        use_nofilter = "nofilter" in opts
        validate_commits = "validate_commits" in opts
        validate_all = "validate_all" in opts
        
        # Any of these options enables GitLab enrichment
        use_gitlab = use_nofilter or validate_commits or validate_all
        
        # Check for start date and last submissions date
        start_date = self.storage.get_start_date()
        target_date = self.storage.get_last_submissions_date()
        
        if not start_date:
            await ctx.send(
                "❌ **No start date set.**\n\n"
                "Set the program start date first using `!tracker start_date MM/DD/YYYY`."
            )
            return
        
        if not target_date:
            await ctx.send(
                "❌ **No submissions date set.**\n\n"
                "Run `!tracker submissions <DATE>` first to set the date filter."
            )
            return
        
        # Calculate current week
        days_since_start = (target_date - start_date).days
        current_week = max(1, (days_since_start // 7) + 1)
        
        # Build status message
        status_lines = [
            f"📂 **Generating Filtered Report**",
            f"• Start Date: {start_date.strftime('%m/%d/%Y')}",
            f"• Target Date: {target_date.strftime('%m/%d/%Y')}",
            f"• Week: {current_week}",
        ]
        
        if use_gitlab:
            if validate_all:
                status_lines.append("• GitLab: Validating commits + MRs")
            elif validate_commits:
                status_lines.append("• GitLab: Validating commits")
            else:
                status_lines.append("• GitLab: Fetching all data (nofilter)")
            status_lines.append("")
            status_lines.append("⏳ Fetching GitLab data (this may take a while)...")
        else:
            status_lines.append("")
            status_lines.append("⏳ Creating report...")
        
        await ctx.send("\n".join(status_lines))
        
        try:
            # Read files
            typeform_data = self.storage.read_file(typeform_file)
            master_data = self.storage.read_file(master_file) if master_file else None
            zoom_data = self.storage.read_file_by_category("zoom")
            app_data = self.storage.read_file_by_category("app")
            
            # Build options dict
            phase_completions = self.storage.get_all_phase_completions()
            bypasses = self.storage.get_all_bypasses()
            process_options = {
                'master_data': master_data,
                'zoom_data': zoom_data,
                'app_data': app_data,
                'start_date': start_date,
                'target_date': target_date,
                'current_week': current_week,
                'filter_by_date': True,
                'phase_completions': phase_completions,
                'bypasses': bypasses
            }
            
            # Add GitLab options if enabled
            if use_gitlab:
                process_options['gitlab_service'] = self.gitlab
                process_options['nofilter'] = use_nofilter
                process_options['validate_commits'] = validate_commits or validate_all
                process_options['validate_mrs'] = validate_all
            
            # Process with date filter
            result = self.processor.process(typeform_data, options=process_options)
            
            if not result.success:
                await ctx.send(f"❌ Processing failed: {result.error_message}")
                return
            
            # Generate output filename
            date_suffix = target_date.strftime("%Y%m%d")
            gitlab_suffix = "_gitlab" if use_gitlab else ""
            output_filename = f"submissions_report_week{current_week}_{date_suffix}{gitlab_suffix}.xlsx"
            
            # Create file and send
            file = discord.File(
                fp=io.BytesIO(result.output_data),
                filename=output_filename
            )
            
            # Build success message
            success_lines = [
                f"✅ **Filtered Report Generated!**",
                f"• Students processed: {result.rows_processed}",
                f"• Filtered through: {target_date.strftime('%m/%d/%Y')}",
                f"• Week: {current_week}",
            ]
            if use_gitlab:
                success_lines.append("• GitLab data: Included")
            
            await ctx.send("\n".join(success_lines), file=file)
            
        except Exception as e:
            await ctx.send(f"❌ Error generating report: {e}")
    
    @commands.command(name='download')
    async def download(self, ctx: commands.Context):
        """Process uploaded CSV files and return a styled Excel file.
        
        Usage:
            1. Upload CSV files using !tracker upload commands
            2. Run !tracker download to generate the report
        """
        # Get the typeform file (primary data source)
        typeform_file = self.storage.get_file("typeform")
        
        if typeform_file is None:
            await ctx.send(
                "❌ **No typeform CSV uploaded.**\n\n"
                "The typeform CSV is required for generating reports.\n"
                "Upload it using `!tracker upload typeform`."
            )
            return
        
        # Check for optional files
        master_file = self.storage.get_file("master")
        zoom_file = self.storage.get_file("zoom")
        
        files_info = [f"• Typeform: `{typeform_file.filename}`"]
        if master_file:
            files_info.append(f"• Master: `{master_file.filename}`")
        if zoom_file:
            files_info.append(f"• Zoom: `{zoom_file.filename}`")
        
        await ctx.send(
            f"📂 **Processing Files:**\n" + "\n".join(files_info) + 
            "\n\n⏳ Creating multi-tab report..."
        )
        
        # Process the files
        try:
            # Read all available files
            typeform_data = self.storage.read_file(typeform_file)
            master_data = self.storage.read_file(master_file) if master_file else None
            zoom_data = self.storage.read_file(zoom_file) if zoom_file else None
            app_file = self.storage.get_file("app")
            app_data = self.storage.read_file(app_file) if app_file else None
            
            # Process with tracker processor (pass all data sources)
            phase_completions = self.storage.get_all_phase_completions()
            bypasses = self.storage.get_all_bypasses()
            
            # Get dates for proper deadline checking
            # If start_date is set, use it along with target_date (last submissions date or today)
            start_date = self.storage.get_start_date()
            target_date = self.storage.get_last_submissions_date() or datetime.now()
            current_week = 1
            if start_date:
                days_since_start = (target_date - start_date).days
                current_week = max(1, (days_since_start // 7) + 1)
            
            result = self.processor.process(
                typeform_data,
                options={
                    'master_data': master_data,
                    'zoom_data': zoom_data,
                    'app_data': app_data,
                    'phase_completions': phase_completions,
                    'bypasses': bypasses,
                    'start_date': start_date,
                    'target_date': target_date,
                    'current_week': current_week,
                    'filter_by_date': True
                }
            )
            
            if not result.success:
                await ctx.send(f"❌ Processing failed: {result.error_message}")
                return
            
            # Generate output filename
            base_name = typeform_file.filename.rsplit('.', 1)[0]
            output_filename = f"{base_name}_report.xlsx"
            
            # Create file from bytes and send
            file = discord.File(
                fp=io.BytesIO(result.output_data),
                filename=output_filename
            )
            
            await ctx.send(
                f"✅ **Tracker Report Generated!**\n"
                f"• Students processed: {result.rows_processed}\n"
                f"• Tabs created:\n"
                f"  └─ Intervention Tracker (all fields)\n"
                f"  └─ P1 - At Risk (red/orange/yellow coding)\n"
                f"  └─ P2 - Flagged (yellow coding)\n"
                f"  └─ P3 - On Track (green coding)\n"
                f"  └─ Weekly Summary (dashboard)",
                file=file
            )
            
        except Exception as e:
            await ctx.send(f"❌ Error processing file: {e}")
    
    # ==================== Phase Completion Commands ====================
    
    @commands.command(name='set_phase_complete')
    async def set_phase_complete(self, ctx: commands.Context, phases: str = None, member_id: str = None):
        """Set a student's completed phase.
        
        Usage: !tracker set_phase_complete <phase(s)> <member_id>
        
        Args:
            phases: Phase number(s) - single (e.g., 2) or comma-separated (e.g., 1,2,3)
            member_id: The student's member ID
        """
        if phases is None or member_id is None:
            await ctx.send(
                "**📝 Set Phase Complete**\n\n"
                "Usage: `!tracker set_phase_complete <phase(s)> <member_id>`\n\n"
                "Examples:\n"
                "• `!tracker set_phase_complete 2 12345`\n"
                "• `!tracker set_phase_complete 1,2,3 12345`\n\n"
                "Use `!tracker get_member_id <discord_username>` to look up a member ID."
            )
            return
        
        # Parse phases - can be single number or comma-separated
        try:
            phase_list = [int(p.strip()) for p in phases.split(',')]
        except ValueError:
            await ctx.send("❌ Invalid phase format. Use a number (e.g., 2) or comma-separated numbers (e.g., 1,2,3).")
            return
        
        # Validate all phases
        invalid_phases = [p for p in phase_list if p < 1 or p > 4]
        if invalid_phases:
            await ctx.send(f"❌ Invalid phase(s): {invalid_phases}. Phases must be between 1 and 4.")
            return
        
        # Use the highest phase (completing phase 3 implies 1 and 2 are done)
        phase = max(phase_list)
        
        # Verify member_id exists in master CSV
        master_file = self.storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        # Look up student name from master CSV
        student_name = ""
        member_info = self._get_member_info(member_id)
        if member_info:
            student_name = member_info.get('name', '')
        else:
            await ctx.send(
                f"❌ Member ID `{member_id}` not found in master roster.\n\n"
                f"Use `!tracker get_member_id <discord_username>` to look up the correct member ID."
            )
            return
        
        # Set the phase completion (pass full list of phases)
        updated_by = f"{ctx.author.name}#{ctx.author.discriminator}" if ctx.author.discriminator != "0" else ctx.author.name
        self.storage.set_phase_complete(member_id, phase_list, updated_by, student_name)
        
        # Build response message
        phases_str = f"Phase {phase_list[0]}" if len(phase_list) == 1 else f"Phases {','.join(map(str, sorted(phase_list)))}"
        
        await ctx.send(
            f"✅ **Phase Complete Updated**\n"
            f"• Name: {student_name}\n"
            f"• Member ID: `{member_id}`\n"
            f"• Completed: **{phases_str}**\n"
            f"• Updated by: {updated_by}"
        )
    
    @commands.command(name='bypass')
    async def bypass_submission(self, ctx: commands.Context, submission_num: int = None, member_id: str = None, *, reason: str = ""):
        """Bypass a submission to mark it as ON_TRACK regardless of interventions.
        
        Usage: !tracker bypass <submission_num> <member_id> [reason]
        
        Submission numbers: Wed W1=1, Sun W1=2, Wed W2=3, Sun W2=4, etc.
        This is used after manually investigating and intervening with an AT_RISK student.
        Bypassed submissions will always show as ON_TRACK in future reports.
        
        Args:
            submission_num: The submission number (see P1/P2/P3 sheets)
            member_id: The student's member ID
            reason: Optional reason for the bypass
        """
        if submission_num is None or member_id is None:
            await ctx.send(
                "**🔓 Bypass Submission**\n\n"
                "Usage: `!tracker bypass <submission_num> <member_id> [reason]`\n\n"
                "**Submission Numbers:** Wed W1=1, Sun W1=2, Wed W2=3, Sun W2=4, etc.\n"
                "Check the 'Submission #' column in P1/P2/P3 sheets.\n\n"
                "Examples:\n"
                "• `!tracker bypass 1 12345` - Bypass Wed Week 1\n"
                "• `!tracker bypass 2 12345 Issue resolved` - Bypass Sun Week 1\n\n"
                "Use `!tracker get_member_id <discord_username>` to look up a member ID."
            )
            return
        
        if submission_num < 1:
            await ctx.send("❌ Submission number must be 1 or greater.")
            return
        
        # Look up student name from master CSV
        member_info = self._get_member_info(member_id)
        if not member_info:
            await ctx.send(
                f"❌ Member ID `{member_id}` not found in master roster.\n\n"
                f"Use `!tracker get_member_id <discord_username>` to look up the correct member ID."
            )
            return
        
        student_name = member_info.get('name', '')
        
        # Calculate week and day from submission_num
        week = (submission_num + 1) // 2
        day = "Wednesday" if submission_num % 2 == 1 else "Sunday"
        
        # Set the bypass
        bypassed_by = f"{ctx.author.name}#{ctx.author.discriminator}" if ctx.author.discriminator != "0" else ctx.author.name
        self.storage.set_bypass(
            member_id=member_id,
            submission_num=submission_num,
            bypassed_by=bypassed_by,
            name=student_name,
            reason=reason
        )
        
        response = (
            f"✅ **Submission Bypassed**\n"
            f"• Name: {student_name}\n"
            f"• Member ID: `{member_id}`\n"
            f"• Submission #: **{submission_num}** ({day} Week {week})\n"
            f"• Bypassed by: {bypassed_by}"
        )
        if reason:
            response += f"\n• Reason: {reason}"
        
        await ctx.send(response)
    
    @commands.command(name='unbypass')
    async def unbypass_submission(self, ctx: commands.Context, submission_num: int = None, member_id: str = None):
        """Remove a bypass from a submission.
        
        Usage: !tracker unbypass <submission_num> <member_id>
        """
        if submission_num is None or member_id is None:
            await ctx.send(
                "**🔒 Remove Bypass**\n\n"
                "Usage: `!tracker unbypass <submission_num> <member_id>`\n\n"
                "Example: `!tracker unbypass 1 12345`"
            )
            return
        
        removed = self.storage.remove_bypass(member_id, submission_num)
        
        if removed:
            week = (submission_num + 1) // 2
            day = "Wednesday" if submission_num % 2 == 1 else "Sunday"
            await ctx.send(f"✅ Bypass removed for member `{member_id}` submission #{submission_num} ({day} Week {week}).")
        else:
            await ctx.send(f"❌ No bypass found for member `{member_id}` submission #{submission_num}.")
    
    @commands.command(name='list_bypasses')
    async def list_bypasses(self, ctx: commands.Context):
        """List all active bypasses."""
        bypasses = self.storage.get_all_bypasses()
        
        if not bypasses:
            await ctx.send("📋 No active bypasses.")
            return
        
        lines = ["**📋 Active Bypasses**\n"]
        for key, data in sorted(bypasses.items()):
            name = data.get('name', 'Unknown')
            member_id = data.get('member_id', 'Unknown')
            submission_num = data.get('submission_num', '?')
            bypassed_by = data.get('bypassed_by', 'Unknown')
            reason = data.get('reason', '')
            
            # Calculate week and day from submission_num
            if isinstance(submission_num, int):
                week = (submission_num + 1) // 2
                day = "Wed" if submission_num % 2 == 1 else "Sun"
                sub_display = f"#{submission_num} ({day} W{week})"
            else:
                sub_display = f"#{submission_num}"
            
            line = f"• **{name}** (`{member_id}`) - {sub_display} by {bypassed_by}"
            if reason:
                line += f"\n  └ Reason: {reason}"
            lines.append(line)
        
        await ctx.send("\n".join(lines))
    
    @commands.command(name='get_member_id')
    async def get_member_id(self, ctx: commands.Context, *, discord_info: str = None):
        """Look up a member ID from Discord display name, username, or user ID.
        
        Usage: !tracker get_member_id <display_name or discord_username or @mention>
        """
        if not discord_info:
            await ctx.send(
                "**🔍 Get Member ID**\n\n"
                "Usage: `!tracker get_member_id <display_name or username>`\n\n"
                "Examples:\n"
                "• `!tracker get_member_id Queen Sydelle` (display name)\n"
                "• `!tracker get_member_id queensydelle` (username)\n"
                "• `!tracker get_member_id @JohnDoe` (mention)\n"
                "• `!tracker get_member_id 123456789012345678` (user ID)"
            )
            return
        
        master_file = self.storage.get_file("master")
        if not master_file:
            await ctx.send("❌ No master roster uploaded. Use `!tracker upload master` first.")
            return
        
        # Clean up the input
        discord_info = discord_info.strip().strip('"').strip("'")
        discord_user = None
        result = None
        
        # Handle @mention format
        if discord_info.startswith('<@') and discord_info.endswith('>'):
            discord_id = discord_info.replace('<@', '').replace('>', '').replace('!', '')
            try:
                discord_user = await self.bot.fetch_user(int(discord_id))
            except:
                pass
        # Handle numeric user ID
        elif discord_info.isdigit():
            try:
                discord_user = await self.bot.fetch_user(int(discord_info))
            except:
                pass
        
        # If we have a discord user from mention/ID, look up by their username
        if discord_user:
            result = self._lookup_member_id_by_discord(discord_user.name)
        
        # Search guild members by display name FIRST (this is the primary use case)
        # Get guild - ctx.guild may be None in some cases, try multiple fallbacks
        guild = ctx.guild
        if not guild and hasattr(ctx.channel, 'guild'):
            guild = ctx.channel.guild
        if not guild and self.bot.guilds:
            # Use first guild the bot is in as fallback
            guild = self.bot.guilds[0]
        if not result and guild:
            search_lower = discord_info.lower()
            
            # Ensure members are cached
            if not guild.chunked:
                try:
                    await guild.chunk()
                except:
                    pass
            
            # Exact match on display name first
            for member in guild.members:
                display = (member.display_name or "").lower()
                if display == search_lower:
                    discord_user = member
                    result = self._lookup_member_id_by_discord(member.name)
                    if result:
                        break
            
            # Partial match on display name
            if not result:
                for member in guild.members:
                    display = (member.display_name or "").lower()
                    global_name = (member.global_name or "").lower() if hasattr(member, 'global_name') else ""
                    
                    if search_lower in display or search_lower in global_name:
                        discord_user = member
                        result = self._lookup_member_id_by_discord(member.name)
                        if result:
                            break
            
            # Try matching by username
            if not result:
                for member in guild.members:
                    if search_lower == member.name.lower():
                        discord_user = member
                        result = self._lookup_member_id_by_discord(member.name)
                        if result:
                            break
        
        # Final fallback: direct CSV lookup (for usernames not in this guild)
        if not result:
            result = self._lookup_member_id_by_discord(discord_info)
        
        if result:
            member_id, name, roster_discord = result
            discord_display = ""
            if discord_user:
                discord_display = f"\n• Discord User: {discord_user.display_name} (`{discord_user.name}`)"
            await ctx.send(
                f"✅ **Member Found**\n"
                f"• Name: {name}\n"
                f"• Member ID: `{member_id}`\n"
                f"• Roster Discord: {roster_discord}{discord_display}"
            )
        else:
            # Show helpful debug info
            found_in_guild = ""
            if ctx.guild and discord_user:
                found_in_guild = f"\n\nFound Discord user `{discord_user.name}` but they're not in the master roster."
            await ctx.send(
                f"❌ No member found matching `{discord_info}`{found_in_guild}\n\n"
                f"Make sure the Discord username matches the master roster."
            )
    
    def _get_member_info(self, member_id: str) -> Optional[dict]:
        """Look up member info by member ID from master CSV.
        
        Returns:
            Dict with 'name', 'discord', 'email' or None if not found
        """
        master_file = self.storage.get_file("master")
        if not master_file:
            return None
        
        try:
            import csv
            master_data = self.storage.read_file(master_file)
            if not master_data:
                return None
            
            text_data = master_data.decode('utf-8-sig')
            text_data = self._preprocess_master_csv(text_data)
            
            reader = csv.DictReader(io.StringIO(text_data))
            rows = list(reader)
            
            if not rows:
                return None
            
            headers = list(rows[0].keys())
            headers_lower = {h.lower(): h for h in headers}
            
            def find_col(possible_names):
                for name in possible_names:
                    if name in headers:
                        return name
                    if name.lower() in headers_lower:
                        return headers_lower[name.lower()]
                return None
            
            member_id_col = find_col(["Member ID", "member_id", "MemberID"])
            name_col = find_col(["Full Name", "Name", "full_name", "Student Name"])
            discord_col = find_col(["Discord Username", "Discord", "discord_username"])
            email_col = find_col(["Email", "email"])
            
            if not member_id_col:
                return None
            
            for row in rows:
                row_member_id = str(row.get(member_id_col, "")).strip()
                if row_member_id == str(member_id).strip():
                    return {
                        'name': str(row.get(name_col, "")).strip() if name_col else "",
                        'discord': str(row.get(discord_col, "")).strip() if discord_col else "",
                        'email': str(row.get(email_col, "")).strip() if email_col else ""
                    }
            
            return None
        except Exception:
            return None
    
    def _preprocess_master_csv(self, master_text: str) -> str:
        """Preprocess master CSV to find actual header row and strip metadata.
        
        The master CSV may have metadata rows at the top before the actual header.
        """
        lines = master_text.splitlines()
        header_row_idx = None
        
        # Find the row containing "Member ID" (the actual header)
        for idx, line in enumerate(lines):
            if "Member ID" in line or "member_id" in line.lower():
                header_row_idx = idx
                break
        
        if header_row_idx is None:
            return master_text
        
        # Get lines from header onwards
        data_lines = lines[header_row_idx:]
        
        # Strip leading empty column if present
        if data_lines and data_lines[0].startswith(','):
            data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
        
        return '\n'.join(data_lines)
    
    def _verify_member_id(self, member_id: str) -> bool:
        """Check if a member ID exists in the master roster."""
        master_file = self.storage.get_file("master")
        if not master_file:
            return False
        
        try:
            import csv
            master_data = self.storage.read_file(master_file)
            text_data = master_data.decode('utf-8-sig')
            
            # Preprocess to find actual header row
            text_data = self._preprocess_master_csv(text_data)
            
            reader = csv.DictReader(io.StringIO(text_data))
            rows = list(reader)
            
            if not rows:
                return False
            
            headers = list(rows[0].keys())
            member_id_col = None
            for col in ["Member ID", "member_id", "MemberID"]:
                if col in headers:
                    member_id_col = col
                    break
            
            if not member_id_col:
                return False
            
            for row in rows:
                if str(row.get(member_id_col, "")).strip() == member_id:
                    return True
            
            return False
        except:
            return False
    
    def _lookup_member_id_by_discord(self, discord_info: str) -> Optional[tuple]:
        """Look up member ID by Discord username, display name, or name.
        
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
            if not master_data:
                return None
            
            text_data = master_data.decode('utf-8-sig')
            
            # Preprocess to find actual header row (skip metadata rows)
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
            discord_lower = discord_info.lower().strip()
            # Remove @ prefix if present
            if discord_lower.startswith('@'):
                discord_lower = discord_lower[1:]
            # Remove discriminator if present (e.g., #1234)
            if '#' in discord_lower:
                discord_lower = discord_lower.split('#')[0]
            
            # Search Discord column first (if found)
            if discord_col:
                
                # Exact match
                for row in rows:
                    discord_username = str(row.get(discord_col, "")).strip()
                    discord_clean = discord_username.lower()
                    # Also clean the stored value
                    if discord_clean.startswith('@'):
                        discord_clean = discord_clean[1:]
                    if '#' in discord_clean:
                        discord_clean = discord_clean.split('#')[0]
                    
                    if discord_clean == discord_lower:
                        member_id = str(row.get(member_id_col, "")).strip()
                        name = str(row.get(name_col, "")).strip() if name_col else ""
                        return (member_id, name, discord_username)
                
                # Partial match (search term contained in discord username)
                for row in rows:
                    discord_username = str(row.get(discord_col, "")).strip()
                    if discord_lower in discord_username.lower():
                        member_id = str(row.get(member_id_col, "")).strip()
                        name = str(row.get(name_col, "")).strip() if name_col else ""
                        return (member_id, name, discord_username)
            
            # Fallback: Search by Name column
            if name_col:
                for row in rows:
                    name = str(row.get(name_col, "")).strip()
                    if discord_lower in name.lower():
                        member_id = str(row.get(member_id_col, "")).strip()
                        discord_username = str(row.get(discord_col, "")).strip() if discord_col else ""
                        return (member_id, name, discord_username)
            
            return None
        except Exception as e:
            print(f"[Tracker] Error looking up member ID: {e}")
            return None
    
    @commands.command(name='no_issues')
    async def no_issues(self, ctx: commands.Context, action: str = None):
        """Show issue status from validated data or run quick check.
        
        Usage: 
            !tracker no_issues           - Show issue status from validated data
            !tracker no_issues quick     - Quick list without validation
            !tracker no_issues validate  - Crawl READMEs to find/validate issue URLs
        
        The default command requires running 'validate' first to generate data.
        """
        if action and action.lower() == 'validate':
            await self._validate_no_issues(ctx)
            return
        
        if action and action.lower() == 'quick':
            await self._quick_no_issues(ctx)
            return
        
        # Default: show from validated JSON
        await self._show_validated_issues(ctx)
    
    async def _show_validated_issues(self, ctx: commands.Context):
        """Show issue status from the validated issues JSON file."""
        import json
        import os
        
        results_file = os.path.join('data', 'uploads', '_validated_issues.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated issues data found.**\n\n"
                "Run `!tracker no_issues validate` first to crawl READMEs and validate issue URLs.\n\n"
                "Or use `!tracker no_issues quick` for a quick check without validation."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated issues file:** {str(e)}")
            return
        
        validated_at = data.get('validated_at', 'Unknown')
        
        # Show info about cached data
        await ctx.send(
            f"📋 **Validated Issues Data**\n"
            f"Last validated: `{validated_at}`\n\n"
            f"Showing results from cached data. Run `!tracker no_issues validate` to refresh.\n"
            f"─────────────────────────────"
        )
        
        # Extract data
        students_with_valid_issue = data.get('students_with_valid_issue', {})
        students_with_invalid_issue = data.get('students_with_invalid_issue', {})
        readme_url_in_issue_field = data.get('readme_url_in_issue_field', {})
        issue_url_in_readme_link = data.get('issue_url_in_readme_link', {})
        issues_found = data.get('issues_found', {})
        no_issue_in_readme = data.get('no_issue_in_readme', {})
        readme_inaccessible = data.get('readme_inaccessible', {})
        readme_timeout = data.get('readme_timeout', {})
        
        # Build consolidated lists
        # Students WITH issues (valid from typeform + found in README + issue in readme_link field)
        students_with_issues: dict = {}
        
        # Add students with valid issue URLs from typeform
        for mid, info in students_with_valid_issue.items():
            students_with_issues[mid] = {
                'name': info['name'],
                'issue_url': info['issue_url'],
                'source': 'typeform'
            }
        
        # Add students who put issue URL in readme_link field (they have a valid issue, just wrong field)
        for mid, info in issue_url_in_readme_link.items():
            if mid not in students_with_issues:
                students_with_issues[mid] = {
                    'name': info['name'],
                    'issue_url': info['issue_url'],
                    'source': 'readme_link_field',
                    'needs_attention': True
                }
        
        # Add students where issues were found in README (only if not already in list)
        for mid, info in issues_found.items():
            if mid not in students_with_issues:
                students_with_issues[mid] = {
                    'name': info['name'],
                    'issue_url': info['issue_url'],
                    'source': info.get('source', 'readme')
                }
        
        # Students WITHOUT issues (no issue in README, excluding those with valid issues)
        students_without_issues: dict = {}
        for mid, info in no_issue_in_readme.items():
            if mid not in students_with_issues:
                students_without_issues[mid] = {
                    'name': info['name'],
                    'readme_link': info.get('readme_link', '')
                }
        
        # Build report
        report = ["📊 **Issue Status Summary**\n"]
        report.append(f"**Students with Issues:** {len(students_with_issues)}")
        report.append(f"**Students without Issues:** {len(students_without_issues)}")
        attention_count = len(students_with_invalid_issue) + len(readme_url_in_issue_field) + len(issue_url_in_readme_link) + len(readme_inaccessible) + len(readme_timeout)
        report.append(f"**Needs Attention:** {attention_count}\n")
        
        # Section 1: Students WITH issues
        if students_with_issues:
            report.append("**✅ Students With Issue URLs:**")
            for mid, info in sorted(students_with_issues.items(), key=lambda x: x[1]['name'].lower()):
                source = info.get('source', 'typeform')
                if source == 'readme':
                    source_tag = " *(from README)*"
                elif source == 'number_reference':
                    source_tag = " *(from #number)*"
                elif source == 'project_shorthand':
                    source_tag = " *(from project#number)*"
                elif source == 'readme_link_field':
                    source_tag = " *(⚠️ in README field!)*"
                else:
                    source_tag = ""
                report.append(f"• **{info['name']}** (`{mid}`){source_tag}")
                report.append(f"  └─ <{info['issue_url']}>")
            report.append("")
        
        # Section 2: Students WITHOUT issues
        if students_without_issues:
            report.append("**❌ Students Without Issue URLs:**")
            for mid, info in sorted(students_without_issues.items(), key=lambda x: x[1]['name'].lower()):
                report.append(f"• **{info['name']}** (`{mid}`)")
                if info.get('readme_link'):
                    report.append(f"  └─ README: <{info['readme_link']}>")
                if info.get('issue_numbers_found'):
                    report.append(f"  └─ ⚠️ Found #{', #'.join(info['issue_numbers_found'])} but couldn't validate")
            report.append("")
        
        # Send main report in chunks
        full_report = "\n".join(report)
        if len(full_report) <= 2000:
            await ctx.send(full_report)
        else:
            chunks = []
            current = ""
            for line in report:
                if len(current) + len(line) + 1 > 1900:
                    chunks.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                chunks.append(current)
            for chunk in chunks:
                await ctx.send(chunk)
        
        # Section 3: Needs Attention
        needs_attention = []
        
        # README URLs put in issue_url field (wrong field)
        if readme_url_in_issue_field:
            needs_attention.append("**⚠️ README URL in Issue Field (wrong field!):**")
            needs_attention.append("*(These students put a README/repo link in the issue URL field)*")
            for mid, info in sorted(readme_url_in_issue_field.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ README: <{info.get('readme_url', 'N/A')}>")
            needs_attention.append("")
        
        # Issue URLs put in readme_link field (wrong field)
        if issue_url_in_readme_link:
            needs_attention.append("**⚠️ Issue URL in README Field (wrong field!):**")
            needs_attention.append("*(These students have valid issues but put them in the README link field)*")
            for mid, info in sorted(issue_url_in_readme_link.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ Issue: <{info['issue_url']}>")
            needs_attention.append("")
        
        # Invalid issue URLs
        if students_with_invalid_issue:
            needs_attention.append("**⚠️ Invalid Issue URLs:**")
            needs_attention.append("*(Expected: gitlab.com/.../issues/{num} or .../work_items/{num})*")
            for mid, info in sorted(students_with_invalid_issue.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info['issue_url']}>")
            needs_attention.append("")
        
        # Inaccessible READMEs
        if readme_inaccessible:
            needs_attention.append("**⚠️ Inaccessible READMEs:**")
            for mid, info in sorted(readme_inaccessible.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info.get('readme_link', 'N/A')}>")
                needs_attention.append(f"  └─ Error: {info.get('error', 'Unknown')}")
            needs_attention.append("")
        
        # Timed out READMEs
        if readme_timeout:
            needs_attention.append("**⏱️ Timed Out (retry later):**")
            for mid, info in sorted(readme_timeout.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info.get('readme_link', 'N/A')}>")
            needs_attention.append("")
        
        if needs_attention:
            await ctx.send("─────────────────────────────")
            attention_text = "\n".join(needs_attention)
            if len(attention_text) <= 2000:
                await ctx.send(attention_text)
            else:
                chunks = []
                current = ""
                for line in needs_attention:
                    if len(current) + len(line) + 1 > 1900:
                        chunks.append(current)
                        current = line
                    else:
                        current += "\n" + line if current else line
                if current:
                    chunks.append(current)
                for chunk in chunks:
                    await ctx.send(chunk)
    
    async def _quick_no_issues(self, ctx: commands.Context):
        """Quick list of students by issue selection status (no validation)."""
        # Check for typeform data
        typeform_file = self.storage.get_file("typeform")
        if not typeform_file:
            await ctx.send("❌ **No typeform data uploaded.** Upload typeform CSV first with `!tracker upload typeform`")
            return
        
        typeform_data = self.storage.read_file(typeform_file)
        
        await ctx.send("🔍 **Analyzing students by issue selection status...**")
        
        try:
            import csv
            from services.tracker_processor import _preprocess_typeform_csv
            
            # Build contact lookup from master CSV
            contact_lookup: dict = {}  # member_id -> {email, discord, phone}
            master_file = self.storage.get_file("master")
            if master_file:
                master_data = self.storage.read_file(master_file)
                master_text = master_data.decode('utf-8-sig')
                
                # Preprocess master CSV - find the header row containing "Member ID"
                lines = master_text.splitlines()
                header_row_idx = None
                for idx, line in enumerate(lines):
                    if "Member ID" in line or "member_id" in line.lower():
                        header_row_idx = idx
                        break
                
                if header_row_idx is not None:
                    master_text = "\n".join(lines[header_row_idx:])
                
                try:
                    m_dialect = csv.Sniffer().sniff(master_text[:4096], delimiters=',\t;|')
                except csv.Error:
                    m_dialect = 'excel'
                m_reader = csv.DictReader(io.StringIO(master_text), dialect=m_dialect)
                m_rows = list(m_reader)
                if m_rows:
                    m_headers = list(m_rows[0].keys())
                    # Find columns
                    m_member_col = next((h for h in m_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                    m_email_col = next((h for h in m_headers if 'email' in h.lower() and 'secondary' not in h.lower()), None)
                    m_discord_col = next((h for h in m_headers if 'discord' in h.lower()), None)
                    
                    for row in m_rows:
                        mid = str(row.get(m_member_col, "")).strip() if m_member_col else ""
                        if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id']:
                            contact_lookup[mid] = {
                                'email': str(row.get(m_email_col, "")).strip() if m_email_col else "",
                                'discord': str(row.get(m_discord_col, "")).strip() if m_discord_col else "",
                                'phone': ""
                            }
            
            # Add phone numbers from app CSV
            app_file = self.storage.get_file("app")
            if app_file:
                app_data = self.storage.read_file(app_file)
                app_text = app_data.decode('utf-8-sig')
                
                # Preprocess app CSV - find the header row containing "Member ID"
                lines = app_text.splitlines()
                header_row_idx = None
                for idx, line in enumerate(lines):
                    if "Member ID" in line or "member_id" in line.lower():
                        header_row_idx = idx
                        break
                
                if header_row_idx is not None:
                    app_text = "\n".join(lines[header_row_idx:])
                
                try:
                    a_dialect = csv.Sniffer().sniff(app_text[:4096], delimiters=',\t;|')
                except csv.Error:
                    a_dialect = 'excel'
                a_reader = csv.DictReader(io.StringIO(app_text), dialect=a_dialect)
                a_rows = list(a_reader)
                if a_rows:
                    a_headers = list(a_rows[0].keys())
                    a_member_col = next((h for h in a_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                    a_phone_col = next((h for h in a_headers if 'phone' in h.lower()), None)
                    
                    for row in a_rows:
                        mid = str(row.get(a_member_col, "")).strip() if a_member_col else ""
                        phone = str(row.get(a_phone_col, "")).strip() if a_phone_col else ""
                        if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id'] and phone:
                            if mid in contact_lookup:
                                contact_lookup[mid]['phone'] = phone
                            else:
                                contact_lookup[mid] = {'email': '', 'discord': '', 'phone': phone}
            
            # Parse typeform CSV (with preprocessing to find header row)
            text = typeform_data.decode('utf-8-sig')
            text = _preprocess_typeform_csv(text)
            
            # Auto-detect delimiter
            sample = text[:4096]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=',\t;|')
            except csv.Error:
                dialect = 'excel'
            
            reader = csv.DictReader(io.StringIO(text), dialect=dialect)
            rows = list(reader)
            
            if not rows:
                await ctx.send("❌ **Typeform CSV is empty.**")
                return
            
            # Find relevant columns
            headers = list(rows[0].keys())
            
            # Find member_id column
            member_id_col = None
            for h in headers:
                h_lower = h.lower()
                if "member id" in h_lower or h_lower == "member_id":
                    member_id_col = h
                    break
            
            # Find name column
            name_col = None
            for h in headers:
                h_lower = h.lower()
                if "discord" in h_lower or "username" in h_lower:
                    continue
                if "name" in h_lower:
                    name_col = h
                    break
            
            # Find discord column in typeform
            tf_discord_col = None
            for h in headers:
                if "discord" in h.lower():
                    tf_discord_col = h
                    break
            
            # Find issue_url column
            issue_col = None
            for h in headers:
                h_lower = h.lower()
                if "gitlab issue" in h_lower or "issue url" in h_lower or "direct link to your gitlab issue" in h_lower:
                    issue_col = h
                    break
            
            # Find why_chosen_complete column
            why_chosen_col = None
            for h in headers:
                if "why i chose this issue" in h.lower() or "why_chosen" in h.lower():
                    why_chosen_col = h
                    break
            
            if not member_id_col:
                await ctx.send(f"❌ **Could not find Member ID column in typeform.**\n\nColumns: {', '.join(headers[:10])}...")
                return
            
            if not issue_col:
                await ctx.send(f"❌ **Could not find Issue URL column in typeform.**\n\nColumns: {', '.join(headers[:10])}...")
                return
            
            # Track unique students - store full info
            student_info: dict = {}  # member_id -> {name, discord, email, phone}
            students_with_issues: set = set()
            students_with_why_chosen: set = set()
            
            for row in rows:
                member_id = str(row.get(member_id_col, "")).strip()
                if not member_id or member_id.lower() in ['#n/a', 'n/a', '']:
                    continue
                
                name = str(row.get(name_col, "")).strip() if name_col else "Unknown"
                tf_discord = str(row.get(tf_discord_col, "")).strip() if tf_discord_col else ""
                
                # Get contact info from lookup, with typeform discord as fallback
                contact = contact_lookup.get(member_id, {})
                discord = contact.get('discord', '') or tf_discord
                email = contact.get('email', '')
                phone = contact.get('phone', '')
                
                student_info[member_id] = {
                    'name': name,
                    'discord': discord,
                    'email': email,
                    'phone': phone
                }
                
                issue_url = str(row.get(issue_col, "")).strip() if issue_col else ""
                why_chosen = str(row.get(why_chosen_col, "")).strip().lower() if why_chosen_col else ""
                
                if issue_url and issue_url.lower() not in ['', 'n/a', '#n/a', 'none']:
                    students_with_issues.add(member_id)
                
                if why_chosen in ['yes', 'true', '1', 'y']:
                    students_with_why_chosen.add(member_id)
            
            # Calculate four categories
            all_students = set(student_info.keys())
            students_without_issues = all_students - students_with_issues
            students_why_no_issue = students_with_why_chosen - students_with_issues  # Has why but no issue (anomaly)
            students_issue_no_why = students_with_issues - students_with_why_chosen  # Has issue but no why
            students_complete = students_with_issues & students_with_why_chosen  # Has both
            
            # Helper function to send a list with contact info
            async def send_list(title: str, student_ids: set, emoji: str = "•"):
                if not student_ids:
                    await ctx.send(f"**{title}**\n✅ None!")
                    return
                
                lines = [f"**{title} ({len(student_ids)} total)**\n"]
                sorted_ids = sorted(student_ids, key=lambda mid: student_info[mid]['name'].lower())
                
                for member_id in sorted_ids:
                    info = student_info[member_id]
                    contact_parts = []
                    if info['discord']:
                        contact_parts.append(f"Discord: {info['discord']}")
                    if info['email']:
                        contact_parts.append(f"Email: {info['email']}")
                    if info['phone']:
                        contact_parts.append(f"Phone: {info['phone']}")
                    
                    contact_str = " | ".join(contact_parts) if contact_parts else "No contact info"
                    lines.append(f"{emoji} **{info['name']}** (`{member_id}`)\n   └─ {contact_str}")
                
                message = "\n".join(lines)
                if len(message) <= 2000:
                    await ctx.send(message)
                else:
                    chunks = []
                    current_chunk = lines[0] + "\n"
                    for line in lines[1:]:
                        if len(current_chunk) + len(line) + 1 > 1900:
                            chunks.append(current_chunk)
                            current_chunk = ""
                        current_chunk += line + "\n"
                    if current_chunk:
                        chunks.append(current_chunk)
                    
                    for i, chunk in enumerate(chunks):
                        if i == 0:
                            await ctx.send(chunk)
                        else:
                            await ctx.send(f"*(continued)*\n{chunk}")
            
            # Send all four lists
            await send_list("❌ Students Without Issue URL", students_without_issues, "❌")
            await send_list("⚠️ Students Without Issue URL but has 'Why I Chose This'", students_why_no_issue, "⚠️")
            await send_list("⚠️ Students With Issue URL but Missing 'Why I Chose This'", students_issue_no_why, "⚠️")
            await send_list("✅ Students With 'Why I Chose This' Complete", students_complete, "✅")
                        
        except Exception as e:
            await ctx.send(f"❌ **Error analyzing data:** {str(e)}")
            print(f"[Tracker] Error in no_issues: {e}")
    
    @commands.command(name='search_issues_title')
    async def search_issues_title(self, ctx: commands.Context, *, search_term: str = None):
        """Search issue titles from validated issues for a specific term.
        
        Usage: 
            !tracker search_issues_title <search_term>
            !tracker search_issues_title NOT:<search_term>
            !tracker search_issues_title <search_term> intervention:<TYPE>
            
        Examples:
            !tracker search_issues_title JsonSafeParse
            !tracker search_issues_title NOT:JsonSafeParse
            !tracker search_issues_title JsonSafeParse intervention:JSON_SAFEPARSE_ISSUE
        
        When intervention parameter is provided, matching students will be saved
        for automatic flagging during submissions_download.
        """
        import json
        import os
        import re
        import asyncio
        from datetime import datetime
        
        if not search_term:
            await ctx.send("❌ **Please provide a search term.**\n\nUsage: `!tracker search_issues_title <search_term>`\nUse `NOT:<term>` to exclude issues containing the term.")
            return
        
        # Parse intervention parameter if provided
        intervention_type = None
        actual_search_term = search_term
        if 'intervention:' in search_term.lower():
            parts = search_term.split()
            intervention_parts = [p for p in parts if p.lower().startswith('intervention:')]
            search_parts = [p for p in parts if not p.lower().startswith('intervention:')]
            
            if intervention_parts:
                intervention_type = intervention_parts[0].split(':', 1)[1].upper()
            actual_search_term = ' '.join(search_parts)
        
        if not actual_search_term.strip():
            await ctx.send("❌ **Please provide a search term.**\n\nUsage: `!tracker search_issues_title <search_term> [intervention:TYPE]`")
            return
        
        # Parse NOT: prefix for exclusion mode
        exclude_mode = False
        actual_search_term = actual_search_term.strip()
        if actual_search_term.upper().startswith('NOT:'):
            exclude_mode = True
            actual_search_term = actual_search_term[4:].strip()
        
        if not actual_search_term:
            await ctx.send("❌ **Please provide a search term after NOT:**")
            return
        
        search_term = actual_search_term
        
        results_file = os.path.join('data', 'uploads', '_validated_issues.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated issues data found.**\n\n"
                "Run `!tracker no_issues validate` first to generate the data."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated issues file:** {str(e)}")
            return
        
        # Collect all (student, issue_url) pairs from various categories
        # Key: (member_id, issue_url) to avoid duplicates while preserving all student-issue pairs
        all_student_issues: dict = {}  # (member_id, issue_url) -> {member_id, name, url}
        
        # Pattern to extract project path and issue IID from URL
        ISSUE_URL_PATTERN = re.compile(
            r'https?://gitlab\.com/([^/]+(?:/[^/]+)*)/-/(?:issues|work_items)/(\d+)',
            re.IGNORECASE
        )
        
        def add_issue(mid: str, name: str, url: str):
            """Add a student-issue pair if URL is valid."""
            if url and ISSUE_URL_PATTERN.match(url):
                key = (mid, url)
                if key not in all_student_issues:
                    all_student_issues[key] = {'member_id': mid, 'name': name, 'url': url}
        
        # Gather from students_with_valid_issue
        for mid, info in data.get('students_with_valid_issue', {}).items():
            name = info.get('name', 'Unknown')
            add_issue(mid, name, info.get('issue_url', ''))
        
        # Gather from issue_url_in_readme_link (they have valid issues, just wrong field)
        for mid, info in data.get('issue_url_in_readme_link', {}).items():
            name = info.get('name', 'Unknown')
            add_issue(mid, name, info.get('issue_url', ''))
        
        # Gather from issues_found (extracted from READMEs) - include ALL issues found
        for mid, info in data.get('issues_found', {}).items():
            name = info.get('name', 'Unknown')
            # Add the primary issue_url
            add_issue(mid, name, info.get('issue_url', ''))
            # Also add all issues found in the README
            for url in info.get('all_issues_found', []):
                add_issue(mid, name, url)
        
        if not all_student_issues:
            await ctx.send("❌ **No valid issue URLs found in validated data.**")
            return
        
        mode_text = f"NOT containing `{search_term}`" if exclude_mode else f"containing `{search_term}`"
        await ctx.send(f"🔍 **Searching {len(all_student_issues)} student-issue pairs for issues {mode_text}**\n\nThis may take a moment...")
        
        # Cache issue titles to avoid re-fetching the same URL
        issue_title_cache: dict = {}  # url -> {title, state} or None if error
        
        # Search each issue via GitLab API
        matching_issues: list = []
        errors: list = []
        processed = 0
        
        def title_matches(title: str) -> bool:
            """Check if title matches search criteria (include or exclude mode)."""
            term_in_title = search_term.lower() in title.lower()
            return not term_in_title if exclude_mode else term_in_title
        
        for (member_id, issue_url), student_info in all_student_issues.items():
            processed += 1
            
            # Progress update every 20 issues
            if processed % 20 == 0:
                await ctx.send(f"⏳ Progress: {processed}/{len(all_student_issues)} checked...")
            
            # Check cache first
            if issue_url in issue_title_cache:
                cached = issue_title_cache[issue_url]
                if cached and title_matches(cached['title']):
                    matching_issues.append({
                        'title': cached['title'],
                        'url': issue_url,
                        'member_id': student_info['member_id'],
                        'name': student_info['name'],
                        'state': cached['state']
                    })
                continue
            
            # Extract project path and issue IID
            match = ISSUE_URL_PATTERN.match(issue_url)
            if not match:
                issue_title_cache[issue_url] = None
                continue
            
            project_path = match.group(1)
            issue_iid = match.group(2)
            
            # Rate limiting
            await asyncio.sleep(0.3)
            
            try:
                import urllib.parse
                encoded_project = urllib.parse.quote(project_path, safe="")
                api_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/issues/{issue_iid}"
                
                issue_data = self.gitlab._make_request(api_url)
                
                if issue_data and issue_data.get('title'):
                    title = issue_data.get('title', '')
                    state = issue_data.get('state', 'unknown')
                    
                    # Cache the result
                    issue_title_cache[issue_url] = {'title': title, 'state': state}
                    
                    # Check if matches (include or exclude mode)
                    if title_matches(title):
                        matching_issues.append({
                            'title': title,
                            'url': issue_url,
                            'member_id': student_info['member_id'],
                            'name': student_info['name'],
                            'state': state
                        })
                else:
                    issue_title_cache[issue_url] = None
                    if issue_data is None:
                        errors.append(f"{student_info['name']} ({student_info['member_id']}): API error for {issue_url}")
            except Exception as e:
                issue_title_cache[issue_url] = None
                errors.append(f"{student_info['name']} ({student_info['member_id']}): {str(e)}")
        
        # If intervention type provided and we have matches, save them
        intervention_saved = False
        if intervention_type and matching_issues:
            interventions_file = os.path.join('data', 'uploads', '_issue_interventions.json')
            
            # Load existing interventions
            existing_interventions = {}
            if os.path.exists(interventions_file):
                try:
                    with open(interventions_file, 'r') as f:
                        existing_interventions = json.load(f)
                except:
                    pass
            
            # Remove old entries with this search term (replace mode)
            keys_to_remove = [k for k, v in existing_interventions.items() 
                             if v.get('search_term', '').lower() == search_term.lower()]
            for key in keys_to_remove:
                del existing_interventions[key]
            
            # Add new entries
            for issue in matching_issues:
                member_id = str(issue['member_id'])
                existing_interventions[member_id] = {
                    'intervention_type': intervention_type,
                    'issue_url': issue['url'],
                    'issue_title': issue['title'],
                    'search_term': search_term,
                    'added_at': datetime.now().isoformat()
                }
            
            # Save
            try:
                with open(interventions_file, 'w') as f:
                    json.dump(existing_interventions, f, indent=2)
                intervention_saved = True
            except Exception as e:
                await ctx.send(f"⚠️ **Error saving interventions:** {str(e)}")
        
        # Build report
        mode_desc = f"NOT containing `{search_term}`" if exclude_mode else f"containing `{search_term}`"
        report = [f"✅ **Search Complete:** Issues {mode_desc}\n"]
        report.append(f"📊 **Results:** {len(matching_issues)} matching issue(s) found out of {len(all_student_issues)} searched\n")
        
        if intervention_saved:
            report.append(f"💾 **Intervention Saved:** `{intervention_type}` applied to {len(matching_issues)} student(s)")
            report.append(f"   └─ These students will be flagged in `!tracker submissions_download`\n")
        
        if matching_issues:
            report.append("**🔗 Matching Issues:**")
            for issue in sorted(matching_issues, key=lambda x: x['name'].lower()):
                state_emoji = "🟢" if issue['state'] == 'opened' else "🔴" if issue['state'] == 'closed' else "⚪"
                report.append(f"• **{issue['name']}** (`{issue['member_id']}`) {state_emoji}")
                report.append(f"  └─ Title: {issue['title']}")
                report.append(f"  └─ <{issue['url']}>")
            report.append("")
        else:
            report.append("📭 **No issues found matching that criteria.**\n")
        
        if errors and len(errors) <= 5:
            report.append(f"⚠️ **Errors ({len(errors)}):**")
            for err in errors[:5]:
                report.append(f"  └─ {err}")
        elif errors:
            report.append(f"⚠️ **{len(errors)} errors occurred during search** (not shown)")
        
        # Send report in chunks
        full_report = "\n".join(report)
        if len(full_report) <= 2000:
            await ctx.send(full_report)
        else:
            chunks = []
            current = ""
            for line in report:
                if len(current) + len(line) + 1 > 1900:
                    chunks.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                chunks.append(current)
            for chunk in chunks:
                await ctx.send(chunk)
    
    @commands.command(name='issue_interventions')
    async def issue_interventions(self, ctx: commands.Context):
        """List all current issue-based interventions.
        
        Usage:
            !tracker issue_interventions
        
        Shows students who will be flagged during submissions_download
        based on their issue search term matches.
        """
        import json
        import os
        
        interventions_file = os.path.join('data', 'uploads', '_issue_interventions.json')
        
        if not os.path.exists(interventions_file):
            await ctx.send("📭 **No issue interventions configured.**\n\nUse `!tracker search_issues_title <term> intervention:<TYPE>` to add some.")
            return
        
        try:
            with open(interventions_file, 'r') as f:
                interventions = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading interventions file:** {str(e)}")
            return
        
        if not interventions:
            await ctx.send("📭 **No issue interventions configured.**\n\nUse `!tracker search_issues_title <term> intervention:<TYPE>` to add some.")
            return
        
        # Group by intervention type
        by_type: dict = {}
        for member_id, info in interventions.items():
            int_type = info.get('intervention_type', 'UNKNOWN')
            if int_type not in by_type:
                by_type[int_type] = []
            by_type[int_type].append({
                'member_id': member_id,
                'issue_title': info.get('issue_title', 'N/A'),
                'search_term': info.get('search_term', 'N/A'),
                'added_at': info.get('added_at', 'N/A')
            })
        
        report = ["📋 **Issue-Based Interventions**\n"]
        report.append(f"Total: {len(interventions)} student(s) across {len(by_type)} intervention type(s)\n")
        
        for int_type, students in sorted(by_type.items()):
            report.append(f"**{int_type}** ({len(students)} students):")
            # Get the search term (should be same for all in this type)
            search_term = students[0]['search_term'] if students else 'N/A'
            report.append(f"  └─ Search term: `{search_term}`")
            for s in sorted(students, key=lambda x: x['member_id']):
                report.append(f"  • `{s['member_id']}` - {s['issue_title'][:50]}...")
            report.append("")
        
        # Send report in chunks
        full_report = "\n".join(report)
        if len(full_report) <= 2000:
            await ctx.send(full_report)
        else:
            chunks = []
            current = ""
            for line in report:
                if len(current) + len(line) + 1 > 1900:
                    chunks.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                chunks.append(current)
            for chunk in chunks:
                await ctx.send(chunk)
    
    @commands.command(name='clear_issue_intervention')
    async def clear_issue_intervention(self, ctx: commands.Context, *, target: str = None):
        """Clear issue-based interventions.
        
        Usage:
            !tracker clear_issue_intervention <member_id>  - Clear specific student
            !tracker clear_issue_intervention clear_all    - Clear all interventions
            !tracker clear_issue_intervention type:<TYPE>  - Clear all of a specific type
        
        Examples:
            !tracker clear_issue_intervention 123456
            !tracker clear_issue_intervention clear_all
            !tracker clear_issue_intervention type:JSON_SAFEPARSE_ISSUE
        """
        import json
        import os
        
        if not target:
            await ctx.send(
                "❌ **Please specify what to clear.**\n\n"
                "Usage:\n"
                "• `!tracker clear_issue_intervention <member_id>` - Clear specific student\n"
                "• `!tracker clear_issue_intervention clear_all` - Clear all\n"
                "• `!tracker clear_issue_intervention type:<TYPE>` - Clear by intervention type"
            )
            return
        
        interventions_file = os.path.join('data', 'uploads', '_issue_interventions.json')
        
        if not os.path.exists(interventions_file):
            await ctx.send("📭 **No issue interventions to clear.**")
            return
        
        try:
            with open(interventions_file, 'r') as f:
                interventions = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading interventions file:** {str(e)}")
            return
        
        original_count = len(interventions)
        
        if target.lower() == 'clear_all':
            interventions = {}
            message = f"🗑️ **Cleared all {original_count} issue intervention(s).**"
        elif target.lower().startswith('type:'):
            int_type = target.split(':', 1)[1].upper()
            keys_to_remove = [k for k, v in interventions.items() 
                             if v.get('intervention_type', '').upper() == int_type]
            for key in keys_to_remove:
                del interventions[key]
            removed_count = len(keys_to_remove)
            message = f"🗑️ **Cleared {removed_count} intervention(s) of type `{int_type}`.**"
        else:
            # Treat as member_id
            member_id = target.strip()
            if member_id in interventions:
                int_type = interventions[member_id].get('intervention_type', 'UNKNOWN')
                del interventions[member_id]
                message = f"🗑️ **Cleared intervention for member `{member_id}` (was `{int_type}`).**"
            else:
                await ctx.send(f"❌ **No intervention found for member `{member_id}`.**")
                return
        
        # Save
        try:
            with open(interventions_file, 'w') as f:
                json.dump(interventions, f, indent=2)
            await ctx.send(message)
        except Exception as e:
            await ctx.send(f"❌ **Error saving interventions file:** {str(e)}")
    
    @commands.command(name='search_dl_issues_title')
    async def search_dl_issues_title(self, ctx: commands.Context, *, search_term: str = None):
        """Search issue titles and download results as CSV with contact info.
        
        Usage: 
            !tracker search_dl_issues_title <search_term>
            !tracker search_dl_issues_title NOT:<search_term>
            
        Examples:
            !tracker search_dl_issues_title JsonSafeParse
            !tracker search_dl_issues_title NOT:JsonSafeParse
        
        Downloads a CSV with: Name, Member ID, Discord, Email, Phone, Issue Title, Issue Description, Issue URL, State
        """
        import json
        import os
        import re
        import csv
        import asyncio
        
        if not search_term:
            await ctx.send("❌ **Please provide a search term.**\n\nUsage: `!tracker search_dl_issues_title <search_term>`\nUse `NOT:<term>` to exclude issues containing the term.")
            return
        
        # Parse NOT: prefix for exclusion mode
        exclude_mode = False
        actual_search_term = search_term.strip()
        if actual_search_term.upper().startswith('NOT:'):
            exclude_mode = True
            actual_search_term = actual_search_term[4:].strip()
        
        if not actual_search_term:
            await ctx.send("❌ **Please provide a search term after NOT:**")
            return
        
        search_term = actual_search_term
        
        results_file = os.path.join('data', 'uploads', '_validated_issues.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated issues data found.**\n\n"
                "Run `!tracker no_issues validate` first to generate the data."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated issues file:** {str(e)}")
            return
        
        # Build contact lookup from master CSV
        contact_lookup: dict = {}  # member_id -> {email, discord, phone}
        master_file = self.storage.get_file("master")
        if master_file:
            master_data = self.storage.read_file(master_file)
            master_text = master_data.decode('utf-8-sig')
            
            # Preprocess master CSV - find the header row containing "Member ID"
            lines = master_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                master_text = "\n".join(lines[header_row_idx:])
            
            try:
                m_dialect = csv.Sniffer().sniff(master_text[:4096], delimiters=',\t;|')
            except csv.Error:
                m_dialect = 'excel'
            m_reader = csv.DictReader(io.StringIO(master_text), dialect=m_dialect)
            m_rows = list(m_reader)
            if m_rows:
                m_headers = list(m_rows[0].keys())
                m_member_col = next((h for h in m_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                m_email_col = next((h for h in m_headers if 'email' in h.lower() and 'secondary' not in h.lower()), None)
                m_discord_col = next((h for h in m_headers if 'discord' in h.lower()), None)
                
                for row in m_rows:
                    mid = str(row.get(m_member_col, "")).strip() if m_member_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id']:
                        contact_lookup[mid] = {
                            'email': str(row.get(m_email_col, "")).strip() if m_email_col else "",
                            'discord': str(row.get(m_discord_col, "")).strip() if m_discord_col else "",
                            'phone': ""
                        }
        
        # Add phone numbers from app CSV
        app_file = self.storage.get_file("app")
        if app_file:
            app_data = self.storage.read_file(app_file)
            app_text = app_data.decode('utf-8-sig')
            
            lines = app_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                app_text = "\n".join(lines[header_row_idx:])
            
            try:
                a_dialect = csv.Sniffer().sniff(app_text[:4096], delimiters=',\t;|')
            except csv.Error:
                a_dialect = 'excel'
            a_reader = csv.DictReader(io.StringIO(app_text), dialect=a_dialect)
            a_rows = list(a_reader)
            if a_rows:
                a_headers = list(a_rows[0].keys())
                a_member_col = next((h for h in a_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                a_phone_col = next((h for h in a_headers if 'phone' in h.lower()), None)
                
                for row in a_rows:
                    mid = str(row.get(a_member_col, "")).strip() if a_member_col else ""
                    phone = str(row.get(a_phone_col, "")).strip() if a_phone_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id'] and phone:
                        if mid in contact_lookup:
                            contact_lookup[mid]['phone'] = phone
                        else:
                            contact_lookup[mid] = {'email': '', 'discord': '', 'phone': phone}
        
        # Collect all (student, issue_url) pairs from various categories
        all_student_issues: dict = {}  # (member_id, issue_url) -> {member_id, name, url}
        
        # Pattern to extract project path and issue IID from URL
        ISSUE_URL_PATTERN = re.compile(
            r'https?://gitlab\.com/([^/]+(?:/[^/]+)*)/-/(?:issues|work_items)/(\d+)',
            re.IGNORECASE
        )
        
        def add_issue(mid: str, name: str, url: str):
            """Add a student-issue pair if URL is valid."""
            if url and ISSUE_URL_PATTERN.match(url):
                key = (mid, url)
                if key not in all_student_issues:
                    all_student_issues[key] = {'member_id': mid, 'name': name, 'url': url}
        
        # Gather from students_with_valid_issue
        for mid, info in data.get('students_with_valid_issue', {}).items():
            name = info.get('name', 'Unknown')
            add_issue(mid, name, info.get('issue_url', ''))
        
        # Gather from issue_url_in_readme_link
        for mid, info in data.get('issue_url_in_readme_link', {}).items():
            name = info.get('name', 'Unknown')
            add_issue(mid, name, info.get('issue_url', ''))
        
        # Gather from issues_found - include ALL issues found
        for mid, info in data.get('issues_found', {}).items():
            name = info.get('name', 'Unknown')
            add_issue(mid, name, info.get('issue_url', ''))
            for url in info.get('all_issues_found', []):
                add_issue(mid, name, url)
        
        if not all_student_issues:
            await ctx.send("❌ **No valid issue URLs found in validated data.**")
            return
        
        mode_text = f"NOT containing `{search_term}`" if exclude_mode else f"containing `{search_term}`"
        await ctx.send(f"🔍 **Searching {len(all_student_issues)} student-issue pairs for issues {mode_text}**\n\nThis may take a moment...")
        
        # Cache issue titles to avoid re-fetching the same URL
        issue_title_cache: dict = {}
        
        def title_matches(title: str) -> bool:
            """Check if title matches search criteria (include or exclude mode)."""
            term_in_title = search_term.lower() in title.lower()
            return not term_in_title if exclude_mode else term_in_title
        
        # Search each issue via GitLab API
        matching_issues: list = []
        errors: list = []
        processed = 0
        
        for (member_id, issue_url), student_info in all_student_issues.items():
            processed += 1
            
            # Progress update every 20 issues
            if processed % 20 == 0:
                await ctx.send(f"⏳ Progress: {processed}/{len(all_student_issues)} checked...")
            
            # Check cache first
            if issue_url in issue_title_cache:
                cached = issue_title_cache[issue_url]
                if cached and title_matches(cached['title']):
                    contact = contact_lookup.get(member_id, {})
                    matching_issues.append({
                        'name': student_info['name'],
                        'member_id': member_id,
                        'discord': contact.get('discord', ''),
                        'email': contact.get('email', ''),
                        'phone': contact.get('phone', ''),
                        'title': cached['title'],
                        'description': cached.get('description', ''),
                        'url': issue_url,
                        'state': cached['state']
                    })
                continue
            
            match = ISSUE_URL_PATTERN.match(issue_url)
            if not match:
                issue_title_cache[issue_url] = None
                continue
            
            project_path = match.group(1)
            issue_iid = match.group(2)
            
            await asyncio.sleep(0.3)
            
            try:
                import urllib.parse
                encoded_project = urllib.parse.quote(project_path, safe="")
                api_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/issues/{issue_iid}"
                
                issue_data = self.gitlab._make_request(api_url)
                
                if issue_data and issue_data.get('title'):
                    title = issue_data.get('title', '')
                    state = issue_data.get('state', 'unknown')
                    description = issue_data.get('description', '') or ''
                    
                    issue_title_cache[issue_url] = {'title': title, 'state': state, 'description': description}
                    
                    if title_matches(title):
                        contact = contact_lookup.get(member_id, {})
                        matching_issues.append({
                            'name': student_info['name'],
                            'member_id': member_id,
                            'discord': contact.get('discord', ''),
                            'email': contact.get('email', ''),
                            'phone': contact.get('phone', ''),
                            'title': title,
                            'description': description,
                            'url': issue_url,
                            'state': state
                        })
                else:
                    issue_title_cache[issue_url] = None
                    if issue_data is None:
                        errors.append(f"{student_info['name']} ({member_id}): API error")
            except Exception as e:
                issue_title_cache[issue_url] = None
                errors.append(f"{student_info['name']} ({member_id}): {str(e)}")
        
        if not matching_issues:
            mode_desc = f"NOT containing `{search_term}`" if exclude_mode else f"containing `{search_term}`"
            await ctx.send(f"📭 **No issues found {mode_desc}**\n\nSearched {len(all_student_issues)} student-issue pairs.")
            return
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header row
        writer.writerow(['Name', 'Member ID', 'Discord', 'Email', 'Phone', 'Issue Title', 'Issue Description', 'Issue URL', 'State'])
        
        # Data rows (sorted by name)
        for issue in sorted(matching_issues, key=lambda x: x['name'].lower()):
            # Truncate description if too long for CSV readability
            desc = issue.get('description', '')
            if len(desc) > 500:
                desc = desc[:497] + '...'
            writer.writerow([
                issue['name'],
                issue['member_id'],
                issue['discord'],
                issue['email'],
                issue['phone'],
                issue['title'],
                desc,
                issue['url'],
                issue['state']
            ])
        
        # Find duplicate issues (same URL used by multiple students)
        # Normalize URLs by removing anchors for comparison
        def normalize_url(url: str) -> str:
            """Remove anchor/fragment from URL for comparison."""
            return url.split('#')[0].rstrip('/')
        
        url_to_students: dict = {}  # normalized_url -> [(name, member_id, original_url), ...]
        seen_url_student: set = set()  # (normalized_url, member_id) to deduplicate same student on same issue
        for issue in matching_issues:
            norm_url = normalize_url(issue['url'])
            key = (norm_url, issue['member_id'])
            if key in seen_url_student:
                continue  # Skip duplicate entry for same student on same issue
            seen_url_student.add(key)
            if norm_url not in url_to_students:
                url_to_students[norm_url] = []
            url_to_students[norm_url].append((issue['name'], issue['member_id'], issue['url']))
        
        # Filter to only duplicates (more than one unique student)
        duplicates = {url: students for url, students in url_to_students.items() if len(students) > 1}
        
        # Add footnote section if there are duplicates
        if duplicates:
            writer.writerow([])  # Blank row
            writer.writerow([])  # Another blank row
            writer.writerow(['--- DUPLICATE ISSUES (Multiple students on same issue) ---'])
            writer.writerow([])
            
            for norm_url, students in sorted(duplicates.items()):
                # Get the issue title from one of the matching issues
                issue_title = next((i['title'] for i in matching_issues if normalize_url(i['url']) == norm_url), 'Unknown')
                writer.writerow([f'Issue: {issue_title}'])
                writer.writerow([f'URL: {norm_url}'])
                writer.writerow([f'Students ({len(students)}):'])
                for name, member_id, orig_url in sorted(students, key=lambda x: x[0].lower()):
                    writer.writerow([f'  - {name} ({member_id})'])
                writer.writerow([])  # Blank row between duplicates
        
        # Create file for Discord
        csv_content = output.getvalue().encode('utf-8')
        
        # Generate filename with search term (sanitized)
        safe_term = re.sub(r'[^\w\-]', '_', search_term)[:30]
        filename = f"issue_search_{safe_term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Send summary and file
        summary = (
            f"✅ **Search Complete for:** `{search_term}`\n\n"
            f"📊 **Results:** {len(matching_issues)} matching issue(s) found out of {len(all_student_issues)} searched\n"
        )
        
        if duplicates:
            summary += f"⚠️ **{len(duplicates)} duplicate issue(s)** found (multiple students on same issue) - see footnote in CSV\n"
        
        if errors:
            summary += f"⚠️ {len(errors)} errors occurred during search\n"
        
        await ctx.send(
            summary,
            file=discord.File(io.BytesIO(csv_content), filename=filename)
        )
    
    async def _validate_no_issues(self, ctx: commands.Context):
        """Crawl READMEs to find issue URLs for students without one.
        
        For each student without an issue_url, fetches their README and extracts
        the latest issue URL found. Results are persisted to a JSON file.
        """
        import csv
        import json
        import re
        import os
        from services.tracker_processor import _preprocess_typeform_csv
        from services.gitlab_service import GitLabService
        
        typeform_file = self.storage.get_file("typeform")
        if not typeform_file:
            await ctx.send("❌ **No typeform data uploaded.** Upload typeform CSV first with `!tracker upload typeform`")
            return
        
        typeform_data = self.storage.read_file(typeform_file)
        
        await ctx.send("🔍 **Validating students without issue URLs - crawling READMEs...**")
        
        try:
            # Parse typeform CSV
            text = typeform_data.decode('utf-8-sig')
            text = _preprocess_typeform_csv(text)
            
            try:
                dialect = csv.Sniffer().sniff(text[:4096], delimiters=',\t;|')
            except csv.Error:
                dialect = 'excel'
            
            reader = csv.DictReader(io.StringIO(text), dialect=dialect)
            rows = list(reader)
            
            if not rows:
                await ctx.send("❌ **Typeform CSV is empty.**")
                return
            
            # Find relevant columns
            headers = list(rows[0].keys())
            
            member_id_col = next((h for h in headers if "member id" in h.lower() or h.lower() == "member_id"), None)
            name_col = next((h for h in headers if "name" in h.lower() and "discord" not in h.lower() and "username" not in h.lower()), None)
            issue_col = next((h for h in headers if "gitlab issue" in h.lower() or "issue url" in h.lower() or "direct link to your gitlab issue" in h.lower()), None)
            readme_col = next((h for h in headers if "readme" in h.lower() and "link" in h.lower()), None)
            
            if not member_id_col:
                await ctx.send(f"❌ **Could not find Member ID column.**")
                return
            if not readme_col:
                await ctx.send(f"❌ **Could not find README link column.**")
                return
            
            # Valid issue URL pattern (must end with -/issues/{num} or -/work_items/{num}, optionally with anchor like #top or #note_123)
            VALID_ISSUE_URL_PATTERN = re.compile(
                r'^https?://gitlab\.com/[^/]+(?:/[^/]+)*/-/(?:issues|work_items)/\d+(?:#[a-zA-Z0-9_-]+)?(?:\?[^#]*)?$',
                re.IGNORECASE
            )
            
            # README/repo URL pattern - detects when someone put a README link in the issue_url field
            # Matches: repo root URLs, blob URLs (files), tree URLs (directories)
            README_URL_PATTERN = re.compile(
                r'^https?://gitlab\.com/[^/]+/[^/]+(?:/-/(?:blob|tree)/|/?(?:\?|#|$))',
                re.IGNORECASE
            )
            
            # Collect students with and without issue_url
            students_no_issue: dict = {}  # member_id -> {name, readme_link}
            students_with_valid_issue: dict = {}  # member_id -> {name, issue_url}
            students_with_invalid_issue: dict = {}  # member_id -> {name, issue_url}
            readme_url_in_issue_field: dict = {}  # member_id -> {name, readme_url} - README in issue field!
            issue_url_in_readme_link: dict = {}  # member_id -> {name, issue_url, readme_link} - issue in readme field!
            
            for row in rows:
                member_id = str(row.get(member_id_col, "")).strip()
                if not member_id or member_id.lower() in ['#n/a', 'n/a', '']:
                    continue
                
                name = str(row.get(name_col, "")).strip() if name_col else "Unknown"
                issue_url = str(row.get(issue_col, "")).strip() if issue_col else ""
                readme_link = str(row.get(readme_col, "")).strip() if readme_col else ""
                
                # Track students who already have issue_url
                if issue_url and issue_url.lower() not in ['', 'n/a', '#n/a', 'none']:
                    # Validate the issue URL format
                    if VALID_ISSUE_URL_PATTERN.match(issue_url):
                        students_with_valid_issue[member_id] = {'name': name, 'issue_url': issue_url}
                    # Check if it looks like a README/repo URL (wrong field!)
                    elif README_URL_PATTERN.match(issue_url):
                        readme_url_in_issue_field[member_id] = {
                            'name': name,
                            'readme_url': issue_url,
                            'note': 'README/repo URL was incorrectly placed in issue URL field'
                        }
                    else:
                        students_with_invalid_issue[member_id] = {'name': name, 'issue_url': issue_url}
                    continue
                
                # Check if readme_link is actually an issue URL (common mistake!)
                if readme_link and VALID_ISSUE_URL_PATTERN.match(readme_link):
                    # This is a special case - issue URL was put in readme_link field
                    issue_url_in_readme_link[member_id] = {
                        'name': name,
                        'issue_url': readme_link,
                        'readme_link': readme_link,
                        'note': 'Issue URL was incorrectly placed in README link field'
                    }
                    continue
                
                # Collect ALL unique readme URLs for each student (not just the latest)
                if readme_link:
                    if member_id not in students_no_issue:
                        students_no_issue[member_id] = {'name': name, 'readme_links': set()}
                    students_no_issue[member_id]['readme_links'].add(readme_link)
            
            if not students_no_issue:
                await ctx.send("✅ **All students have issue URLs!** Nothing to validate.")
                return
            
            await ctx.send(f"📊 Found **{len(students_no_issue)}** students without issue URL. Crawling their READMEs...")
            
            # Initialize GitLab service
            gitlab_service = GitLabService()
            
            # Issue URL pattern for GitLab (for extracting from README)
            ISSUE_PATTERN = re.compile(
                r'https?://gitlab\.com/[^/]+(?:/[^/]+)*/-/(?:issues|work_items)/\d+(?:#[a-zA-Z0-9_-]+)?',
                re.IGNORECASE
            )
            
            # Issue number reference pattern (e.g., #586126, [#586126], Issue: #586126)
            ISSUE_NUMBER_PATTERN = re.compile(
                r'(?:issue[:\s]*)?[\[\(]?#(\d{4,})[\]\)]?',
                re.IGNORECASE
            )
            
            # Project shorthand pattern (e.g., gitlab-org/gitlab#586041, [gitlab-org/gitlab#586041])
            # Captures: group(1) = project path (e.g., gitlab-org/gitlab), group(2) = issue number
            PROJECT_ISSUE_SHORTHAND_PATTERN = re.compile(
                r'[\[\(]?([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)#(\d{4,})[\]\)]?',
                re.IGNORECASE
            )
            
            # Default GitLab project for issue number lookup
            DEFAULT_GITLAB_PROJECT = "gitlab-org/gitlab"
            
            # Results tracking
            issues_found: dict = {}  # member_id -> {name, readme_link, issue_url}
            readme_inaccessible: dict = {}  # member_id -> {name, readme_link, error}
            readme_timeout: dict = {}  # member_id -> {name, readme_link, attempts}
            no_issue_in_readme: dict = {}  # member_id -> {name, readme_link}
            
            processed = 0
            total = len(students_no_issue)
            
            # Rate limiting: 1 second delay between requests to be safe
            API_DELAY = 1.0
            MAX_RETRIES = 3
            
            import socket
            import ssl
            import time
            
            for member_id, info in students_no_issue.items():
                processed += 1
                name = info['name']
                readme_links = list(info['readme_links'])  # Convert set to list
                
                # Progress update every 10 students
                if processed % 10 == 0:
                    await ctx.send(f"⏳ Progress: {processed}/{total} students checked...")
                
                # Track all issues found across all README URLs for this student
                student_all_issues: list = []
                student_readme_links_crawled: list = []
                student_errors: list = []
                repos_crawled: set = set()  # Track repos already fully crawled
                
                for readme_link in readme_links:
                    repo_path, platform = gitlab_service.extract_repo_from_readme_link(readme_link)
                    
                    if not repo_path:
                        student_errors.append(f"{readme_link}: Could not extract repo path")
                        continue
                    
                    file_path = gitlab_service.extract_file_path_from_url(readme_link)
                    is_tree = gitlab_service.is_tree_url(readme_link)
                    
                    readme_content = None
                    all_readme_contents = []
                    last_error = None
                    
                    for attempt in range(MAX_RETRIES):
                        try:
                            if attempt > 0 or processed > 1:
                                await asyncio.sleep(API_DELAY)
                            
                            if is_tree or not file_path:
                                # Tree URL or repo root: fetch ALL README files in the repo
                                # Only do this once per repo
                                if repo_path not in repos_crawled:
                                    all_readme_contents = gitlab_service.fetch_all_readme_contents(repo_path)
                                    repos_crawled.add(repo_path)
                                    if all_readme_contents:
                                        readme_content = "\n\n".join([content for _, content in all_readme_contents])
                                        student_readme_links_crawled.append(f"{readme_link} (all READMEs)")
                                        break
                                else:
                                    # Already crawled this repo
                                    break
                            else:
                                # Specific file URL
                                readme_content = gitlab_service.fetch_file_content(repo_path, file_path)
                                if readme_content:
                                    student_readme_links_crawled.append(readme_link)
                            
                            if not readme_content and repo_path not in repos_crawled:
                                # Fall back to fetching all READMEs
                                all_readme_contents = gitlab_service.fetch_all_readme_contents(repo_path)
                                repos_crawled.add(repo_path)
                                if all_readme_contents:
                                    readme_content = "\n\n".join([content for _, content in all_readme_contents])
                                    student_readme_links_crawled.append(f"{readme_link} (all READMEs)")
                            
                            if readme_content:
                                break
                            
                        except (socket.timeout, TimeoutError, ssl.SSLError) as e:
                            last_error = f'Timeout (attempt {attempt + 1}/{MAX_RETRIES})'
                            print(f"[Validate] Timeout for {name}, attempt {attempt + 1}/{MAX_RETRIES}")
                            if attempt < MAX_RETRIES - 1:
                                await asyncio.sleep(2)
                            continue
                        except Exception as e:
                            last_error = str(e)
                            break
                    
                    if readme_content:
                        # Find issue URLs in this README content
                        issues_in_readme = ISSUE_PATTERN.findall(readme_content)
                        for issue in issues_in_readme:
                            if issue not in student_all_issues:
                                student_all_issues.append(issue)
                    elif last_error:
                        student_errors.append(f"{readme_link}: {last_error}")
                
                # After crawling all README URLs for this student
                if student_all_issues:
                    latest_issue = student_all_issues[-1]
                    issues_found[member_id] = {
                        'name': name,
                        'readme_link': readme_links[0] if len(readme_links) == 1 else ', '.join(readme_links[:3]),
                        'readme_links_crawled': student_readme_links_crawled,
                        'all_issues_found': student_all_issues,
                        'source': 'url'
                    }
                else:
                    # No full URLs found - need to search through all README content
                    # Combine all README content for pattern searching
                    combined_content = ""
                    for readme_link in readme_links:
                        repo_path, _ = gitlab_service.extract_repo_from_readme_link(readme_link)
                        if repo_path and repo_path in repos_crawled:
                            # We already have content from this repo
                            continue
                    
                    # Try to fetch content if we don't have any yet
                    if not combined_content and readme_links:
                        for readme_link in readme_links:
                            repo_path, _ = gitlab_service.extract_repo_from_readme_link(readme_link)
                            if repo_path:
                                try:
                                    all_readme_contents = gitlab_service.fetch_all_readme_contents(repo_path)
                                    if all_readme_contents:
                                        combined_content = "\n\n".join([content for _, content in all_readme_contents])
                                        break
                                except Exception:
                                    continue
                    
                    if not combined_content:
                        combined_content = ""
                    
                    # Try project shorthand pattern (e.g., gitlab-org/gitlab#586041)
                    shorthand_matches = PROJECT_ISSUE_SHORTHAND_PATTERN.findall(combined_content) if combined_content else []
                    
                    if shorthand_matches:
                        # Try to validate shorthand references against GitLab
                        validated_issue = None
                        for project_path, issue_num in reversed(shorthand_matches):  # Start from last (most recent)
                            # Construct potential issue URL
                            potential_url = f"https://gitlab.com/{project_path}/-/issues/{issue_num}"
                            
                            # Verify the issue exists
                            try:
                                await asyncio.sleep(0.5)  # Rate limit
                                import urllib.parse
                                encoded_project = urllib.parse.quote(project_path, safe="")
                                check_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/issues/{issue_num}"
                                issue_data = gitlab_service._make_request(check_url)
                                
                                if issue_data and issue_data.get('iid'):
                                    validated_issue = potential_url
                                    break
                            except Exception as e:
                                print(f"[Validate] Error checking issue {project_path}#{issue_num}: {e}")
                                continue
                        
                        if validated_issue:
                            issues_found[member_id] = {
                                'name': name,
                                'readme_link': readme_links[0] if len(readme_links) == 1 else ', '.join(readme_links[:3]),
                                'readme_links': list(readme_links),
                                'issue_url': validated_issue,
                                'all_issues_found': [f"{p}#{n}" for p, n in shorthand_matches],
                                'source': 'project_shorthand'
                            }
                        else:
                            # Try the simpler issue number pattern as fallback
                            issue_number_matches = ISSUE_NUMBER_PATTERN.findall(combined_content) if combined_content else []
                            if issue_number_matches:
                                # Fall through to issue number validation below
                                pass
                            else:
                                no_issue_in_readme[member_id] = {
                                    'name': name,
                                    'readme_link': readme_links[0] if readme_links else '',
                                    'readme_links': list(readme_links),
                                    'shorthand_found': [f"{p}#{n}" for p, n in shorthand_matches],
                                    'note': 'Project shorthand found but could not validate'
                                }
                                continue
                    
                    # No shorthand found or validation failed - try to find issue number references like #586126
                    if member_id not in issues_found:
                        issue_number_matches = ISSUE_NUMBER_PATTERN.findall(combined_content) if combined_content else []
                        
                        if issue_number_matches:
                            # Try to validate issue numbers against GitLab
                            validated_issue = None
                            for issue_num in reversed(issue_number_matches):  # Start from last (most recent)
                                # Construct potential issue URL
                                potential_url = f"https://gitlab.com/{DEFAULT_GITLAB_PROJECT}/-/issues/{issue_num}"
                                
                                # Verify the issue exists
                                try:
                                    await asyncio.sleep(0.5)  # Rate limit
                                    import urllib.parse
                                    encoded_project = urllib.parse.quote(DEFAULT_GITLAB_PROJECT, safe="")
                                    check_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/issues/{issue_num}"
                                    issue_data = gitlab_service._make_request(check_url)
                                    
                                    if issue_data and issue_data.get('iid'):
                                        validated_issue = potential_url
                                        break
                                except Exception as e:
                                    print(f"[Validate] Error checking issue #{issue_num}: {e}")
                                    continue
                            
                            if validated_issue:
                                issues_found[member_id] = {
                                    'name': name,
                                    'readme_link': readme_links[0] if len(readme_links) == 1 else ', '.join(readme_links[:3]),
                                    'readme_links': list(readme_links),
                                    'issue_url': validated_issue,
                                    'all_issues_found': [f"#{num}" for num in issue_number_matches],
                                    'source': 'number_reference'
                                }
                            else:
                                # Found issue numbers but couldn't validate them
                                no_issue_in_readme[member_id] = {
                                    'name': name,
                                    'readme_link': readme_links[0] if readme_links else '',
                                    'readme_links': list(readme_links),
                                    'issue_numbers_found': list(set(issue_number_matches)),
                                    'note': 'Issue numbers found but could not validate against gitlab-org/gitlab'
                                }
                        elif student_errors and all("timeout" in e.lower() for e in student_errors):
                            readme_timeout[member_id] = {
                                'name': name,
                                'readme_link': readme_links[0] if readme_links else '',
                                'readme_links': list(readme_links),
                                'attempts': MAX_RETRIES
                            }
                        elif student_errors:
                            readme_inaccessible[member_id] = {
                                'name': name,
                                'readme_link': readme_links[0] if readme_links else '',
                                'readme_links': list(readme_links),
                                'error': '; '.join(student_errors[:3])
                            }
                        else:
                            no_issue_in_readme[member_id] = {
                                'name': name,
                                'readme_link': readme_links[0] if readme_links else '',
                                'readme_links': list(readme_links)
                            }
            
            # Save results to file
            results = {
                'validated_at': datetime.now().isoformat(),
                'students_with_valid_issue': students_with_valid_issue,
                'students_with_invalid_issue': students_with_invalid_issue,
                'readme_url_in_issue_field': readme_url_in_issue_field,
                'issue_url_in_readme_link': issue_url_in_readme_link,
                'issues_found': issues_found,
                'no_issue_in_readme': no_issue_in_readme,
                'readme_inaccessible': readme_inaccessible,
                'readme_timeout': readme_timeout
            }
            
            results_file = os.path.join('data', 'uploads', '_validated_issues.json')
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            # Build report
            report = ["✅ **README Validation Complete**\n"]
            report.append(f"📊 **Summary:**")
            report.append(f"• Valid issue URL: **{len(students_with_valid_issue)}**")
            report.append(f"• ⚠️ Invalid issue URL: **{len(students_with_invalid_issue)}**")
            report.append(f"• ⚠️ README URL in issue field: **{len(readme_url_in_issue_field)}** (wrong field)")
            report.append(f"• ⚠️ Issue URL in README field: **{len(issue_url_in_readme_link)}** (wrong field)")
            report.append(f"• Issues found in README: **{len(issues_found)}**")
            report.append(f"• No issue in README: **{len(no_issue_in_readme)}**")
            report.append(f"• README inaccessible: **{len(readme_inaccessible)}**")
            report.append(f"• Timed out (can retry): **{len(readme_timeout)}**\n")
            
            # Students with INVALID issue URLs (show first as these need attention)
            if students_with_invalid_issue:
                report.append("**❌ Students With INVALID Issue URL:**")
                report.append("*(Expected format: gitlab.com/.../issues/{num} or .../work_items/{num})*")
                for mid, data in sorted(students_with_invalid_issue.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['issue_url']}>")
                report.append("")
            
            # Students who put README URL in issue_url field (wrong field!)
            if readme_url_in_issue_field:
                report.append("**⚠️ Students With README URL in Issue Field (wrong field!):**")
                report.append("*(These students put a README/repo link in the issue URL field)*")
                for mid, data in sorted(readme_url_in_issue_field.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ README: <{data['readme_url']}>")
                report.append("")
            
            # Students who put issue URL in readme_link field (special case - has valid issue but wrong field!)
            if issue_url_in_readme_link:
                report.append("**⚠️ Students With Issue URL in README Field (wrong field!):**")
                report.append("*(These students have valid issues but put them in the README link field instead of issue URL field)*")
                for mid, data in sorted(issue_url_in_readme_link.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ Issue: <{data['issue_url']}>")
                report.append("")
            
            # Students who already have valid issue URLs
            if students_with_valid_issue:
                report.append("**✅ Students With Valid Issue URL (in typeform):**")
                for mid, data in sorted(students_with_valid_issue.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['issue_url']}>")
                report.append("")
            
            # Issues found in README
            if issues_found:
                report.append("**🔗 Issues Found in README (extracted):**")
                for mid, data in sorted(issues_found.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['issue_url']}>")
                    if data.get('source') == 'project_shorthand':
                        report.append(f"     *(found via project shorthand: {data.get('all_issues_found', [])})*")
                report.append("")
            
            # No issue found in README
            if no_issue_in_readme:
                report.append("**📭 No Issue Found in README:**")
                for mid, data in sorted(no_issue_in_readme.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['readme_link']}>")
                    if data.get('shorthand_found'):
                        report.append(f"     *(unvalidated shorthand found: {data.get('shorthand_found')})*")
                report.append("")
            
            # Send report in chunks
            full_report = "\n".join(report)
            if len(full_report) <= 2000:
                await ctx.send(full_report)
            else:
                chunks = []
                current = ""
                for line in report:
                    if len(current) + len(line) + 1 > 1900:
                        chunks.append(current)
                        current = line
                    else:
                        current += "\n" + line if current else line
                if current:
                    chunks.append(current)
                
                for chunk in chunks:
                    await ctx.send(chunk)
            
            # Report inaccessible READMEs separately if any
            if readme_inaccessible:
                inacc_report = ["**⚠️ READMEs Not Accessible:**"]
                for mid, data in sorted(readme_inaccessible.items(), key=lambda x: x[1]['name'].lower()):
                    inacc_report.append(f"• **{data['name']}** (`{mid}`)")
                    inacc_report.append(f"  └─ Link: <{data['readme_link']}>")
                    inacc_report.append(f"  └─ Error: {data['error']}")
                
                inacc_text = "\n".join(inacc_report)
                if len(inacc_text) <= 2000:
                    await ctx.send(inacc_text)
                else:
                    chunks = []
                    current = ""
                    for line in inacc_report:
                        if len(current) + len(line) + 1 > 1900:
                            chunks.append(current)
                            current = line
                        else:
                            current += "\n" + line if current else line
                    if current:
                        chunks.append(current)
                    for chunk in chunks:
                        await ctx.send(chunk)
            
            # Report timed out READMEs separately if any
            if readme_timeout:
                timeout_report = ["**⏱️ READMEs Timed Out (can retry later):**"]
                for mid, data in sorted(readme_timeout.items(), key=lambda x: x[1]['name'].lower()):
                    timeout_report.append(f"• **{data['name']}** (`{mid}`)")
                    timeout_report.append(f"  └─ Link: <{data['readme_link']}>")
                    timeout_report.append(f"  └─ Attempts: {data['attempts']}")
                
                timeout_text = "\n".join(timeout_report)
                if len(timeout_text) <= 2000:
                    await ctx.send(timeout_text)
                else:
                    chunks = []
                    current = ""
                    for line in timeout_report:
                        if len(current) + len(line) + 1 > 1900:
                            chunks.append(current)
                            current = line
                        else:
                            current += "\n" + line if current else line
                    if current:
                        chunks.append(current)
                    for chunk in chunks:
                        await ctx.send(chunk)
            
            await ctx.send(f"💾 Results saved to `{results_file}`")
            
        except Exception as e:
            await ctx.send(f"❌ **Error validating:** {str(e)}")
            print(f"[Tracker] Error in validate_no_issues: {e}")
            import traceback
            traceback.print_exc()
    
    # ==================== Download Issues (CSV Export) ====================
    
    @commands.command(name='dl_issues')
    async def download_issues(self, ctx: commands.Context):
        """Download validated issues as CSV with contact info.
        
        Usage: 
            !tracker dl_issues
        
        Downloads a CSV with all students and their issue status:
        Name, Member ID, Discord, Email, Phone, Status, Issue URL, Source, Notes
        """
        import json
        import os
        import csv
        from io import StringIO
        from datetime import datetime
        
        results_file = os.path.join('data', 'uploads', '_validated_issues.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated issues data found.**\n\n"
                "Run `!tracker no_issues validate` first to generate the data."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated issues file:** {str(e)}")
            return
        
        validated_at = data.get('validated_at', 'Unknown')
        await ctx.send(f"📥 **Exporting validated issues to CSV...**\nData from: `{validated_at}`")
        
        # Build contact lookup from master CSV
        contact_lookup: dict = {}  # member_id -> {email, discord, phone}
        master_file = self.storage.get_file("master")
        if master_file:
            master_data = self.storage.read_file(master_file)
            master_text = master_data.decode('utf-8-sig')
            
            lines = master_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                master_text = "\n".join(lines[header_row_idx:])
            
            try:
                m_dialect = csv.Sniffer().sniff(master_text[:4096], delimiters=',\t;|')
            except csv.Error:
                m_dialect = 'excel'
            m_reader = csv.DictReader(io.StringIO(master_text), dialect=m_dialect)
            m_rows = list(m_reader)
            if m_rows:
                m_headers = list(m_rows[0].keys())
                m_member_col = next((h for h in m_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                m_email_col = next((h for h in m_headers if 'email' in h.lower() and 'secondary' not in h.lower()), None)
                m_discord_col = next((h for h in m_headers if 'discord' in h.lower()), None)
                
                for row in m_rows:
                    mid = str(row.get(m_member_col, "")).strip() if m_member_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id']:
                        contact_lookup[mid] = {
                            'email': str(row.get(m_email_col, "")).strip() if m_email_col else "",
                            'discord': str(row.get(m_discord_col, "")).strip() if m_discord_col else "",
                            'phone': ""
                        }
        
        # Add phone numbers from app CSV
        app_file = self.storage.get_file("app")
        if app_file:
            app_data = self.storage.read_file(app_file)
            app_text = app_data.decode('utf-8-sig')
            
            lines = app_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                app_text = "\n".join(lines[header_row_idx:])
            
            try:
                a_dialect = csv.Sniffer().sniff(app_text[:4096], delimiters=',\t;|')
            except csv.Error:
                a_dialect = 'excel'
            a_reader = csv.DictReader(io.StringIO(app_text), dialect=a_dialect)
            a_rows = list(a_reader)
            if a_rows:
                a_headers = list(a_rows[0].keys())
                a_member_col = next((h for h in a_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                a_phone_col = next((h for h in a_headers if 'phone' in h.lower()), None)
                
                for row in a_rows:
                    mid = str(row.get(a_member_col, "")).strip() if a_member_col else ""
                    phone = str(row.get(a_phone_col, "")).strip() if a_phone_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id'] and phone:
                        if mid in contact_lookup:
                            contact_lookup[mid]['phone'] = phone
                        else:
                            contact_lookup[mid] = {'email': '', 'discord': '', 'phone': phone}
        
        # Extract all categories from validated data
        students_with_valid_issue = data.get('students_with_valid_issue', {})
        students_with_invalid_issue = data.get('students_with_invalid_issue', {})
        readme_url_in_issue_field = data.get('readme_url_in_issue_field', {})
        issue_url_in_readme_link = data.get('issue_url_in_readme_link', {})
        issues_found = data.get('issues_found', {})
        no_issue_in_readme = data.get('no_issue_in_readme', {})
        readme_inaccessible = data.get('readme_inaccessible', {})
        readme_timeout = data.get('readme_timeout', {})
        
        # Build list of all students with their info
        all_students: list = []
        
        # Category 1: Students with valid issue from typeform
        for mid, info in students_with_valid_issue.items():
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Has Issue',
                'issue_url': info['issue_url'],
                'source': 'Typeform',
                'notes': ''
            })
        
        # Category 2: Issue URL in README link field (wrong field but valid)
        for mid, info in issue_url_in_readme_link.items():
            # Skip if already added
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Has Issue (Wrong Field)',
                'issue_url': info['issue_url'],
                'source': 'README Link Field',
                'notes': 'Issue URL was placed in README link field'
            })
        
        # Category 3: Issues found in README
        for mid, info in issues_found.items():
            # Skip if already added
            if any(s['member_id'] == mid for s in all_students):
                continue
            source = info.get('source', 'readme')
            source_display = {
                'readme': 'README',
                'number_reference': 'README (#number)',
                'project_shorthand': 'README (project#number)'
            }.get(source, 'README')
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Has Issue',
                'issue_url': info['issue_url'],
                'source': source_display,
                'notes': ''
            })
        
        # Category 4: README URL in issue field (wrong field)
        for mid, info in readme_url_in_issue_field.items():
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Invalid (Wrong Field)',
                'issue_url': info.get('readme_url', ''),
                'source': 'Issue URL Field',
                'notes': 'README/repo URL was placed in issue URL field'
            })
        
        # Category 5: Invalid issue URLs
        for mid, info in students_with_invalid_issue.items():
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Invalid Issue URL',
                'issue_url': info['issue_url'],
                'source': 'Typeform',
                'notes': 'URL does not match expected GitLab issue format'
            })
        
        # Category 6: No issue in README
        for mid, info in no_issue_in_readme.items():
            # Skip if already added (has valid issue elsewhere)
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'No Issue Found',
                'issue_url': '',
                'source': 'README',
                'notes': f"README: {info.get('readme_link', 'N/A')}"
            })
        
        # Category 7: Inaccessible READMEs
        for mid, info in readme_inaccessible.items():
            # Skip if already added
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'README Inaccessible',
                'issue_url': '',
                'source': 'N/A',
                'notes': f"Error: {info.get('error', 'Unknown')} | README: {info.get('readme_link', 'N/A')}"
            })
        
        # Category 8: Timed out READMEs
        for mid, info in readme_timeout.items():
            # Skip if already added
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'README Timeout',
                'issue_url': '',
                'source': 'N/A',
                'notes': f"Timed out after {info.get('attempts', 'N/A')} attempts | README: {info.get('readme_link', 'N/A')}"
            })
        
        # Build CSV
        output = StringIO()
        writer = csv.writer(output)
        
        # Header row
        writer.writerow(['Name', 'Member ID', 'Discord', 'Email', 'Phone', 'Status', 'Issue URL', 'Source', 'Notes'])
        
        # Sort by name
        all_students.sort(key=lambda x: x['name'].lower())
        
        # Data rows
        for student in all_students:
            contact = contact_lookup.get(student['member_id'], {})
            writer.writerow([
                student['name'],
                student['member_id'],
                contact.get('discord', ''),
                contact.get('email', ''),
                contact.get('phone', ''),
                student['status'],
                student['issue_url'],
                student['source'],
                student['notes']
            ])
        
        # Count statistics
        has_issue_count = sum(1 for s in all_students if s['status'] in ['Has Issue', 'Has Issue (Wrong Field)'])
        no_issue_count = sum(1 for s in all_students if s['status'] == 'No Issue Found')
        needs_attention_count = sum(1 for s in all_students if s['status'] not in ['Has Issue', 'No Issue Found'])
        
        # Add summary footer
        writer.writerow([])
        writer.writerow(['--- SUMMARY ---'])
        writer.writerow([f'Total Students: {len(all_students)}'])
        writer.writerow([f'With Issues: {has_issue_count}'])
        writer.writerow([f'Without Issues: {no_issue_count}'])
        writer.writerow([f'Needs Attention: {needs_attention_count}'])
        writer.writerow([f'Data validated at: {validated_at}'])
        
        # Create file for Discord
        csv_content = output.getvalue().encode('utf-8')
        filename = f"issues_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Send summary and file
        summary = (
            f"✅ **Export Complete**\n\n"
            f"📊 **Summary:**\n"
            f"• Total Students: {len(all_students)}\n"
            f"• With Issues: {has_issue_count}\n"
            f"• Without Issues: {no_issue_count}\n"
            f"• Needs Attention: {needs_attention_count}\n"
        )
        
        await ctx.send(summary, file=discord.File(io.BytesIO(csv_content), filename=filename))
    
    # ==================== Merge Request Tracking Commands ====================
    
    @commands.command(name='no_mr')
    async def no_mr(self, ctx: commands.Context, action: str = None):
        """Show MR status from validated data or run validation.
        
        Usage: 
            !tracker no_mr           - Show MR status from validated data
            !tracker no_mr validate  - Crawl READMEs to find/validate MR URLs
        
        The default command requires running 'validate' first to generate data.
        """
        if action and action.lower() == 'validate':
            await self._validate_no_mrs(ctx)
            return
        
        # Default: show from validated JSON
        await self._show_validated_mrs(ctx)
    
    async def _show_validated_mrs(self, ctx: commands.Context):
        """Show MR status from the validated MRs JSON file."""
        import json
        import os
        
        results_file = os.path.join('data', 'uploads', '_validated_mrs.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated MR data found.**\n\n"
                "Run `!tracker no_mr validate` first to crawl READMEs and validate MR URLs."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated MRs file:** {str(e)}")
            return
        
        validated_at = data.get('validated_at', 'Unknown')
        
        # Show info about cached data
        await ctx.send(
            f"📋 **Validated MR Data**\n"
            f"Last validated: `{validated_at}`\n\n"
            f"Showing results from cached data. Run `!tracker no_mr validate` to refresh.\n"
            f"─────────────────────────────"
        )
        
        # Extract data
        students_with_valid_mr = data.get('students_with_valid_mr', {})
        students_with_invalid_mr = data.get('students_with_invalid_mr', {})
        readme_url_in_mr_field = data.get('readme_url_in_mr_field', {})
        mr_url_in_readme_link = data.get('mr_url_in_readme_link', {})
        mrs_found = data.get('mrs_found', {})
        no_mr_in_readme = data.get('no_mr_in_readme', {})
        readme_inaccessible = data.get('readme_inaccessible', {})
        readme_timeout = data.get('readme_timeout', {})
        mr_author_mismatch = data.get('mr_author_mismatch', {})
        
        # Count totals
        total_with_mrs = len(students_with_valid_mr) + len(mr_url_in_readme_link) + len(mrs_found)
        
        # Students WITHOUT MRs
        students_without_mrs: dict = {}
        for mid, info in no_mr_in_readme.items():
            if mid not in students_with_valid_mr and mid not in mr_url_in_readme_link and mid not in mrs_found:
                students_without_mrs[mid] = {
                    'name': info['name'],
                    'readme_link': info.get('readme_link', '')
                }
        
        # Build report
        report = ["📊 **MR Status Summary**\n"]
        report.append(f"**Students with MRs:** {total_with_mrs}")
        report.append(f"**Students without MRs:** {len(students_without_mrs)}")
        attention_count = len(students_with_invalid_mr) + len(readme_url_in_mr_field) + len(mr_author_mismatch) + len(readme_inaccessible) + len(readme_timeout)
        report.append(f"**Needs Attention:** {attention_count}\n")
        
        # Section 1: Students with explicit MR URL (from typeform)
        if students_with_valid_mr:
            report.append("**📝 Students With Explicit MR URL (Typeform):**")
            for mid, info in sorted(students_with_valid_mr.items(), key=lambda x: x[1]['name'].lower()):
                merged_tag = " **(MERGED)**" if info.get('is_merged') else ""
                report.append(f"• **{info['name']}** (`{mid}`){merged_tag}")
                report.append(f"  └─ MR: <{info['mr_url']}>")
                expected = info.get('expected_author', '')
                actual = info.get('actual_author', '')
                if expected and actual:
                    match_icon = "✅" if info.get('author_match') else "⚠️"
                    report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
                    if info.get('alternate_mr'):
                        report.append(f"  └─ 🔄 Alt MR in README: <{info['alternate_mr']}>")
            report.append("")
        
        # Section 2: MR URLs in README field (wrong field)
        if mr_url_in_readme_link:
            report.append("**⚠️ MR URL in README Field (wrong field!):**")
            for mid, info in sorted(mr_url_in_readme_link.items(), key=lambda x: x[1]['name'].lower()):
                merged_tag = " **(MERGED)**" if info.get('is_merged') else ""
                report.append(f"• **{info['name']}** (`{mid}`){merged_tag}")
                report.append(f"  └─ MR: <{info['mr_url']}>")
                expected = info.get('expected_author', '')
                actual = info.get('actual_author', '')
                if expected and actual:
                    match_icon = "✅" if info.get('author_match') else "⚠️"
                    report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
            report.append("")
        
        # Section 3: MRs found in README (crawled)
        if mrs_found:
            report.append("**🔗 MRs Found in README (Crawled):**")
            for mid, info in sorted(mrs_found.items(), key=lambda x: x[1]['name'].lower()):
                note = info.get('note', '')
                merged_tag = " **(MERGED)**" if info.get('is_merged') else ""
                report.append(f"• **{info['name']}** (`{mid}`){merged_tag}")
                if note:
                    report.append(f"  └─ MR: <{info['mr_url']}> *(corrected)*")
                else:
                    report.append(f"  └─ MR: <{info['mr_url']}>")
                expected = info.get('expected_author', '')
                actual = info.get('actual_author', '')
                if expected and actual:
                    match_icon = "✅" if info.get('author_match') else "⚠️"
                    report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
            report.append("")
        
        # Section 4: Students WITHOUT MRs
        if students_without_mrs:
            report.append("**❌ Students Without MR URLs:**")
            for mid, info in sorted(students_without_mrs.items(), key=lambda x: x[1]['name'].lower()):
                report.append(f"• **{info['name']}** (`{mid}`)")
                if info.get('readme_link'):
                    report.append(f"  └─ README: <{info['readme_link']}>")
            report.append("")
        
        # Send main report in chunks
        full_report = "\n".join(report)
        if len(full_report) <= 2000:
            await ctx.send(full_report)
        else:
            chunks = []
            current = ""
            for line in report:
                if len(current) + len(line) + 1 > 1900:
                    chunks.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                chunks.append(current)
            for chunk in chunks:
                await ctx.send(chunk)
        
        # Section 3: Needs Attention
        needs_attention = []
        
        # README URLs put in mr_url field
        if readme_url_in_mr_field:
            needs_attention.append("**⚠️ README URL in MR Field (wrong field!):**")
            needs_attention.append("*(These students put a README/repo link in the MR URL field)*")
            for mid, info in sorted(readme_url_in_mr_field.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ README: <{info.get('readme_url', 'N/A')}>")
            needs_attention.append("")
        
        # Invalid MR URLs
        if students_with_invalid_mr:
            needs_attention.append("**⚠️ Invalid MR URLs:**")
            needs_attention.append("*(Expected: gitlab.com/.../merge_requests/{num})*")
            for mid, info in sorted(students_with_invalid_mr.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info['mr_url']}>")
            needs_attention.append("")
        
        # MR author mismatch
        if mr_author_mismatch:
            needs_attention.append("**⚠️ MR Author Mismatch (not student's MR!):**")
            needs_attention.append("*(The MR was authored by someone else, no student MR found in README)*")
            for mid, info in sorted(mr_author_mismatch.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`) - Expected: `{info['expected_author']}`")
                all_mrs_details = info.get('all_mrs_details', [])
                if all_mrs_details:
                    for i, mr_detail in enumerate(all_mrs_details, 1):
                        needs_attention.append(f"  └─ MR {i}: <{mr_detail['url']}> by `{mr_detail['author']}`")
                else:
                    needs_attention.append(f"  └─ MR: <{info['mr_url']}> by `{info['actual_author']}`")
            needs_attention.append("")
        
        # Inaccessible READMEs
        if readme_inaccessible:
            needs_attention.append("**⚠️ Inaccessible READMEs:**")
            for mid, info in sorted(readme_inaccessible.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info.get('readme_link', 'N/A')}>")
                needs_attention.append(f"  └─ Error: {info.get('error', 'Unknown')}")
            needs_attention.append("")
        
        # Timed out READMEs
        if readme_timeout:
            needs_attention.append("**⏱️ Timed Out (retry later):**")
            for mid, info in sorted(readme_timeout.items(), key=lambda x: x[1]['name'].lower()):
                needs_attention.append(f"• **{info['name']}** (`{mid}`)")
                needs_attention.append(f"  └─ <{info.get('readme_link', 'N/A')}>")
            needs_attention.append("")
        
        if needs_attention:
            await ctx.send("─────────────────────────────")
            attention_text = "\n".join(needs_attention)
            if len(attention_text) <= 2000:
                await ctx.send(attention_text)
            else:
                chunks = []
                current = ""
                for line in needs_attention:
                    if len(current) + len(line) + 1 > 1900:
                        chunks.append(current)
                        current = line
                    else:
                        current += "\n" + line if current else line
                if current:
                    chunks.append(current)
                for chunk in chunks:
                    await ctx.send(chunk)
    
    async def _validate_no_mrs(self, ctx: commands.Context):
        """Crawl READMEs to find MR URLs for students without one.
        
        For each student without an mr_url, fetches their README and extracts
        the latest MR URL found. Results are persisted to a JSON file.
        """
        import csv
        import json
        import re
        import os
        from services.tracker_processor import _preprocess_typeform_csv
        from services.gitlab_service import GitLabService
        
        typeform_file = self.storage.get_file("typeform")
        if not typeform_file:
            await ctx.send("❌ **No typeform data uploaded.** Upload typeform CSV first with `!tracker upload typeform`")
            return
        
        typeform_data = self.storage.read_file(typeform_file)
        
        # Load master CSV to get GitLab usernames for author validation
        master_file = self.storage.get_file("master")
        gitlab_lookup: dict = {}  # member_id -> gitlab_username
        
        if master_file:
            master_data = self.storage.read_file(master_file)
            master_text = master_data.decode('utf-8-sig')
            
            # Find header row
            lines = master_text.splitlines()
            header_idx = 0
            for i, line in enumerate(lines):
                if 'member id' in line.lower():
                    header_idx = i
                    break
            
            master_csv_text = '\n'.join(lines[header_idx:])
            try:
                dialect = csv.Sniffer().sniff(master_csv_text[:4096], delimiters=',\t;|')
            except csv.Error:
                dialect = 'excel'
            
            master_reader = csv.DictReader(io.StringIO(master_csv_text), dialect=dialect)
            master_rows = list(master_reader)
            
            if master_rows:
                master_headers = list(master_rows[0].keys())
                
                # Find columns
                master_member_id_col = next((h for h in master_headers if 'member id' in h.lower() or h.lower() == 'member_id'), None)
                gitlab_col = next((h for h in master_headers if 'gitlab' in h.lower() or 'github' in h.lower()), None)
                
                if master_member_id_col and gitlab_col:
                    for row in master_rows:
                        mid = str(row.get(master_member_id_col, "")).strip()
                        gitlab_username = str(row.get(gitlab_col, "")).strip().lower()
                        # Strip @ prefix if present (master CSV may have @username format)
                        if gitlab_username.startswith('@'):
                            gitlab_username = gitlab_username[1:]
                        if mid and gitlab_username and gitlab_username not in ['', 'n/a', '#n/a']:
                            gitlab_lookup[mid] = gitlab_username
                    
                    print(f"[MR Validate] Built GitLab lookup with {len(gitlab_lookup)} entries")
        
        await ctx.send("🔍 **Validating MR URLs - crawling READMEs for missing MRs...**")
        
        try:
            # Parse typeform CSV
            text = typeform_data.decode('utf-8-sig')
            text = _preprocess_typeform_csv(text)
            
            try:
                dialect = csv.Sniffer().sniff(text[:4096], delimiters=',\t;|')
            except csv.Error:
                dialect = 'excel'
            
            reader = csv.DictReader(io.StringIO(text), dialect=dialect)
            rows = list(reader)
            
            if not rows:
                await ctx.send("❌ **Typeform CSV is empty.**")
                return
            
            # Find relevant columns
            headers = list(rows[0].keys())
            
            member_id_col = next((h for h in headers if "member id" in h.lower() or h.lower() == "member_id"), None)
            name_col = next((h for h in headers if "name" in h.lower() and "discord" not in h.lower() and "username" not in h.lower()), None)
            mr_col = next((h for h in headers if "merge request" in h.lower() or "mr url" in h.lower() or "mr_url" in h.lower() or "direct link to your merge request" in h.lower()), None)
            readme_col = next((h for h in headers if "readme" in h.lower() and "link" in h.lower()), None)
            
            if not member_id_col:
                await ctx.send(f"❌ **Could not find Member ID column.**")
                return
            if not readme_col:
                await ctx.send(f"❌ **Could not find README link column.**")
                return
            if not mr_col:
                await ctx.send(f"❌ **Could not find MR URL column.**\n\nSearched for columns containing 'merge request', 'mr url', 'mr_url'\n\nAvailable columns: {', '.join(headers[:15])}...")
                return
            
            # Valid MR URL pattern
            VALID_MR_URL_PATTERN = re.compile(
                r'^https?://gitlab\.com/[^/]+(?:/[^/]+)*/-/merge_requests/\d+(?:#[a-zA-Z0-9_-]+)?(?:\?[^#]*)?$',
                re.IGNORECASE
            )
            
            # README/repo URL pattern
            README_URL_PATTERN = re.compile(
                r'^https?://gitlab\.com/[^/]+/[^/]+(?:/-/(?:blob|tree)/|/?(?:\?|#|$))',
                re.IGNORECASE
            )
            
            # Collect students with and without mr_url
            students_no_mr: dict = {}
            students_with_valid_mr: dict = {}
            students_with_invalid_mr: dict = {}
            readme_url_in_mr_field: dict = {}
            mr_url_in_readme_link: dict = {}
            mr_author_mismatch: dict = {}  # MR author doesn't match student's GitLab username
            
            # Pattern for extracting repo path and IID from MR URL
            MR_EXTRACT_PATTERN = re.compile(
                r'https?://gitlab\.com/([^/]+(?:/[^/]+)*)/-/merge_requests/(\d+)',
                re.IGNORECASE
            )
            
            for row in rows:
                member_id = str(row.get(member_id_col, "")).strip()
                if not member_id or member_id.lower() in ['#n/a', 'n/a', '']:
                    continue
                
                name = str(row.get(name_col, "")).strip() if name_col else "Unknown"
                mr_url = str(row.get(mr_col, "")).strip() if mr_col else ""
                readme_link = str(row.get(readme_col, "")).strip() if readme_col else ""
                
                # Track students who already have mr_url
                if mr_url and mr_url.lower() not in ['', 'n/a', '#n/a', 'none']:
                    if VALID_MR_URL_PATTERN.match(mr_url):
                        students_with_valid_mr[member_id] = {'name': name, 'mr_url': mr_url}
                    elif README_URL_PATTERN.match(mr_url):
                        readme_url_in_mr_field[member_id] = {
                            'name': name,
                            'readme_url': mr_url,
                            'note': 'README/repo URL was incorrectly placed in MR URL field'
                        }
                    else:
                        students_with_invalid_mr[member_id] = {'name': name, 'mr_url': mr_url}
                    continue
                
                # Check if readme_link is actually an MR URL
                if readme_link and VALID_MR_URL_PATTERN.match(readme_link):
                    mr_url_in_readme_link[member_id] = {
                        'name': name,
                        'mr_url': readme_link,
                        'readme_link': readme_link,
                        'note': 'MR URL was incorrectly placed in README link field'
                    }
                    continue
                
                # Collect ALL unique readme URLs for each student (not just the latest)
                if readme_link:
                    if member_id not in students_no_mr:
                        students_no_mr[member_id] = {'name': name, 'readme_links': set()}
                    students_no_mr[member_id]['readme_links'].add(readme_link)
            
            if not students_no_mr:
                await ctx.send("✅ **All students have MR URLs!** Nothing to validate.")
                return
            
            await ctx.send(f"📊 Found **{len(students_no_mr)}** students without MR URL. Crawling their READMEs...")
            
            # Initialize GitLab service
            gitlab_service = GitLabService()
            
            # MR URL pattern for extracting from README
            MR_PATTERN = re.compile(
                r'https?://gitlab\.com/[^/]+(?:/[^/]+)*/-/merge_requests/\d+(?:#[a-zA-Z0-9_-]+)?',
                re.IGNORECASE
            )
            
            # Results tracking
            mrs_found: dict = {}
            readme_inaccessible: dict = {}
            readme_timeout: dict = {}
            no_mr_in_readme: dict = {}
            
            processed = 0
            total = len(students_no_mr)
            
            # Count total README URLs to crawl
            total_readme_urls = sum(len(info['readme_links']) for info in students_no_mr.values())
            
            API_DELAY = 1.0
            MAX_RETRIES = 3
            
            import socket
            import ssl
            import time
            
            for member_id, info in students_no_mr.items():
                processed += 1
                name = info['name']
                readme_links = list(info['readme_links'])  # Convert set to list
                
                if processed % 10 == 0:
                    await ctx.send(f"⏳ Progress: {processed}/{total} students checked...")
                
                # Track all MRs found across all README URLs for this student
                student_all_mrs: list = []
                student_readme_links_crawled: list = []
                student_errors: list = []
                repos_crawled: set = set()  # Track repos already fully crawled
                
                for readme_link in readme_links:
                    repo_path, platform = gitlab_service.extract_repo_from_readme_link(readme_link)
                    
                    if not repo_path:
                        student_errors.append(f"{readme_link}: Could not extract repo path")
                        continue
                    
                    file_path = gitlab_service.extract_file_path_from_url(readme_link)
                    is_tree = gitlab_service.is_tree_url(readme_link)
                    
                    readme_content = None
                    all_readme_contents = []
                    last_error = None
                    
                    for attempt in range(MAX_RETRIES):
                        try:
                            if attempt > 0 or processed > 1:
                                await asyncio.sleep(API_DELAY)
                            
                            if is_tree or not file_path:
                                # Tree URL or repo root: fetch ALL README files in the repo
                                # Only do this once per repo
                                if repo_path not in repos_crawled:
                                    all_readme_contents = gitlab_service.fetch_all_readme_contents(repo_path)
                                    repos_crawled.add(repo_path)
                                    if all_readme_contents:
                                        readme_content = "\n\n".join([content for _, content in all_readme_contents])
                                        student_readme_links_crawled.append(f"{readme_link} (all READMEs)")
                                        break
                                else:
                                    # Already crawled this repo
                                    break
                            else:
                                # Specific file URL
                                readme_content = gitlab_service.fetch_file_content(repo_path, file_path)
                                if readme_content:
                                    student_readme_links_crawled.append(readme_link)
                            
                            if not readme_content and repo_path not in repos_crawled:
                                # Fall back to fetching all READMEs
                                all_readme_contents = gitlab_service.fetch_all_readme_contents(repo_path)
                                repos_crawled.add(repo_path)
                                if all_readme_contents:
                                    readme_content = "\n\n".join([content for _, content in all_readme_contents])
                                    student_readme_links_crawled.append(f"{readme_link} (all READMEs)")
                            
                            if readme_content:
                                break
                                
                        except (socket.timeout, ssl.SSLError, TimeoutError) as e:
                            last_error = f"Timeout (attempt {attempt + 1})"
                            continue
                        except Exception as e:
                            last_error = str(e)
                            break
                    
                    if readme_content:
                        # Find MR URLs in this README content
                        mrs_in_readme = MR_PATTERN.findall(readme_content)
                        for mr in mrs_in_readme:
                            if mr not in student_all_mrs:
                                student_all_mrs.append(mr)
                    elif last_error:
                        student_errors.append(f"{readme_link}: {last_error}")
                
                # After crawling all README URLs for this student
                if student_all_mrs:
                    latest_mr = student_all_mrs[-1]
                    mrs_found[member_id] = {
                        'name': name,
                        'readme_link': readme_links[0] if len(readme_links) == 1 else ', '.join(readme_links[:3]),
                        'readme_links_crawled': student_readme_links_crawled,
                        'mr_url': latest_mr,
                        'all_mrs_found': student_all_mrs,
                        'source': 'readme'
                    }
                elif student_errors and all("timeout" in e.lower() for e in student_errors):
                    readme_timeout[member_id] = {
                        'name': name,
                        'readme_link': readme_links[0] if readme_links else '',
                        'readme_links': list(readme_links),
                        'attempts': MAX_RETRIES
                    }
                elif student_errors:
                    readme_inaccessible[member_id] = {
                        'name': name,
                        'readme_link': readme_links[0] if readme_links else '',
                        'readme_links': list(readme_links),
                        'error': '; '.join(student_errors[:3])
                    }
                else:
                    no_mr_in_readme[member_id] = {
                        'name': name,
                        'readme_link': readme_links[0] if readme_links else '',
                        'readme_links': list(readme_links)
                    }
            
            # Validate MR authors if we have GitLab username lookup
            if gitlab_lookup:
                # Collect all MRs to verify: (member_id, mr_url, name, source, all_mrs)
                mrs_to_verify = []
                
                # From students_with_valid_mr (typeform)
                for mid, info in list(students_with_valid_mr.items()):
                    # Check if we also have README MRs for this student (from students_no_mr crawl)
                    all_mrs = mrs_found.get(mid, {}).get('all_mrs_found', [])
                    mrs_to_verify.append((mid, info['mr_url'], info['name'], 'typeform', all_mrs))
                
                # From mrs_found (README)
                for mid, info in list(mrs_found.items()):
                    if mid not in students_with_valid_mr:  # Don't duplicate
                        mrs_to_verify.append((mid, info['mr_url'], info['name'], 'readme', info.get('all_mrs_found', [])))
                
                # From mr_url_in_readme_link (MR URL in README field - still valid MR)
                for mid, info in list(mr_url_in_readme_link.items()):
                    mrs_to_verify.append((mid, info['mr_url'], info['name'], 'readme_field', []))
                
                await ctx.send(f"🔍 **Validating MR authors for {len(mrs_to_verify)} MRs...**")
                
                verified_count = 0
                mismatch_count = 0
                found_alternate_count = 0
                total_to_verify = len(mrs_to_verify)
                
                for idx, (mid, mr_url, name, source, all_mrs) in enumerate(mrs_to_verify, 1):
                    if idx % 20 == 0:
                        await ctx.send(f"⏳ Progress: {idx}/{total_to_verify} MRs checked...")
                    
                    expected_gitlab = gitlab_lookup.get(mid, "")
                    
                    if not expected_gitlab:
                        continue  # No GitLab username to compare against
                    
                    # Extract repo path and MR IID from URL
                    mr_match = MR_EXTRACT_PATTERN.search(mr_url)
                    if not mr_match:
                        continue
                    
                    repo_path = mr_match.group(1)
                    mr_iid = mr_match.group(2)
                    
                    # Add delay to avoid rate limiting
                    await asyncio.sleep(0.5)
                    
                    try:
                        mr_data = gitlab_service.verify_merge_request(repo_path, mr_iid)
                        
                        if mr_data.get('exists'):
                            actual_author = mr_data.get('author', '').lower()
                            mr_state = mr_data.get('state', '')
                            is_merged = mr_state == 'merged'
                            verified_count += 1
                            
                            # Store validation result for all sources
                            validation_info = {
                                'expected_author': expected_gitlab,
                                'actual_author': actual_author,
                                'author_match': actual_author == expected_gitlab,
                                'mr_state': mr_state,
                                'is_merged': is_merged,
                                'merged_at': mr_data.get('merged_at', '')
                            }
                            
                            if actual_author and actual_author != expected_gitlab:
                                # Primary MR has wrong author - scan all_mrs_found backwards to find one they authored
                                found_student_mr = None
                                all_mrs_details = []  # Track all MRs checked with their authors
                                
                                # Add the primary MR to the list
                                all_mrs_details.append({
                                    'url': mr_url,
                                    'author': actual_author
                                })
                                
                                if all_mrs:
                                    # Scan backwards (latest first), skip the one we already checked
                                    checked_urls = {mr_url.lower().rstrip('/')}
                                    for alt_mr_url in reversed(all_mrs):
                                        alt_normalized = alt_mr_url.lower().rstrip('/')
                                        if alt_normalized in checked_urls:
                                            continue
                                        checked_urls.add(alt_normalized)
                                        
                                        alt_match = MR_EXTRACT_PATTERN.search(alt_mr_url)
                                        if not alt_match:
                                            continue
                                        
                                        alt_repo = alt_match.group(1)
                                        alt_iid = alt_match.group(2)
                                        
                                        await asyncio.sleep(0.3)
                                        
                                        try:
                                            alt_mr_data = gitlab_service.verify_merge_request(alt_repo, alt_iid)
                                            if alt_mr_data.get('exists'):
                                                alt_author = alt_mr_data.get('author', '').lower()
                                                alt_state = alt_mr_data.get('state', '')
                                                alt_merged = alt_state == 'merged'
                                                all_mrs_details.append({
                                                    'url': alt_mr_url,
                                                    'author': alt_author,
                                                    'mr_state': alt_state,
                                                    'is_merged': alt_merged
                                                })
                                                if alt_author == expected_gitlab:
                                                    found_student_mr = {
                                                        'url': alt_mr_url,
                                                        'title': alt_mr_data.get('title', ''),
                                                        'author': alt_author,
                                                        'mr_state': alt_state,
                                                        'is_merged': alt_merged,
                                                        'merged_at': alt_mr_data.get('merged_at', '')
                                                    }
                                                    break
                                        except Exception:
                                            continue
                                
                                if found_student_mr:
                                    # Found an MR the student actually authored - update their record
                                    found_alternate_count += 1
                                    print(f"[MR Validate] Found alternate MR for {name}: {found_student_mr['url']}")
                                    
                                    if source == 'typeform' and mid in students_with_valid_mr:
                                        # Keep in valid but note we found a different one in README
                                        students_with_valid_mr[mid]['alternate_mr'] = found_student_mr['url']
                                        students_with_valid_mr[mid]['note'] = f"Typeform MR by {actual_author}, but found student's MR in README"
                                        students_with_valid_mr[mid].update(validation_info)
                                    elif source == 'readme' and mid in mrs_found:
                                        # Update to the correct MR
                                        mrs_found[mid]['mr_url'] = found_student_mr['url']
                                        mrs_found[mid]['original_mr'] = mr_url
                                        mrs_found[mid]['note'] = f"Original MR by {actual_author}, updated to student's MR"
                                        mrs_found[mid]['expected_author'] = expected_gitlab
                                        mrs_found[mid]['actual_author'] = found_student_mr['author']
                                        mrs_found[mid]['author_match'] = True
                                        mrs_found[mid]['mr_state'] = found_student_mr.get('mr_state', '')
                                        mrs_found[mid]['is_merged'] = found_student_mr.get('is_merged', False)
                                        mrs_found[mid]['merged_at'] = found_student_mr.get('merged_at', '')
                                    elif source == 'readme_field' and mid in mr_url_in_readme_link:
                                        mr_url_in_readme_link[mid]['alternate_mr'] = found_student_mr['url']
                                        mr_url_in_readme_link[mid].update(validation_info)
                                else:
                                    # No alternate found - mark as mismatch
                                    mismatch_count += 1
                                    mr_author_mismatch[mid] = {
                                        'name': name,
                                        'mr_url': mr_url,
                                        'expected_author': expected_gitlab,
                                        'actual_author': actual_author,
                                        'mr_title': mr_data.get('title', ''),
                                        'source': source,
                                        'all_mrs_checked': len(all_mrs_details),
                                        'all_mrs_details': all_mrs_details  # Store all MRs with authors
                                    }
                                    
                                    # Remove from original category
                                    if source == 'typeform' and mid in students_with_valid_mr:
                                        del students_with_valid_mr[mid]
                                    elif source == 'readme' and mid in mrs_found:
                                        del mrs_found[mid]
                                    elif source == 'readme_field' and mid in mr_url_in_readme_link:
                                        del mr_url_in_readme_link[mid]
                            else:
                                # Author matches - store validation info
                                if source == 'typeform' and mid in students_with_valid_mr:
                                    students_with_valid_mr[mid].update(validation_info)
                                elif source == 'readme' and mid in mrs_found:
                                    mrs_found[mid].update(validation_info)
                                elif source == 'readme_field' and mid in mr_url_in_readme_link:
                                    mr_url_in_readme_link[mid].update(validation_info)
                    except Exception as e:
                        print(f"[MR Validate] Error verifying MR for {name}: {e}")
                
                print(f"[MR Validate] Verified {verified_count} MRs, found {mismatch_count} author mismatches, {found_alternate_count} corrected via README scan")
            else:
                found_alternate_count = 0
            
            # Save results to JSON
            from datetime import datetime
            results = {
                'validated_at': datetime.now().isoformat(),
                'students_with_valid_mr': students_with_valid_mr,
                'students_with_invalid_mr': students_with_invalid_mr,
                'readme_url_in_mr_field': readme_url_in_mr_field,
                'mr_url_in_readme_link': mr_url_in_readme_link,
                'mrs_found': mrs_found,
                'no_mr_in_readme': no_mr_in_readme,
                'readme_inaccessible': readme_inaccessible,
                'readme_timeout': readme_timeout,
                'mr_author_mismatch': mr_author_mismatch
            }
            
            results_file = os.path.join('data', 'uploads', '_validated_mrs.json')
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            # Build report
            report = ["✅ **README Validation Complete (MRs)**\n"]
            report.append(f"📊 **Summary:**")
            report.append(f"• Valid MR URL: **{len(students_with_valid_mr)}**")
            report.append(f"• ⚠️ Invalid MR URL: **{len(students_with_invalid_mr)}**")
            report.append(f"• ⚠️ README URL in MR field: **{len(readme_url_in_mr_field)}** (wrong field)")
            report.append(f"• ⚠️ MR URL in README field: **{len(mr_url_in_readme_link)}** (wrong field)")
            report.append(f"• ⚠️ MR author mismatch: **{len(mr_author_mismatch)}** (not student's MR)")
            if found_alternate_count > 0:
                report.append(f"• ✅ Auto-corrected: **{found_alternate_count}** (found student's MR in README)")
            report.append(f"• MRs found in README: **{len(mrs_found)}**")
            report.append(f"• No MR in README: **{len(no_mr_in_readme)}**")
            report.append(f"• README inaccessible: **{len(readme_inaccessible)}**")
            report.append(f"• Timed out (can retry): **{len(readme_timeout)}**\n")
            
            # Students with INVALID MR URLs
            if students_with_invalid_mr:
                report.append("**❌ Students With INVALID MR URL:**")
                report.append("*(Expected format: gitlab.com/.../merge_requests/{num})*")
                for mid, data in sorted(students_with_invalid_mr.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['mr_url']}>")
                report.append("")
            
            # README URLs in MR field
            if readme_url_in_mr_field:
                report.append("**⚠️ README URL in MR Field (wrong field!):**")
                for mid, data in sorted(readme_url_in_mr_field.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ <{data['readme_url']}>")
                report.append("")
            
            # Students with explicit MR URL (from typeform) - show validation status
            if students_with_valid_mr:
                report.append("**📝 Students With Explicit MR URL (Typeform):**")
                for mid, data in sorted(students_with_valid_mr.items(), key=lambda x: x[1]['name'].lower()):
                    merged_tag = " **(MERGED)**" if data.get('is_merged') else ""
                    report.append(f"• **{data['name']}** (`{mid}`){merged_tag}")
                    report.append(f"  └─ MR: <{data['mr_url']}>")
                    expected = data.get('expected_author', '')
                    actual = data.get('actual_author', '')
                    if expected and actual:
                        match_icon = "✅" if data.get('author_match') else "⚠️"
                        report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
                        if data.get('alternate_mr'):
                            report.append(f"  └─ 🔄 Alt MR in README: <{data['alternate_mr']}>")
                    elif not expected:
                        report.append(f"  └─ ℹ️ No GitLab username in master CSV")
                report.append("")
            
            # MR URLs in README field (wrong field) - show validation status
            if mr_url_in_readme_link:
                report.append("**⚠️ MR URL in README Field (wrong field!):**")
                for mid, data in sorted(mr_url_in_readme_link.items(), key=lambda x: x[1]['name'].lower()):
                    merged_tag = " **(MERGED)**" if data.get('is_merged') else ""
                    report.append(f"• **{data['name']}** (`{mid}`){merged_tag}")
                    report.append(f"  └─ MR: <{data['mr_url']}>")
                    expected = data.get('expected_author', '')
                    actual = data.get('actual_author', '')
                    if expected and actual:
                        match_icon = "✅" if data.get('author_match') else "⚠️"
                        report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
                        if data.get('alternate_mr'):
                            report.append(f"  └─ 🔄 Alt MR in README: <{data['alternate_mr']}>")
                report.append("")
            
            # MR author mismatch
            if mr_author_mismatch:
                report.append("**⚠️ MR Author Mismatch (not student's MR!):**")
                report.append("*(The MR was authored by someone else, no student MR found in README)*")
                for mid, data in sorted(mr_author_mismatch.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`) - Expected: `{data['expected_author']}`")
                    all_mrs_details = data.get('all_mrs_details', [])
                    if all_mrs_details:
                        for i, mr_detail in enumerate(all_mrs_details, 1):
                            merged_indicator = " (MERGED)" if mr_detail.get('is_merged') else ""
                            report.append(f"  └─ MR {i}: <{mr_detail['url']}> by `{mr_detail['author']}`{merged_indicator}")
                    else:
                        report.append(f"  └─ MR: <{data['mr_url']}> by `{data['actual_author']}`")
                report.append("")
            
            # MRs found in README (crawled)
            if mrs_found:
                report.append("**🔗 MRs Found in README (Crawled):**")
                for mid, data in sorted(mrs_found.items(), key=lambda x: x[1]['name'].lower()):
                    merged_tag = " **(MERGED)**" if data.get('is_merged') else ""
                    report.append(f"• **{data['name']}** (`{mid}`){merged_tag}")
                    if data.get('note'):
                        report.append(f"  └─ MR: <{data['mr_url']}> *(corrected)*")
                    else:
                        report.append(f"  └─ MR: <{data['mr_url']}>")
                    expected = data.get('expected_author', '')
                    actual = data.get('actual_author', '')
                    if expected and actual:
                        match_icon = "✅" if data.get('author_match') else "⚠️"
                        report.append(f"  └─ {match_icon} Expected: `{expected}` | Actual: `{actual}`")
                report.append("")
            
            # No MR in README
            if no_mr_in_readme:
                report.append("**❌ No MR Found in README:**")
                for mid, data in sorted(no_mr_in_readme.items(), key=lambda x: x[1]['name'].lower()):
                    report.append(f"• **{data['name']}** (`{mid}`)")
                    report.append(f"  └─ README: <{data['readme_link']}>")
                report.append("")
            
            # Send report in chunks
            full_report = "\n".join(report)
            if len(full_report) <= 2000:
                await ctx.send(full_report)
            else:
                chunks = []
                current = ""
                for line in report:
                    if len(current) + len(line) + 1 > 1900:
                        chunks.append(current)
                        current = line
                    else:
                        current += "\n" + line if current else line
                if current:
                    chunks.append(current)
                for chunk in chunks:
                    await ctx.send(chunk)
            
            # Report inaccessible READMEs
            if readme_inaccessible:
                inacc_report = ["**⚠️ Inaccessible READMEs:**"]
                for mid, data in sorted(readme_inaccessible.items(), key=lambda x: x[1]['name'].lower()):
                    inacc_report.append(f"• **{data['name']}** (`{mid}`)")
                    inacc_report.append(f"  └─ Link: <{data['readme_link']}>")
                    inacc_report.append(f"  └─ Error: {data['error']}")
                
                inacc_text = "\n".join(inacc_report)
                if len(inacc_text) <= 2000:
                    await ctx.send(inacc_text)
                else:
                    chunks = []
                    current = ""
                    for line in inacc_report:
                        if len(current) + len(line) + 1 > 1900:
                            chunks.append(current)
                            current = line
                        else:
                            current += "\n" + line if current else line
                    if current:
                        chunks.append(current)
                    for chunk in chunks:
                        await ctx.send(chunk)
            
            # Report timed out READMEs
            if readme_timeout:
                timeout_report = ["**⏱️ READMEs Timed Out (can retry later):**"]
                for mid, data in sorted(readme_timeout.items(), key=lambda x: x[1]['name'].lower()):
                    timeout_report.append(f"• **{data['name']}** (`{mid}`)")
                    timeout_report.append(f"  └─ Link: <{data['readme_link']}>")
                    timeout_report.append(f"  └─ Attempts: {data['attempts']}")
                
                timeout_text = "\n".join(timeout_report)
                if len(timeout_text) <= 2000:
                    await ctx.send(timeout_text)
                else:
                    chunks = []
                    current = ""
                    for line in timeout_report:
                        if len(current) + len(line) + 1 > 1900:
                            chunks.append(current)
                            current = line
                        else:
                            current += "\n" + line if current else line
                    if current:
                        chunks.append(current)
                    for chunk in chunks:
                        await ctx.send(chunk)
            
            await ctx.send(f"💾 Results saved to `{results_file}`")
            
        except Exception as e:
            await ctx.send(f"❌ **Error validating:** {str(e)}")
            print(f"[Tracker] Error in validate_no_mrs: {e}")
            import traceback
            traceback.print_exc()
    
    @commands.command(name='dl_mr')
    async def download_mrs(self, ctx: commands.Context):
        """Download validated MRs as CSV with contact info.
        
        Usage: 
            !tracker dl_mr
        
        Downloads a CSV with all students and their MR status:
        Name, Member ID, Discord, Email, Phone, Status, MR URL, Source, Notes
        """
        import json
        import os
        import csv
        from io import StringIO
        from datetime import datetime
        
        results_file = os.path.join('data', 'uploads', '_validated_mrs.json')
        
        # Check if file exists
        if not os.path.exists(results_file):
            await ctx.send(
                "❌ **No validated MR data found.**\n\n"
                "Run `!tracker no_mr validate` first to generate the data."
            )
            return
        
        # Load the validated data
        try:
            with open(results_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            await ctx.send(f"❌ **Error reading validated MRs file:** {str(e)}")
            return
        
        validated_at = data.get('validated_at', 'Unknown')
        await ctx.send(f"📥 **Exporting validated MRs to CSV...**\nData from: `{validated_at}`")
        
        # Build contact lookup from master CSV
        contact_lookup: dict = {}
        master_file = self.storage.get_file("master")
        if master_file:
            master_data = self.storage.read_file(master_file)
            master_text = master_data.decode('utf-8-sig')
            
            lines = master_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                master_text = "\n".join(lines[header_row_idx:])
            
            try:
                m_dialect = csv.Sniffer().sniff(master_text[:4096], delimiters=',\t;|')
            except csv.Error:
                m_dialect = 'excel'
            m_reader = csv.DictReader(io.StringIO(master_text), dialect=m_dialect)
            m_rows = list(m_reader)
            if m_rows:
                m_headers = list(m_rows[0].keys())
                m_member_col = next((h for h in m_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                m_email_col = next((h for h in m_headers if 'email' in h.lower() and 'secondary' not in h.lower()), None)
                m_discord_col = next((h for h in m_headers if 'discord' in h.lower()), None)
                
                for row in m_rows:
                    mid = str(row.get(m_member_col, "")).strip() if m_member_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id']:
                        contact_lookup[mid] = {
                            'email': str(row.get(m_email_col, "")).strip() if m_email_col else "",
                            'discord': str(row.get(m_discord_col, "")).strip() if m_discord_col else "",
                            'phone': ""
                        }
        
        # Add phone numbers from app CSV
        app_file = self.storage.get_file("app")
        if app_file:
            app_data = self.storage.read_file(app_file)
            app_text = app_data.decode('utf-8-sig')
            
            lines = app_text.splitlines()
            header_row_idx = None
            for idx, line in enumerate(lines):
                if "Member ID" in line or "member_id" in line.lower():
                    header_row_idx = idx
                    break
            
            if header_row_idx is not None:
                app_text = "\n".join(lines[header_row_idx:])
            
            try:
                a_dialect = csv.Sniffer().sniff(app_text[:4096], delimiters=',\t;|')
            except csv.Error:
                a_dialect = 'excel'
            a_reader = csv.DictReader(io.StringIO(app_text), dialect=a_dialect)
            a_rows = list(a_reader)
            if a_rows:
                a_headers = list(a_rows[0].keys())
                a_member_col = next((h for h in a_headers if 'member' in h.lower() and 'id' in h.lower()), None)
                a_phone_col = next((h for h in a_headers if 'phone' in h.lower()), None)
                
                for row in a_rows:
                    mid = str(row.get(a_member_col, "")).strip() if a_member_col else ""
                    phone = str(row.get(a_phone_col, "")).strip() if a_phone_col else ""
                    if mid and mid.lower() not in ['#n/a', 'n/a', '', 'member id'] and phone:
                        if mid in contact_lookup:
                            contact_lookup[mid]['phone'] = phone
                        else:
                            contact_lookup[mid] = {'email': '', 'discord': '', 'phone': phone}
        
        # Extract all categories from validated data
        students_with_valid_mr = data.get('students_with_valid_mr', {})
        students_with_invalid_mr = data.get('students_with_invalid_mr', {})
        readme_url_in_mr_field = data.get('readme_url_in_mr_field', {})
        mr_url_in_readme_link = data.get('mr_url_in_readme_link', {})
        mrs_found = data.get('mrs_found', {})
        no_mr_in_readme = data.get('no_mr_in_readme', {})
        readme_inaccessible = data.get('readme_inaccessible', {})
        readme_timeout = data.get('readme_timeout', {})
        mr_author_mismatch = data.get('mr_author_mismatch', {})
        
        # Build list of all students with sort order
        # Sort order: 1=Valid+Matching, 2=Valid+Mismatched/Unknown, 3=Wrong Field, 4=Invalid, 5=No MR, 6=Inaccessible
        all_students: list = []
        
        # Category 1: Students with valid MR from typeform
        for mid, info in students_with_valid_mr.items():
            author_match = info.get('author_match', None)
            expected = info.get('expected_author', '')
            actual = info.get('actual_author', '')
            
            if author_match is True:
                status = 'Valid (Author Verified)'
                sort_order = 1
                notes = f"Verified Author: {actual}"
            elif author_match is False:
                status = 'Valid (Author Mismatch)'
                sort_order = 2
                notes = f"Expected: {expected} | Actual: {actual}"
                if info.get('alternate_mr'):
                    notes += f" | Alt MR: {info['alternate_mr']}"
            else:
                status = 'Valid (Not Verified)'
                sort_order = 2
                notes = 'Author not verified (no GitLab username in master)'
            
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': status,
                'mr_url': info['mr_url'],
                'source': 'Typeform',
                'notes': notes,
                'sort_order': sort_order
            })
        
        # Category 2: MR URL in README link field (wrong field but valid MR)
        for mid, info in mr_url_in_readme_link.items():
            if any(s['member_id'] == mid for s in all_students):
                continue
            
            author_match = info.get('author_match', None)
            expected = info.get('expected_author', '')
            actual = info.get('actual_author', '')
            
            if author_match is True:
                status = 'Wrong Field (Author Verified)'
                sort_order = 3
                notes = f"MR in README field | Verified Author: {actual}"
            elif author_match is False:
                status = 'Wrong Field (Author Mismatch)'
                sort_order = 3
                notes = f"MR in README field | Expected: {expected} | Actual: {actual}"
            else:
                status = 'Wrong Field (Not Verified)'
                sort_order = 3
                notes = 'MR URL was placed in README link field'
            
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': status,
                'mr_url': info['mr_url'],
                'source': 'README Link Field',
                'notes': notes,
                'sort_order': sort_order
            })
        
        # Category 3: MRs found in README
        for mid, info in mrs_found.items():
            if any(s['member_id'] == mid for s in all_students):
                continue
            
            author_match = info.get('author_match', None)
            expected = info.get('expected_author', '')
            actual = info.get('actual_author', '')
            
            if author_match is True:
                status = 'Valid (Author Verified)'
                sort_order = 1
                notes = f"Verified Author: {actual}"
                if info.get('note'):
                    notes += f" | {info['note']}"
            elif author_match is False:
                status = 'Valid (Author Mismatch)'
                sort_order = 2
                notes = f"Expected: {expected} | Actual: {actual}"
            else:
                status = 'Valid (Not Verified)'
                sort_order = 2
                notes = 'Author not verified (no GitLab username in master)'
            
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': status,
                'mr_url': info['mr_url'],
                'source': 'README',
                'notes': notes,
                'sort_order': sort_order
            })
        
        # Category 4: README URL in MR field
        for mid, info in readme_url_in_mr_field.items():
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Invalid (README in MR Field)',
                'mr_url': info.get('readme_url', ''),
                'source': 'MR URL Field',
                'notes': 'README/repo URL was placed in MR URL field',
                'sort_order': 4
            })
        
        # Category 5: Invalid MR URLs
        for mid, info in students_with_invalid_mr.items():
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Invalid MR URL',
                'mr_url': info['mr_url'],
                'source': 'Typeform',
                'notes': 'URL does not match expected GitLab MR format',
                'sort_order': 4
            })
        
        # Category 6: MR author mismatch (not student's MR)
        for mid, info in mr_author_mismatch.items():
            # Build detailed notes with all MRs checked
            all_mrs_details = info.get('all_mrs_details', [])
            if all_mrs_details:
                mrs_list = "; ".join([f"{mr['url']} by {mr['author']}" for mr in all_mrs_details])
                notes = f"Expected: {info['expected_author']} | Checked: {mrs_list}"
            else:
                notes = f"Expected: {info['expected_author']} | Actual: {info['actual_author']}"
            
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'Author Mismatch',
                'mr_url': info['mr_url'],
                'source': info.get('source', 'Unknown').title(),
                'notes': notes,
                'sort_order': 4
            })
        
        # Category 7: No MR in README
        for mid, info in no_mr_in_readme.items():
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'No MR Found',
                'mr_url': '',
                'source': 'README',
                'notes': f"README: {info.get('readme_link', 'N/A')}",
                'sort_order': 5
            })
        
        # Category 8: Inaccessible READMEs
        for mid, info in readme_inaccessible.items():
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'README Inaccessible',
                'mr_url': '',
                'source': 'N/A',
                'notes': f"Error: {info.get('error', 'Unknown')} | README: {info.get('readme_link', 'N/A')}",
                'sort_order': 6
            })
        
        # Category 9: Timed out READMEs
        for mid, info in readme_timeout.items():
            if any(s['member_id'] == mid for s in all_students):
                continue
            all_students.append({
                'member_id': mid,
                'name': info['name'],
                'status': 'README Timeout',
                'mr_url': '',
                'source': 'N/A',
                'notes': f"Timed out after {info.get('attempts', 'N/A')} attempts | README: {info.get('readme_link', 'N/A')}",
                'sort_order': 6
            })
        
        # Build CSV
        output = StringIO()
        writer = csv.writer(output)
        
        # Header row
        writer.writerow(['Name', 'Member ID', 'Discord', 'Email', 'Phone', 'Status', 'MR URL', 'Source', 'Notes'])
        
        # Sort by: 1) sort_order (valid+matching first), 2) status, 3) name
        all_students.sort(key=lambda x: (x.get('sort_order', 99), x['status'], x['name'].lower()))
        
        # Data rows
        for student in all_students:
            contact = contact_lookup.get(student['member_id'], {})
            writer.writerow([
                student['name'],
                student['member_id'],
                contact.get('discord', ''),
                contact.get('email', ''),
                contact.get('phone', ''),
                student['status'],
                student['mr_url'],
                student['source'],
                student['notes']
            ])
        
        # Count statistics based on new status values
        valid_statuses = ['Valid (Author Verified)', 'Valid (Not Verified)', 'Valid (Author Mismatch)']
        wrong_field_statuses = ['Wrong Field (Author Verified)', 'Wrong Field (Not Verified)', 'Wrong Field (Author Mismatch)']
        no_mr_statuses = ['No MR Found', 'README Inaccessible', 'README Timeout']
        
        has_mr_count = sum(1 for s in all_students if s['status'] in valid_statuses + wrong_field_statuses)
        no_mr_count = sum(1 for s in all_students if s['status'] in no_mr_statuses)
        needs_attention_count = sum(1 for s in all_students if s['status'] not in valid_statuses + no_mr_statuses)
        
        # Add summary footer
        writer.writerow([])
        writer.writerow(['--- SUMMARY ---'])
        writer.writerow([f'Total Students: {len(all_students)}'])
        writer.writerow([f'With MRs: {has_mr_count}'])
        writer.writerow([f'Without MRs: {no_mr_count}'])
        writer.writerow([f'Needs Attention: {needs_attention_count}'])
        writer.writerow([f'Data validated at: {validated_at}'])
        
        # Create file for Discord
        csv_content = output.getvalue().encode('utf-8')
        filename = f"mrs_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Send summary and file
        summary = (
            f"✅ **Export Complete**\n\n"
            f"📊 **Summary:**\n"
            f"• Total Students: {len(all_students)}\n"
            f"• With MRs: {has_mr_count}\n"
            f"• Without MRs: {no_mr_count}\n"
            f"• Needs Attention: {needs_attention_count}\n"
        )
        
        await ctx.send(summary, file=discord.File(io.BytesIO(csv_content), filename=filename))


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(TrackerCog(bot))
