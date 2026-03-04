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
    !tracker no_issues        - List all unique students without an issue_url
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
                    'current_week': current_week
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
    async def no_issues(self, ctx: commands.Context):
        """List students by issue selection status with contact info.
        
        Usage: !tracker no_issues
        
        Shows four lists with email, Discord, and phone:
        1. Students WITHOUT an issue URL
        2. Students WITHOUT issue URL but HAS 'Why I Chose This' (anomaly)
        3. Students WITH issue URL but MISSING 'Why I Chose This'
        4. Students WITH 'Why I Chose This' complete
        """
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


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(TrackerCog(bot))
