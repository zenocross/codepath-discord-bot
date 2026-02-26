"""Tracker module for processing CSV files to Excel.

Commands:
    !tracker upload           - Interactive upload wizard
    !tracker upload master    - Upload master roster CSV
    !tracker upload typeform  - Upload typeform responses CSV
    !tracker upload zoom      - Upload zoom attendance CSV
    !tracker download         - Generate Excel report from uploaded CSVs
    !tracker files            - Show status of uploaded CSV files
    !tracker clear <type>     - Clear specific CSV file
    !tracker clearall         - Clear all uploaded CSV files
    !tracker start_date       - Set or view program start date
    !tracker submissions      - Real-time submission checking
    !tracker submissions_download [options] - Download report filtered by submissions date
        Options: nofilter, validate_commits, validate_all
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
    "zoom": "Zoom Attendance (lecture/office hours attendance)"
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
    
    @commands.group(name='clear', invoke_without_command=True)
    async def clear(self, ctx: commands.Context):
        """Clear uploaded CSV files. Use subcommands to specify which file."""
        await ctx.send(
            "**🗑️ Clear CSV Files**\n\n"
            "Use one of the following commands:\n"
            "• `!tracker clear master` - Remove master roster CSV\n"
            "• `!tracker clear typeform` - Remove typeform responses CSV\n"
            "• `!tracker clear zoom` - Remove zoom attendance CSV\n"
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
            
            # Process with date filter
            result = self.processor.process_submissions(
                typeform_data,
                master_data=master_data,
                start_date=start_date,
                target_date=target_date,
                current_week=current_week
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
            
            # Build options dict
            process_options = {
                'master_data': master_data,
                'zoom_data': zoom_data,
                'start_date': start_date,
                'target_date': target_date,
                'current_week': current_week,
                'filter_by_date': True
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
            
            # Process with tracker processor (pass all data sources)
            result = self.processor.process(
                typeform_data,
                options={
                    'master_data': master_data,
                    'zoom_data': zoom_data
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
                f"  └─ Master Tracker (all fields)\n"
                f"  └─ P1 - At Risk (red/orange/yellow coding)\n"
                f"  └─ P2 - Flagged (yellow coding)\n"
                f"  └─ P3 - On Track (green coding)\n"
                f"  └─ Weekly Summary (dashboard)",
                file=file
            )
            
        except Exception as e:
            await ctx.send(f"❌ Error processing file: {e}")


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(TrackerCog(bot))
