"""Report service for student activity analysis.

Aggregates data from:
- Typeform CSVs (all submissions, multiple readme URLs)
- GitLab API (commits, MRs from READMEs)
- Validated MRs JSON

Provides per-student and aggregate reporting.
"""

import csv
import io
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

from services.file_processor import FileStorageService
from services.gitlab_service import GitLabService, COMMIT_PATTERN, MR_PATTERN


@dataclass
class CommitInfo:
    """Information about a single commit."""
    sha: str
    url: str
    repo_path: str
    author_name: str = ""
    author_email: str = ""
    created_at: Optional[datetime] = None
    title: str = ""
    is_owned: bool = False
    week_number: int = 0


@dataclass
class MRInfo:
    """Information about a single merge request."""
    iid: str
    url: str
    repo_path: str
    title: str = ""
    state: str = ""
    author: str = ""
    created_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    is_merged: bool = False
    is_owned: bool = False
    week_number: int = 0


@dataclass
class StudentReport:
    """Comprehensive report for a single student."""
    student_id: str
    member_id: str
    name: str
    gitlab_username: str = ""
    discord_username: str = ""
    
    readme_urls: List[str] = field(default_factory=list)
    
    commits: List[CommitInfo] = field(default_factory=list)
    merge_requests: List[MRInfo] = field(default_factory=list)
    
    commits_by_week: Dict[int, List[CommitInfo]] = field(default_factory=lambda: defaultdict(list))
    mrs_by_week: Dict[int, List[MRInfo]] = field(default_factory=lambda: defaultdict(list))
    
    total_commits: int = 0
    total_mrs: int = 0
    merged_mrs: int = 0
    open_mrs: int = 0
    closed_mrs: int = 0
    owned_commits: int = 0
    owned_mrs: int = 0
    
    # Submission tracking (10 weeks expected)
    wed_submissions_by_week: Dict[int, bool] = field(default_factory=dict)
    sun_submissions_by_week: Dict[int, bool] = field(default_factory=dict)
    total_wed_submissions: int = 0
    total_sun_submissions: int = 0
    total_expected_weeks: int = 10
    
    validation_errors: List[str] = field(default_factory=list)
    
    @property
    def weeks_with_commits(self) -> List[int]:
        """Return sorted list of weeks that have commits."""
        return sorted(self.commits_by_week.keys())
    
    @property
    def weeks_with_mrs(self) -> List[int]:
        """Return sorted list of weeks that have MRs."""
        return sorted(self.mrs_by_week.keys())
    
    @property
    def weeks_with_wed_submissions(self) -> List[int]:
        """Return sorted list of weeks with Wednesday submissions."""
        return sorted([w for w, submitted in self.wed_submissions_by_week.items() if submitted])
    
    @property
    def weeks_with_sun_submissions(self) -> List[int]:
        """Return sorted list of weeks with Sunday submissions."""
        return sorted([w for w, submitted in self.sun_submissions_by_week.items() if submitted])


class ReportService:
    """Service for generating student activity reports."""
    
    def __init__(self, storage: Optional[FileStorageService] = None, 
                 gitlab: Optional[GitLabService] = None,
                 start_date: Optional[datetime] = None):
        """Initialize the report service.
        
        Args:
            storage: File storage service for accessing CSVs
            gitlab: GitLab service for API calls
            start_date: Program start date for week calculations
        """
        self.storage = storage or FileStorageService()
        self.gitlab = gitlab or GitLabService()
        self.start_date = start_date or self._load_start_date()
        
        # Cache for validated MRs data
        self._validated_mrs_cache: Optional[Dict] = None
    
    def _load_start_date(self) -> datetime:
        """Load the program start date from settings."""
        settings_path = "data/uploads/_tracker_settings.json"
        try:
            if os.path.exists(settings_path):
                with open(settings_path, 'r') as f:
                    settings = json.load(f)
                    date_str = settings.get('start_date')
                    if date_str:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except Exception as e:
            print(f"[ReportService] Error loading start date: {e}")
        
        return datetime(2026, 2, 23, tzinfo=timezone.utc)
    
    def get_week_number(self, date: datetime) -> int:
        """Calculate the week number for a given date.
        
        Args:
            date: The date to calculate week for
        
        Returns:
            Week number (1-indexed, week 1 is the first week)
        """
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        
        start = self.start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        
        days_diff = (date - start).days
        return max(1, (days_diff // 7) + 1)
    
    def _load_validated_mrs(self) -> Dict:
        """Load the validated MRs JSON data."""
        if self._validated_mrs_cache is not None:
            return self._validated_mrs_cache
        
        mrs_path = "data/uploads/_validated_mrs.json"
        try:
            if os.path.exists(mrs_path):
                with open(mrs_path, 'r') as f:
                    self._validated_mrs_cache = json.load(f)
                    return self._validated_mrs_cache
        except Exception as e:
            print(f"[ReportService] Error loading validated MRs: {e}")
        
        self._validated_mrs_cache = {}
        return self._validated_mrs_cache
    
    def _get_gitlab_username_from_master(self, member_id: str) -> str:
        """Get GitLab username from master CSV file.
        
        Args:
            member_id: The student's member ID
        
        Returns:
            GitLab username (without @ prefix) or empty string if not found
        """
        uploads_dir = "data/uploads"
        
        try:
            # Find master CSV file
            master_files = [f for f in os.listdir(uploads_dir) 
                          if f.startswith("master_") and f.endswith(".csv")]
            if not master_files:
                return ""
            
            master_path = os.path.join(uploads_dir, sorted(master_files)[-1])
            
            with open(master_path, 'r', encoding='utf-8-sig') as f:
                # Master CSV may have metadata rows before the header
                # Skip rows until we find one containing "Member ID"
                header_row = None
                for line in f:
                    if 'Member ID' in line:
                        header_row = line.strip().split(',')
                        break
                
                if not header_row:
                    return ""
                
                # Read remaining rows as data
                reader = csv.DictReader(f, fieldnames=header_row)
                for row in reader:
                    row_member_id = row.get('Member ID', '').strip()
                    if row_member_id == member_id:
                        # Try different column names for GitLab username
                        for col in ['GitLab Username', 'Gitlab Username', 'gitlab_username', 'GitLab']:
                            username = row.get(col, '').strip()
                            if username:
                                # Remove @ prefix if present
                                return username.lstrip('@').lstrip('.')
                        break
        except Exception as e:
            print(f"[ReportService] Error reading master CSV: {e}")
        
        return ""
    
    def _get_all_typeform_csvs(self) -> List[str]:
        """Get paths to all typeform CSV files in the uploads directory."""
        uploads_dir = "data/uploads"
        csv_files = []
        
        try:
            for filename in os.listdir(uploads_dir):
                if filename.startswith("typeform_") and filename.endswith(".csv"):
                    csv_files.append(os.path.join(uploads_dir, filename))
        except Exception as e:
            print(f"[ReportService] Error listing typeform CSVs: {e}")
        
        return sorted(csv_files)
    
    def _parse_typeform_csv(self, csv_path: str) -> List[Dict]:
        """Parse a typeform CSV file into records.
        
        Args:
            csv_path: Path to the CSV file
        
        Returns:
            List of dicts with student data from each row
        """
        records = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
            if len(lines) < 5:
                return records
            
            header_line = lines[3]
            reader = csv.DictReader(io.StringIO('\n'.join([header_line] + lines[4:])))
            
            for row in reader:
                member_id = row.get('Member ID', '').strip()
                if not member_id or member_id == '0':
                    continue
                
                readme_url = row.get('Link to your contribution README', '').strip()
                mr_url = row.get('Direct link to your Merge Request (MR) or Pull Request (PR)', '').strip()
                name = row.get("What's your name?", '').strip()
                
                # Extract submission type (Wednesday/Sunday)
                submission_type = row.get('Which submission are you completing?', '').strip()
                submission_for = row.get('Submission for', '').strip()
                
                # Determine if it's a Wednesday or Sunday submission
                is_wednesday = False
                is_sunday = False
                if 'wednesday' in submission_type.lower() or submission_for == 'BR':
                    is_wednesday = True
                elif 'sunday' in submission_type.lower() or submission_for == 'SUN':
                    is_sunday = True
                
                # Extract week number
                week_str = row.get('Week', '').strip()
                week_num = 0
                try:
                    week_num = int(week_str) if week_str else 0
                except ValueError:
                    pass
                
                # Parse submission date to calculate week if not provided
                submission_date_str = row.get('Submitted At', '') or row.get('Date Submitted', '')
                submission_date = None
                if submission_date_str:
                    try:
                        # Try different date formats
                        for fmt in ['%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d-%b-%y', '%Y-%m-%d']:
                            try:
                                submission_date = datetime.strptime(submission_date_str.strip(), fmt)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                
                records.append({
                    'member_id': member_id,
                    'name': name,
                    'readme_url': readme_url,
                    'mr_url': mr_url,
                    'is_wednesday': is_wednesday,
                    'is_sunday': is_sunday,
                    'week': week_num,
                    'submission_date': submission_date,
                    'raw': row
                })
        except Exception as e:
            print(f"[ReportService] Error parsing {csv_path}: {e}")
        
        return records
    
    def collect_student_data(self, student_id: str) -> Dict:
        """Collect all data for a specific student from typeform CSVs.
        
        Args:
            student_id: The member ID of the student
        
        Returns:
            Dict with student info and all unique readme URLs, MR URLs, and submissions
        """
        readme_urls: Set[str] = set()
        mr_urls: Set[str] = set()
        names: Set[str] = set()
        wed_submissions: Dict[int, bool] = {}
        sun_submissions: Dict[int, bool] = {}
        
        for csv_path in self._get_all_typeform_csvs():
            records = self._parse_typeform_csv(csv_path)
            
            for record in records:
                if record['member_id'] == student_id:
                    if record['name']:
                        names.add(record['name'])
                    
                    if record['readme_url']:
                        for url in record['readme_url'].split(','):
                            url = url.strip()
                            if url and 'gitlab.com' in url.lower():
                                readme_urls.add(url)
                    
                    if record['mr_url']:
                        for url in record['mr_url'].split(','):
                            url = url.strip()
                            if url and 'gitlab.com' in url.lower():
                                mr_urls.add(url)
                    
                    # Track submissions by week
                    week = record.get('week', 0)
                    if week == 0 and record.get('submission_date'):
                        week = self.get_week_number(record['submission_date'])
                    
                    if week > 0:
                        if record.get('is_wednesday'):
                            wed_submissions[week] = True
                        if record.get('is_sunday'):
                            sun_submissions[week] = True
        
        validated_mrs = self._load_validated_mrs()
        gitlab_username = ""
        
        # First try to get from validated MRs
        for bucket in ['students_with_valid_mr', 'mrs_found', 'mr_url_in_readme_link']:
            if student_id in validated_mrs.get(bucket, {}):
                data = validated_mrs[bucket][student_id]
                if data.get('expected_author'):
                    gitlab_username = data['expected_author']
                    break
        
        # Fallback: try to get from master CSV if not found
        if not gitlab_username:
            gitlab_username = self._get_gitlab_username_from_master(student_id)
        
        return {
            'member_id': student_id,
            'name': list(names)[0] if names else '',
            'readme_urls': list(readme_urls),
            'mr_urls': list(mr_urls),
            'gitlab_username': gitlab_username,
            'wed_submissions': wed_submissions,
            'sun_submissions': sun_submissions
        }
    
    def collect_all_students_data(self) -> Dict[str, Dict]:
        """Collect data for all students from typeform CSVs.
        
        Returns:
            Dict mapping member_id to student data
        """
        students: Dict[str, Dict] = {}
        
        for csv_path in self._get_all_typeform_csvs():
            records = self._parse_typeform_csv(csv_path)
            
            for record in records:
                member_id = record['member_id']
                
                if member_id not in students:
                    students[member_id] = {
                        'member_id': member_id,
                        'name': record['name'],
                        'readme_urls': set(),
                        'mr_urls': set()
                    }
                elif record['name'] and not students[member_id]['name']:
                    students[member_id]['name'] = record['name']
                
                if record['readme_url']:
                    for url in record['readme_url'].split(','):
                        url = url.strip()
                        if url and 'gitlab.com' in url.lower():
                            students[member_id]['readme_urls'].add(url)
                
                if record['mr_url']:
                    for url in record['mr_url'].split(','):
                        url = url.strip()
                        if url and 'gitlab.com' in url.lower():
                            students[member_id]['mr_urls'].add(url)
        
        for member_id, data in students.items():
            data['readme_urls'] = list(data['readme_urls'])
            data['mr_urls'] = list(data['mr_urls'])
        
        validated_mrs = self._load_validated_mrs()
        for bucket in ['students_with_valid_mr', 'mrs_found', 'mr_url_in_readme_link']:
            for member_id, mr_data in validated_mrs.get(bucket, {}).items():
                if member_id in students and mr_data.get('expected_author'):
                    students[member_id]['gitlab_username'] = mr_data['expected_author']
        
        # Fallback: get GitLab usernames from master CSV for students without one
        for member_id, data in students.items():
            if not data.get('gitlab_username'):
                data['gitlab_username'] = self._get_gitlab_username_from_master(member_id)
        
        return students
    
    def _extract_repo_from_url(self, url: str) -> Optional[str]:
        """Extract repository path from a GitLab URL.
        
        Handles various URL formats including:
        - Standard blob/tree URLs: gitlab.com/user/repo/-/blob/...
        - Nested namespaces: gitlab.com/group/subgroup/repo/-/...
        - Plain repo URLs: gitlab.com/user/repo
        - URLs without /-/ marker: gitlab.com/user/repo/file.md
        - URLs with .git suffix: gitlab.com/user/repo.git
        """
        if not url:
            return None
        
        url = url.strip()
        
        # Pattern 1: URLs with /-/ marker (handles nested namespaces)
        # gitlab.com/user/repo/-/blob/... or gitlab.com/group/sub/repo/-/...
        match = re.search(r'gitlab\.com/([^/]+(?:/[^/]+)+)/-/', url)
        if match:
            repo_path = match.group(1)
            return repo_path.rstrip('.git')
        
        # Pattern 2: Plain repo URL or URL ending with repo name
        # gitlab.com/user/repo or gitlab.com/user/repo.git
        match = re.search(r'gitlab\.com/([^/]+/[^/]+?)(?:\.git)?(?:/|\?|#|$)', url)
        if match:
            return match.group(1)
        
        # Pattern 3: URL with file path but no /-/ marker
        # gitlab.com/user/repo/file.md
        match = re.search(r'gitlab\.com/([^/]+/[^/]+)', url)
        if match:
            repo_path = match.group(1)
            # Strip .git suffix if present
            return repo_path.rstrip('.git') if repo_path.endswith('.git') else repo_path
        
        return None
    
    def _fetch_readme_content(self, readme_url: str) -> Tuple[Optional[str], str]:
        """Fetch README content from a GitLab URL.
        
        Returns:
            Tuple of (content, repo_path) or (None, error)
        """
        repo_path = self._extract_repo_from_url(readme_url)
        if not repo_path:
            return None, "Could not extract repo path"
        
        if '/-/blob/' in readme_url:
            match = re.search(r'/-/blob/[^/]+/(.+?)(?:\?|$)', readme_url)
            if match:
                file_path = match.group(1)
                content = self.gitlab.fetch_file_content(repo_path, file_path)
                if content:
                    return content, repo_path
        
        content = self.gitlab.fetch_readme(repo_path)
        if content:
            return content, repo_path
        
        return None, repo_path
    
    def _fetch_all_readmes_from_repos(self, readme_urls: List[str]) -> List[Tuple[str, str]]:
        """Fetch ALL README files from all unique repos referenced in the URLs.
        
        This matches the tracker's behavior of scanning all READMEs in a repo,
        not just the specific URL submitted.
        
        Args:
            readme_urls: List of README URLs from student submissions
        
        Returns:
            List of tuples: [(repo_path, combined_content), ...]
        """
        repos_crawled: Set[str] = set()
        results: List[Tuple[str, str]] = []
        
        for readme_url in readme_urls:
            repo_path = self._extract_repo_from_url(readme_url)
            if not repo_path or repo_path in repos_crawled:
                continue
            
            repos_crawled.add(repo_path)
            
            # Fetch ALL README files from this repo (like tracker does)
            all_readme_contents = self.gitlab.fetch_all_readme_contents(repo_path)
            
            if all_readme_contents:
                # Combine all README contents
                combined = "\n\n".join([content for _, content in all_readme_contents])
                results.append((repo_path, combined))
            else:
                # Fallback to single README fetch
                content, _ = self._fetch_readme_content(readme_url)
                if content:
                    results.append((repo_path, content))
        
        return results
    
    def generate_student_report(self, student_id: str, 
                                 validate_ownership: bool = False) -> StudentReport:
        """Generate a comprehensive report for a student.
        
        Args:
            student_id: The member ID of the student
            validate_ownership: If True, verify commits belong to student
        
        Returns:
            StudentReport with all collected data
        """
        student_data = self.collect_student_data(student_id)
        
        report = StudentReport(
            student_id=student_id,
            member_id=student_id,
            name=student_data.get('name', ''),
            gitlab_username=student_data.get('gitlab_username', ''),
            readme_urls=student_data.get('readme_urls', []),
            wed_submissions_by_week=student_data.get('wed_submissions', {}),
            sun_submissions_by_week=student_data.get('sun_submissions', {})
        )
        
        # Calculate submission totals
        report.total_wed_submissions = len([w for w, s in report.wed_submissions_by_week.items() if s])
        report.total_sun_submissions = len([w for w, s in report.sun_submissions_by_week.items() if s])
        
        seen_commits: Set[str] = set()
        seen_mrs: Set[str] = set()
        
        # Fetch ALL READMEs from all referenced repos (aligns with tracker behavior)
        repo_contents = self._fetch_all_readmes_from_repos(report.readme_urls)
        
        for repo_path, content in repo_contents:
            if not content:
                continue
            
            links = self.gitlab.parse_gitlab_links(content)
            
            for commit_link in links.get('commits', []):
                commit_key = f"{commit_link['repo_path']}:{commit_link['sha']}"
                if commit_key in seen_commits:
                    continue
                seen_commits.add(commit_key)
                
                commit_data = self.gitlab.verify_commit(
                    commit_link['repo_path'], 
                    commit_link['sha']
                )
                
                created_at = None
                week_num = 0
                author_name = ""
                author_email = ""
                title = ""
                is_owned = False
                
                if commit_data.get('exists'):
                    author_name = commit_data.get('author_name', '')
                    author_email = commit_data.get('author_email', '')
                    title = commit_data.get('title', '')
                    
                    if commit_data.get('created_at'):
                        try:
                            created_at = datetime.fromisoformat(
                                commit_data['created_at'].replace('Z', '+00:00')
                            )
                            week_num = self.get_week_number(created_at)
                        except Exception:
                            pass
                    
                    # Always calculate ownership for accurate counting
                    if report.gitlab_username:
                        username_lower = report.gitlab_username.lower()
                        email_prefix = author_email.split('@')[0].lower() if author_email else ''
                        is_owned = (
                            username_lower in author_name.lower() or
                            username_lower in email_prefix or
                            email_prefix in username_lower
                        )
                
                commit_info = CommitInfo(
                    sha=commit_link['sha'],
                    url=commit_link['url'],
                    repo_path=commit_link['repo_path'],
                    author_name=author_name,
                    author_email=author_email,
                    created_at=created_at,
                    title=title,
                    is_owned=is_owned,
                    week_number=week_num
                )
                
                report.commits.append(commit_info)
                if week_num > 0:
                    report.commits_by_week[week_num].append(commit_info)
            
            for mr_link in links.get('merge_requests', []):
                mr_key = f"{mr_link['repo_path']}:{mr_link['iid']}"
                if mr_key in seen_mrs:
                    continue
                seen_mrs.add(mr_key)
                
                mr_data = self.gitlab.verify_merge_request(
                    mr_link['repo_path'],
                    mr_link['iid']
                )
                
                created_at = None
                merged_at = None
                week_num = 0
                is_merged = False
                is_owned = False
                author = ""
                title = ""
                state = ""
                
                if mr_data.get('exists'):
                    author = mr_data.get('author', '')
                    title = mr_data.get('title', '')
                    state = mr_data.get('state', '')
                    is_merged = state == 'merged'
                    
                    if mr_data.get('created_at'):
                        try:
                            created_at = datetime.fromisoformat(
                                mr_data['created_at'].replace('Z', '+00:00')
                            )
                            week_num = self.get_week_number(created_at)
                        except Exception:
                            pass
                    
                    if mr_data.get('merged_at'):
                        try:
                            merged_at = datetime.fromisoformat(
                                mr_data['merged_at'].replace('Z', '+00:00')
                            )
                        except Exception:
                            pass
                    
                    # Always calculate ownership for accurate merged MR counting
                    if report.gitlab_username:
                        is_owned = author.lower() == report.gitlab_username.lower()
                
                mr_info = MRInfo(
                    iid=mr_link['iid'],
                    url=mr_link['url'],
                    repo_path=mr_link['repo_path'],
                    title=title,
                    state=state,
                    author=author,
                    created_at=created_at,
                    merged_at=merged_at,
                    is_merged=is_merged,
                    is_owned=is_owned,
                    week_number=week_num
                )
                
                report.merge_requests.append(mr_info)
                if week_num > 0:
                    report.mrs_by_week[week_num].append(mr_info)
        
        # Also process direct MR URLs from typeform submissions
        # These are MRs the student submitted directly, not found in READMEs
        mr_urls = student_data.get('mr_urls', [])
        for mr_url in mr_urls:
            # Parse MR URL to extract repo and iid
            import re
            mr_match = re.search(r'gitlab\.com/([^/]+(?:/[^/]+)+)/-/merge_requests/(\d+)', mr_url)
            if not mr_match:
                continue
            
            repo_path = mr_match.group(1)
            iid = mr_match.group(2)
            mr_key = f"{repo_path}:{iid}"
            
            if mr_key in seen_mrs:
                continue
            seen_mrs.add(mr_key)
            
            mr_data = self.gitlab.verify_merge_request(repo_path, iid)
            
            merged_at = None
            week_num = 0
            is_merged = False
            is_owned = False
            author = ""
            title = ""
            state = ""
            created_at = None
            
            if mr_data.get('exists'):
                author = mr_data.get('author', '')
                title = mr_data.get('title', '')
                state = mr_data.get('state', '')
                is_merged = state == 'merged'
                
                if mr_data.get('created_at'):
                    try:
                        created_at = datetime.fromisoformat(
                            mr_data['created_at'].replace('Z', '+00:00')
                        )
                        week_num = self.get_week_number(created_at)
                    except Exception:
                        pass
                
                if mr_data.get('merged_at'):
                    try:
                        merged_at = datetime.fromisoformat(
                            mr_data['merged_at'].replace('Z', '+00:00')
                        )
                    except Exception:
                        pass
                
                # Always calculate ownership for accurate merged MR counting
                if report.gitlab_username:
                    is_owned = author.lower() == report.gitlab_username.lower()
            
            mr_info = MRInfo(
                iid=iid,
                url=mr_url,
                repo_path=repo_path,
                title=title,
                state=state,
                author=author,
                created_at=created_at,
                merged_at=merged_at,
                is_merged=is_merged,
                is_owned=is_owned,
                week_number=week_num
            )
            
            report.merge_requests.append(mr_info)
            if week_num > 0:
                report.mrs_by_week[week_num].append(mr_info)
        
        report.total_commits = len(report.commits)
        report.total_mrs = len(report.merge_requests)
        report.open_mrs = sum(1 for mr in report.merge_requests if mr.state == 'opened')
        report.closed_mrs = sum(1 for mr in report.merge_requests if mr.state == 'closed')
        report.owned_commits = sum(1 for c in report.commits if c.is_owned)
        report.owned_mrs = sum(1 for mr in report.merge_requests if mr.is_owned)
        
        # Count merged MRs only if student owns them (author matches)
        # This excludes MRs by other students that were merely referenced
        report.merged_mrs = sum(1 for mr in report.merge_requests 
                               if mr.is_merged and mr.is_owned)
        
        if validate_ownership:
            for commit in report.commits:
                if not commit.is_owned:
                    report.validation_errors.append(
                        f"Commit {commit.sha[:8]} by '{commit.author_name}' "
                        f"does not match expected username '{report.gitlab_username}'"
                    )
            
            for mr in report.merge_requests:
                if not mr.is_owned:
                    report.validation_errors.append(
                        f"MR !{mr.iid} by '{mr.author}' "
                        f"does not match expected username '{report.gitlab_username}'"
                    )
        
        return report
    
    def generate_all_reports(self, validate_ownership: bool = False,
                              progress_callback=None) -> List[StudentReport]:
        """Generate reports for all students.
        
        Args:
            validate_ownership: If True, verify commits belong to students
            progress_callback: Optional callback(current, total) for progress updates
        
        Returns:
            List of StudentReport objects
        """
        students = self.collect_all_students_data()
        reports = []
        total = len(students)
        
        for i, (member_id, data) in enumerate(students.items()):
            if progress_callback:
                progress_callback(i + 1, total)
            
            report = self.generate_student_report(member_id, validate_ownership)
            reports.append(report)
        
        return reports
    
    def format_report_embed(self, report: StudentReport) -> Dict:
        """Format a StudentReport as an embed-ready dict.
        
        Returns:
            Dict with embed fields for Discord
        """
        total_weeks = max(
            max(report.weeks_with_commits, default=0),
            max(report.weeks_with_mrs, default=0)
        )
        
        week_breakdown = []
        for week in range(1, total_weeks + 1):
            commits = len(report.commits_by_week.get(week, []))
            mrs = len(report.mrs_by_week.get(week, []))
            if commits > 0 or mrs > 0:
                week_breakdown.append(f"Week {week}: {commits} commits, {mrs} MRs")
        
        mr_summary = []
        for mr in report.merge_requests:
            status = "✅ Merged" if mr.is_merged else f"📝 {mr.state}"
            mr_summary.append(f"{status}: {mr.title[:50]}{'...' if len(mr.title) > 50 else ''}")
        
        return {
            'title': f"Report: {report.name or report.member_id}",
            'description': f"Member ID: `{report.member_id}`\nGitLab: `{report.gitlab_username or 'Unknown'}`",
            'fields': [
                {
                    'name': '📊 Summary',
                    'value': (
                        f"**Total Commits:** {report.total_commits}\n"
                        f"**Total MRs:** {report.total_mrs}\n"
                        f"**Merged MRs:** {report.merged_mrs}\n"
                        f"**READMEs Checked:** {len(report.readme_urls)}"
                    ),
                    'inline': True
                },
                {
                    'name': '📅 Activity by Week',
                    'value': '\n'.join(week_breakdown) if week_breakdown else 'No activity tracked',
                    'inline': False
                },
                {
                    'name': '🔀 Merge Requests',
                    'value': '\n'.join(mr_summary[:5]) if mr_summary else 'No MRs found',
                    'inline': False
                }
            ],
            'readme_urls': report.readme_urls,
            'validation_errors': report.validation_errors
        }
    
    def export_reports_csv(self, reports: List[StudentReport]) -> bytes:
        """Export all reports to a CSV file.
        
        Returns:
            CSV content as bytes
        """
        output = io.StringIO()
        writer = csv.writer(output)
        
        writer.writerow([
            'Member ID', 'Name', 'GitLab Username',
            'Total Commits', 'Total MRs', 'Open MRs', 'Closed MRs', 'Merged MRs',
            'Owned Commits', 'Owned MRs',
            'Wed Submissions', 'Sun Submissions', 'Total Expected Weeks',
            'Wed Submission Weeks', 'Sun Submission Weeks',
            'README URLs', 'Weeks With Commits', 'Weeks With MRs'
        ])
        
        for report in reports:
            writer.writerow([
                report.member_id,
                report.name,
                report.gitlab_username,
                report.total_commits,
                report.total_mrs,
                report.open_mrs,
                report.closed_mrs,
                report.merged_mrs,
                report.owned_commits,
                report.owned_mrs,
                report.total_wed_submissions,
                report.total_sun_submissions,
                report.total_expected_weeks,
                ', '.join(map(str, report.weeks_with_wed_submissions)),
                ', '.join(map(str, report.weeks_with_sun_submissions)),
                '; '.join(report.readme_urls),
                ', '.join(map(str, report.weeks_with_commits)),
                ', '.join(map(str, report.weeks_with_mrs))
            ])
        
        return output.getvalue().encode('utf-8')
