"""GitLab API service for fetching commit and MR data from student repositories.

Provides functionality to:
- Fetch README content from GitLab repos
- Parse commit and MR links from README content
- Verify commits and MRs via GitLab API
- Extract weekly commit data and MR status
"""

import os
import re
import json
import base64
import urllib.request
import urllib.error
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

GITLAB_URL = "https://gitlab.com"

# Regex patterns for GitLab links
COMMIT_PATTERN = re.compile(
    r'https?://gitlab\.com/([^/]+(?:/[^/]+)+)/-/commit/([a-f0-9]+)',
    re.IGNORECASE
)
MR_PATTERN = re.compile(
    r'https?://gitlab\.com/([^/]+(?:/[^/]+)+)/-/merge_requests/(\d+)',
    re.IGNORECASE
)

# Pattern to extract repo path from README link (GitHub or GitLab)
README_REPO_PATTERN = re.compile(
    r'https?://(?:github|gitlab)\.com/([^/]+/[^/]+)',
    re.IGNORECASE
)


@dataclass
class GitLabResult:
    """Result of GitLab API enrichment for a student."""
    success: bool
    readme_found: bool = False
    readme_owned_by_student: bool = False
    mr_in_readme: bool = False
    mr_status: str = ""
    mr_created_date: str = ""
    mr_comment_count: int = 0
    commits_this_week: int = 0
    last_commit_date: str = ""
    days_since_commit: int = 0
    commit_links_found: int = 0
    mr_links_found: int = 0
    commits_not_owned: int = 0
    error_message: str = ""


class GitLabService:
    """Service for interacting with GitLab API."""
    
    def __init__(self, token: Optional[str] = None):
        """Initialize with optional GitLab token.
        
        Args:
            token: GitLab personal access token. If None, reads from GITLAB_TOKEN env var.
        """
        self.token = token or os.environ.get("GITLAB_TOKEN")
        if self.token:
            print(f"[GitLabService] Initialized with token")
        else:
            print(f"[GitLabService] No token - API rate limits apply (60 req/hour)")
    
    def _make_request(self, url: str, timeout: int = 10) -> Optional[Dict]:
        """Make an authenticated request to GitLab API.
        
        Args:
            url: The API URL to request
            timeout: Request timeout in seconds
        
        Returns:
            The parsed JSON response, or None on error
        """
        import socket
        import ssl
        
        req = urllib.request.Request(url)
        if self.token:
            req.add_header("PRIVATE-TOKEN", self.token)
        
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 401:
                print(f"[GitLabService] Auth error (401): Token may be invalid")
            elif e.code == 403:
                print(f"[GitLabService] Forbidden (403): Token may lack read_api scope")
            elif e.code == 404:
                pass  # Expected for missing resources
            return None
        except urllib.error.URLError as e:
            print(f"[GitLabService] URL error: {e}")
            return None
        except (socket.timeout, TimeoutError) as e:
            print(f"[GitLabService] Timeout: {e}")
            return None
        except ssl.SSLError as e:
            print(f"[GitLabService] SSL error: {e}")
            return None
        except Exception as e:
            print(f"[GitLabService] Request error: {e}")
            return None
    
    def fetch_readme(self, repo_path: str) -> Optional[str]:
        """Fetch README.md content from a GitLab repository.
        
        Args:
            repo_path: The full path to the repo (e.g., "username/project")
        
        Returns:
            The README content as a string, or None if not found.
        """
        encoded_path = urllib.parse.quote(repo_path, safe="")
        
        # First check if project exists and get default branch
        project_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}"
        project_data = self._make_request(project_url)
        
        if not project_data:
            return None  # Project doesn't exist, skip all README checks
        
        default_branch = project_data.get("default_branch", "main")
        
        # Try fetching README with default branch first
        readme_files = ["README.md", "readme.md"]
        branches = [default_branch] if default_branch else ["main", "master"]
        
        for readme_name in readme_files:
            encoded_file = urllib.parse.quote(readme_name, safe="")
            
            for branch in branches:
                api_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}/repository/files/{encoded_file}?ref={branch}"
                data = self._make_request(api_url)
                
                if data and "content" in data:
                    try:
                        content = base64.b64decode(data["content"]).decode("utf-8")
                        return content
                    except Exception:
                        continue
        
        return None
    
    def fetch_readme_from_github(self, repo_path: str) -> Optional[str]:
        """Fetch README.md content from a GitHub repository.
        
        Args:
            repo_path: The full path to the repo (e.g., "username/project")
        
        Returns:
            The README content as a string, or None if not found.
        """
        # First check if repo exists with a single API call
        repo_check_url = f"https://api.github.com/repos/{repo_path}"
        req = urllib.request.Request(repo_check_url)
        req.add_header("Accept", "application/vnd.github.v3+json")
        req.add_header("User-Agent", "Discord-Bot")
        
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                repo_data = json.loads(response.read().decode("utf-8"))
                default_branch = repo_data.get("default_branch", "main")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # Repo doesn't exist, skip all README checks
            default_branch = "main"
        except Exception:
            default_branch = "main"
        
        # Try fetching README with default branch first, then fallback
        readme_files = ["README.md", "readme.md"]
        branches = [default_branch] if default_branch else ["main", "master"]
        if default_branch and default_branch not in ["main", "master"]:
            branches.append("main")
        
        for readme_name in readme_files:
            for branch in branches:
                api_url = f"https://api.github.com/repos/{repo_path}/contents/{readme_name}?ref={branch}"
                
                req = urllib.request.Request(api_url)
                req.add_header("Accept", "application/vnd.github.v3+json")
                req.add_header("User-Agent", "Discord-Bot")
                
                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        data = json.loads(response.read().decode("utf-8"))
                        if data and "content" in data:
                            content = base64.b64decode(data["content"]).decode("utf-8")
                            return content
                except Exception:
                    continue
        
        return None
    
    def parse_gitlab_links(self, readme_content: str, owner_repo: Optional[str] = None) -> Dict[str, List[Dict]]:
        """Parse GitLab commit and MR links from README content.
        
        Args:
            readme_content: The README content to parse
            owner_repo: Namespace/owner to filter by (e.g., "gitlab-community").
                        Only links to repos under this namespace will be included.
        
        Returns:
            dict with 'commits' and 'merge_requests' lists
        """
        commits = []
        merge_requests = []
        
        # Find all commit links
        for match in COMMIT_PATTERN.finditer(readme_content):
            repo_path = match.group(1)
            commit_sha = match.group(2)
            
            # Filter by owner repo namespace if specified
            if owner_repo and not repo_path.startswith(f"{owner_repo}/"):
                continue
            
            commits.append({
                "repo_path": repo_path,
                "sha": commit_sha,
                "url": match.group(0)
            })
        
        # Find all MR links
        for match in MR_PATTERN.finditer(readme_content):
            repo_path = match.group(1)
            mr_iid = match.group(2)
            
            # Filter by owner repo namespace if specified
            if owner_repo and not repo_path.startswith(f"{owner_repo}/"):
                continue
            
            merge_requests.append({
                "repo_path": repo_path,
                "iid": mr_iid,
                "url": match.group(0)
            })
        
        return {"commits": commits, "merge_requests": merge_requests}
    
    def verify_commit(self, repo_path: str, commit_sha: str) -> Dict[str, Any]:
        """Verify a commit exists and get its details.
        
        Returns:
            dict with commit info including created_at date
        """
        encoded_path = urllib.parse.quote(repo_path, safe="")
        api_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}/repository/commits/{commit_sha}"
        
        data = self._make_request(api_url)
        
        if data:
            return {
                "sha": data.get("id"),
                "short_sha": data.get("short_id"),
                "title": data.get("title"),
                "author_name": data.get("author_name"),
                "author_email": data.get("author_email"),
                "created_at": data.get("created_at"),
                "exists": True
            }
        else:
            return {"sha": commit_sha, "exists": False}
    
    def verify_merge_request(self, repo_path: str, mr_iid: str) -> Dict[str, Any]:
        """Verify a merge request exists and get its status.
        
        Returns:
            dict with MR info including state, comment count
        """
        encoded_path = urllib.parse.quote(repo_path, safe="")
        api_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}/merge_requests/{mr_iid}"
        
        data = self._make_request(api_url)
        
        if data:
            state = data.get("state", "unknown")
            return {
                "iid": data.get("iid"),
                "title": data.get("title"),
                "state": state,
                "author": data.get("author", {}).get("username"),
                "created_at": data.get("created_at"),
                "merged_at": data.get("merged_at"),
                "closed_at": data.get("closed_at"),
                "user_notes_count": data.get("user_notes_count", 0),
                "exists": True
            }
        else:
            return {"iid": mr_iid, "exists": False, "state": "not_found"}
    
    def extract_repo_from_readme_link(self, readme_link: str) -> Tuple[Optional[str], str]:
        """Extract repository path and platform from a README link.
        
        Args:
            readme_link: URL to a README file (GitHub or GitLab)
        
        Returns:
            Tuple of (repo_path, platform) where platform is 'github' or 'gitlab'
        """
        if not readme_link:
            return None, ""
        
        readme_link = readme_link.strip()
        
        if "github.com" in readme_link.lower():
            match = README_REPO_PATTERN.search(readme_link)
            if match:
                return match.group(1), "github"
        elif "gitlab.com" in readme_link.lower():
            match = README_REPO_PATTERN.search(readme_link)
            if match:
                return match.group(1), "gitlab"
        
        return None, ""
    
    def extract_file_path_from_url(self, url: str) -> Optional[str]:
        """Extract the file path from a GitLab or GitHub blob URL.
        
        Args:
            url: Full URL to a file (e.g., https://gitlab.com/user/repo/-/blob/main/path/to/file.md)
        
        Returns:
            The file path (e.g., "path/to/file.md") or None if not a file URL
        """
        if not url:
            return None
        
        url = url.strip()
        
        # GitLab pattern: /-/blob/branch/path/to/file
        gitlab_match = re.search(r'/-/blob/[^/]+/(.+?)(?:\?|$)', url)
        if gitlab_match:
            return gitlab_match.group(1)
        
        # GitHub pattern: /blob/branch/path/to/file
        github_match = re.search(r'/blob/[^/]+/(.+?)(?:\?|$)', url)
        if github_match:
            return github_match.group(1)
        
        return None
    
    def fetch_file_content(self, repo_path: str, file_path: str, platform: str = "gitlab") -> Optional[str]:
        """Fetch content of a specific file from a repository.
        
        Args:
            repo_path: The full path to the repo (e.g., "username/project")
            file_path: The path to the file within the repo (e.g., "contribution-1-README.md")
            platform: 'gitlab' or 'github'
        
        Returns:
            The file content as a string, or None if not found.
        """
        if platform == "github":
            return self._fetch_github_file(repo_path, file_path)
        else:
            return self._fetch_gitlab_file(repo_path, file_path)
    
    def _fetch_gitlab_file(self, repo_path: str, file_path: str) -> Optional[str]:
        """Fetch a specific file from GitLab."""
        encoded_path = urllib.parse.quote(repo_path, safe="")
        
        # Get default branch
        project_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}"
        project_data = self._make_request(project_url)
        
        if not project_data:
            return None
        
        default_branch = project_data.get("default_branch", "main")
        branches = [default_branch] if default_branch else ["main", "master"]
        
        encoded_file = urllib.parse.quote(file_path, safe="")
        
        for branch in branches:
            api_url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}/repository/files/{encoded_file}?ref={branch}"
            data = self._make_request(api_url)
            
            if data and "content" in data:
                try:
                    content = base64.b64decode(data["content"]).decode("utf-8")
                    return content
                except Exception:
                    continue
        
        return None
    
    def _fetch_github_file(self, repo_path: str, file_path: str) -> Optional[str]:
        """Fetch a specific file from GitHub."""
        # Get default branch first
        repo_check_url = f"https://api.github.com/repos/{repo_path}"
        req = urllib.request.Request(repo_check_url)
        req.add_header("Accept", "application/vnd.github.v3+json")
        req.add_header("User-Agent", "Discord-Bot")
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                repo_data = json.loads(response.read().decode("utf-8"))
                default_branch = repo_data.get("default_branch", "main")
        except Exception:
            default_branch = "main"
        
        branches = [default_branch] if default_branch else ["main", "master"]
        
        for branch in branches:
            api_url = f"https://api.github.com/repos/{repo_path}/contents/{file_path}?ref={branch}"
            
            req = urllib.request.Request(api_url)
            req.add_header("Accept", "application/vnd.github.v3+json")
            req.add_header("User-Agent", "Discord-Bot")
            
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    data = json.loads(response.read().decode("utf-8"))
                    if data and "content" in data:
                        content = base64.b64decode(data["content"]).decode("utf-8")
                        return content
            except Exception:
                continue
        
        return None
    
    def get_week_start(self, date: datetime) -> datetime:
        """Get the Monday of the week for a given date."""
        days_since_monday = date.weekday()
        return (date - timedelta(days=days_since_monday)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    
    def enrich_student_data(
        self,
        readme_link: str,
        mr_url: str,
        owner_repo: Optional[str] = None,
        current_date: Optional[datetime] = None,
        validate_commits: bool = True,
        validate_mrs: bool = True,
        expected_owner: Optional[str] = None
    ) -> GitLabResult:
        """Enrich student data with GitLab API information.
        
        Args:
            readme_link: URL to student's README file
            mr_url: MR URL submitted by student in typeform
            owner_repo: Namespace to filter commits/MRs (None = no filter)
            current_date: Date to calculate "this week" from (defaults to now)
            validate_commits: Whether to validate commit links
            validate_mrs: Whether to validate MR links
            expected_owner: Expected GitHub/GitLab username for ownership validation
        
        Returns:
            GitLabResult with enriched data
        """
        current_date = current_date or datetime.now(timezone.utc)
        current_week_start = self.get_week_start(current_date)
        
        # Extract repo path from readme link
        repo_path, platform = self.extract_repo_from_readme_link(readme_link)
        
        if not repo_path:
            return GitLabResult(
                success=False,
                error_message="Could not extract repo path from README link"
            )
        
        # Check if README is on student's own repo
        readme_owned_by_student = False
        if expected_owner:
            # repo_path is like "username/repo-name"
            repo_owner = repo_path.split("/")[0].lower() if "/" in repo_path else repo_path.lower()
            readme_owned_by_student = (repo_owner == expected_owner.lower())
        
        # Fetch README content
        if platform == "github":
            readme_content = self.fetch_readme_from_github(repo_path)
        else:
            readme_content = self.fetch_readme(repo_path)
        
        if not readme_content:
            return GitLabResult(
                success=False,
                readme_found=False,
                error_message="README not found or repository inaccessible"
            )
        
        # Parse GitLab links from README (no filtering - get all links)
        links = self.parse_gitlab_links(readme_content, owner_repo=owner_repo)
        
        # Check if submitted MR URL is in the README
        mr_in_readme = False
        mr_status = ""
        mr_created_date = ""
        mr_comment_count = 0
        
        if validate_mrs and mr_url:
            mr_url_normalized = mr_url.strip().rstrip("/").lower()
            for mr_link in links["merge_requests"]:
                if mr_link["url"].lower().rstrip("/") == mr_url_normalized:
                    mr_in_readme = True
                    # Verify MR and get details
                    mr_data = self.verify_merge_request(mr_link["repo_path"], mr_link["iid"])
                    if mr_data.get("exists"):
                        mr_status = mr_data.get("state", "")
                        mr_created_date = mr_data.get("created_at", "")
                        mr_comment_count = mr_data.get("user_notes_count", 0)
                    break
            
            # If not found in parsed links, try to verify the submitted URL directly
            if not mr_in_readme and mr_url:
                mr_match = MR_PATTERN.search(mr_url)
                if mr_match:
                    mr_repo = mr_match.group(1)
                    mr_iid = mr_match.group(2)
                    mr_data = self.verify_merge_request(mr_repo, mr_iid)
                    if mr_data.get("exists"):
                        mr_status = mr_data.get("state", "")
                        mr_created_date = mr_data.get("created_at", "")
                        mr_comment_count = mr_data.get("user_notes_count", 0)
        
        # Process commits to get weekly count and last commit date
        commits_this_week = 0
        last_commit_date = ""
        last_commit_datetime = None
        commits_not_owned = 0
        
        if validate_commits:
            for commit_link in links["commits"]:
                commit_repo_path = commit_link["repo_path"]
                
                # Check ownership - commit should be on a repo owned by the student
                if expected_owner:
                    # Extract owner from commit repo path (e.g., "gitlab-community/gitlab" -> "gitlab-community")
                    commit_repo_owner = commit_repo_path.split("/")[0].lower() if "/" in commit_repo_path else ""
                    
                    # If the commit is NOT on a repo owned by the expected owner, flag it
                    # Note: This checks if the student is committing to someone else's repo
                    # which is actually expected behavior for open source contributions
                    # So we check if commits are on the student's OWN repo (their fork)
                    if commit_repo_owner and commit_repo_owner != expected_owner.lower():
                        commits_not_owned += 1
                
                commit_data = self.verify_commit(commit_repo_path, commit_link["sha"])
                
                if commit_data.get("exists") and commit_data.get("created_at"):
                    try:
                        commit_date_str = commit_data["created_at"]
                        commit_dt = datetime.fromisoformat(commit_date_str.replace("Z", "+00:00"))
                        
                        # Check if commit is in current week
                        commit_week_start = self.get_week_start(commit_dt)
                        if commit_week_start >= current_week_start:
                            commits_this_week += 1
                        
                        # Track last commit date
                        if last_commit_datetime is None or commit_dt > last_commit_datetime:
                            last_commit_datetime = commit_dt
                            last_commit_date = commit_date_str
                    except Exception:
                        continue
        
        # Calculate days since last commit
        days_since_commit = 0
        if last_commit_datetime:
            delta = current_date - last_commit_datetime
            days_since_commit = max(0, delta.days)
        
        return GitLabResult(
            success=True,
            readme_found=True,
            readme_owned_by_student=readme_owned_by_student,
            mr_in_readme=mr_in_readme,
            mr_status=mr_status,
            mr_created_date=mr_created_date,
            mr_comment_count=mr_comment_count,
            commits_this_week=commits_this_week,
            last_commit_date=last_commit_date,
            days_since_commit=days_since_commit,
            commit_links_found=len(links["commits"]),
            mr_links_found=len(links["merge_requests"]),
            commits_not_owned=commits_not_owned
        )








