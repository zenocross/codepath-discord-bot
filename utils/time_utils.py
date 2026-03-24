"""Time utility functions for formatting and calculations."""

from datetime import datetime, timedelta, timezone
from typing import Optional


# Week cutoff: Wednesday 5PM UTC (12PM EST / 9AM PST)
WEEK_CUTOFF_DAY = 2  # Wednesday (Monday=0)
WEEK_CUTOFF_HOUR_UTC = 17  # 5PM UTC


def get_program_week(start_date: datetime, target_date: Optional[datetime] = None) -> int:
    """Calculate program week number with Wednesday 5PM UTC cutoff.
    
    Week transitions occur on Wednesday at 5PM UTC (12PM EST / 9AM PST).
    If target_date is before Wednesday 5PM UTC, it counts as the previous week.
    
    Args:
        start_date: Program start date
        target_date: Date to calculate week for (defaults to now)
        
    Returns:
        Week number (1-based)
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc)
    
    # Make dates timezone-aware if needed
    if target_date.tzinfo is None:
        target_date = target_date.replace(tzinfo=timezone.utc)
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    
    # Find the Wednesday 5PM UTC cutoff that applies to target_date
    days_since_cutoff_day = (target_date.weekday() - WEEK_CUTOFF_DAY) % 7
    
    # Calculate the most recent Wednesday at 5PM UTC
    last_cutoff = target_date.replace(
        hour=WEEK_CUTOFF_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    last_cutoff = last_cutoff - timedelta(days=days_since_cutoff_day)
    
    # If we haven't reached the cutoff time yet, use the previous week's cutoff
    if target_date < last_cutoff:
        last_cutoff = last_cutoff - timedelta(days=7)
    
    # Calculate weeks from start date to this cutoff
    days_since_start = (last_cutoff - start_date).days
    return max(1, (days_since_start // 7) + 1)


def format_time_until(target: Optional[datetime]) -> str:
    """Format time remaining until a datetime.
    
    Args:
        target: Target datetime (should be timezone-aware or will be treated as UTC)
        
    Returns:
        Human-readable time remaining string
    """
    if target is None:
        return "N/A"
    
    now = datetime.now(timezone.utc)
    
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    
    delta = target - now
    
    if delta.total_seconds() < 0:
        return "overdue"
    
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes = remainder // 60
    
    if hours > 24:
        days = hours // 24
        hours = hours % 24
        return f"{days}d {hours}h {minutes}m"
    
    return f"{hours}h {minutes}m"


def format_datetime_gmt(dt: Optional[datetime], format_str: str = '%Y-%m-%d %H:%M GMT') -> str:
    """Format a datetime in GMT/UTC with a custom format.
    
    Args:
        dt: datetime to format
        format_str: strftime format string
        
    Returns:
        Formatted datetime string or 'N/A' if None
    """
    if dt is None:
        return "N/A"
    return dt.strftime(format_str)


def parse_time_string(time_str: str) -> tuple[int, int]:
    """Parse a time string in HH:MM format.
    
    Args:
        time_str: Time string like "09:00" or "14:30"
        
    Returns:
        Tuple of (hour, minute)
        
    Raises:
        ValueError: If format is invalid or values out of range
    """
    parts = time_str.split(':')
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Time out of range: {time_str}")
    
    return hour, minute


def parse_day_of_week(day_str: str) -> int:
    """Parse a day of week string to integer.
    
    Args:
        day_str: Day string like 'mon', 'tue', 'monday', etc.
        
    Returns:
        Integer 0-6 (Monday = 0)
        
    Raises:
        ValueError: If day string is not recognized
    """
    days = {
        'mon': 0, 'monday': 0,
        'tue': 1, 'tuesday': 1,
        'wed': 2, 'wednesday': 2,
        'thu': 3, 'thursday': 3,
        'fri': 4, 'friday': 4,
        'sat': 5, 'saturday': 5,
        'sun': 6, 'sunday': 6,
    }
    
    normalized = day_str.lower().strip()
    
    if normalized in days:
        return days[normalized]
    
    # Try first 3 characters
    if normalized[:3] in days:
        return days[normalized[:3]]
    
    raise ValueError(f"Unrecognized day: {day_str}")


# Re-export scheduler functions for backwards compatibility
from services.scheduler_service import SchedulerService

calculate_next_run = SchedulerService.calculate_next_run
get_interval_delta = SchedulerService.get_interval_delta

