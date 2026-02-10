"""Scheduler service for managing scheduled message timing."""

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional


class SchedulerService:
    """Handles schedule time calculations and management."""
    
    @staticmethod
    def calculate_next_run(schedule_type: str, config: Dict) -> datetime:
        """Calculate the next run time for a scheduled message - aligned to flat time boundaries.
        
        Args:
            schedule_type: Type of schedule ('minutely', 'hourly', 'daily', 'weekly')
            config: Configuration dict with timing parameters
            
        Returns:
            datetime of the next scheduled run (UTC)
        """
        now = datetime.now(timezone.utc)
        
        if schedule_type == 'minutely':
            minutes = config.get('minutes', 5)
            # Round down to current flat minute, then find next aligned minute
            base = now.replace(second=0, microsecond=0)
            # Find the next minute that's aligned to the interval
            current_minute = base.minute
            # Calculate next aligned minute mark
            next_aligned = ((current_minute // minutes) + 1) * minutes
            if next_aligned >= 60:
                # Rolls over to next hour
                hours_to_add = next_aligned // 60
                next_aligned = next_aligned % 60
                base = base + timedelta(hours=hours_to_add)
            return base.replace(minute=next_aligned)
        
        elif schedule_type == 'hourly':
            hours = config.get('hours', 1)
            # Align to flat hour boundaries
            base = now.replace(minute=0, second=0, microsecond=0)
            current_hour = base.hour
            # Calculate next aligned hour mark
            next_aligned = ((current_hour // hours) + 1) * hours
            if next_aligned >= 24:
                # Rolls over to next day
                days_to_add = next_aligned // 24
                next_aligned = next_aligned % 24
                base = base + timedelta(days=days_to_add)
            return base.replace(hour=next_aligned)
        
        elif schedule_type == 'daily':
            target_hour = config.get('hour', 9)
            target_minute = config.get('minute', 0)
            next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            return next_run
        
        elif schedule_type == 'weekly':
            target_day = config.get('day', 0)  # 0 = Monday
            target_hour = config.get('hour', 9)
            target_minute = config.get('minute', 0)
            days_ahead = target_day - now.weekday()
            if days_ahead < 0 or (days_ahead == 0 and now.hour * 60 + now.minute >= target_hour * 60 + target_minute):
                days_ahead += 7
            next_run = now + timedelta(days=days_ahead)
            next_run = next_run.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            return next_run
        
        return now + timedelta(hours=1)  # Default fallback
    
    @staticmethod
    def get_interval_delta(schedule_type: str, config: Dict) -> timedelta:
        """Get the interval timedelta for a schedule type.
        
        Args:
            schedule_type: Type of schedule
            config: Configuration dict with timing parameters
            
        Returns:
            timedelta representing the interval
        """
        if schedule_type == 'minutely':
            return timedelta(minutes=config.get('minutes', 5))
        elif schedule_type == 'hourly':
            return timedelta(hours=config.get('hours', 1))
        elif schedule_type == 'daily':
            return timedelta(days=1)
        elif schedule_type == 'weekly':
            return timedelta(weeks=1)
        return timedelta(hours=1)  # Default fallback
    
    @staticmethod
    def format_schedule_frequency(schedule_type: str, config: Dict) -> str:
        """Format a schedule's frequency for display.
        
        Args:
            schedule_type: Type of schedule
            config: Configuration dict with timing parameters
            
        Returns:
            Human-readable frequency string
        """
        if schedule_type == 'minutely':
            return f"Every {config.get('minutes', 5)} minutes"
        elif schedule_type == 'hourly':
            return f"Every {config.get('hours', 1)} hours"
        elif schedule_type == 'daily':
            return f"Daily at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        elif schedule_type == 'weekly':
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            return f"Every {days[config.get('day', 0)]} at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        return schedule_type
    
    @staticmethod
    def format_schedule_frequency_short(schedule_type: str, config: Dict) -> str:
        """Format a schedule's frequency in short form for display.
        
        Args:
            schedule_type: Type of schedule
            config: Configuration dict with timing parameters
            
        Returns:
            Short human-readable frequency string
        """
        if schedule_type == 'minutely':
            return f"Every {config.get('minutes', 5)}m"
        elif schedule_type == 'hourly':
            return f"Every {config.get('hours', 1)}h"
        elif schedule_type == 'daily':
            return f"Daily at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        elif schedule_type == 'weekly':
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            return f"Weekly on {days[config.get('day', 0)]} at {config.get('hour', 0):02d}:{config.get('minute', 0):02d} GMT"
        return schedule_type
    
    @staticmethod
    def is_recently_sent(last_sent: Optional[datetime], threshold_seconds: int = 30) -> bool:
        """Check if a schedule was recently sent (to prevent duplicates).
        
        Args:
            last_sent: datetime of last send, or None
            threshold_seconds: How recent counts as "recently sent"
            
        Returns:
            True if sent within threshold, False otherwise
        """
        if not last_sent:
            return False
        
        now = datetime.now(timezone.utc)
        
        if isinstance(last_sent, str):
            last_sent = datetime.fromisoformat(last_sent)
        
        if last_sent.tzinfo is None:
            last_sent = last_sent.replace(tzinfo=timezone.utc)
        
        return (now - last_sent).total_seconds() < threshold_seconds

