"""Persistence service for JSON file load/save operations."""

import json
import os
from datetime import datetime
from typing import Dict, Set, List, Any, Optional

from bot.config import Config


class PersistenceService:
    """Handles all JSON file persistence operations."""
    
    @staticmethod
    def load_subscriptions() -> tuple[Dict[int, Dict], Dict[int, Set[str]]]:
        """Load subscriptions from JSON file.
        
        Returns:
            Tuple of (subscriptions dict, seen_issues dict)
        """
        subscriptions: Dict[int, Dict] = {}
        seen_issues: Dict[int, Set[str]] = {}
        
        try:
            if os.path.exists(Config.SUBSCRIPTIONS_FILE):
                with open(Config.SUBSCRIPTIONS_FILE, 'r') as f:
                    data = json.load(f)
                    for channel_id_str, sub_data in data.items():
                        channel_id = int(channel_id_str)
                        subscriptions[channel_id] = {
                            'url': sub_data['url'],
                            'labels': set(sub_data.get('labels', [])),
                            'last_checked': datetime.fromisoformat(
                                sub_data.get('last_checked', datetime.now().isoformat())
                            )
                        }
                        seen_issues[channel_id] = set(sub_data.get('seen_issues', []))
        except Exception as e:
            print(f"Error loading subscriptions: {e}")
        
        return subscriptions, seen_issues
    
    @staticmethod
    def save_subscriptions(subscriptions: Dict[int, Dict], seen_issues: Dict[int, Set[str]]) -> None:
        """Save subscriptions to JSON file."""
        try:
            data = {}
            for channel_id, sub_data in subscriptions.items():
                data[str(channel_id)] = {
                    'url': sub_data['url'],
                    'labels': list(sub_data['labels']),
                    'last_checked': sub_data['last_checked'].isoformat(),
                    'seen_issues': list(seen_issues.get(channel_id, []))
                }
            with open(Config.SUBSCRIPTIONS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving subscriptions: {e}")
    
    @staticmethod
    def load_channel_groups() -> Dict[str, List[int]]:
        """Load channel groups from JSON file."""
        channel_groups: Dict[str, List[int]] = {}
        
        try:
            if os.path.exists(Config.CHANNEL_GROUPS_FILE):
                with open(Config.CHANNEL_GROUPS_FILE, 'r') as f:
                    channel_groups = json.load(f)
        except Exception as e:
            print(f"Error loading channel groups: {e}")
        
        return channel_groups
    
    @staticmethod
    def save_channel_groups(channel_groups: Dict[str, List[int]]) -> None:
        """Save channel groups to JSON file."""
        try:
            with open(Config.CHANNEL_GROUPS_FILE, 'w') as f:
                json.dump(channel_groups, f, indent=2)
        except Exception as e:
            print(f"Error saving channel groups: {e}")
    
    @staticmethod
    def load_dm_groups() -> Dict[str, List[Dict[str, Any]]]:
        """Load DM groups from JSON file.
        
        Returns:
            Dict mapping group names to lists of user dicts {user_id: int, username: str}
        """
        dm_groups: Dict[str, List[Dict[str, Any]]] = {}
        
        try:
            if os.path.exists(Config.DM_GROUPS_FILE):
                with open(Config.DM_GROUPS_FILE, 'r') as f:
                    dm_groups = json.load(f)
        except Exception as e:
            print(f"Error loading DM groups: {e}")
        
        return dm_groups
    
    @staticmethod
    def save_dm_groups(dm_groups: Dict[str, List[Dict[str, Any]]]) -> None:
        """Save DM groups to JSON file."""
        try:
            with open(Config.DM_GROUPS_FILE, 'w') as f:
                json.dump(dm_groups, f, indent=2)
        except Exception as e:
            print(f"Error saving DM groups: {e}")
    
    @staticmethod
    def load_scheduled_messages() -> Dict[str, Dict]:
        """Load scheduled messages from JSON file."""
        scheduled_messages: Dict[str, Dict] = {}
        
        try:
            if os.path.exists(Config.SCHEDULED_MESSAGES_FILE):
                with open(Config.SCHEDULED_MESSAGES_FILE, 'r') as f:
                    data = json.load(f)
                    for schedule_id, sched in data.items():
                        sched['next_run'] = (
                            datetime.fromisoformat(sched['next_run']) 
                            if sched.get('next_run') else None
                        )
                        sched['last_sent'] = (
                            datetime.fromisoformat(sched['last_sent']) 
                            if sched.get('last_sent') else None
                        )
                        scheduled_messages[schedule_id] = sched
        except Exception as e:
            print(f"Error loading scheduled messages: {e}")
        
        return scheduled_messages
    
    @staticmethod
    def save_scheduled_messages(scheduled_messages: Dict[str, Dict]) -> None:
        """Save scheduled messages to JSON file."""
        try:
            data = {}
            for schedule_id, sched in scheduled_messages.items():
                sched_data = {**sched}
                # Serialize datetime fields
                if sched.get('next_run'):
                    sched_data['next_run'] = (
                        sched['next_run'].isoformat() 
                        if hasattr(sched['next_run'], 'isoformat') 
                        else sched['next_run']
                    )
                if sched.get('last_sent'):
                    sched_data['last_sent'] = (
                        sched['last_sent'].isoformat() 
                        if hasattr(sched['last_sent'], 'isoformat') 
                        else sched['last_sent']
                    )
                data[schedule_id] = sched_data
            with open(Config.SCHEDULED_MESSAGES_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving scheduled messages: {e}")
    
    @staticmethod
    def load_allowed_users() -> Set[int]:
        """Load allowed users from JSON file."""
        allowed_users: Set[int] = set()
        
        try:
            if os.path.exists(Config.ALLOWED_USERS_FILE):
                with open(Config.ALLOWED_USERS_FILE, 'r') as f:
                    allowed_users = set(json.load(f))
            # Always include bot owner
            if Config.BOT_OWNER_ID:
                allowed_users.add(Config.BOT_OWNER_ID)
        except Exception as e:
            print(f"Error loading allowed users: {e}")
        
        return allowed_users
    
    @staticmethod
    def save_allowed_users(allowed_users: Set[int]) -> None:
        """Save allowed users to JSON file."""
        try:
            with open(Config.ALLOWED_USERS_FILE, 'w') as f:
                json.dump(list(allowed_users), f, indent=2)
        except Exception as e:
            print(f"Error saving allowed users: {e}")

