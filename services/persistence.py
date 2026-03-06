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
    
    @staticmethod
    def load_game_points() -> Dict[str, int]:
        """Load game points from JSON file.
        
        Returns:
            Dict mapping discord_username to points
        """
        game_points: Dict[str, int] = {}
        
        try:
            if os.path.exists(Config.GAME_POINTS_FILE):
                with open(Config.GAME_POINTS_FILE, 'r') as f:
                    game_points = json.load(f)
        except Exception as e:
            print(f"Error loading game points: {e}")
        
        return game_points
    
    @staticmethod
    def save_game_points(game_points: Dict[str, int]) -> None:
        """Save game points to JSON file."""
        try:
            with open(Config.GAME_POINTS_FILE, 'w') as f:
                json.dump(game_points, f, indent=2)
        except Exception as e:
            print(f"Error saving game points: {e}")
    
    @staticmethod
    def load_trivia_state() -> Dict[str, Any]:
        """Load trivia state from JSON file.
        
        Returns:
            Dict with trivia state (channel_id, used_questions, current_question, answered_by, interval_minutes)
        """
        defaults: Dict[str, Any] = {
            'channel_id': None,
            'used_questions': [],
            'current_question': None,
            'answered_by': None,
            'interval_minutes': 5,
            'question_number': 0,
            'trivia_points': {}
        }
        
        try:
            if os.path.exists(Config.TRIVIA_STATE_FILE):
                with open(Config.TRIVIA_STATE_FILE, 'r') as f:
                    loaded = json.load(f)
                    # Merge with defaults to ensure new fields exist
                    defaults.update(loaded)
        except Exception as e:
            print(f"Error loading trivia state: {e}")
        
        return defaults
    
    @staticmethod
    def save_trivia_state(trivia_state: Dict[str, Any]) -> None:
        """Save trivia state to JSON file."""
        try:
            with open(Config.TRIVIA_STATE_FILE, 'w') as f:
                json.dump(trivia_state, f, indent=2)
        except Exception as e:
            print(f"Error saving trivia state: {e}")
    
    @staticmethod
    def load_trivia_questions() -> List[Dict[str, Any]]:
        """Load trivia questions from JSON file.
        
        Returns:
            List of question dicts with id, question, answer
        """
        questions: List[Dict[str, Any]] = []
        
        try:
            if os.path.exists(Config.TRIVIA_QUESTIONS_FILE):
                with open(Config.TRIVIA_QUESTIONS_FILE, 'r') as f:
                    data = json.load(f)
                    questions = data.get('questions', [])
        except Exception as e:
            print(f"Error loading trivia questions: {e}")
        
        return questions
    
    @staticmethod
    def get_trivia_points() -> int:
        """Get points awarded per correct trivia answer.
        
        Returns:
            Points per correct answer (default 10)
        """
        try:
            if os.path.exists(Config.TRIVIA_QUESTIONS_FILE):
                with open(Config.TRIVIA_QUESTIONS_FILE, 'r') as f:
                    data = json.load(f)
                    return data.get('points_per_correct', 10)
        except Exception:
            pass
        return 10
    
    @staticmethod
    def load_community_state() -> Dict[str, Any]:
        """Load community tracking state from JSON file.
        
        Returns:
            Dict with community state including channels, points, and settings
        """
        defaults: Dict[str, Any] = {
            'channels': {},
            'community_points': {},
            'default_points': {
                'first_post': 5,
                'first_response': 8,
                'subsequent_response': 2
            },
            'processed_messages': {}
        }
        
        try:
            if os.path.exists(Config.COMMUNITY_STATE_FILE):
                with open(Config.COMMUNITY_STATE_FILE, 'r') as f:
                    loaded = json.load(f)
                    defaults.update(loaded)
        except Exception as e:
            print(f"Error loading community state: {e}")
        
        return defaults
    
    @staticmethod
    def save_community_state(community_state: Dict[str, Any]) -> None:
        """Save community tracking state to JSON file."""
        try:
            with open(Config.COMMUNITY_STATE_FILE, 'w') as f:
                json.dump(community_state, f, indent=2)
        except Exception as e:
            print(f"Error saving community state: {e}")
    
    @staticmethod
    def load_dm_feed_channel() -> Optional[int]:
        """Load DM feed channel ID from JSON file.
        
        Returns:
            Channel ID if set, None otherwise
        """
        try:
            if os.path.exists(Config.DM_FEED_FILE):
                with open(Config.DM_FEED_FILE, 'r') as f:
                    data = json.load(f)
                    return data.get('channel_id')
        except Exception as e:
            print(f"Error loading DM feed channel: {e}")
        
        return None
    
    @staticmethod
    def save_dm_feed_channel(channel_id: Optional[int]) -> None:
        """Save DM feed channel ID to JSON file."""
        try:
            with open(Config.DM_FEED_FILE, 'w') as f:
                json.dump({'channel_id': channel_id}, f, indent=2)
        except Exception as e:
            print(f"Error saving DM feed channel: {e}")

