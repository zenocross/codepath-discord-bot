# GitLab RSS Discord Bot

A Discord bot that monitors GitLab RSS feeds for new issues and posts them to Discord channels with customizable label filtering.

## Features

- 📡 Subscribe Discord channels to GitLab project RSS feeds
- 🏷️ Filter issues by specific labels
- 🎨 Color-coded embeds based on issue type
- 💾 Persistent storage of subscriptions and seen issues
- ⚙️ Easy configuration per channel

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Create a Discord Bot

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and give it a name
3. Go to the "Bot" section and click "Add Bot"
4. Under "Privileged Gateway Intents", enable:
   - Message Content Intent
5. Copy the bot token

### 3. Invite the Bot to Your Server

1. Go to the "OAuth2" > "URL Generator" section
2. Select scopes: `bot`
3. Select bot permissions:
   - Send Messages
   - Embed Links
   - Read Message History
4. Copy the generated URL and open it in your browser to invite the bot

### 4. Set Environment Variable

```bash
export DISCORD_BOT_TOKEN='your-bot-token-here'
```

Or on Windows:
```cmd
set DISCORD_BOT_TOKEN=your-bot-token-here
```

### 5. Run the Bot

```bash
python gitlab-discord-bot.py
```

## Getting the GitLab RSS Feed URL

GitLab provides RSS/Atom feeds for issues. To get the URL:

1. Go to your GitLab project
2. Navigate to Issues
3. The RSS feed URL format is:
   ```
   https://gitlab.com/[namespace]/[project]/-/issues.atom
   ```

For filtered feeds (e.g., only open issues):
```
https://gitlab.com/[namespace]/[project]/-/issues.atom?state=opened
```

## Usage

All commands use the prefix `!gitlab`

### Subscribe to a Feed

Subscribe the current channel to a GitLab RSS feed:

```
!gitlab subscribe https://gitlab.com/group/project/-/issues.atom
```

### Filter by Labels

Filter issues to only show specific labels:

```
!gitlab filter backend frontend type::bug
```

To track only quick wins and bugs:
```
!gitlab filter quick-win type::bug
```

To track community bonus issues:
```
!gitlab filter community-bonus::100 community-bonus::200 community-bonus::500
```

To clear all filters (show all issues):
```
!gitlab filter
```

### Check Status

View the current subscription settings:

```
!gitlab status
```

### View Available Labels

See all supported labels:

```
!gitlab labels
```

### Unsubscribe

Remove the subscription from the current channel:

```
!gitlab unsubscribe
```

### Help

Display help information:

```
!gitlab help
```

## Available Labels

The bot supports filtering by these GitLab labels:

**Component:**
- `backend`
- `frontend`
- `documentation`

**Type:**
- `type::bug`
- `type::feature`
- `type::maintenance`

**Difficulty:**
- `quick-win`
- `quick-win::first-time-contributor`

**Community Bonus:**
- `community-bonus::100`
- `community-bonus::200`
- `community-bonus::300`
- `community-bonus::500`

**Other:**
- `co-create`

## How It Works

1. The bot checks RSS feeds every 5 minutes (configurable)
2. New issues are detected and checked against label filters
3. Matching issues are posted to Discord as rich embeds
4. Seen issues are tracked to avoid duplicates
5. Subscriptions persist across bot restarts in `subscriptions.json`

## Customization

### Change Check Interval

Edit `CHECK_INTERVAL_MINUTES` in the code:

```python
CHECK_INTERVAL_MINUTES = 5  # Check every 5 minutes
```

### Modify Label Colors

The bot automatically colors embeds:
- 🔴 Red for bugs
- 🟢 Green for features
- 🔵 Blue for other issues

You can customize this in the `post_issue` method.

## Example Workflow

1. **Set up monitoring for a project:**
   ```
   !gitlab subscribe https://gitlab.com/myorg/myproject/-/issues.atom
   ```

2. **Filter for beginner-friendly issues:**
   ```
   !gitlab filter quick-win::first-time-contributor
   ```

3. **Check what's being tracked:**
   ```
   !gitlab status
   ```

4. **Add more labels to track:**
   ```
   !gitlab filter quick-win::first-time-contributor community-bonus::100 type::bug
   ```

## Troubleshooting

**Bot doesn't respond:**
- Check that Message Content Intent is enabled in Discord Developer Portal
- Verify the bot has permission to send messages in the channel

**No issues appearing:**
- Verify the RSS URL is correct (test in a browser)
- Check if label filters are too restrictive
- Wait up to 5 minutes for the next check cycle

**Issues not matching labels:**
- GitLab must tag issues with labels in the RSS feed
- Some labels may use different formats in the feed (hyphens vs colons)

## Files

- `gitlab-discord-bot.py` - Main bot code
- `requirements.txt` - Python dependencies
- `subscriptions.json` - Automatically created to store subscription data

## Notes

- Each Discord channel can have one RSS feed subscription
- Multiple channels can subscribe to the same feed with different filters
- The bot remembers which issues it has already posted
- Subscriptions and seen issues persist across bot restarts

## License

Free to use and modify as needed!
