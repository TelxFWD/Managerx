import asyncio
import json
import logging
import os
import signal
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button, utils
from telethon.tl.functions.channels import EditBannedRequest, GetParticipantsRequest
from telethon.tl.types import ChatBannedRights, ChannelParticipantsSearch, ChannelParticipantsAdmins
from telethon.errors.rpcerrorlist import UserNotParticipantError, ChannelPrivateError, FloodWaitError, ChatAdminRequiredError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)

# Load environment variables from .env file
load_dotenv()

# Get API credentials from environment variables
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
bot_token = os.getenv('TELEGRAM_BOT_TOKEN')

# Check if credentials are available
if not all([api_id, api_hash, bot_token]):
    logger.error("Missing API credentials. Please set them in .env file.")
    raise ValueError("Missing API credentials. Please set them in .env file.")

# Convert api_id to int if it's a string
api_id = int(api_id) if api_id else None

# Create bot instance with proper session name
bot = TelegramClient('bot_session', api_id, api_hash)

# Admin users who can use sensitive commands (load from env if possible)
admin_users_str = os.getenv('ADMIN_USERS', '7819751787,1558210832,8052410476')
admin_users = set(int(uid.strip()) for uid in admin_users_str.split(',') if uid.strip().isdigit())

# File paths for persisting data
CHANNEL_GROUPS_FILE = 'channel_groups.json'
AUTHORIZED_USERS_FILE = 'authorized_users.json'

# Configurable delay between actions to avoid Telegram restrictions
DELAY_BETWEEN_ACTIONS = 2  # Seconds, adjustable based on testing

# Create locks for file operations
channel_groups_lock = asyncio.Lock()
authorized_users_lock = asyncio.Lock()

# Initialize global variables
channel_groups = {}
authorized_users = set()

# Load channel groups and authorized users with error handling
async def load_data():
    global channel_groups, authorized_users

    try:
        with open(CHANNEL_GROUPS_FILE, 'r') as f:
            channel_groups = json.load(f)
        logger.info(f"Loaded {len(channel_groups)} channel groups")
    except FileNotFoundError:
        logger.info(f"Channel groups file not found. Creating a new one.")
        channel_groups = {}
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON in channel groups file. Starting with empty data.")
        channel_groups = {}

    try:
        with open(AUTHORIZED_USERS_FILE, 'r') as f:
            authorized_users = set(json.load(f))
        logger.info(f"Loaded {len(authorized_users)} authorized users")
    except FileNotFoundError:
        logger.info(f"Authorized users file not found. Creating a new one.")
        authorized_users = set()
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON in authorized users file. Starting with empty data.")
        authorized_users = set()

# Save channel groups with lock
async def save_channel_groups():
    async with channel_groups_lock:
        try:
            with open(CHANNEL_GROUPS_FILE, 'w') as f:
                json.dump(channel_groups, f)
            logger.info("Channel groups saved successfully")
        except Exception as e:
            logger.error(f"Error saving channel groups: {e}")

# Save authorized users with lock
async def save_authorized_users():
    async with authorized_users_lock:
        try:
            with open(AUTHORIZED_USERS_FILE, 'w') as f:
                json.dump(list(authorized_users), f)
            logger.info("Authorized users saved successfully")
        except Exception as e:
            logger.error(f"Error saving authorized users: {e}")

# Helper function to check if the user is an admin
def is_admin(user_id):
    return user_id in admin_users if user_id else False

# Helper function for self-healing retry with exponential backoff
async def retry_operation(operation, max_retries=3, base_delay=1):
    retries = 0
    while retries < max_retries:
        try:
            await operation()
            logger.info("Operation succeeded")
            return True  # Indicate success
        except FloodWaitError as fwe:
            wait_time = fwe.seconds
            logger.warning(f"Flood wait detected. Waiting {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            retries += 1
        except Exception as e:
            logger.error(f"Operation failed: {e}")
            if retries == max_retries - 1:
                logger.error(f"Max retries reached. Operation failed.")
                return False  # Indicate failure
            wait_time = base_delay * (2 ** retries)
            logger.info(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            retries += 1
    return False

# Handle the '/start' command
@bot.on(events.NewMessage(pattern='/start'))
async def start_command(event):
    welcome_message = """
👋 **Welcome to BanBot!**

This bot helps manage users across multiple Telegram channels.

**Key commands:**
- `/help` - Show all available commands
- `/ban` - Ban a user from a group of channels
- `/unban` - Unban a user
- `/stats` - Show channel statistics

**Admin features:**
- Channel grouping
- User authorization
- Bulk moderation

Send `/help` to see detailed command usage.
"""
    await event.reply(welcome_message)

# Handle the '/list' command
@bot.on(events.NewMessage(pattern='/list'))
async def list_groups(event):
    if not is_admin(event.sender_id):
        await event.reply("You are not authorized to use this command.")
        return

    if not channel_groups:
        await event.reply("No channel groups defined yet. Use `/addchannel` to create groups.")
        return

    group_names = "\n".join([f"`{group}` - {len(channels)} channels" for group, channels in channel_groups.items()])
    await event.reply(f"📋 **Available Groups:**\n\n{group_names}")

# Handle the '/help' command
@bot.on(events.NewMessage(pattern='/help'))
async def help_command(event):
    help_text = """
**BanBot Commands:**

`/ban [username/user_id] [group_name] [delay_in_minutes]`
   - Bans the specified user from all channels in the given group after the specified delay (in minutes).

`/unban [username/user_id] [group_name]`
   - Unbans the specified user from all channels in the given group.

`/unbanall [username/user_id]`
   - Unbans the specified user from all managed channels.

`/stats [group_name]`
   - Shows member details (name, username, joined date) for each channel in the specified group.

`/addchannel [group_name] [channel_id/username]`
   - Adds a channel to the specified group.

`/authorize [user_id/username]`
   - Authorizes a user for all managed channels.

`/deauthorize [user_id/username]`
   - Deauthorizes a user from all managed channels.

`/kickunauthorized [group_name]`
   - Kicks all unauthorized users from channels in the specified group.

`/kickallunauthorized`
   - Kicks all unauthorized users from all managed channels.

`/list`
   - Lists all available channel groups.

`/testchannel [channel_id/username]`
   - Tests if the bot can resolve a channel ID or username.

**Additional Features:**
- Send a username to the bot in a private message to get ban options.

**Available Group Names:**
{}
    """.format("\n".join(f"- `{group}` ({len(channels)} channels)" for group, channels in channel_groups.items()))

    await event.reply(help_text)

# Handle the '/addchannel' command
@bot.on(events.NewMessage(pattern='/addchannel'))
async def add_channel(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 3:
        await event.reply('⚠️ Invalid command format. Usage: `/addchannel [group_name] [channel_id/username]`')
        return

    group_name = args[1].lower()
    channel_identifier = args[2]

    try:
        if channel_identifier.startswith('-100') and channel_identifier[3:].isdigit():
            channel_id = int(channel_identifier)
            channel = await bot.get_entity(channel_id)
        else:
            channel = await bot.get_entity(channel_identifier)
            channel_id = channel.id

        # Verify bot has admin rights
        admins = await bot.get_participants(channel, filter=ChannelParticipantsAdmins)
        bot_id = (await bot.get_me()).id
        if not any(participant.user_id == bot_id for participant in admins):
            await event.reply(f'⚠️ Bot is not an admin in {channel.title or channel_id}. Please grant admin rights and try again.')
            return

        if group_name not in channel_groups:
            channel_groups[group_name] = []

        if channel_id not in channel_groups[group_name]:
            channel_groups[group_name].append(channel_id)
            await save_channel_groups()
            await event.reply(f'✅ Channel {channel.title or channel_id} added to group `{group_name}`.')
        else:
            await event.reply(f'ℹ️ Channel {channel.title or channel_id} is already in group `{group_name}`.')
    except ValueError as ve:
        logger.error(f"ValueError in add_channel for {channel_identifier}: {ve}")
        await event.reply('⚠️ Invalid channel ID or username. Ensure the bot is a member of the channel and try again.')
    except Exception as e:
        logger.error(f"Error adding channel {channel_identifier}: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/testchannel' command
@bot.on(events.NewMessage(pattern='/testchannel'))
async def test_channel(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 2:
        await event.reply('⚠️ Invalid command format. Usage: `/testchannel [channel_id/username]`')
        return

    channel_identifier = args[1]
    try:
        if channel_identifier.startswith('-100') and channel_identifier[3:].isdigit():
            channel_id = int(channel_identifier)
            channel = await bot.get_entity(channel_id)
        else:
            channel = await bot.get_entity(channel_identifier)
        await event.reply(f"✅ Successfully resolved {channel.title or channel_id} (ID: -100{channel.id})")
    except ValueError as ve:
        logger.error(f"ValueError testing {channel_identifier}: {ve}")
        await event.reply('⚠️ Cannot resolve channel ID/username. Ensure the bot is a member.')
    except Exception as e:
        logger.error(f"Error testing {channel_identifier}: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/authorize' command
@bot.on(events.NewMessage(pattern='/authorize'))
async def authorize_user(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 2:
        await event.reply('⚠️ Invalid command format. Usage: `/authorize [user_id/username]`')
        return

    user_id_or_username = args[1]

    try:
        if user_id_or_username.isdigit():
            user = await bot.get_entity(int(user_id_or_username))
        else:
            user = await bot.get_entity(user_id_or_username)
        authorized_users.add(user.id)
        await save_authorized_users()
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"
        await event.reply(f'✅ User {user_display} has been authorized.')
    except ValueError:
        await event.reply('⚠️ Invalid user ID or username.')
    except Exception as e:
        logger.error(f"Error authorizing user: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/deauthorize' command
@bot.on(events.NewMessage(pattern='/deauthorize'))
async def deauthorize_user(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 2:
        await event.reply('⚠️ Invalid command format. Usage: `/deauthorize [user_id/username]`')
        return

    user_id_or_username = args[1]

    try:
        if user_id_or_username.isdigit():
            user = await bot.get_entity(int(user_id_or_username))
        else:
            user = await bot.get_entity(user_id_or_username)
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"

        if user.id in authorized_users:
            authorized_users.remove(user.id)
            await save_authorized_users()
            await event.reply(f'✅ User {user_display} has been deauthorized.')
        else:
            await event.reply(f'ℹ️ User {user_display} was not in the authorized list.')
    except ValueError:
        await event.reply('⚠️ Invalid user ID or username.')
    except Exception as e:
        logger.error(f"Error deauthorizing user: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/kickunauthorized' command
@bot.on(events.NewMessage(pattern='/kickunauthorized'))
async def kick_unauthorized(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 2:
        await event.reply('⚠️ Invalid command format. Usage: `/kickunauthorized [group_name]`')
        return

    group_name = args[1].lower()

    if group_name not in channel_groups:
        await event.reply(f'⚠️ Group not found. Available groups: {", ".join(channel_groups.keys())}')
        return

    progress_message = await event.reply(f"🔄 Starting to kick unauthorized users from `{group_name}` channels. This may take a while...")

    results = []
    total_kicked = 0
    total_channels = len(channel_groups[group_name])
    processed_channels = 0

    for channel_id in channel_groups[group_name]:
        try:
            channel = await bot.get_entity(channel_id)
            channel_name = getattr(channel, 'title', str(channel_id))

            participants = await bot(GetParticipantsRequest(
                channel, ChannelParticipantsSearch(''), offset=0, limit=1000, hash=0
            ))

            channel_kicked = 0
            for participant in participants.participants:
                if participant.user_id not in authorized_users and participant.user_id not in admin_users:
                    try:
                        # Verify the user exists and is valid
                        await bot.get_entity(participant.user_id)
                    except ValueError as ve:
                        logger.info(f"Skipping invalid participant {participant.user_id} in {channel_name}: {ve}")
                        continue

                    async def kick_action():
                        await bot(EditBannedRequest(
                            channel, participant.user_id,
                            ChatBannedRights(until_date=None, view_messages=True)
                        ))
                    result = await retry_operation(kick_action)
                    if result:
                        logger.info(f"Kicked user {participant.user_id} from {channel_name}")
                        channel_kicked += 1
                        total_kicked += 1
                    else:
                        logger.info(f"Failed to kick user {participant.user_id} from {channel_name}")
                    await asyncio.sleep(DELAY_BETWEEN_ACTIONS)

            results.append(f"✅ {channel_name}: Kicked {channel_kicked} users")

        except Exception as e:
            logger.error(f"Error processing channel {channel_id}: {e}")
            results.append(f"❌ Error with channel {channel_id}: {str(e)}")

        processed_channels += 1
        if processed_channels % 2 == 0 or processed_channels == total_channels:
            await progress_message.edit(f"🔄 Processed {processed_channels}/{total_channels} channels. {total_kicked} users kicked so far...")

    summary = "\n".join(results)
    await progress_message.edit(f"✅ **Kick operation complete**\n\nTotal kicked: {total_kicked} users\n\n{summary}")

# Handle the '/kickallunauthorized' command
@bot.on(events.NewMessage(pattern='/kickallunauthorized'))
async def kick_all_unauthorized(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    progress_message = await event.reply("🔄 Starting to kick unauthorized users from all managed channels. This may take a while...")

    total_kicked = 0
    all_results = []

    for group_name, channels in channel_groups.items():
        group_results = []
        total_channels = len(channels)
        processed_channels = 0

        for channel_id in channels:
            try:
                channel = await bot.get_entity(channel_id)
                channel_name = getattr(channel, 'title', str(channel_id))

                participants = await bot(GetParticipantsRequest(
                    channel, ChannelParticipantsSearch(''), offset=0, limit=1000, hash=0
                ))

                channel_kicked = 0
                for participant in participants.participants:
                    if participant.user_id not in authorized_users and participant.user_id not in admin_users:
                        try:
                            # Verify the user exists and is valid
                            await bot.get_entity(participant.user_id)
                        except ValueError as ve:
                            logger.info(f"Skipping invalid participant {participant.user_id} in {channel_name}: {ve}")
                            continue

                        async def kick_action():
                            await bot(EditBannedRequest(
                                channel, participant.user_id,
                                ChatBannedRights(until_date=None, view_messages=True)
                            ))
                        result = await retry_operation(kick_action)
                        if result:
                            logger.info(f"Kicked user {participant.user_id} from {channel_name}")
                            channel_kicked += 1
                            total_kicked += 1
                        else:
                            logger.info(f"Failed to kick user {participant.user_id} from {channel_name}")
                        await asyncio.sleep(DELAY_BETWEEN_ACTIONS)

                group_results.append(f"✅ {channel_name}: Kicked {channel_kicked} users")

            except Exception as e:
                logger.error(f"Error processing channel {channel_id} in group {group_name}: {e}")
                group_results.append(f"❌ Error with channel {channel_id}: {str(e)}")

            processed_channels += 1
            if processed_channels % 2 == 0 or processed_channels == total_channels:
                await progress_message.edit(f"🔄 Processed {processed_channels}/{total_channels} channels in {group_name}. {total_kicked} users kicked so far...")

        all_results.extend([f"**{group_name} Results:**\n" + "\n".join(group_results)])

    summary = "\n\n".join(all_results)
    await progress_message.edit(f"✅ **Kick all unauthorized operation complete**\n\nTotal kicked: {total_kicked} users\n\n{summary}")

# Handle the '/ban' command
@bot.on(events.NewMessage(pattern='/ban'))
async def ban_user(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    try:
        parts = event.raw_text.split()
        if len(parts) < 3:
            await event.reply('⚠️ Invalid command format. Usage: `/ban [username/user_id] [group_name] [delay_in_minutes]`')
            return

        user_identifier = parts[1]
        group_name = parts[2].lower()

        delay = 0
        if len(parts) > 3:
            try:
                delay = int(parts[3])
            except ValueError:
                await event.reply('⚠️ Invalid delay value. Using delay = 0.')

        if group_name not in channel_groups:
            await event.reply(f'⚠️ Invalid group name. Available groups: {", ".join(channel_groups.keys())}')
            return

        try:
            if user_identifier.isdigit():
                user = await bot.get_entity(int(user_identifier))
            else:
                user = await bot.get_entity(user_identifier)
            await ban_user_in_group(event, group_name, user.id, delay)
        except ValueError:
            await event.reply('⚠️ Invalid username or user ID. Please check and try again.')
        except Exception as e:
            logger.error(f"Error fetching user in ban command: {e}")
            await event.reply(f'❌ An error occurred: {str(e)}')

    except Exception as e:
        logger.error(f"Error in ban command: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/unban' command
@bot.on(events.NewMessage(pattern='/unban'))
async def unban_user(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 3:
        await event.reply('⚠️ Invalid usage. Please provide a user ID/username and a group name.')
        return

    user_to_unban = args[1]
    group_name = args[2].lower()

    if group_name not in channel_groups:
        await event.reply(f'⚠️ Invalid group name. Available groups: {", ".join(channel_groups.keys())}. Please use the exact group name as listed.')
        return

    try:
        if user_to_unban.isdigit():
            user = await bot.get_entity(int(user_to_unban))
        else:
            user = await bot.get_entity(user_to_unban)
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"

        unban_results = []
        for channel_id in channel_groups[group_name]:
            try:
                channel = await bot.get_entity(channel_id)
                async def unban_action():
                    await bot(EditBannedRequest(
                        channel, user,
                        ChatBannedRights(
                            until_date=None,
                            view_messages=False,
                            send_messages=False,
                            send_media=False,
                            send_stickers=False,
                            send_gifs=False,
                            send_games=False,
                            send_inline=False,
                            embed_links=False
                        )
                    ))
                result = await retry_operation(unban_action)
                if result:
                    unban_results.append(f"✅ Unbanned from {getattr(channel, 'title', channel_id)}")
                else:
                    unban_results.append(f"❌ Failed to unban from {getattr(channel, 'title', channel_id)}")
                await asyncio.sleep(DELAY_BETWEEN_ACTIONS)
            except Exception as e:
                unban_results.append(f"❌ Failed to unban from {getattr(channel, 'title', channel_id)}: {str(e)}")
                logger.error(f"Error unbanning user {user.id} from channel {channel_id}: {e}")

        result_text = "\n".join(unban_results)
        await event.reply(f"**Unban results for {user_display}:**\n\n{result_text}")
    except ValueError:
        await event.reply('⚠️ Invalid user ID or username.')
    except Exception as e:
        logger.error(f"Error in unban command: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/unbanall' command
@bot.on(events.NewMessage(pattern='/unbanall'))
async def unban_all(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.raw_text.split()
    if len(args) != 2:
        await event.reply('⚠️ Invalid usage. Please provide a user ID/username.')
        return

    user_to_unban = args[1]

    try:
        if user_to_unban.isdigit():
            user = await bot.get_entity(int(user_to_unban))
        else:
            user = await bot.get_entity(user_to_unban)
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"

        progress_message = await event.reply(f"🔄 Starting to unban {user_display} from all managed channels. This may take a while...")
        unban_results = []
        total_unbanned = 0
        total_channels = sum(len(channels) for channels in channel_groups.values())
        processed_channels = 0

        for group_name, channels in channel_groups.items():
            for channel_id in channels:
                try:
                    channel = await bot.get_entity(channel_id)
                    async def unban_action():
                        await bot(EditBannedRequest(
                            channel, user,
                            ChatBannedRights(
                                until_date=None,
                                view_messages=False,
                                send_messages=False,
                                send_media=False,
                                send_stickers=False,
                                send_gifs=False,
                                send_games=False,
                                send_inline=False,
                                embed_links=False
                            )
                        ))
                    result = await retry_operation(unban_action)
                    if result:
                        total_unbanned += 1
                        unban_results.append(f"✅ Unbanned from {getattr(channel, 'title', channel_id)} in {group_name}")
                    else:
                        unban_results.append(f"❌ Failed to unban from {getattr(channel, 'title', channel_id)} in {group_name}")
                    await asyncio.sleep(DELAY_BETWEEN_ACTIONS)
                except Exception as e:
                    unban_results.append(f"❌ Failed to unban from {getattr(channel, 'title', channel_id)} in {group_name}: {str(e)}")
                    logger.error(f"Error unbanning user {user.id} from channel {channel_id}: {e}")

                processed_channels += 1
                if processed_channels % 2 == 0 or processed_channels == total_channels:
                    await progress_message.edit(f"🔄 Processed {processed_channels}/{total_channels} channels. {total_unbanned} unbans so far...")

        result_text = "\n".join(unban_results)
        await progress_message.edit(f"✅ **Unban all operation complete for {user_display}**\n\nTotal unbanned: {total_unbanned} channels\n\n{result_text}")
    except ValueError:
        await event.reply('⚠️ Invalid user ID or username.')
    except Exception as e:
        logger.error(f"Error in unbanall command: {e}")
        await event.reply(f'❌ An error occurred: {str(e)}')

# Handle the '/stats' command
@bot.on(events.NewMessage(pattern='/stats'))
async def channel_stats(event):
    if not is_admin(event.sender_id):
        await event.reply("⛔ You are not authorized to use this command.")
        return

    try:
        args = event.raw_text.split()
        if len(args) != 2:
            await event.reply('⚠️ Invalid command format. Usage: `/stats [group_name]`')
            return

        group_name = args[1].lower()

        if group_name not in channel_groups:
            await event.reply(f'⚠️ Invalid group name. Available groups: {", ".join(channel_groups.keys())}')
            return

        MAX_MESSAGE_LENGTH = 3800
        MEMBERS_PER_MESSAGE = 50

        for channel_id in channel_groups.get(group_name, []):
            try:
                channel = await bot.get_entity(channel_id)
                participants = await bot(GetParticipantsRequest(
                    channel, ChannelParticipantsSearch(''), offset=0, limit=1000, hash=0
                ))

                total_members = participants.count
                status_message = await event.respond(f"📊 **Stats for Channel:** {channel.title} (Total Members: {total_members})")

                real_users = []
                for participant in participants.participants:
                    try:
                        user = await bot.get_entity(participant.user_id)
                        if user and not user.bot:
                            joined_date = participant.date.strftime("%Y-%m-%d") if hasattr(participant, 'date') else "Unknown"
                            real_users.append({
                                "name": utils.get_display_name(user),
                                "username": user.username or "N/A",
                                "id": user.id,
                                "joined": joined_date
                            })
                    except Exception as e:
                        logger.error(f"Error getting user entity {participant.user_id}: {e}")

                chunks = [real_users[i:i + MEMBERS_PER_MESSAGE] for i in range(0, len(real_users), MEMBERS_PER_MESSAGE)]

                for i, chunk in enumerate(chunks):
                    member_details = []
                    for idx, user in enumerate(chunk, start=i * MEMBERS_PER_MESSAGE + 1):
                        member_details.append(
                            f"{idx}. **{user['name']}**\n"
                            f"   [@{user['username']}](tg://user?id={user['id']}) - Joined: {user['joined']}"
                        )

                    chunk_text = "\n".join(member_details)
                    await event.respond(
                        f"📋 **Members ({i+1}/{len(chunks)}):**\n\n{chunk_text}",
                        link_preview=False
                    )
            except ChannelPrivateError:
                await event.reply(f"🔒 Channel {channel_id}: Private - Bot cannot access")
            except Exception as e:
                logger.error(f"Error processing channel stats for {channel_id}: {e}")
                await event.reply(f"❌ Error getting stats for channel {channel_id}: {str(e)}")
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await event.reply(f"❌ An error occurred: {str(e)}")

# Handle private messages for ban button functionality
@bot.on(events.NewMessage(func=lambda e: e.is_private))
async def handle_private_message(event):
    if event.raw_text.startswith('/'):
        return

    user_id = event.sender_id
    if not is_admin(user_id):
        await event.reply("⛔ You are not authorized to use this bot.")
        return

    message_text = event.raw_text.strip()
    logger.info(f"Received private message: {message_text} from user {user_id}")

    try:
        if message_text.isdigit():
            user = await bot.get_entity(int(message_text))
        else:
            user = await bot.get_entity(message_text)
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"

        buttons = []
        for group_name in channel_groups:
            button = [Button.inline(f"Ban from {group_name}", data=f'ban_{group_name}_{user.id}')]
            buttons.append(button)

        if buttons:
            await event.reply(f"Select a group to ban {user_display} from:", buttons=buttons)
        else:
            await event.reply("No channel groups configured. Use `/addchannel` to create groups first.")
    except ValueError:
        await event.reply("⚠️ Invalid username or user ID. Please check and try again.")
    except Exception as e:
        logger.error(f"Error in private message handler: {e}")
        await event.reply(f"❌ An error occurred while processing your request: {str(e)}")

# Handle button clicks
@bot.on(events.CallbackQuery(pattern=r'ban_'))
async def ban_button_handler(event):
    user_id = event.sender_id
    if not is_admin(user_id):
        await event.answer("⛔ You are not authorized to use this function.", alert=True)
        return

    try:
        _, group_name, user_id_str = event.data.decode().split("_")
        user_id = int(user_id_str)

        logger.info(f"Ban button clicked for user ID {user_id} in group {group_name}")
        await event.answer(f"Processing ban request...")

        await event.edit("Ban in progress...")
        await ban_user_in_group(event, group_name, user_id)
    except Exception as e:
        logger.error(f"Error in ban button handler: {e}")
        await event.answer(f"Error: {str(e)}", alert=True)

# Ban user implementation with proper error handling
async def ban_user_in_group(event, group_name, user_id, delay=0):
    try:
        user = await bot.get_entity(user_id)
        user_display = f"@{user.username}" if user.username else f"ID:{user.id}"

        delay_message = f" in {delay} minutes" if delay > 0 else ""
        response_message = f"🔄 User {user_display} will be banned from the `{group_name}` group{delay_message}."

        if isinstance(event, events.CallbackQuery):
            await event.edit(response_message)
            initial_message = await bot.get_messages(event.chat_id, ids=event.message_id)
        else:
            initial_message = await event.respond(response_message)

        if delay > 0:
            for i in range(min(delay, 5)):
                if delay > 5 and i == 4:
                    remaining = delay - 5
                    await initial_message.edit(f"🕒 Waiting {remaining} more minutes before banning {user_display}...")
                    break

                await asyncio.sleep(60)
                await initial_message.edit(f"🕒 Banning {user_display} in {delay-i-1} minutes...")

            if delay > 5:
                await asyncio.sleep((delay - 5) * 60)

        ban_results = []
        for channel_id in channel_groups[group_name]:
            try:
                channel = await bot.get_entity(channel_id)
                async def ban_action():
                    await bot(EditBannedRequest(channel, user, ChatBannedRights(until_date=None, view_messages=True)))
                result = await retry_operation(ban_action)
                if result:
                    ban_results.append(f"✅ Banned from {getattr(channel, 'title', channel_id)}")
                else:
                    ban_results.append(f"❌ Failed to ban from {getattr(channel, 'title', channel_id)}")
                await asyncio.sleep(DELAY_BETWEEN_ACTIONS)
            except UserNotParticipantError:
                ban_results.append(f"⚠️ Not in channel {getattr(channel, 'title', channel_id)}")
            except ChatAdminRequiredError:
                ban_results.append(f"❌ Missing admin rights in {getattr(channel, 'title', channel_id)}")
            except Exception as e:
                ban_results.append(f"❌ Error in {getattr(channel, 'title', channel_id)}: {str(e)}")
                logger.error(f"Ban error for user {user_id} in channel {channel_id}: {e}")

        ban_summary = "\n".join(ban_results)
        result_message = f"🚫 **Ban results for {user_display}:**\n\n{ban_summary}"
        await initial_message.edit(result_message)

    except Exception as e:
        logger.error(f"Error in ban_user_in_group for user {user_id}: {e}")
        error_message = f"❌ An error occurred: {str(e)}"
        if isinstance(event, events.CallbackQuery):
            await event.edit(error_message)
        else:
            await event.reply(error_message)

# Main function to run the bot
async def main():
    await load_data()
    logger.info("✅ Bot is successfully running!")
    await bot.start(bot_token=bot_token)
    try:
        await bot.run_until_disconnected()
    except Exception as e:
        logger.error(f"Bot disconnected with error: {e}")
    finally:
        await bot.disconnect()

# Shutdown handler to clean up tasks
def handle_shutdown(loop):
    tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    loop.stop()
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    logger.info("Shutdown complete")

if __name__ == '__main__':
    # Create a .env file template if it doesn't exist
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("""# Telegram API Credentials
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_BOT_TOKEN=

# Comma-separated list of admin user IDs
ADMIN_USERS=7819751787,987654321
""")
        logger.info("Created .env file template - please fill in your credentials")

    # Run the bot with explicit event loop management
    loop = asyncio.get_event_loop()
    try:
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown, loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error running bot: {e}")
    finally:
        handle_shutdown(loop)
