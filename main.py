import logging
import asyncio
import gandiva_api as gapi
import yandex_api as yapi
import db_module as db
import utils
import re
import configparser

# Path to the .ini file
CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
DB_URL      = config.get('Database', 'url')
# Create the database and tables
DB_ENGINE   = db.create_database(DB_URL)
DB_SESSION  = db.get_session(DB_ENGINE)

async def sync_comments(g_tasks, sync_mode: int, get_comments_execution: str):
    """
    Synchronizes comments between services.
    sync_mode can be 1 or 2:
    1 - sync all comments
    2 - sync only comments for programmers
    get_comments_execution (sync/async):
    sync - get all comments consecutively
    async - get all comments concurrently
    """
    logging.info("Syncing comments...")
    g_tasks_ids = utils.extract_task_ids(g_tasks)
    tasks_comments = []
    added_comment_count = 0
    logging.info("Fetching comments... [Gandiva]")
    if get_comments_execution == 'async':
        tasks_comments = await gapi.get_comments_for_tasks_concurrently(g_tasks_ids)
    elif get_comments_execution == 'sync':
        tasks_comments = await gapi.get_comments_for_tasks_consecutively(g_tasks_ids)
    logging.info("Adding comments... [Yandex Tracker]")
    for task_id, comments in tasks_comments.items():
        yandex_task = db.find_task_by_gandiva_id(session=DB_SESSION, task_id_gandiva=task_id)
        yandex_task_id = yandex_task.task_id_yandex
        yandex_comments = await yapi.get_all_comments(yandex_task_id=yandex_task_id)
        logging.debug(f"Syncing comments for task {task_id}...")
        # Extract g_comment_ids from yandex_comments
        existing_g_comment_ids = set()
        for y_comment in yandex_comments:
            text = y_comment.get('text', '')
            # Check if the comment contains a g_comment_id in the format [g_comment_id]
            match = re.match(r'\[(\d+)\]', text)
            if match:
                existing_g_comment_ids.add(match.group(1))  # Extract the g_comment_id

        for comment in comments:
            # Extract required fields
            g_comment_id = str(comment['Id'])  # Ensure g_comment_id is a string
            text_html = comment['Text']
            text = utils.html_to_yandex_format(text_html)
            author = comment['Author']
            author_name = f"{author['FirstName']} {author['LastName']}"
            addressees = comment.get('Addressees', [])

            # Check if this g_comment_id already exists in Yandex comments
            if g_comment_id in existing_g_comment_ids:
                continue  # Skip adding if the comment already exists

            # Handle sync_mode 2 (only sync comments for programmers)
            if sync_mode == 2:
                send_comment = False
                for addressee in addressees:
                    if addressee['User']['Id'] == gapi.GAND_PROGRAMMER_ID:
                        send_comment = True
                        break
                if not send_comment:
                    continue  # Skip comment if the programmer is not found

            # Send the comment to Yandex if sync_mode is not 2 or if GANDIVA_PROGRAMMER_ID is found
            result = await yapi.add_comment(
                yandex_task_id=yandex_task_id,
                comment=text,
                g_comment_id=g_comment_id,
                author_name=author_name)
            
            if isinstance(result, dict):
                added_comment_count += 1

    # Log the total number of added tasks
    logging.info(f"Total comments added: {added_comment_count}")

async def sync_services(queue: str, sync_mode: str, board_id: int):
    """
    Syncronize Gandiva and Yandex Tracker services.
    queue: working queue in Yandex Tracker.
    sync_mode: which comments to sync.
    1 - all comments, 2 - only for programmers.
    """
    
    logging.info(f"Sync started!")
    await yapi.check_access_token(yapi.YA_ACCESS_TOKEN)
    await gapi.get_access_token(gapi.GAND_LOGIN, gapi.GAND_PASSWORD)
    g_tasks = await gapi.get_all_tasks(gapi.GroupsOfStatuses.in_progress)
    
    await yapi.add_tasks(g_tasks, queue=queue)
    ya_tasks = await yapi.get_all_tasks(queue)
    await yapi.edit_tasks(g_tasks, ya_tasks)
    await yapi.batch_move_tasks_status(g_tasks, ya_tasks)

    g_finished_tasks = await gapi.get_all_tasks(gapi.GroupsOfStatuses.finished)
    await yapi.batch_move_tasks_status(g_finished_tasks, ya_tasks)

    await sync_comments(g_tasks, sync_mode, 'async')



    await yapi.create_weekly_release_sprint(board_id)
    logging.info("Sync finished successfully!")


async def run_sync_services_periodically(queue: str, sync_mode: int, board_id: int, interval_minutes: int = 30):
    """Runs sync_services every interval_minutes."""
    while True:
        try:
            await sync_services(queue, sync_mode, board_id)  # Call sync_services
        except Exception as e:
            # Log any error that happens in sync_services
            print(f"Error during sync_services: {e}")
        
        # Wait for the specified interval before running sync_services again
        logging.info(f"Next sync in {interval_minutes} minutes")
        logging.info("-" * 40)

        await asyncio.sleep(interval_minutes * 60)

async def update_tasks_in_db(queue: str):
    await yapi.check_access_token(yapi.YA_ACCESS_TOKEN)
    ya_tasks = await yapi.get_all_tasks(queue)
    db.add_tasks_to_db(session=DB_SESSION, tasks=ya_tasks)

# Main function to start the bot
async def main():
    utils.setup_logging()
    
    try:
        sync_mode = config.getint('Settings', 'sync_mode')
        queue = config.get('Settings', 'queue')
        board_id = config.getint('Settings', 'board_id')
        interval_minutes = config.getint('Settings', 'interval_minutes')
    except Exception as e:  # Catch any exception
        logging.warning(f"Error fetching config values: {e}. Using default values.")
        sync_mode = 1
        queue = "TEA"
        board_id = 52
        interval_minutes = 5
    
    await update_tasks_in_db(queue = queue)
    # Start sync_services in the background and run every N minutes
    await run_sync_services_periodically(queue, sync_mode, board_id, interval_minutes=interval_minutes)

if __name__ == "__main__":
    asyncio.run(main())