import logging
import asyncio
import gandiva_api as gapi
import yandex_api as yapi
import db_module as db
import utils
import re
import configparser

CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
DB_URL              = config.get('Database', 'url')
# Create the database and tables
DB_ENGINE           = db.create_database(DB_URL)
DB_SESSION          = db.get_session(DB_ENGINE)
# To test work on few tasks (for careful testing)
FEW_DATA            = False

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
    g_task_comments = []
    added_comment_count = 0
    edited_comment_count = 0
    logging.info("Fetching comments... [Gandiva]")
    if get_comments_execution == 'async':
        g_task_comments = await gapi.get_comments_for_tasks_concurrently(g_tasks_ids)
    elif get_comments_execution == 'sync':
        g_task_comments = await gapi.get_comments_for_tasks_consecutively(g_tasks_ids)
    logging.info("Updating comments... [Yandex Tracker]")
    
    for g_task_id, comments in g_task_comments.items():
        y_task = db.get_task_by_gandiva_id(session=DB_SESSION, g_task_id=g_task_id)
        if y_task is None:
            logging.warning(f"Task {g_task_id} was not found in database, skipping...")
            continue
        y_task_id = y_task.task_id_yandex
        y_comments = await yapi.get_all_comments(y_task_id=y_task_id)
        logging.debug(f"Syncing comments for task {g_task_id}...")

        # Extract g_comment_ids from Yandex comments
        existing_g_comments = {}
        y_comment_texts = {}

        for y_comment in y_comments:
            y_text = y_comment.get('text', '')
            y_comment_id = y_comment.get('id')  # Get the y_comment_id

            # Check if the comment contains a g_comment_id in the format [g_comment_id]
            match = re.match(r'\[(\d+)\]', y_text)
            
            if match:
                g_comment_id = match.group(1)  # Extract the g_comment_id
                existing_g_comments[g_comment_id] = y_comment_id  # Map y_comment_id to g_comment_id
                y_comment_texts[g_comment_id] = y_text  # Map g_comment_id to y_text for comparison

        for comment in comments:
            # Extract required fields
            g_comment_id    = str(comment['Id'])  # Ensure g_comment_id is a string
            text_html       = comment['Text']
            g_text          = utils.html_to_yandex_format(text_html)
            author          = comment['Author']
            author_name     = f"{author['FirstName']} {author['LastName']}"
            addressees      = comment.get('Addressees', [])

            # Handle sync_mode 2 (only sync comments for programmers)
            if sync_mode == 2:
                send_comment = False
                for addressee in addressees:
                    if addressee['User']['Id'] == gapi.GAND_PROGRAMMER_ID:
                        send_comment = True
                        break
                if not send_comment:
                    continue  # Skip comment if the programmer is not found

            # Check if this g_comment_id already exists in Yandex comments
            if g_comment_id in existing_g_comments.keys():
                y_comment_id = existing_g_comments.get(g_comment_id)
                y_text = y_comment_texts.get(g_comment_id)
                y_text = y_text.split('\n', 1)[1]

                # Only edit the comment if the contents are different
                if y_text == g_text:
                    logging.debug(f"Skipping comment {g_comment_id} as contents are the same.")
                    continue

                logging.info(f"Editing comment {g_comment_id} in Yandex task {y_task_id}")
                result = await yapi.edit_comment(
                    y_task_id=y_task_id,
                    comment=g_text,
                    g_comment_id=g_comment_id,
                    y_comment_id=y_comment_id,
                    author_name=author_name)
                if result:
                    edited_comment_count += 1

            # Send the comment to Yandex if sync_mode is not 2 or if GANDIVA_PROGRAMMER_ID is found
            result = await yapi.add_comment(
                y_task_id=y_task_id,
                comment=g_text,
                g_comment_id=g_comment_id,
                author_name=author_name)
            
            if isinstance(result, dict):
                added_comment_count += 1

    # Log the total number of added and edited tasks
    logging.info(f"Total comments added: {added_comment_count}")
    logging.info(f"Total comments edited: {edited_comment_count}")

async def run_sync_services_periodically(queue: str, sync_mode: int, board_id: int, to_get_followers: bool = False,
                                         use_summaries: bool = False, interval_minutes: int = 30):
    """Runs sync_services every interval_minutes."""
    while True:
        try:
            await sync_services(queue, sync_mode, board_id, to_get_followers, use_summaries)  # Call sync_services
        except Exception as e:
            # Log any error that happens in sync_services
            logging.error(f"Error during sync_services: {e}")
        
        # Wait for the specified interval before running sync_services again
        logging.info(f"Next sync in {interval_minutes} minutes")
        logging.info("-" * 40)

        await asyncio.sleep(interval_minutes * 60)

async def update_tasks_in_db(queue: str):
    y_tasks = await yapi.get_all_tasks(query=yapi.get_query_all(queue))
    db.add_tasks(session=DB_SESSION, y_tasks=y_tasks)

async def update_users_department_in_db():
    department_analyst_dict = utils.extract_department_analysts('department_analyst.csv')
    users = await yapi.get_all_users()
    department_uid = utils.map_department_to_user_id(department_analyst_dict, users)
    db.add_user_department_mapping(session=DB_SESSION, department_user_mapping=department_uid)

async def update_it_users_in_db():
    
    it_users    = utils.extract_it_users('it_users.csv')
    y_users     = await yapi.get_all_users()
    it_uids     = await utils.map_emails_to_ids(it_users, y_users)
    g_tasks     = await gapi.get_all_tasks()
    g_users     = utils.extract_unique_gandiva_users(g_tasks)
    uids_y_g    = utils.map_it_uids_to_gandiva_ids(it_uids, g_users)
    db.add_or_update_user(session=DB_SESSION, user_data=uids_y_g)


# Main function to start the bot
async def main():
    utils.setup_logging()
    logging.info("-------------------- APPLICATION STARTED --------------------")
    
    try:
        sync_mode = config.getint('Settings', 'sync_mode')
        to_get_followers = config.getboolean('Settings', 'to_get_followers')
        use_summaries = config.getboolean('Settings', 'use_summaries')
        queue = config.get('Settings', 'queue')
        board_id = config.getint('Settings', 'board_id')
        interval_minutes = config.getint('Settings', 'interval_minutes')
    except Exception as e:  # Catch any exception
        logging.warning(f"Error fetching config values: {e}. Using default values.")
        use_summaries = False
        to_get_followers = False
        sync_mode = 1
        queue = "TEA"
        board_id = 52
        interval_minutes = 5
    await update_db(queue)
    # Start sync_services in the background and run every N minutes
    logging.info(f"Settings used in config:\nsync_mode: {sync_mode}\nqueue: {queue}\nboard_id: {board_id}\nto_get_followers: {to_get_followers}\nuse_summaries: {use_summaries}\ninterval_minutes: {interval_minutes}")
    await run_sync_services_periodically(queue, sync_mode, board_id, to_get_followers, use_summaries=use_summaries, interval_minutes=interval_minutes)

async def update_db(queue):
    await yapi.check_access_token(yapi.YA_ACCESS_TOKEN)
    await gapi.get_access_token(gapi.GAND_LOGIN, gapi.GAND_PASSWORD)
    await update_tasks_in_db(queue = queue)
    await update_users_department_in_db()
    await update_it_users_in_db()

async def sync_services(queue: str, sync_mode: str, board_id: int, to_get_followers: bool, use_summaries: bool):
    """
    Synchronize Gandiva and Yandex Tracker services.
    queue: working queue in Yandex Tracker.
    sync_mode: which comments to sync.
    1 - all comments, 2 - only for programmers.
    """
    
    logging.info(f"Sync started!")
    await yapi.check_access_token(yapi.YA_ACCESS_TOKEN)
    await gapi.get_access_token(gapi.GAND_LOGIN, gapi.GAND_PASSWORD)
    g_tasks = await gapi.get_all_tasks(gapi.GroupsOfStatuses.in_progress)
    if FEW_DATA:
        found_task_id   = 190069
        found_task      = None
        for g_task in g_tasks:
            if g_task['Id'] == found_task_id:
                found_task = g_task
                break
        g_tasks = g_tasks[:12]
        g_tasks.append(found_task)

    # ++
    # ya_tasks = await yapi.get_all_tasks(queue)
    y_tasks = await yapi.get_all_tasks(query=yapi.get_query_in_progress(queue))
    # --

    # get gandiva_task_ids from summary and gandiva_task_id fields and combine all
    not_closed_task_ids = {}
    # NOTE: careful with or True
    if use_summaries or True:
        not_closed_task_ids = utils.extract_task_ids_from_summaries(y_tasks)
    not_closed_task_ids_2 = utils.extract_task_ids_from_gandiva_task_id(y_tasks)
    not_closed_task_ids.update(not_closed_task_ids_2)
    
    # NOTE: uncomment to add new tasks
    await yapi.add_tasks(g_tasks, queue=queue, non_closed_ya_task_ids=not_closed_task_ids)
    
    await yapi.edit_tasks(g_tasks, y_tasks, to_get_followers, use_summaries)
    
    y_tasks_new = await yapi.get_all_tasks(query=yapi.get_query_in_progress(queue))
    await yapi.batch_move_tasks_status(g_tasks, y_tasks_new)
    
    g_finished_tasks = await gapi.get_all_tasks(gapi.GroupsOfStatuses.finished)
    if FEW_DATA:
        g_finished_tasks = g_finished_tasks[:12]
        g_finished_tasks.append(found_task)
    
    await yapi.batch_move_tasks_status(g_finished_tasks, y_tasks)
    
    # NOTE: uncomment to sync_comments
    # await sync_comments(g_tasks, sync_mode, 'async')
    await yapi.create_weekly_release_sprint(board_id)
    logging.info("Sync finished successfully!")

if __name__ == "__main__":
    asyncio.run(main())