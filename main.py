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
PATH_TO_EXCEL       = config.get('Yandex', 'path_to_excel')
# Create the database and tables
DB_ENGINE           = db.create_database(DB_URL)
DB_SESSION          = db.get_session(DB_ENGINE)
# To test work on few tasks (for careful testing)
FEW_DATA            = False

async def sync_comments(g_tasks, sync_mode: int, get_comments_execution: str = 'async'):
    """
    Synchronizes comments between services.
    sync_mode can be 1 or 2:
    1 - sync all comments
    2 - sync only comments for programmers
    get_comments_execution (default is async):
    sync - get all comments consecutively
    async - get all comments concurrently
    """
    logging.info("Syncing comments...")
    g_tasks_ids = utils.extract_task_ids(g_tasks)
    g_task_comments = []
    added_comment_to_y_count = 0
    added_comment_to_g_count = 0
    edited_comment_in_y_count = 0
    edited_comment_in_g_count = 0
    logging.info("Fetching comments...")
    if get_comments_execution == 'async':
        g_task_comments = await gapi.get_comments_for_tasks_concurrently(g_tasks_ids)
    elif get_comments_execution == 'sync':
        g_task_comments = await gapi.get_comments_for_tasks_consecutively(g_tasks_ids)
    logging.info("Updating comments...")
    
    for g_task_id, g_comments in g_task_comments.items():
        existing_g_comments, comment_texts_in_gandiva = utils.extract_existing_comments_from_gandiva(g_comments)
        y_task = db.get_task_by_gandiva_id(session=DB_SESSION, g_task_id=g_task_id)
        if y_task is None:
            logging.warning(f"Task {g_task_id} was not found in database, skipping...")
            continue
        y_task_id = y_task.task_id_yandex
        y_comments = await yapi.get_comments(y_task_id=y_task_id)
        logging.debug(f"Syncing comments for task {g_task_id}...")

        existing_comments_in_yandex = {}
        comment_texts_in_yandex = {}

        for y_comment in y_comments:
            y_text          = y_comment.get('text', '')
            y_comment_id    = y_comment.get('id')
            y_summonees     = y_comment.get('summonees', ())
            y_author_id     = None
            if y_comment.get('createdBy') and y_comment.get('createdBy').get('id'):
                y_author_id = y_comment.get('createdBy').get('id')
            
            # if comment's author is NOT YRobot and YRobot is mentioned (in summonees): DONE
            # Add comment to gandiva, continue (we don't need to add it back to Yandex, duh) DONE
            # Unique check: if y_comment_id is in gandiva comments already -> don't add

            # -> Gandiva section
            if y_author_id:
                if y_author_id != yapi.YA_ROBOT_ID and utils.is_id_in_summonees(yapi.YA_ROBOT_ID, y_summonees):
                    y_author = y_comment.get('createdBy').get('display')
                    y_text = utils.remove_mentions(y_text)
                    y_text_html = utils.markdown_to_html(y_text)
                    y_comment_id = str(y_comment_id)
                    g_task = gapi.get_task_by_id_from_list(g_tasks, g_task_id)
                    g_addressees = [g_task['Initiator']['Id'], g_task['Contractor']['Id']]
                    if y_comment_id in existing_g_comments:
                        g_comment_id = existing_g_comments.get(y_comment_id)
                        g_text = comment_texts_in_gandiva.get(y_comment_id)
                        if '<br>' in g_text:
                            g_text = g_text.split('<br>', 1)[1]
                        else:
                            logging.warning(f"Yandex comment {y_comment_id} has no <br> in its html, skipping...")
                            continue
                        if g_text == y_text_html:
                            logging.debug(f"Skipping comment {g_comment_id} as contents are the same.")
                            continue
                        
                        response = await gapi.edit_comment(g_comment_id, y_comment_id, y_text_html, y_author, g_addressees)
                        if response: edited_comment_in_g_count += 1
                    else:
                        response = await gapi.add_comment(g_task_id, y_text_html, y_comment_id, y_author, g_addressees)
                        if response: added_comment_to_g_count += 1
                    continue
            else:
                logging.error(f"No author found for Yandex comment {y_comment_id}")


            # Check if the comment contains a g_comment_id in the format [g_comment_id]
            match = re.match(r'\[(\d+)\]', y_text)
            if match:
                g_comment_id = match.group(1)  # Extract the g_comment_id
                existing_comments_in_yandex[g_comment_id] = y_comment_id  # Map y_comment_id to g_comment_id
                comment_texts_in_yandex[g_comment_id] = y_text  # Map g_comment_id to y_text for comparison
        
        # -> Yandex section
        for g_comment in g_comments:
            # Skip Robot's comments
            if utils.is_g_comment_author_this(g_comment, gapi.GAND_ROBOT_ID): continue
            
            # Handle sync_mode 2 (only sync comments for programmers)
            addressees      = g_comment.get('Addressees', [])
            if sync_mode == 2 and not utils.g_addressee_exists(addressees, gapi.GAND_PROGRAMMER_ID): continue

            # Extract required fields
            g_comment_id    = str(g_comment['Id'])  # Ensure g_comment_id is a string
            text_html       = g_comment['Text']
            g_text          = utils.html_to_yandex_format(text_html)
            author          = g_comment['Author']
            author_name     = f"{author['FirstName']} {author['LastName']}"

            
            # Check if this g_comment_id already exists in Yandex comments
            
            if g_comment_id in existing_comments_in_yandex:
                y_comment_id = existing_comments_in_yandex.get(g_comment_id)
                y_text = comment_texts_in_yandex.get(g_comment_id)
                y_text = y_text.split('\n', 1)[1]
                # Only edit the comment if the contents are different
                if y_text == g_text:
                    logging.debug(f"Skipping comment {g_comment_id} as contents are the same.")
                    continue
                

                response = await yapi.edit_comment(y_task_id, g_text, g_comment_id, y_comment_id, author_name)
                if response:
                    edited_comment_in_y_count += 1
                    logging.info(f"Edited comment {g_comment_id} in Yandex task {y_task_id}!")
                else:
                    logging.error(f"Failed editing comment {g_comment_id} in Yandex task {y_task_id}.")
            else:
                # Send the comment to Yandex if sync_mode is not 2 or if GANDIVA_PROGRAMMER_ID is found
                response = await yapi.add_comment(y_task_id, g_text, g_comment_id, author_name)
                if isinstance(response, dict):
                    added_comment_to_y_count += 1

    # Log the total number of added and edited tasks
    logging.info(f"Total comments added to Yandex: {added_comment_to_y_count}")
    logging.info(f"Total comments edited in Yandex: {edited_comment_in_y_count}")
    logging.info(f"Total comments added to Gandiva: {added_comment_to_g_count}")
    logging.info(f"Total comments edited in Gandiva: {edited_comment_in_g_count}")




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
    y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))
    db.add_tasks(session=DB_SESSION, y_tasks=y_tasks)

async def update_users_department_in_db(excel_obj):
    # department_analyst_dict = utils.extract_department_analysts('department_analyst_nd.csv')
    
    department_analyst_dict = utils.extract_department_analysts_from_excel(excel_obj)
    users                   = await yapi.get_all_users()
    department_user_nd_mapping = utils.map_department_nd_to_user_id(department_analyst_dict, users)
    return db.add_user_department_nd_mapping(session=DB_SESSION, department_user_mapping=department_user_nd_mapping)

async def update_it_users_in_db(excel_obj):
    
    # it_users_dict    = utils.extract_it_users('it_users.csv')
    it_users_dict   = utils.extract_it_users_from_excel(excel_obj)
    y_users         = await yapi.get_all_users()
    it_uids         = await utils.map_emails_to_ids(it_users_dict, y_users)
    g_tasks         = await gapi.get_tasks(gapi.GroupsOfStatuses.in_progress)
    g_users         = utils.extract_unique_gandiva_users(g_tasks)
    uids_y_g        = utils.map_it_uids_to_g_ids(it_uids, g_users)
    return db.add_or_update_user(session=DB_SESSION, user_data=uids_y_g)


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
    logging.info(f"Settings used in config:sync_mode: {sync_mode}; queue: {queue}; board_id: {board_id}; to_get_followers: {to_get_followers}; use_summaries: {use_summaries}; interval_minutes: {interval_minutes}")
    await run_sync_services_periodically(queue, sync_mode, board_id, to_get_followers, use_summaries=use_summaries, interval_minutes=interval_minutes)

async def update_db(queue):
    # db.update_database_schema(DB_ENGINE)
    db.clean_department_names(DB_SESSION)
    logging.info('Checking updates in database (full check)...')
    await yapi.check_access_token(yapi.YA_ACCESS_TOKEN)
    await gapi.get_access_token(gapi.GAND_LOGIN, gapi.GAND_PASSWORD)
    await update_tasks_in_db(queue = queue)

    await update_db_excel_data()
    db.find_duplicate_gandiva_tasks(DB_SESSION)

async def update_db_excel_data():
    logging.info('Checking updates in database (Excel data)...')
    excel_bytes = await yapi.download_file_from_yandex_disk(path=PATH_TO_EXCEL)
    excel_obj   = utils.read_excel_from_bytes(excel_bytes)
    res_1 = await update_users_department_in_db(excel_obj)
    res_2 = await update_it_users_in_db(excel_obj)
    if res_1 or res_2:
        utils.EXCEL_UPDATED_IN_YANDEX_DISK = True
    else:
        utils.EXCEL_UPDATED_IN_YANDEX_DISK = False

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

    # Fetch tasks from both services
    g_tasks_all                     = await gapi.get_tasks(gapi.GroupsOfStatuses._all) # all
    g_tasks_in_progress             = gapi.extract_tasks_by_status(g_tasks_all, gapi.GroupsOfStatuses.in_progress) # full sync [3, 4, 6, 8, 13]
    g_tasks_in_progress_or_waiting  = gapi.extract_tasks_by_status(g_tasks_all, gapi.GroupsOfStatuses.in_progress_or_waiting) # add
    g_tasks_waiting                 = gapi.extract_tasks_by_status(g_tasks_all, gapi.GroupsOfStatuses.waiting) # part edit
    # g_tasks_waiting_or_finished     = gapi.extract_tasks_by_status(g_tasks_all, gapi.GroupsOfStatuses.waiting_or_finished) # move status
    y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))

    # Get gandiva_task_ids from Summary (if enabled in config), gandiva_task_id and combine all
    not_closed_task_ids = {}
    if use_summaries: not_closed_task_ids = utils.extract_task_ids_from_summaries(y_tasks)
    not_closed_task_ids_2 = utils.extract_task_ids_from_gandiva_task_id(y_tasks)
    not_closed_task_ids.update(not_closed_task_ids_2)
    
    # Add tasks if not in Tracker
    tasks_added = await yapi.add_tasks(g_tasks_in_progress_or_waiting, queue=queue, non_closed_ya_task_ids=not_closed_task_ids)
    if tasks_added > 0: y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))

    # Tasks work
    await yapi.batch_move_tasks_status(g_tasks_all, y_tasks)
    await yapi.edit_tasks(g_tasks_in_progress, y_tasks, to_get_followers, use_summaries)
    await yapi.edit_tasks(g_tasks_waiting, y_tasks, to_get_followers, use_summaries, edit_descriptions=False)   
    
    await sync_comments(g_tasks_in_progress, sync_mode)
    # Release sprint
    await yapi.create_weekly_release_sprint(board_id)

    # Handle anomalies
    await yapi.handle_cancelled_tasks_still_have_g_task_ids(queue)
    await gapi.handle_waiting_for_analyst_or_no_contractor_no_required_start_date(g_tasks_in_progress_or_waiting)
    await yapi.handle_in_work_but_waiting_for_analyst(g_tasks_in_progress, y_tasks)

    # Update database from Excel file in Yandex Disk
    await update_db_excel_data()
    logging.info("Sync finished successfully!")

if __name__ == "__main__":
    asyncio.run(main())