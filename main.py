import logging
import asyncio
import gandiva_api as gapi
from gandiva_api import gc
import yandex_api as yapi
from yandex_api import yc
import db_module as db
import utils
import re
from pandas import DataFrame
from typing import Any

# Globals
MAX_COMMENT_LENGTH = 20_000
TEST_FUNCTION = False


async def sync_comments(g_tasks: list[dict], sync_mode: int, get_comments_execution: str = 'async') -> bool:
    """
    Synchronizes comments between Gandiva and Yandex tasks.

    sync_mode:
        0 - Skip comments sync
        1 - Sync all comments
        2 - Sync only comments for programmers

    get_comments_execution (default is async):
        'sync' - Get comments consecutively
        'async' - Get comments concurrently
    """
    if sync_mode == 0:
        logging.info("Skipped comment sync.")
        return True
    logging.info("Starting comment sync process...")

    g_task_comments = await fetch_comments_for_tasks(g_tasks, get_comments_execution)

    added_comment_to_y_count = 0
    added_comment_to_g_count = 0
    edited_comment_in_y_count = 0
    edited_comment_in_g_count = 0

    for g_task in g_tasks:
        g_task_id = g_task['Id']
        g_comments: list[dict] = g_task_comments.get(g_task_id, [])

        if not g_comments:
            logging.debug(f"No comments found in Gandiva task {g_task_id}.")
            continue

        sync_result = await sync_task_comments(g_task, g_comments, sync_mode)

        # Update counts based on results from sync_task_comments
        added_comment_to_y_count += sync_result['added_to_yandex']
        added_comment_to_g_count += sync_result['added_to_gandiva']
        edited_comment_in_y_count += sync_result['edited_in_yandex']
        edited_comment_in_g_count += sync_result['edited_in_gandiva']

    log_sync_results(
        added_comment_to_y_count, added_comment_to_g_count,
        edited_comment_in_y_count, edited_comment_in_g_count)

    return True


async def fetch_comments_for_tasks(g_tasks, execution_mode: str):
    logging.info("Fetching comments from Gandiva...")

    g_task_ids = extract_task_ids(g_tasks)
    get_comments = gapi.get_comments_generator(execution_mode)

    return await get_comments(g_task_ids)


def extract_task_ids(g_tasks):
    return [g_task['Id'] for g_task in g_tasks]


async def sync_task_comments(g_task: dict[str, Any], g_comments: list[dict], sync_mode: int):
    y_task = fetch_yandex_task(g_task)

    if not y_task:
        logging.warning(
            f"Yandex task for Gandiva task {g_task['Id']} not found.")
        return initialize_comment_counts()

    y_task_id = str(y_task.task_id_yandex)
    y_comments = await yapi.get_comments(y_task_id)

    contractor: dict[str, Any] | None = g_task.get('Contractor')
    if not contractor:
        logging.error("Contractor not found in task.")
        return initialize_comment_counts()

    contractor_id = contractor.get('Id')
    if not contractor_id:
        logging.error("Contractor ID not found in task.")
        return initialize_comment_counts()

    contractor_id = str(contractor_id)
    existing_g_comments, g_comment_texts = extract_gandiva_comments(g_comments)
    existing_y_comments, y_comment_texts = extract_yandex_comments(y_comments)

    added_to_yandex, edited_in_yandex = await sync_gandiva_comments_to_yandex(
        y_task_id, g_comments, existing_y_comments,
        y_comment_texts, contractor_id, sync_mode
    )

    added_to_gandiva, edited_in_gandiva = await sync_yandex_comments_to_gandiva(
        g_task, y_comments, existing_g_comments, g_comment_texts
    )

    return create_comment_sync_summary(
        added_to_yandex, added_to_gandiva, edited_in_yandex, edited_in_gandiva)


def fetch_yandex_task(g_task):
    db_session = db.get_db_session()
    return db.get_task_by_gandiva_id(session=db_session, g_task_id=g_task['Id'])


def initialize_comment_counts():
    return {'added_to_yandex': 0,
            'added_to_gandiva': 0,
            'edited_in_yandex': 0,
            'edited_in_gandiva': 0}


def create_comment_sync_summary(
        added_to_yandex, added_to_gandiva, edited_in_yandex, edited_in_gandiva):
    return {
        'added_to_yandex': added_to_yandex,
        'added_to_gandiva': added_to_gandiva,
        'edited_in_yandex': edited_in_yandex,
        'edited_in_gandiva': edited_in_gandiva
    }


def extract_gandiva_comments(g_comments):
    return utils.extract_existing_comments_from_gandiva(g_comments)


def should_skip_comment_sync(sync_mode, sync_programmers, g_comment, g_task_contractor):
    """Determines if the comment sync should be skipped based on sync mode and addressees."""
    return (sync_mode == sync_programmers
            and not is_programmer_or_contractor_in_addressees(g_comment, g_task_contractor))


async def sync_gandiva_comments_to_yandex(
        y_task_id, g_comments: list[dict], existing_y_comments,
        y_comment_texts, g_task_contractor, sync_mode):
    """Sync Gandiva comments (and their answers) to Yandex."""

    added_to_yandex = 0
    edited_in_yandex = 0
    sync_programmers = 2

    async def process_comments_recursively(comments: list[dict]):
        """Helper function to process comments and their answers recursively."""
        nonlocal added_to_yandex, edited_in_yandex
        for comment in comments:
            added_to_yandex, edited_in_yandex = await process_g_comment(
                comment, sync_mode, sync_programmers, g_task_contractor, existing_y_comments,
                y_task_id, y_comment_texts, added_to_yandex, edited_in_yandex)

            # Recursively process any answers
            if comment.get('Answers'):
                await process_comments_recursively(comment['Answers'])

    # Start processing the top-level comments and their answers
    await process_comments_recursively(g_comments)

    return added_to_yandex, edited_in_yandex


def author_g_comment_is_robot(g_comment):
    return utils.is_g_comment_author_this(g_comment, gc.robot_id)


async def process_g_comment(g_comment, sync_mode, sync_programmers, g_task_contractor,
                            existing_y_comments, y_task_id, y_comment_texts,
                            added_to_yandex, edited_in_yandex) -> tuple[int, int]:

    if author_g_comment_is_robot(g_comment):
        return added_to_yandex, edited_in_yandex

    if should_skip_comment_sync(sync_mode, sync_programmers, g_comment, g_task_contractor):
        return added_to_yandex, edited_in_yandex

    g_comment_id = str(g_comment['Id'])
    g_text = utils.html_to_yandex_format(g_comment['Text'])
    if len(g_text) > MAX_COMMENT_LENGTH:
        return added_to_yandex, edited_in_yandex

    author_name = get_author_name(g_comment['Author'])

    if g_comment_id not in existing_y_comments:
        response = await yapi.add_comment(y_task_id, g_text, g_comment_id, author_name)
        if isinstance(response, dict):
            added_to_yandex += 1
        return added_to_yandex, edited_in_yandex
    y_comment_id = existing_y_comments[g_comment_id]
    y_text = y_comment_texts[g_comment_id].split('\n', 1)[1]

    if y_text == g_text:
        logging.debug(
            f"Skipping Yandex comment {g_comment_id} (content matches).")
        return added_to_yandex, edited_in_yandex

    response = await yapi.edit_comment(
        y_task_id, g_text, g_comment_id, y_comment_id, author_name)
    if isinstance(response, dict):
        edited_in_yandex += 1
    return added_to_yandex, edited_in_yandex


async def sync_yandex_comments_to_gandiva(g_task, y_comments,
                                          existing_g_comments, g_comment_texts):
    """Sync Yandex comments to Gandiva."""
    added_to_gandiva = 0
    edited_in_gandiva = 0

    for y_comment in y_comments:

        if should_skip_yandex_comment(y_comment):
            continue

        y_comment_id = str(y_comment.get('id'))
        y_text = y_comment.get('text', '')
        if len(y_text) > MAX_COMMENT_LENGTH:
            continue

        y_text_html = utils.markdown_to_html(utils.remove_mentions(y_text))
        g_addressees = get_addressees_for_g_task(g_task)
        y_comment_author = y_comment.get('createdBy', {}).get('display')

        if y_comment_id in existing_g_comments:
            g_comment_id = existing_g_comments.get(y_comment_id)
            g_text = g_comment_texts.get(y_comment_id).split('<br>', 1)[1]

            if g_text != y_text_html:
                await gapi.edit_comment(g_comment_id, y_comment_id, y_text_html,
                                        y_comment_author, g_addressees)
                edited_in_gandiva += 1
        else:
            await gapi.add_comment(g_task['Id'], y_text_html,
                                   y_comment_id, y_comment_author, g_addressees)
            added_to_gandiva += 1

    return added_to_gandiva, edited_in_gandiva


def should_skip_yandex_comment(y_comment) -> bool:
    y_author_id = y_comment.get('createdBy', {}).get('id')
    if not y_author_id:
        return True
    y_summonees = y_comment.get('summonees', ())

    if yc.robot_id is None:
        logging.warning(
            "Robot id was not found in config... "
            "(you shouldn't see this message *normally*)")
        return True

    return (y_author_id == yc.robot_id
            or not utils.id_in_summonees_exists(yc.robot_id, y_summonees))


def extract_yandex_comments(y_comments):
    """Extract and map Yandex comments to a dictionary for easy lookup."""
    existing_comments = {}
    comment_texts = {}

    for y_comment in y_comments:
        y_comment_id = str(y_comment.get('id'))
        text = y_comment.get('text', '')
        match = re.match(r'\[(\d+)]', text)

        if match:
            g_comment_id = match.group(1)
            existing_comments[g_comment_id] = y_comment_id
            comment_texts[g_comment_id] = text

    return existing_comments, comment_texts


def get_author_name(author):
    """Extract full name of the comment's author."""
    return f"{author['FirstName']} {author['LastName']}"


def get_addressees_for_g_task(g_task):
    """Get addressees for Gandiva task."""
    addressees = [g_task['Initiator']['Id']]
    contractor_id = g_task.get('Contractor', {}).get('Id')
    if contractor_id:
        addressees.append(contractor_id)
    return addressees


def is_programmer_or_contractor_in_addressees(g_comment, contractor_id):
    """Check if the programmer or contractor is in the addressees of the comment."""
    addressees = g_comment.get('Addressees', [])
    return (utils.g_addressee_exists(addressees, gc.programmer_id)
            or utils.g_addressee_exists(addressees, contractor_id))


def log_sync_results(added_to_yandex, added_to_gandiva,
                     edited_in_yandex, edited_in_gandiva):
    """Log the results of the comment sync."""
    logging.info(f"Comments added to Yandex: {added_to_yandex}")
    logging.info(f"Comments added to Gandiva: {added_to_gandiva}")
    logging.info(f"Comments edited in Yandex: {edited_in_yandex}")
    logging.info(f"Comments edited in Gandiva: {edited_in_gandiva}")


async def run_sync_services_periodically(queue: str, sync_mode: int, board_id: str,
                                         to_get_followers: bool = False,
                                         use_summaries: bool = False,
                                         interval_minutes: int = 30):
    """Runs sync_services every interval_minutes."""
    while True:
        try:
            await sync_services(queue, sync_mode, board_id,
                                to_get_followers, use_summaries)
        except Exception as e:
            # Log any error that happens in sync_services
            logging.error(f"Error during sync_services: {e}")

        # Wait for the specified interval before running sync_services again
        logging.info(f"Next sync in {interval_minutes} minute(s)")
        logging.info("-" * 40)

        await asyncio.sleep(interval_minutes * 60)


async def update_tasks_in_db(queue: str):
    y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))
    db_session = db.get_db_session()
    db.add_tasks(db_session, y_tasks, yc.fid_gandiva_task_id)


async def update_users_department_in_db(excel_obj: dict[str, DataFrame]):

    dep_analyst_dict = utils.extract_department_analysts_from_excel(excel_obj)
    users = await yapi.get_all_users()
    dep_user_nd_mapping = utils.map_department_nd_to_user_id(
        dep_analyst_dict, users)
    db_session = db.get_db_session()
    return db.add_user_department_nd_mapping(
        session=db_session, department_user_mapping=dep_user_nd_mapping)


async def update_it_users_in_db(excel_obj: dict[str, DataFrame]):

    it_users_dict = utils.extract_it_users_from_excel(excel_obj)
    y_users = await yapi.get_all_users()
    it_uids = await utils.map_emails_to_ids(it_users_dict, y_users)
    g_tasks = await gapi.get_tasks(gapi.Statuses.in_progress)
    g_users = utils.extract_unique_gandiva_users(g_tasks)
    uids_y_g = utils.map_it_uids_to_g_ids(it_uids, g_users)

    db_session = db.get_db_session()
    return db.add_or_update_user(session=db_session, user_data=uids_y_g)


async def test():
    sync_mode = 2
    err = False
    stop = True
    g_tasks = await gapi.get_tasks(gapi.Statuses.in_progress)

    res = await sync_comments(g_tasks, sync_mode)
    return res, err, stop


async def main():
    config = utils.ConfigObject
    logging_level = config.get('Settings', 'logging_level', fallback="INFO")
    utils.setup_logging(logging_level)
    logging.info(
        "-------------------- APPLICATION STARTED --------------------")

    chars = '"\''

    sync_mode = config.getint('Settings', 'sync_mode', fallback=0)
    to_get_followers = config.getboolean(
        'Settings', 'to_get_followers', fallback=False)
    use_summaries = config.getboolean(
        'Settings', 'use_summaries', fallback=False)
    interval_minutes = config.getint(
        'Settings', 'interval_minutes', fallback=5)
    db_url = (
        config.get('Database', 'url', fallback='sqlite:///project.db')
        .strip(chars))
    board_id = config.get('Settings', 'board_id').strip(chars)
    queue = config.get('Settings', 'queue').strip(chars)

    db_init(db_url)

    if TEST_FUNCTION:
        _, _, stop = await test()
        if stop:
            return

    await update_db(queue)

    info_settings = (
        f"Settings used in config: logging_level: {logging_level}; "
        f"sync_mode: {sync_mode}; queue: {queue}; board_id: {board_id}; "
        f"to_get_followers: {to_get_followers}; use_summaries: {use_summaries}; "
        f"interval_minutes: {interval_minutes}")
    logging.info(info_settings)

    await run_sync_services_periodically(
        queue, sync_mode, board_id, to_get_followers,
        use_summaries=use_summaries,
        interval_minutes=interval_minutes)


def db_init(db_url: str):
    db.set_db_url(db_url)


async def update_db(queue: str) -> None:
    db_session = db.get_db_session()
    db.clean_department_names(db_session)
    logging.info('Checking updates in database (full check)...')
    await yapi.check_access_token(yc.oauth_token)
    await update_tasks_in_db(queue=queue)

    await update_db_from_excel_data()
    db.find_duplicate_gandiva_tasks(db_session)


async def update_db_from_excel_data() -> None:
    logging.info('Checking updates in database (Excel data)...')

    excel_bytes = await yapi.download_file_from_yandex_disk(path=yc.path_to_excel)
    if excel_bytes is None:
        logging.error('Error occured while downloading excel file...')
        return

    excel_obj = utils.read_excel_from_bytes(excel_bytes)
    if excel_obj is None:
        logging.error('Error occured while reading excel file...')
        return

    departments_updated = await update_users_department_in_db(excel_obj)
    users_updated = await update_it_users_in_db(excel_obj)
    if departments_updated or users_updated:
        utils.EXCEL_UPDATED_IN_YANDEX_DISK = True
    else:
        utils.EXCEL_UPDATED_IN_YANDEX_DISK = False


async def sync_services(queue: str, sync_mode: int, board_id: str,
                        to_get_followers: bool, use_summaries: bool):
    """
    Synchronize Gandiva and Yandex Tracker services.
    queue: working queue in Yandex Tracker.
    sync_mode: which comments to sync.
    1 - all comments, 2 - only for programmers.
    """

    logging.info(f"Sync started!")
    await yapi.check_access_token(yc.oauth_token)

    g_tasks_all = await gapi.get_tasks(gapi.Statuses.all_)
    g_tasks_in_progress = gapi.extract_tasks_by_status(
        g_tasks_all, gapi.Statuses.in_progress)
    g_tasks_in_progress_or_waiting = gapi.extract_tasks_by_status(
        g_tasks_all, gapi.Statuses.in_progress_or_waiting)
    g_tasks_waiting = gapi.extract_tasks_by_status(
        g_tasks_all, gapi.Statuses.waiting)

    y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))

    tasks_added = await add_tasks(queue, use_summaries, g_tasks_in_progress_or_waiting, y_tasks)
    if tasks_added > 0:
        y_tasks = await yapi.get_tasks(query=yapi.get_query_in_progress(queue))

    await handle_tasks(
        sync_mode, to_get_followers, use_summaries, g_tasks_all,
        g_tasks_in_progress, g_tasks_waiting, y_tasks)

    await yapi.create_weekly_release_sprint(board_id)
    await handle_anomalies(queue, g_tasks_in_progress, g_tasks_in_progress_or_waiting, y_tasks)
    await update_db_from_excel_data()
    logging.info("Sync finished successfully!")


async def add_tasks(queue, use_summaries, g_tasks_in_progress_or_waiting, y_tasks) -> int:
    not_closed_task_ids = get_not_closed_task_ids(use_summaries, y_tasks)
    tasks_added = await yapi.add_tasks(
        g_tasks_in_progress_or_waiting, queue=queue, non_closed_ya_task_ids=not_closed_task_ids)

    return tasks_added


def get_not_closed_task_ids(
        use_summaries: bool,
        y_tasks: list[dict[str, str]]
) -> dict[str, str]:
    """
    Extracts task IDs for Yandex tasks that are not closed.

    :param use_summaries: Boolean indicating whether to extract task IDs from summaries.
    :param y_tasks: List of Yandex task dictionaries containing task information.
    :return: A dictionary with task IDs as keys and Yandex task keys as values.
    """
    not_closed_task_ids: dict[str, str] = {}
    if use_summaries:
        not_closed_task_ids = utils.extract_task_ids_from_summaries(y_tasks)

    not_closed_task_ids_2 = utils.extract_task_ids_from_gandiva_task_id(
        y_tasks, yc.fid_gandiva_task_id)
    not_closed_task_ids.update(not_closed_task_ids_2)

    return not_closed_task_ids


async def handle_tasks(
        sync_mode: int,
        to_get_followers: bool,
        use_summaries: bool,
        g_tasks_all: list[dict[str, Any]],
        g_tasks_in_progress: list[dict[str, Any]],
        g_tasks_waiting: list[dict[str, Any]],
        y_tasks: list[dict[str, str]]
) -> None:
    await yapi.batch_move_tasks_status(g_tasks_all, y_tasks)
    await yapi.edit_tasks(g_tasks_in_progress, y_tasks, to_get_followers, use_summaries)
    await yapi.edit_tasks(
        g_tasks_waiting, y_tasks, to_get_followers, use_summaries, edit_descriptions=False)
    await sync_comments(g_tasks_in_progress, sync_mode)


async def handle_anomalies(queue, g_tasks_in_progress, g_tasks_in_progress_or_waiting, y_tasks):
    await yapi.handle_cancelled_tasks_still_have_g_task_ids(queue)
    await gapi.handle_no_contractor_or_waiting_for_analyst(g_tasks_in_progress_or_waiting)
    await yapi.handle_in_work_but_waiting_for_analyst(g_tasks_in_progress, y_tasks)

if __name__ == "__main__":
    asyncio.run(main())
