import logging
import json
import asyncio
import gandiva_api as gapi
import datetime
import utils
from utils import perform_http_request
import db_module as db
import urllib.parse
import re
from configparser import ConfigParser


class YandexConfig:
    """Encapsulate Yandex-related configuration and API tokens."""

    def __init__(self, config: ConfigParser):
        chars = '"\''

        section = str('Yandex')
        self.x_cloud_org_id = config.get(
            section, 'x_cloud_org_id').strip(chars)
        self.oauth_token = config.get(section, 'oauth_token').strip(chars)
        self.oauth_refresh_token = str()
        self.client_id = config.get(section, 'client_id').strip(chars)
        self.client_secret = config.get(section, 'client_secret').strip(chars)
        self.robot_id = config.get(section, 'robot_id').strip(chars)
        self.disk_oauth_token = config.get(
            section, 'disk_oauth_token').strip(chars)
        self.path_to_excel = config.get(section, 'path_to_excel').strip(chars)

        section = str('YandexCustomFieldIds')
        self.fid_gandiva = config.get(section, 'gandiva').strip(chars)
        self.fid_initiator = config.get(section, 'initiator').strip(chars)
        self.fid_initiator_department = (config.get(section, 'initiator_department')
                                         .strip(chars))
        self.fid_analyst = config.get(section, 'analyst').strip(chars)
        self.fid_gandiva_task_id = config.get(
            section, 'gandiva_task_id').strip(chars)
        self.fid_nd = config.get(section, 'nd').strip(chars)
        self.fid_date_created_in_g = config.get(
            section, 'date_created_in_g').strip(chars)

        # OAuth URLs
        self.oauth_token_url = "https://oauth.yandex.ru/token"
        self.info_token_url = "https://login.yandex.ru/info"

        # API URLs
        self.gandiva_task_url = 'https://gandiva.s-stroy.ru/Request/Edit/'
        self.api_host = "https://api.tracker.yandex.net"

        # Headers
        self.headers = {
            "X-Cloud-Org-Id": self.x_cloud_org_id,
            "Authorization": f"OAuth {self.oauth_token}"
        }
        self.disk_headers = {
            "Authorization": f"OAuth {self.disk_oauth_token}"
        }


yc = YandexConfig(utils.ConfigObject)


def get_query_in_progress(queue):
    query = (
        'Resolution: empty() "Status Type": !cancelled "Status Type": '
        f'!done Queue: {queue} "Sort by": Updated DESC')
    return query


def get_query_all(queue):
    query = f'Queue: {queue} "Sort by": Updated DESC'
    return query


# Authorization block

async def refresh_access_token(refresh_token: str | None = None) -> str | None:
    if refresh_token is None:
        refresh_token = yc.oauth_refresh_token

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": yc.client_id,
        "client_secret": yc.client_secret
    }

    body_url_encoded = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = await perform_http_request(
        "POST", yc.oauth_token_url, headers=headers, body=body_url_encoded)

    if not isinstance(response, dict):
        logging.error("Failed to refresh access token.")
        return None

    access_token = str(response.get('access_token'))
    refresh_token = str(response.get('refresh_token'))

    yc.oauth_token = access_token
    yc.oauth_refresh_token = refresh_token

    return access_token


async def check_access_token(access_token) -> None:
    account_info = await get_account_info(access_token)

    if account_info and isinstance(account_info, dict):
        logging.info(f"Authorized user: {account_info['login']}")
        return None

    new_access_token = await refresh_access_token()
    return await check_access_token(new_access_token)


async def get_access_token_via_captcha() -> str | None:
    captcha_code = input(
        "Code (get from https://oauth.yandex.ru/authorize?"
        f"response_type=code&client_id={yc.client_id}): "
    )

    body = {
        "grant_type": "authorization_code",
        "code": captcha_code,
        "client_id": yc.client_id,
        "client_secret": yc.client_secret,
    }
    json_body = json.dumps(body)
    response_json = await perform_http_request("POST", yc.oauth_token_url, body=json_body)

    if response_json and isinstance(response_json, dict):
        logging.info("Successfully retrieved access token.")
        return response_json.get("access_token")

    logging.error("Failed to retrieve access token. Please try again.")
    return None


# Utils specific to Yandex

async def g_to_y_fields(g_task: dict, edit: bool, to_get_followers: bool = False) -> dict | None:
    g_task_id = str(g_task['Id'])
    initiator = g_task.get('Initiator')
    if not isinstance(initiator, dict):
        logging.error(f"Initiator is empty in task {g_task_id}!")
        return None
    initiator_name = f"{initiator.get('FirstName')} {initiator.get('LastName')}"
    description = utils.html_to_yandex_format(g_task['Description'])
    initiator_id = g_task['Initiator']['Id']
    initiator_department = await gapi.get_department_by_user_id(g_user_id=initiator_id)

    nd = await get_nd_by_department(initiator_department)
    y_assignee_id = await get_y_assignee_id(g_task.get('Contractor'))

    y_start_date = get_date_convert(g_task, "RequiredStartDate")
    y_date_created_in_g = get_date_convert(g_task, "CreateDate")

    if not edit:
        return create_add_task_payload(
            g_task_id, initiator_name, description, y_assignee_id, initiator_id,
            initiator_department, y_start_date, nd, y_date_created_in_g)

    followers = await get_followers(g_task_id, to_get_followers)

    return create_edit_task_payload(
        g_task_id, initiator_name, description, initiator_id, initiator_department,
        y_start_date, y_assignee_id, followers, nd, y_date_created_in_g)


async def get_nd_by_department(department_name):
    if not department_name:
        return None
    db_session = db.get_db_session()
    return db.get_nd_by_department_name(session=db_session, department_name=department_name)


async def get_y_assignee_id(g_assignee):
    if not g_assignee:
        return None
    g_assignee_id = g_assignee.get('Id')
    db_session = db.get_db_session()
    assignee = db.get_user_by_gandiva_id(
        session=db_session, g_user_id=g_assignee_id)
    return assignee.yandex_user_id if assignee else None


async def get_followers(g_task_id, to_get_followers):
    if not to_get_followers:
        return []
    g_task_detailed = await gapi.get_task(g_task_id)
    if isinstance(g_task_detailed, dict):
        observers = utils.extract_observers_from_detailed_task(g_task_detailed)
        db_session = db.get_db_session()
        return db.convert_gandiva_observers_to_yandex_followers(
            session=db_session, gandiva_observers=observers)
    return []


def create_add_task_payload(
        g_task_id, initiator_name, description, y_assignee_id, initiator_id,
        initiator_department, y_start_date, nd, y_date_created_in_g) -> dict:
    return {
        'gandiva_task_id': g_task_id,
        'initiator_name': initiator_name,
        'description': description,
        'assignee_id_yandex': y_assignee_id,
        'task_type': "improvement",
        'initiator_id': initiator_id,
        'initiator_department': initiator_department,
        'yandex_start_date': y_start_date,
        'nd': nd,
        'date_created_in_g': y_date_created_in_g
    }


def create_edit_task_payload(
        g_task_id, initiator_name, description, initiator_id, initiator_department,
        y_start_date, y_assignee_id, followers, nd, y_date_created_in_g) -> dict:
    return {
        'gandiva_task_id': g_task_id,
        'initiator_name': initiator_name,
        'description': description,
        'initiator_id': initiator_id,
        'initiator_department': initiator_department,
        'yandex_start_date': y_start_date,
        'assignee_id_yandex': y_assignee_id,
        'followers': followers,
        'nd': nd,
        'date_created_in_g': y_date_created_in_g
    }


def get_date_convert(g_task: dict, key: str):
    g_start_date = g_task.get(key)
    y_start_date = None
    if g_start_date:
        y_start_date = utils.convert_gandiva_to_yandex_date(g_start_date)
    return y_start_date


# Functions

async def get_account_info(access_token):
    """
    Gets Yandex account info by providing the access_token.
    """
    data = {
        "oauth_token": access_token,
        "format": "json",
    }

    response = await perform_http_request(
        'POST', yc.info_token_url, headers=yc.headers, body=json.dumps(data))
    return response


async def get_page_of_tasks(
        queue: str | None = None, keys: str | list[str] | None = None,
        _filter: dict | None = None, query: str | None = None,
        order: str | None = None, expand: str | None = None,
        login: str | None = None, page_number: int = 1, per_page: int = 5000) -> list | None:
    # Initialize the request body
    body: dict = {}

    # Respect the priority of parameters: queue > keys > filter > query
    # Our priority of parameters: query > queue > filter
    if query:
        body["query"] = query
    elif queue:
        body["filter"] = {"queue": queue}
        if login:
            body["filter"]["assignee"] = login
        if _filter:
            body["filter"].update(_filter)
    elif keys:
        body["keys"] = keys
    elif _filter:
        body["filter"] = _filter
        if login:
            body["filter"]["assignee"] = login
    elif login:
        body["filter"] = {"assignee": login}  # Allow filtering by login alone

    # Include sorting order if filters are used
    if "filter" in body and order:
        body["order"] = order

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL with the expand and perPage parameters
    url = f"{yc.api_host}/v2/issues/_search?perPage={per_page}"
    if page_number > 1:
        url += f"&page={page_number}"
    if expand:
        url += f"&expand={expand}"

    # Use the helper function to make the POST request
    res = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    # Return the response JSON or [] if the request failed
    if res and isinstance(res, list):
        return res
    elif not res:
        return []
    else:
        logging.error(
            f"Page {page_number} of tasks with query: {body} is empty.")
        return None


async def get_tasks_count(queue: str | None = None, _filter: dict | None = None,
                          query: str | None = None, assignee: str | None = None):
    # Initialize the request body
    body: dict = {}

    # Respect the priority of parameters: queue > keys > filter > query
    # Our priority is: query > queue > filter
    if query:
        body["query"] = query
    elif queue:
        body["filter"] = {"queue": queue}
        if assignee:
            body["filter"]["assignee"] = assignee
        if _filter:
            body["filter"].update(_filter)
    elif _filter:
        body["filter"] = _filter
        if assignee:
            body["filter"]["assignee"] = assignee
    elif assignee:
        # Allow filtering by login alone
        body["filter"] = {"assignee": assignee}

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL with the expand and perPage parameters
    url = f"{yc.api_host}/v2/issues/_count"

    # Use the helper function to make the POST request
    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    # Return the response JSON or None if the request failed
    if response:
        return response
    else:
        logging.error(f"Tasks with query: {body} were not found.")
        return None


async def get_tasks(
        queue: str | None = None, keys: str | list[str] | None = None,
        _filter: dict | None = None, query: str | None = None,
        order: str | None = None, expand: str | None = None, login: str | None = None
) -> list[dict]:
    if not queue and not query:
        raise ValueError("Queue or query have to be passed to this function.")

    # tasks_count = await get_tasks_count(queue=queue, filter=filter, query=query,assignee=login)
    per_page = 2000
    all_y_tasks = []
    page_number = 1
    while True:
        y_tasks = await get_page_of_tasks(
            queue=queue, keys=keys, _filter=_filter, query=query,
            order=order, expand=expand, login=login, page_number=page_number,
            per_page=per_page)
        if y_tasks is None:
            raise ValueError("[429] Too many requests.")
        all_y_tasks.extend(y_tasks)
        if len(y_tasks) < per_page:
            break
        page_number += 1

    # Append the tasks to the all_yandex_tasks list

    return all_y_tasks


async def get_tasks_by_gandiva_ids(g_ids: list[str]) -> dict | None:
    """Get tasks by their Gandiva IDs"""

    # Initialize the request body
    body = {
        "filter": {yc.fid_gandiva_task_id: g_ids}
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{yc.api_host}/v2/issues/_search?perPage=5000"

    # Use the helper function to make the POST request
    res = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    # Return the response JSON or None if the request failed
    if res and isinstance(res, dict):
        return res
    else:
        logging.error(f"Failed to get tasks with Gandiva IDs: {g_ids}")
        return None


async def get_page_of_users(page: int = 1, per_page: int = 100) -> list[dict] | None:
    """Returns page of users """

    url = f"{yc.api_host}/v2/users?perPage={per_page}&page={page}"

    res = await perform_http_request("GET", url, headers=yc.headers)

    if not (res and isinstance(res, list)):
        logging.error(f"Failed to get users.")
        return None

    return res


async def get_all_users(per_page: int = 100) -> list:
    """
    """
    all_users = list()
    page = 1
    while True:
        users = await get_page_of_users(page=page, per_page=per_page)
        if users is None:
            break
        all_users.extend(users)
        if len(users) < per_page:
            break
        page += 1

    return all_users


async def get_task(y_task_id: str) -> dict | None:
    """Returns task (dictionary type) """

    url = f"{yc.api_host}/v2/issues/{y_task_id}"

    res = await perform_http_request("GET", url, headers=yc.headers)

    if not (res and isinstance(res, dict)):
        logging.error(f"Failed to get task {y_task_id}.")
        return None
    return res


async def add_task(
        g_task_id, initiator_name, queue,
        description="Без описания", y_assignee=None,
        task_type=None, initiator_department=None, start=None, nd=None,
        y_step=None, date_created_in_g=None):
    g_task_id = str(g_task_id)
    db_session = db.get_db_session()
    task_in_db = db.get_task_by_gandiva_id(db_session, g_task_id=g_task_id)
    if task_in_db:
        return task_in_db

    # Construct the task summary
    task_summary = f"{g_task_id}. {description[:50]}..."

    # Prepare the request body
    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        yc.fid_initiator: initiator_name,
        yc.fid_gandiva: yc.gandiva_task_url + g_task_id,
        yc.fid_initiator_department: g_task_id,
        "unique": queue + g_task_id
    }

    # Optionally add other fields
    if y_assignee:
        body["assignee"] = y_assignee
    if task_type:
        body["type"] = task_type
    if initiator_department:
        body[yc.fid_initiator_department] = initiator_department
        if db_session is None:
            db_session = db.get_db_session()
        analyst = db.get_user_id_by_department(
            session=db_session, department_name=initiator_department)
        if analyst:
            body[yc.fid_analyst] = analyst
        if y_step in [0, 1, 2]:
            body["assignee"] = analyst
    if nd:
        body[yc.fid_nd] = nd
    if date_created_in_g:
        body[yc.fid_date_created_in_g] = date_created_in_g
    if start:
        body["start"] = start

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{yc.api_host}/v2/issues"

    # Use the helper function to make the POST request
    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    # Process the response
    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to add task {g_task_id}")
        return None
    task_id_yandex = response.get('key')

    if not task_id_yandex:
        logging.error(
            f"Failed to retrieve task key from response for task {g_task_id}")
        return None

    # Add task to the database
    if db_session is None:
        db_session = db.get_db_session()
    db.add_or_update_task(session=db_session,
                          g_task_id=g_task_id, y_task_id=task_id_yandex)
    return response


async def add_tasks(g_tasks: list[dict], queue: str, non_closed_ya_task_ids: dict) -> int:
    """
    Adds tasks from Gandiva to Yandex Tracker if they do not already exist.
    """
    logging.info("Adding tasks...")

    # Initialize a counter for added tasks
    added_task_count = 0

    for g_task in g_tasks:

        g_task_id = str(g_task['Id'])
        if g_task_id in non_closed_ya_task_ids.keys():
            logging.debug(f"Task {g_task_id} already exists.")
            db_session = db.get_db_session()
            db.add_or_update_task(
                session=db_session, g_task_id=g_task_id,
                y_task_id=non_closed_ya_task_ids[g_task_id])
            continue

        fields = await g_to_y_fields(g_task, edit=False)
        if not isinstance(fields, dict):
            logging.error("Fields is None, dict expected!")
            return 0
        initiator_name = fields.get('initiator_name')
        initiator_department = fields.get('initiator_department')
        description = str(fields.get('description'))
        y_assignee = fields.get('assignee_id_yandex')
        task_type = fields.get('task_type')
        start = fields.get('yandex_start_date')
        nd = fields.get('nd')
        y_date_created_in_g = fields.get('date_created_in_g')

        new_description = await description_with_attachments(g_task_id, description)
        if new_description is None:
            new_description = description

        g_status = g_task['Status']
        y_status = utils.g_to_y_status(g_status)
        if not y_status:
            continue
        y_step = utils.y_status_to_step(y_status)

        # Call add_task and check if a task was successfully added
        result = await add_task(
            g_task_id, initiator_name, initiator_department=initiator_department,
            description=new_description, queue=queue, y_assignee=y_assignee,
            task_type=task_type, start=start, nd=nd, y_step=y_step,
            date_created_in_g=y_date_created_in_g)

        # Check if the result indicates the task was added
        if isinstance(result, dict):
            added_task_count += 1
        elif isinstance(result, db.Task):
            logging.debug(f"Task {g_task_id} already exists.")
        else:
            logging.debug(
                f"Task {g_task_id} could not be added or already exists.")

    # Log the total number of added tasks
    logging.info(f"Total tasks added: {added_task_count}")
    return added_task_count


def edit_field_if_empty(y_task: dict, field_id: str, new_value: str):
    """
    Helper function to determine if a field needs to be updated based on it being empty or not.

    :param y_task: The task object from Yandex.
    :param field_id: The field ID to check in the Yandex task.
    :param new_value: The new value that might need to be applied.
    :return: The new value if it needs to be updated, None otherwise.
    """
    if new_value and not y_task.get(field_id):
        return new_value
    return None


def edit_field_if_different_content(y_task: dict, field_id: str, new_value: str):
    """
    Helper function to determine if a field needs to be updated based on it being empty or not.

    :param y_task: The task object from Yandex.
    :param field_id: The field ID to check in the Yandex task.
    :param new_value: The new value that might need to be applied.
    :return: The new value if it needs to be updated, None otherwise.
    """
    if y_task.get(field_id) and new_value != y_task.get(field_id):
        return new_value
    return None


async def update_task_if_needed(
        y_task, g_task_id, initiator_name, initiator_department, description, y_start_date,
        y_assignee, followers, nd, y_step, g_contractor_id, y_date_created_in_g, initiator_id):
    """
    Check which fields need to be updated and call edit_task if any updates are necessary.
    """
    y_task_id: str = y_task.get('key')

    if initiator_department:
        initiator_department = utils.normalize_department_name(
            initiator_department)
    # Custom followers handler
    edit_followers = None
    if len(y_task.get('followers', ())) < len(followers):
        edit_followers = followers
    current_y_assignee = y_task.get('assignee', {}).get('id')
    # Custom assignee handler
    edit_assignee = None
    if y_step not in [0, 1, 2] and current_y_assignee != y_assignee:
        edit_assignee = y_assignee
    if description:
        edit_description = edit_field_if_different_content(
            y_task, 'description', description)
    else:
        edit_description = None
    gandiva = yc.gandiva_task_url + g_task_id
    edit_fields = {
        'edit_initiator_name': edit_field_if_empty(y_task, yc.fid_initiator, initiator_name),
        'edit_initiator_department': edit_field_if_empty(
            y_task, yc.fid_initiator_department, initiator_department),
        'edit_description': edit_description,
        'edit_yandex_start_date': edit_field_if_empty(y_task, 'start', y_start_date),
        'edit_assignee_yandex_id': edit_assignee,
        'edit_analyst': None,
        'edit_followers': edit_followers,
        'edit_gandiva_task_id': edit_field_if_empty(
            y_task, yc.fid_initiator_department, g_task_id),
        'edit_gandiva': edit_field_if_empty(y_task, yc.fid_gandiva, gandiva),
        'edit_nd': edit_field_if_empty(y_task, yc.fid_nd, nd),
        'y_date_created_in_g': edit_field_if_empty(
            y_task, yc.fid_date_created_in_g, y_date_created_in_g)
    }

    db_session = db.get_db_session()
    update_analyst_and_assignee(y_task, initiator_department, y_step, g_contractor_id,
                                current_y_assignee, edit_fields, initiator_id, db_session)

    if not any(edit_fields.values()):
        logging.debug(f"Task {g_task_id} is already up-to-date.")
        return False

    response_json = await edit_task(
        y_task_id=y_task_id,
        initiator_name=edit_fields['edit_initiator_name'],
        initiator_department=edit_fields['edit_initiator_department'],
        description=edit_fields['edit_description'],
        start=edit_fields['edit_yandex_start_date'],
        analyst=edit_fields['edit_analyst'],
        y_assignee_id=edit_fields['edit_assignee_yandex_id'],
        followers=edit_fields['edit_followers'],
        g_task_id=edit_fields['edit_gandiva_task_id'],
        gandiva=edit_fields['edit_gandiva'],
        nd=edit_fields['edit_nd'],
        date_created_in_g=edit_fields['y_date_created_in_g'])

    if not response_json:
        return False

    return True


def update_analyst_and_assignee(y_task, initiator_department, y_step, g_contractor_id,
                                current_y_assignee, edit_fields, g_initiator_id, db_session):
    y_initiator = db.get_user_by_gandiva_id(
        session=db_session, g_user_id=g_initiator_id)
    cur_y_analyst = y_task.get(yc.fid_analyst, {}).get('id')
    y_initiator_id = str(y_initiator.yandex_user_id) if y_initiator else None
    if not db.is_user_analyst(session=db_session, yandex_user_id=y_initiator_id):
        if initiator_department:
            analyst = db.get_user_id_by_department(
                session=db_session, department_name=initiator_department)
            if analyst:
                if utils.EXCEL_UPDATED_IN_YANDEX_DISK:
                    # Check if different content
                    if cur_y_analyst != analyst:
                        edit_fields['edit_analyst'] = analyst
                else:
                    # Check if empty or waiting for analyst
                    if (not cur_y_analyst
                            and g_contractor_id in [None, gapi.gc.waiting_analyst_id]):
                        edit_fields['edit_analyst'] = analyst
                if y_step in [0, 1, 2] and current_y_assignee != analyst:
                    edit_fields['edit_assignee_yandex_id'] = analyst
    else:
        if cur_y_analyst != y_initiator_id:
            edit_fields['edit_analyst'] = y_initiator_id


def create_y_tasks_dict(y_tasks, use_summaries):
    """
    Creates a dictionary to map Yandex task unique IDs to their corresponding task objects.
    If `use_summaries` is True, it maps based on task IDs scraped from summaries.
    If `use_summaries` is False, it maps based on gandiva task id field.

    :param y_tasks: List of Yandex task objects.
    :param use_summaries: Whether to use task summaries to map task IDs.
    :return: Dictionary with task IDs as keys and Yandex task objects as values.
    """
    y_tasks_dict = {}
    fid = yc.fid_gandiva_task_id
    if not use_summaries:
        y_tasks_dict = {yt[fid]: yt for yt in y_tasks if yt.get(fid)}
        return y_tasks_dict
        # Map based on task IDs extracted from summaries

    for task in y_tasks:
        summary = task.get('summary', '')

        match = re.match(r'^(\d+)', summary)
        if match:
            task_id = match.group(1)  # Get the matched task ID
            y_tasks_dict[task_id] = task  # Map task_id to task object

    return y_tasks_dict


async def edit_tasks(
        g_tasks, y_tasks, to_get_followers, use_summaries=False, edit_descriptions=True) -> None:
    """
    Edits tasks in Yandex Tracker based on the data from Gandiva.
    Only updates tasks that are outdated.
    """
    logging.info(f"Editing tasks...")

    # Create a dictionary to map Yandex task unique IDs to their corresponding task objects
    y_tasks_dict = create_y_tasks_dict(
        y_tasks=y_tasks, use_summaries=use_summaries)

    # Initialize a counter for edited tasks
    edited_task_count = 0

    for g_task in g_tasks:
        g_task_id = str(g_task['Id'])

        # Find the corresponding Yandex task using the dictionary
        y_task = y_tasks_dict.get(g_task_id)
        if not y_task:
            continue

        fields = await g_to_y_fields(g_task, edit=True, to_get_followers=to_get_followers)
        if not isinstance(fields, dict):
            logging.error("Fields is None, dict expected!")
            return None
        initiator_name = fields.get('initiator_name')
        initiator_id = fields.get('initiator_id')
        initiator_department = fields.get('initiator_department')
        description = fields.get('description')
        y_start_date = fields.get('yandex_start_date')
        y_assignee_id = fields.get('assignee_id_yandex')
        followers = fields.get('followers')
        nd = fields.get('nd')
        y_date_created_in_g = fields.get('date_created_in_g')
        g_status = g_task['Status']
        y_status = utils.g_to_y_status(g_status)
        if not y_status:
            continue
        y_step = utils.y_status_to_step(y_status)
        g_contractor = g_task.get('Contractor')
        g_contractor_id = None
        if g_contractor:
            g_contractor_id = g_contractor.get('Id')
        if edit_descriptions and isinstance(description, str):
            description = await description_with_attachments(g_task_id, description)
        else:
            description = None
        res = await update_task_if_needed(
            y_task, g_task_id, initiator_name, initiator_department,
            description, y_start_date, y_assignee_id, followers, nd,
            y_step, g_contractor_id, y_date_created_in_g, initiator_id
        )
        if res:
            edited_task_count += 1
            y_task_id = y_task['key']
            db_session = db.get_db_session()
            db.add_or_update_task(session=db_session,
                                  g_task_id=g_task_id, y_task_id=y_task_id)

    # Log the total number of edited tasks
    logging.info(f"Total tasks edited: {edited_task_count}")


async def description_with_attachments(g_task_id: str, description: str) -> str | None:
    """Appends formatted attachments to the task description."""

    g_task_detailed = await gapi.get_task(g_task_id)
    if not isinstance(g_task_detailed, dict):
        return None

    attachments_text = utils.format_attachments(
        g_task_detailed.get('Attachments'))
    return f"{description}\n\n{attachments_text}"


async def edit_task(
        y_task_id: str,
        summary: str | None = None,
        description: str | None = None,
        y_assignee_id: str | None = None,
        task_type: str | None = None,
        priority: str | None = None,
        parent: str | None = None,
        sprint: str | None = None,
        followers: list | None = None,
        initiator_name: str | None = None,
        initiator_department: str | None = None,
        analyst: str | None = None,
        start: str | None = None,
        g_task_id: str | None = None,
        gandiva: str | None = None,
        nd: str | None = None,
        date_created_in_g: str | None = None
) -> dict | None:
    """Edits a Yandex task's attributes."""

    y_task_id = str(y_task_id)
    body = {k: v for k, v in {
        "summary": summary,
        "description": description,
        "assignee": y_assignee_id,
        "type": task_type,
        "priority": priority,
        "parent": parent,
        "sprint": sprint,
        "followers": {"set": followers} if followers else None,
        yc.fid_initiator: initiator_name,
        yc.fid_initiator_department: initiator_department,
        yc.fid_nd: nd,
        yc.fid_date_created_in_g: date_created_in_g,
        yc.fid_analyst: analyst,
        "start": start,
        yc.fid_gandiva: gandiva,
        yc.fid_initiator_department: g_task_id if g_task_id is not None else None
    }.items() if v is not None}

    body_json = json.dumps(body)
    url = f"{yc.api_host}/v2/issues/{y_task_id}"

    response = await perform_http_request('PATCH', url, headers=yc.headers, body=body_json)
    if not response or isinstance(response, bool):
        logging.error(f"Failed editing task {y_task_id}")
        return None

    logging.info(f"Successfully edited task {y_task_id}")
    return response


async def move_task_status(
        y_task_id: str, transition_id: str, comment: str = "", resolution: str = "fixed"
) -> dict | None:
    """Moves a Yandex task to a new status."""

    y_task_id = str(y_task_id)
    comment = comment or f"Task status has been moved to {transition_id} for {y_task_id}."

    body = {
        "comment": comment,
        "resolution": resolution if transition_id == "close" else None
    }

    body_json = json.dumps({k: v for k, v in body.items() if v is not None})
    url = f"{yc.api_host}/v2/issues/{y_task_id}/transitions/{transition_id}/_execute"

    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)
    if isinstance(response, bool):
        logging.warning("Got response of type bool, dict | None expected")
        return None

    return response


async def move_tasks_status(tasks: list[dict], new_status: str) -> dict | None:
    """Moves specified tasks to the given status."""

    url = f"{yc.api_host}/v2/bulkchange/_transition"
    transition = utils.y_status_to_y_transition(new_status)
    task_keys = utils.extract_task_keys(tasks)

    body = {
        "transition": transition,
        "issues": task_keys
    }

    response = await perform_http_request(
        'POST', url, headers=yc.headers, body=json.dumps(body)
    )

    if not (
        response
        and isinstance(response, dict)
    ):
        logging.error(f"Failed to move tasks to status {new_status}.")
        return None

    logging.info(f"Moved {len(task_keys)} tasks to status {new_status}.")
    return response


async def move_status_task_groups(grouped_tasks: dict[str, list[dict]]) -> list:
    """Moves each group of Yandex tasks to a new status asynchronously."""

    move_tasks = [
        move_tasks_status(tasks, new_status)
        for new_status, tasks in grouped_tasks.items()
    ]

    return await asyncio.gather(*move_tasks)


async def batch_move_tasks_status(
        g_tasks: list[dict], y_tasks: list[dict], should_filter: bool = True
) -> None:
    """Batch moves tasks to their updated statuses based on grouping."""

    grouped_tasks = group_tasks_by_status(
        g_tasks, y_tasks, to_filter=should_filter)
    await move_status_task_groups(grouped_tasks)
    logging.info("Task statuses are up-to-date!")


async def batch_edit_tasks(values: dict, task_ids: list[str]) -> dict | None:
    """Bulk update tasks in Yandex Tracker."""

    if not task_ids:
        logging.warning("No task IDs provided for bulk update.")
        return None

    if not values:
        logging.warning("No values provided for bulk update.")
        return None

    url = f"{yc.api_host}/v2/bulkchange/_update"

    body = {
        "issues": task_ids,
        "values": values
    }
    json_body = json.dumps(body)

    response = await perform_http_request("POST", url, headers=yc.headers, body=json_body)
    if not (response and isinstance(response, dict)):
        logging.error("Failed to perform bulk change for tasks: %s", task_ids)
        return None

    logging.info("Bulk change successful for tasks: %s", task_ids)
    return response


def filter_tasks_with_gandiva_task_id(y_tasks):
    """Filters and returns only the tasks that contain the initiator_department key."""
    return [task for task in y_tasks if yc.fid_initiator_department in task]


def filter_tasks_with_unique(y_tasks):
    """Filters and returns only the tasks that contain the 'unique' key."""
    return [task for task in y_tasks if 'unique' in task]


async def add_existing_tracker_tasks_to_db() -> None:
    """Fetches existing tracker tasks and adds those with Gandiva task IDs to the database."""

    tasks = await get_tasks()
    filtered_tasks = filter_tasks_with_gandiva_task_id(tasks)

    if not filtered_tasks:
        logging.info("No tasks with Gandiva task IDs found.")
        return

    db_session = db.get_db_session()
    db.add_tasks(session=db_session, y_tasks=filtered_tasks)

    logging.info(f"Added {len(filtered_tasks)} tasks to the database.")


async def get_sprints_on_board(board_id: str) -> list[dict] | None:
    """Fetches and returns sprints for the specified board."""

    url = f"{yc.api_host}/v2/boards/{board_id}/sprints"
    response = await perform_http_request('GET', url, headers=yc.headers)

    if isinstance(response, list):
        return response

    logging.error(f"Failed to retrieve sprints for board {board_id}.")
    return None


async def create_sprint(board_id: str, name: str, start_date: str, end_date: str) -> None:
    """Creates sprint for the given board.
    date_format: YYYY-MM-DD"""
    url = f"{yc.api_host}/v2/sprints"
    body = {
        "name": name,
        "board":
            {
                "id": board_id
            },
        "startDate": start_date,
        "endDate": end_date
    }
    body_json = json.dumps(body)
    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)
    if not response:
        logging.error(f"Failed to create new sprint.")
        return None

    logging.info(
        f"Sprint '{name}' [{start_date} - {end_date}] was successfully created.")
    return None


async def create_weekly_release_sprint(
        board_id: str, day_of_the_week: int = 3, sprint_length_days: int = 8
) -> dict | None:
    """Creates a sprint starting on weekday and ending the following weekday, 
    named 'Релиз [End Date]'.

    Skips creation if a sprint for the same period already exists."""

    start_date = get_next_day_of_the_week(day_of_the_week)
    end_date = start_date + datetime.timedelta(days=sprint_length_days)
    sprint_name = format_sprint_name(end_date)

    existing_sprints = await get_sprints_on_board(board_id)
    if existing_sprints is None:
        logging.error(
            f"Failed to retrieve existing sprints on board {board_id}.")
        return None

    if sprint_already_exists(existing_sprints, start_date, end_date, sprint_name):
        logging.info(
            f"Sprint '{sprint_name}' [{start_date} - {end_date}] already exists.")
        return next(sprint for sprint in existing_sprints if sprint['name'] == sprint_name)

    await create_sprint(board_id, sprint_name, start_date.isoformat(), end_date.isoformat())
    return None


def get_next_day_of_the_week(day_of_the_week: int) -> datetime.date:
    today = datetime.date.today()
    return today + datetime.timedelta((day_of_the_week - today.weekday()) % 7)


def format_sprint_name(end_date: datetime.date) -> str:
    day = end_date.day
    month = utils.MONTHS_RUSSIAN[end_date.month]
    return f"Релиз {day} {month}"


def sprint_already_exists(
        existing_sprints: list,
        start_date: datetime.date,
        end_date: datetime.date,
        sprint_name: str
) -> bool:
    return any(
        datetime.date.fromisoformat(sprint['startDate']) == start_date and
        datetime.date.fromisoformat(sprint['endDate']) == end_date and
        sprint['name'] == sprint_name
        for sprint in existing_sprints
    )


async def get_page_of_comments(
        y_task_id: str, per_page: int = 100, after_id: str | None = None
) -> list | None:
    """Fetches comments for the given Yandex task with pagination support."""

    url = f"{yc.api_host}/v2/issues/{y_task_id}/comments?perPage={per_page}"
    if after_id:
        url += f"&id={after_id}"

    response = await perform_http_request('GET', url, headers=yc.headers)
    return response if isinstance(response, list) else None


async def get_comments(y_task_id: str, per_page: int = 100) -> list:
    """Retrieves all comments for the given Yandex task, handling pagination."""

    comments = []
    after_id = None

    while True:
        page_comments = await get_page_of_comments(
            y_task_id, per_page=per_page, after_id=after_id
        )
        if not page_comments:
            break

        comments.extend(page_comments)

        if len(page_comments) < per_page:
            break

        after_id = page_comments[-1]['id']

    return comments


async def add_comment(
        y_task_id: str, comment: str, g_comment_id: str | None = None,
        author_name: str | None = None):
    url = f"{yc.api_host}/v2/issues/{y_task_id}/comments"

    comment = format_g_comment_to_y(comment, g_comment_id, author_name)

    body = {
        "text": comment
    }

    body_json = json.dumps(body)
    response = await perform_http_request("POST", url=url, headers=yc.headers, body=body_json)

    if not response:
        logging.error(f"Comment was not added to task {y_task_id}.")
        return None

    logging.debug(f"Comment was successfully added to task {y_task_id}!")
    return response


def format_g_comment_to_y(comment, g_comment_id, author_name):
    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"
        return comment
    return comment


async def edit_comment(y_task_id: str, comment: str, g_comment_id: str | None = None,
                       y_comment_id: str | None = None, author_name: str | None = None):
    url = f"{yc.api_host}/v2/issues/{y_task_id}/comments/{y_comment_id}"

    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"

    body = {
        "text": comment
    }
    json_body = json.dumps(body)
    response = await perform_http_request("PATCH", url=url, headers=yc.headers, body=json_body)

    if not response:
        logging.error(f"Comment was not edited in task {y_task_id}.")
        return None

    logging.debug(f"Comment was successfully edited in task {y_task_id}!")
    return response


async def remove_followers_in_tasks(y_task_ids: list[str], followers: list[str]):
    url = f"{yc.api_host}/v2/bulkchange/_update"
    body = {
        "issues": y_task_ids,
        "values": {"followers": {"remove": followers}}
    }
    json_body = json.dumps(body)
    response = await perform_http_request('POST', url, headers=yc.headers, body=json_body)

    return response


def group_tasks_by_status(
        g_tasks: list, y_tasks: list, to_filter: bool = True) -> dict:
    """Groups tasks by their current Gandiva status with optional filtering."""
    grouped_tasks: dict = {}

    for gandiva_task in g_tasks:
        g_task_id = gandiva_task['Id']
        g_status = gandiva_task['Status']
        yandex_task = find_matching_yandex_task(y_tasks, g_task_id)

        if not yandex_task:
            continue

        g_status_step, y_status_step = map_task_status_steps(
            g_status, yandex_task)
        g_status_step = adjust_status_if_needed(gandiva_task, g_status_step)

        if to_filter and g_status_step <= y_status_step:
            continue

        group_task_by_status(gandiva_task, yandex_task, grouped_tasks)

    return grouped_tasks


def find_matching_yandex_task(yandex_tasks: list[dict], gandiva_task_id: str) -> dict | None:
    """Finds the corresponding Yandex task based on Gandiva task ID."""
    return next(
        (
            task
            for task in yandex_tasks
            if task.get(yc.fid_initiator_department) == str(gandiva_task_id)
        ),
        None
    )


def map_task_status_steps(gandiva_status: str, yandex_task: dict) -> tuple[int, int]:
    """Maps Gandiva status to Yandex task status steps."""

    y_status = yandex_task.get('status')
    if not isinstance(y_status, dict):
        logging.error("Invalid status structure in Yandex task.")
        return 0, 0

    y_key = y_status.get('key')
    if not y_key:
        logging.error("Yandex status key is missing.")
        return 0, 0

    yandex_status = utils.g_to_y_status(gandiva_status)
    if not yandex_status:
        return 0, 0

    g_step = utils.y_status_to_step(yandex_status)
    y_step = utils.y_status_to_step(y_key)

    return (g_step, y_step) if g_step and y_step else (0, 0)


def adjust_status_if_needed(gandiva_task: dict, g_status_step: int) -> int:
    """Adjusts the Gandiva status step based on specific conditions."""
    waiting_analyst_id = gapi.gc.waiting_analyst_id
    contractor_id = gandiva_task.get('Contractor', {}).get('Id')
    writing_spec = 3
    if (g_status_step == writing_spec
        and contractor_id
            and contractor_needs_adjustment(contractor_id, waiting_analyst_id)):
        g_status_step = 2
    return g_status_step


def contractor_needs_adjustment(contractor_id: str, waiting_analyst_id: str) -> bool:
    """Checks if the contractor requires status adjustment."""
    users_to_skip = [1001, 878, 1020, 852, 410]
    db_session = db.get_db_session()
    return contractor_id == waiting_analyst_id or (
        not db.get_user_by_gandiva_id(
            session=db_session, g_user_id=contractor_id)
        and contractor_id not in users_to_skip)


def group_task_by_status(gandiva_task: dict, yandex_task: dict, grouped_tasks: dict) -> None:
    """Groups the task by its Gandiva status."""
    g_status = utils.g_to_y_status(gandiva_task['Status'])
    if g_status not in grouped_tasks:
        grouped_tasks[g_status] = []
    grouped_tasks[g_status].append(yandex_task)


async def handle_in_work_but_waiting_for_analyst(g_tasks: list[dict], y_tasks: list[dict]) -> bool:
    """Handles tasks in work that are waiting for an analyst."""

    needed_status = 'twowaitingfortheanalyst'
    anomaly_g_tasks = list()
    anomaly_y_tasks = list()
    users_to_skip = {1001, 878, 1020, 852, 410}
    writing_spec = 3

    for g_task in g_tasks:
        y_status = utils.g_to_y_status(g_task['Status'])
        if not y_status:
            continue
        if (utils.y_status_to_step(y_status)) == writing_spec:
            contractor = g_task.get('Contractor', {}).get('Id')
            db_session = db.get_db_session()

            if (
                contractor == gapi.gc.waiting_analyst_id
                or (
                    not db.get_user_by_gandiva_id(
                        session=db_session, g_user_id=contractor)
                    and contractor not in users_to_skip
                )
            ):
                anomaly_g_tasks.append(g_task)

    cursed_g_task_ids = list(map(str, utils.extract_task_ids(anomaly_g_tasks)))

    if not anomaly_g_tasks:
        logging.info("No anomalies found!")
        return True

    for y_task in y_tasks:
        g_task_id = y_task.get(yc.fid_initiator_department)

        if g_task_id in cursed_g_task_ids and g_task_id:
            status = y_task.get('status', {})
            if not isinstance(status, dict):
                logging.error
                continue
            if status.get('key') != needed_status:
                anomaly_y_tasks.append(y_task)
            else:
                logging.debug(
                    f'Task {y_task.get("key")} already in status {needed_status}')

    if not anomaly_y_tasks:
        logging.info("No anomalies found!")
        return True

    if await move_tasks_status(anomaly_y_tasks, needed_status):
        logging.info(f"Handled {len(anomaly_y_tasks)} anomaly tasks!")

    return bool(anomaly_y_tasks)


async def handle_cancelled_tasks_still_have_g_task_ids(queue: str) -> bool:
    """Removes Gandiva task IDs from cancelled Yandex tasks."""

    query = (f'DEV."Номер заявки в Гандиве": notEmpty() '
             f'(Resolution: notEmpty() or "Status Type": cancelled, done) '
             f'Queue: {queue} Status: onecancelled "Sort by": Updated DESC')

    y_tasks = await get_tasks(query=query)

    if not y_tasks:
        logging.info("No anomalies found!")
        return True

    count = 0
    for y_task in y_tasks:
        y_task_id = y_task.get('key')
        if not isinstance(y_task_id, str):
            logging.warning(
                f"y_task_id got type {type(y_task_id)}; str expected")
            continue
        if await process_cancelled_task(y_task_id):
            count += 1

    if count:
        logging.info(
            f"Handled {count} cancelled tasks (removed g_task_id from them)!")
    else:
        logging.info("No g_task_id removed.")

    return count > 0


async def process_cancelled_task(y_task_id: str) -> bool:
    """Removes the Gandiva task ID from a cancelled task."""

    res = await edit_task(y_task_id, g_task_id='')
    if not res:
        logging.error(
            f"Failed removing GandivaTaskId from cancelled task {y_task_id}.")
        return False
    logging.info(f"Removed GandivaTaskId from cancelled task {y_task_id}!")
    return True


async def download_file_from_yandex_disk(path):
    url = f'https://cloud-api.yandex.net/v1/disk/resources/download?path={path}'
    file_dict = await perform_http_request(method='GET', url=url, headers=yc.disk_headers)
    if not (file_dict and isinstance(file_dict, dict)):
        logging.error(f"Failed getting file url from Yandex Disk.")
        return
    file_url = str(file_dict.get('href'))
    file = await perform_http_request(method='GET', url=file_url, headers=yc.disk_headers)
    return file
