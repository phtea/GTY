import asyncio
import datetime
import json
import logging
import re
import urllib.parse
from configparser import ConfigParser
from typing import Any

# Project-Specific Modules
import db_module as db
from db_module import Task
import utils
from utils import perform_http_request
from common_utils import normalize_department_name
import gandiva_api as gapi


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


def get_query_in_progress(queue: str) -> str:
    """
    Get a query to filter tasks in Yandex Tracker that are in progress.

    Args:
        queue (str): The queue to filter tasks from.

    Returns:
        str: The query string.
    """
    query = (
        'Resolution: empty() "Status Type": !cancelled "Status Type": '
        f'!done Queue: {queue} "Sort by": Updated DESC')
    return query


def get_query_all(queue: str) -> str:
    query = f'Queue: {queue} "Sort by": Updated DESC'
    return query


# Authorization block

async def refresh_access_token(refresh_token: str | None = None) -> str | None:
    if refresh_token is None:
        refresh_token = yc.oauth_refresh_token

    body: dict[str, str] = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": yc.client_id,
        "client_secret": yc.client_secret,
    }

    body_url_encoded: str = urllib.parse.urlencode(body)

    headers: dict[str, str] = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response: dict[str, Any] | Any = await perform_http_request(
        "POST", yc.oauth_token_url, headers=headers, body=body_url_encoded)
    if response is None:
        logging.error("Failed to refresh access token.")
        return None

    access_token: str | None = response.get('access_token')
    new_refresh_token: str | None = response.get('refresh_token')
    if not (access_token and new_refresh_token):
        logging.error("Failed to retrieve access token.")
        return None

    yc.oauth_token = access_token
    yc.oauth_refresh_token = new_refresh_token

    return access_token


async def check_access_token(access_token: str) -> None:
    account_info = await get_account_info(access_token)

    if isinstance(account_info, dict) and account_info:
        logging.info(f"Authorized user: {account_info['login']}")
        return None

    new_access_token = await refresh_access_token()
    if new_access_token is None:
        logging.error("Failed to refresh access token.")
        return None
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

async def g_to_y_fields(
    g_task: dict[str, Any],
    edit: bool,
    to_get_followers: bool = False,
) -> dict[str, Any] | None:
    """
    Converts a Gandiva task into a Yandex Tracker task payload.

    This function is used when creating a new task in Yandex Tracker
    or when editing an existing one.
    It takes a Gandiva task, a boolean indicating whether the task is
    being created or edited, and a boolean indicating whether to get followers for the task,
    and returns a dictionary containing the fields for the Yandex Tracker task.

    The returned dictionary contains the following fields:

    - summary: a string containing the task summary.
    - description: a string containing the task description.
    - assignee: a string containing the Yandex Tracker ID of the assignee.
    - followers: a list of strings containing the Yandex Tracker IDs of the followers.
    - initiator_id: an integer containing the ID of the initiator in Gandiva.
    - initiator_name: a string containing the name of the initiator.
    - initiator_department: a string containing the department of the initiator.
    - yandex_start_date: a string containing the start date of the task in the format %Y-%m-%d.
    - nd: a string containing the number of days until the deadline.
    - g_task_id: a string containing the ID of the task in Gandiva.
    - gandiva: a string containing the URL of the task in Gandiva.
    - date_created_in_g: a string containing the date and time of creation of the
    task in Gandiva in the format %Y-%m-%dT%H:%M:%S.%f%z.

    The function returns None if the task is invalid (e.g. if the initiator is missing).

    :param g_task: a dictionary containing the fields of the Gandiva task.
    :param edit: a boolean indicating whether the task is being created or edited.
    :param to_get_followers: a boolean indicating whether to get followers for the task.
    :return: a dictionary containing the fields for the Yandex
    Tracker task, or None if the task is invalid.
    """
    g_task_id = str(g_task["Id"])

    initiator: dict[str, str] | None = g_task.get("Initiator")
    if not isinstance(initiator, dict):
        logging.error(f"Initiator is empty in task {g_task_id}!")
        return None

    initiator_first_name = initiator.get("FirstName")
    initiator_second_name = initiator.get("LastName")
    if not initiator_first_name or not initiator_second_name:
        logging.error(f"Initiator is empty in task {g_task_id}!")
        return None

    initiator_name = f"{initiator_first_name} {initiator_second_name}"
    description = utils.html_to_yandex_format(g_task["Description"])
    initiator_id = g_task["Initiator"]["Id"]
    initiator_department = await gapi.get_department_by_user_id(g_user_id=initiator_id)

    nd = await get_nd_by_department(initiator_department)
    y_assignee_id = await get_y_assignee_id(g_task.get("Contractor"))

    y_start_date = get_date_convert(g_task, "RequiredStartDate")
    y_date_created_in_g = get_date_convert(g_task, "CreateDate")

    if not edit:
        return create_add_task_payload(
            g_task_id,
            initiator_name,
            description,
            y_assignee_id,
            initiator_id,
            initiator_department,
            y_start_date,
            nd,
            y_date_created_in_g,
        )

    followers = await get_followers(g_task_id, to_get_followers)

    return create_edit_task_payload(
        g_task_id,
        initiator_name,
        description,
        initiator_id,
        initiator_department,
        y_start_date,
        y_assignee_id,
        followers,
        nd,
        y_date_created_in_g,
    )


async def get_nd_by_department(
        department_name: str | None
) -> str | None:
    if not department_name:
        return None
    db_session = db.get_db_session()
    return db.get_nd_by_department_name(session=db_session, department_name=department_name)


async def get_y_assignee_id(
        g_assignee: dict[str, Any] | None
) -> str | None:
    """Retrieve Yandex assignee ID from Gandiva assignee."""
    if not g_assignee:
        return None

    g_assignee_id = g_assignee.get('Id')
    if not g_assignee_id:
        return None
    g_assignee_id = str(g_assignee_id)

    db_session = db.get_db_session()
    assignee = db.get_user_by_gandiva_id(
        session=db_session, g_user_id=g_assignee_id)
    return str(assignee.yandex_user_id) if assignee else None


async def get_followers(
        g_task_id: str, to_get_followers: bool
) -> list[str]:
    if not to_get_followers:
        return []
    g_task_detailed = await gapi.get_task(g_task_id)
    if isinstance(g_task_detailed, dict):
        observers = utils.extract_observers_from_detailed_task(g_task_detailed)
        db_session = db.get_db_session()
        return db.convert_gandiva_observers_to_yandex_followers(
            session=db_session, gandiva_observers=observers
        )
    return []


def create_add_task_payload(
        g_task_id: str,
        initiator_name: str,
        description: str,
        y_assignee_id: str | None,
        initiator_id: str,
        initiator_department: str | None,
        y_start_date: str | None,
        nd: str | None,
        y_date_created_in_g: str | None
) -> dict[str, str | None]:
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
        g_task_id: str,
        initiator_name: str,
        description: str,
        initiator_id: str,
        initiator_department: str | None,
        y_start_date: str | None,
        y_assignee_id: str | None,
        followers: list[str],
        nd: str | None,
        y_date_created_in_g: str | None) -> dict[str, str | list[str] | None]:
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


def get_date_convert(
        g_task: dict[str, Any],
        key: str
) -> str | None:
    g_start_date = g_task.get(key)
    y_start_date: str | None = None
    if g_start_date:
        y_start_date = utils.convert_gandiva_to_yandex_date(g_start_date)
    return y_start_date


# Functions

async def get_account_info(
        access_token: str
) -> dict[str, Any] | None:
    """
    Gets Yandex account info by providing the access_token.
    """
    body: dict[str, str] = {
        "oauth_token": access_token,
        "format": "json",
    }
    json_body = json.dumps(body)

    response = await perform_http_request(
        'POST', yc.info_token_url, headers=yc.headers, body=json_body
    )
    if not (response and isinstance(response, dict)):
        return

    return response


async def get_page_of_tasks(
    queue: str | None = None, keys: str | list[str] | None = None,
    _filter: dict[str, Any] | None = None, query: str | None = None,
    order: str | None = None, expand: str | None = None,
    login: str | None = None, page_number: int = 1, per_page: int = 5000
) -> list[dict[str, Any]] | None:

    body: dict[str, Any] = {}
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
        body["filter"] = {"assignee": login}

    if "filter" in body and order:
        body["order"] = order

    body_json = json.dumps(body)

    url = f"{yc.api_host}/v2/issues/_search?perPage={per_page}"
    if page_number > 1:
        url += f"&page={page_number}"
    if expand:
        url += f"&expand={expand}"

    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    if response is None:
        logging.error(
            f"Failed to get page of tasks with query: {body} and page number: {page_number}."
        )
        return None

    if not isinstance(response, list):
        logging.error(f"Got type: {type(response)}; list expected")
        return None

    logging.debug(f"Page of tasks fetched.")
    return response


async def get_tasks_count(
        queue: str | None = None, filter: dict[str, Any] | None = None,
        query: str | None = None, assignee: str | None = None
) -> int | None:
    body: dict[str, Any] = {}

    if query:
        body["query"] = query
    elif queue:
        body["filter"] = {"queue": queue}
        if assignee:
            body["filter"]["assignee"] = assignee
        if filter:
            body["filter"].update(filter)
    elif filter:
        body["filter"] = filter
        if assignee:
            body["filter"]["assignee"] = assignee
    elif assignee:
        body["filter"] = {"assignee": assignee}

    url = f"{yc.api_host}/v2/issues/_count"
    response = await perform_http_request('POST', url, headers=yc.headers, body=json.dumps(body))

    if not (response and isinstance(response, int)):
        logging.error(f"Tasks with query: {body} were not found.")
        return None

    return response


async def get_tasks(
        queue: str | None = None,
        keys: str | list[str] | None = None,
        _filter: dict[str, Any] | None = None,
        query: str | None = None,
        order: str | None = None,
        expand: str | None = None,
        login: str | None = None
) -> list[dict[str, Any]]:
    if not queue and not query:
        raise ValueError("Queue or query must be specified.")

    per_page = 2000
    all_y_tasks: list[dict[str, Any]] = []
    page_number = 1
    while True:
        y_tasks = await get_page_of_tasks(
            queue=queue, keys=keys, _filter=_filter, query=query,
            order=order, expand=expand, login=login, page_number=page_number,
            per_page=per_page)
        if y_tasks is None:
            raise ValueError("y_tasks is None, list expected.")
        all_y_tasks.extend(y_tasks)
        if len(y_tasks) < per_page:
            break
        page_number += 1

    return all_y_tasks


async def get_tasks_by_gandiva_ids(g_ids: list[str]) -> dict[str, Any] | None:
    """Get tasks by their Gandiva IDs"""

    body: dict[str, Any] = {
        "filter": {yc.fid_gandiva_task_id: g_ids}
    }

    body_json: str = json.dumps(body)

    url: str = f"{yc.api_host}/v2/issues/_search?perPage=5000"

    response = await perform_http_request(
        'POST', url, headers=yc.headers, body=body_json
    )
    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to get tasks with Gandiva IDs: {g_ids}")
        return None

    return response


async def get_page_of_users(
    page: int = 1,
    per_page: int = 100
) -> list[dict[str, Any]] | None:
    """Returns page of users """

    url = f"{yc.api_host}/v2/users?perPage={per_page}&page={page}"

    response = await perform_http_request("GET", url, headers=yc.headers)
    if not (response and isinstance(response, list)):
        logging.error(f"Failed to get users.")
        return None

    return response


async def get_all_users(per_page: int = 100) -> list[dict[str, Any]]:
    """
    Retrieves all users from the Yandex Tracker instance.

    :param per_page: The number of users to retrieve per page.
    :return: A list of dictionaries representing the users.
    """
    all_users: list[dict[str, Any]] = []
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


async def get_task(y_task_id: str) -> dict[str, Any] | None:
    """Returns task (dictionary type) """

    url = f"{yc.api_host}/v2/issues/{y_task_id}"

    response = await perform_http_request("GET", url, headers=yc.headers)
    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to get task {y_task_id}.")
        return None

    return response


async def add_task(
        g_task_id: str,
        initiator_name: str,
        queue: str,
        description: str = "Без описания",
        y_assignee: str | None = None,
        task_type: str | None = None,
        initiator_department: str | None = None,
        start: str | None = None,
        nd: str | None = None,
        y_step: int | None = None,
        date_created_in_g: str | None = None
) -> dict[str, Any] | None | Task:
    db_session = db.get_db_session()

    if task_in_db := db.get_task_by_gandiva_id(db_session, g_task_id=g_task_id):
        return task_in_db

    task_summary = f"{g_task_id}. {description[:50]}..."

    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        yc.fid_initiator: initiator_name,
        yc.fid_gandiva: yc.gandiva_task_url + g_task_id,
        yc.fid_gandiva_task_id: g_task_id,
    }

    if y_assignee:
        body["assignee"] = y_assignee
    if task_type:
        body["type"] = task_type
    if initiator_department:
        body[yc.fid_initiator_department] = initiator_department
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

    body_json = json.dumps(body)

    url = f"{yc.api_host}/v2/issues"

    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)

    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to add task {g_task_id}")
        return None
    task_id_yandex = response.get('key')

    if not task_id_yandex:
        logging.error(
            f"Failed to retrieve task key from response for task {g_task_id}")
        return None

    db.add_or_update_task(session=db_session,
                          g_task_id=g_task_id, y_task_id=task_id_yandex)
    return response


async def add_tasks(
    g_tasks: list[dict[str, Any]],
    queue: str,
    non_closed_ya_task_ids: dict[str, str]
) -> int:
    logging.info("Adding tasks...")
    added_task_count = 0

    for g_task in g_tasks:
        g_task_id = str(g_task['Id'])
        if g_task_id in non_closed_ya_task_ids:
            logging.debug(f"Task {g_task_id} already exists.")
            db.add_or_update_task(
                session=db.get_db_session(),
                g_task_id=g_task_id,
                y_task_id=non_closed_ya_task_ids[g_task_id]
            )
            continue

        fields = await g_to_y_fields(g_task, edit=False)
        if not isinstance(fields, dict):
            logging.error("Fields is None, dict expected!")
            return 0

        initiator_name = fields.get('initiator_name')
        if not initiator_name:
            continue

        new_description = await description_with_attachments(
            g_task_id, str(fields.get('description'))
        ) or str(fields.get('description'))

        y_status = utils.g_to_y_status(str(g_task['Status']))
        if not y_status:
            continue

        y_step = utils.y_status_to_step(y_status)
        if not y_step:
            continue

        result = await add_task(
            g_task_id,
            initiator_name,
            initiator_department=fields.get('initiator_department'),
            description=new_description,
            queue=queue,
            y_assignee=fields.get('assignee_id_yandex'),
            task_type=fields.get('task_type'),
            start=fields.get('yandex_start_date'),
            nd=fields.get('nd'),
            y_step=y_step,
            date_created_in_g=fields.get('date_created_in_g')
        )

        if isinstance(result, dict):
            logging.info(f"Added task {g_task_id}")
            added_task_count += 1

    logging.info(f"Total tasks added: {added_task_count}")
    return added_task_count


def edit_field_if_empty(
    y_task: dict[str, Any], field_id: str, new_value: str | None
) -> str | None:
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


def edit_field_if_different_content(
    y_task: dict[str, Any], field_id: str, new_value: str
) -> str | None:
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
    y_task: dict[str, Any],
    g_task_id: str,
    initiator_name: str,
    initiator_department: str | None,
    description: str | None,
    y_start_date: str | None,
    y_assignee: str | None,
    followers: list[str] | None,
    nd: str | None,
    y_step: int,
    g_contractor_id: str | None,
    y_date_created_in_g: str | None,
    initiator_id: str
) -> bool:
    """
    Check which fields need to be updated and call edit_task if any updates are necessary.
    """
    y_task_id: str = y_task.get('key', '')
    initiator_department = normalize_department_name(
        initiator_department) if initiator_department else None

    # Determine fields to edit
    edit_followers = determine_followers(y_task, followers)
    edit_assignee = determine_assignee(y_task, y_step, y_assignee)
    edit_description = determine_description(y_task, description)

    gandiva: str = yc.gandiva_task_url + g_task_id

    # Gather fields for updating
    edit_initiator_name = edit_field_if_empty(
        y_task, yc.fid_initiator, initiator_name)
    edit_initiator_department = edit_field_if_empty(
        y_task, yc.fid_initiator_department, initiator_department)
    edit_yandex_start_date = edit_field_if_empty(y_task, 'start', y_start_date)
    edit_gandiva_task_id = edit_field_if_empty(
        y_task, yc.fid_gandiva, g_task_id)
    edit_gandiva = edit_field_if_empty(y_task, yc.fid_gandiva, gandiva)
    edit_nd = edit_field_if_empty(y_task, yc.fid_nd, nd)
    y_date_created_in_g = edit_field_if_empty(
        y_task, yc.fid_date_created_in_g, y_date_created_in_g)

    assignee_id = None
    assignee: dict[str, Any] | None = y_task.get('assignee') or {}
    if isinstance(assignee, dict) and 'id' in assignee:
        assignee_id = str(assignee.get('id'))

    db_session = db.get_db_session()
    update_analyst_and_assignee(
        y_task,
        initiator_department,
        y_step,
        g_contractor_id,
        assignee_id,
        initiator_id,
        db_session
    )

    if not any((
        edit_initiator_name,
        edit_initiator_department,
        edit_description,
        edit_yandex_start_date,
        edit_assignee,
        edit_followers,
        edit_gandiva_task_id,
        edit_gandiva,
        edit_nd,
        y_date_created_in_g
    )):

        logging.debug(f"Task {g_task_id} is already up-to-date.")
        return False

    response_json = await edit_task(
        y_task_id=y_task_id,
        initiator_name=edit_initiator_name,
        initiator_department=edit_initiator_department,
        description=edit_description,
        start=edit_yandex_start_date,
        analyst=None,  # Placeholder for analyst update logic
        y_assignee_id=edit_assignee,
        followers=edit_followers,
        g_task_id=edit_gandiva_task_id,
        gandiva=edit_gandiva,
        nd=edit_nd,
        date_created_in_g=y_date_created_in_g
    )

    return response_json is not None


def determine_followers(
    y_task: dict[str, Any],
    new_followers: list[str] | None
) -> list[str] | None:
    """Determine if followers need to be updated."""

    if not new_followers:
        return None

    current_followers = y_task.get('followers', [])
    return new_followers if len(current_followers) < len(new_followers) else None


def determine_assignee(y_task: dict[str, Any], y_step: int, new_assignee: str | None) -> str | None:
    """Determine if the assignee needs to be updated."""
    current_assignee = y_task.get('assignee', {}).get('id')
    return new_assignee if y_step not in [0, 1, 2] and current_assignee != new_assignee else None


def determine_description(y_task: dict[str, Any], new_description: str | None) -> str | None:
    """Determine if the description needs to be updated."""
    return edit_field_if_different_content(y_task, 'description', new_description) if new_description else None


def update_analyst_and_assignee(
    y_task: dict[str, Any],
    initiator_department: str | None,
    y_step: int,
    g_contractor_id: str | None,
    current_y_assignee: str | None,
    g_initiator_id: str,
    db_session: db.Session,
) -> tuple[str | None, str | None]:
    """
    Update the analyst and assignee based on the initiator's department and conditions.

    Returns:
        Tuple of (new_analyst_id, new_assignee_id) that should be updated.
    """
    y_initiator = db.get_user_by_gandiva_id(
        session=db_session, g_user_id=g_initiator_id)
    cur_y_analyst = y_task.get(yc.fid_analyst, {}).get('id')
    y_initiator_id = str(y_initiator.yandex_user_id) if y_initiator else None

    new_analyst_id: str | None = None
    new_assignee_id: str | None = None

    if not y_initiator_id:
        return new_analyst_id, new_assignee_id

    if not db.is_user_analyst(session=db_session, yandex_user_id=y_initiator_id):
        new_analyst_id = assign_new_analyst(
            db_session, initiator_department, cur_y_analyst, g_contractor_id
        )
        if new_analyst_id and y_step in [0, 1, 2] and current_y_assignee != new_analyst_id:
            new_assignee_id = new_analyst_id
    else:
        if cur_y_analyst != y_initiator_id:
            new_analyst_id = y_initiator_id

    return new_analyst_id, new_assignee_id


def assign_new_analyst(
    db_session: db.Session,
    initiator_department: str | None,
    current_analyst_id: str | None,
    g_contractor_id: str | None
) -> str | None:
    """Determine the new analyst ID based on the initiator's department and current conditions."""
    if initiator_department:
        analyst = db.get_user_id_by_department(
            session=db_session, department_name=initiator_department
        )
        if not analyst:
            return

        if utils.EXCEL_UPDATED_IN_YANDEX_DISK:
            content_is_different = current_analyst_id != analyst
            if content_is_different:
                return analyst

        if (
            not current_analyst_id
            and g_contractor_id in [None, gapi.gc.waiting_analyst_id]
        ):
            return analyst
    return None


def create_y_tasks_dict(
        y_tasks: list[dict[str, Any]],
        use_summaries: bool
) -> dict[str, dict[str, Any]]:
    """
    Creates a dictionary to map Yandex task unique IDs to their corresponding task objects.
    If `use_summaries` is True, it maps based on task IDs scraped from summaries.
    If `use_summaries` is False, it maps based on gandiva task id field.

    :param y_tasks: List of Yandex task objects.
    :param use_summaries: Whether to use task summaries to map task IDs.
    :return: Dictionary with task IDs as keys and Yandex task objects as values.
    """
    y_tasks_dict: dict[str, dict[str, Any]] = {}
    fid: str = yc.fid_gandiva_task_id

    if not use_summaries:
        y_tasks_dict = {yt[fid]: yt for yt in y_tasks if yt.get(fid)}
        return y_tasks_dict

    for task in y_tasks:
        summary: str = task.get('summary', '')

        match = re.match(r'^(\d+)', summary)
        if match:
            task_id: str = match.group(1)  # Get the matched task ID
            y_tasks_dict[task_id] = task  # Map task_id to task object

    return y_tasks_dict


async def edit_tasks(
        g_tasks: list[dict[str, Any]],
        y_tasks: list[dict[str, Any]],
        to_get_followers: bool,
        use_summaries: bool = False,
        edit_descriptions: bool = True) -> None:
    """
    Edits tasks in Yandex Tracker based on the data from Gandiva.
    Only updates tasks that are outdated.
    """
    logging.info(f"Editing tasks...")

    y_tasks_dict: dict[str, dict[str, Any]] = create_y_tasks_dict(
        y_tasks=y_tasks, use_summaries=use_summaries)

    edited_task_count: int = 0

    for g_task in g_tasks:
        g_task_id: str = str(g_task['Id'])

        y_task: dict[str, Any] | None = y_tasks_dict.get(g_task_id)
        if not y_task:
            continue

        fields: dict[str, Any] | None = await g_to_y_fields(g_task, edit=True, to_get_followers=to_get_followers)
        if not isinstance(fields, dict):
            logging.error("Fields is None, dict expected!")
            return None

        initiator_name: str | None = fields.get('initiator_name')
        if not initiator_name:
            logging.error("Initiator name is None, str expected!")
            continue

        initiator_id: str | None = fields.get('initiator_id')
        if not initiator_id:
            logging.error("Initiator ID is None, str expected!")
            continue

        initiator_department: str | None = fields.get('initiator_department')
        description: str | None = fields.get('description')
        y_start_date: str | None = fields.get('yandex_start_date')
        y_assignee_id: str | None = fields.get('assignee_id_yandex')
        followers: list[str] | None = fields.get('followers')
        nd: str | None = fields.get('nd')
        y_date_created_in_g: str | None = fields.get('date_created_in_g')
        g_status: str = str(g_task['Status'])

        y_status: str | None = utils.g_to_y_status(g_status)
        if not y_status:
            continue

        y_step = utils.y_status_to_step(y_status)
        if y_step is None:
            logging.error(f"Invalid Yandex task status: {y_status}")
            continue

        g_contractor: dict[str, Any] | None = g_task.get('Contractor')
        g_contractor_id: str | None = None
        if g_contractor:
            g_contractor_id = g_contractor.get('Id')
        if edit_descriptions and isinstance(description, str):
            description = await description_with_attachments(g_task_id, description)
        else:
            description = None
        response = await update_task_if_needed(
            y_task, g_task_id, initiator_name, initiator_department,
            description, y_start_date, y_assignee_id, followers, nd,
            y_step, g_contractor_id, y_date_created_in_g, initiator_id
        )
        if response:
            edited_task_count += 1
            y_task_id: str = y_task['key']
            db_session: db.Session = db.get_db_session()
            db.add_or_update_task(session=db_session,
                                  g_task_id=g_task_id, y_task_id=y_task_id)

    logging.info(f"Total tasks edited: {edited_task_count}")


async def description_with_attachments(
        g_task_id: str,
        description: str
) -> str | None:
    """Appends formatted attachments to the task description."""

    g_task_detailed: dict[str, Any] | None = await gapi.get_task(g_task_id)
    if not isinstance(g_task_detailed, dict):
        return None

    attachments_text: str = utils.format_attachments(
        g_task_detailed.get('Attachments', []))
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
        followers: list[Any] | None = None,
        initiator_name: str | None = None,
        initiator_department: str | None = None,
        analyst: str | None = None,
        start: str | None = None,
        g_task_id: str | None = None,
        gandiva: str | None = None,
        nd: str | None = None,
        date_created_in_g: str | None = None
) -> dict[str, Any] | None:
    """Edits a Yandex task's attributes."""

    y_task_id = str(y_task_id)

    body: dict[str, Any] = {k: v for k, v in {  # type: ignore
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
        yc.fid_gandiva_task_id: g_task_id,
        "start": start,
        yc.fid_gandiva: gandiva
    }.items() if v is not None}

    if g_task_id == '':
        body[yc.fid_gandiva_task_id] = None

    body_json = json.dumps(body)
    url = f"{yc.api_host}/v2/issues/{y_task_id}"

    response = await perform_http_request(
        'PATCH', url, headers=yc.headers, body=body_json
    )
    if not (response and isinstance(response, dict)):
        logging.error(f"Failed editing task {y_task_id}")
        return

    logging.info(f"Successfully edited task {y_task_id}")
    return response


async def move_task_status(
        y_task_id: str,
        transition_id: str,
        comment: str = "",
        resolution: str | None = "fixed"
) -> dict[str, Any] | None:
    """Moves a Yandex task to a new status."""

    y_task_id = str(y_task_id)
    comment = comment or f"Task status has been moved to {transition_id} for {y_task_id}."

    body: dict[str, str | None] = {
        "comment": comment,
        "resolution": resolution if transition_id == "close" else None
    }

    body_json = json.dumps({k: v for k, v in body.items() if v is not None})
    url = f"{yc.api_host}/v2/issues/{y_task_id}/transitions/{transition_id}/_execute"

    response = await perform_http_request('POST', url, headers=yc.headers, body=body_json)
    if not (response and isinstance(response, dict)):
        logging.warning(
            f"Got response of type {type(response)}, dict expected")
        return

    return response


async def move_tasks_status(
        tasks: list[dict[str, str]],
        new_status: str
) -> dict[str, Any] | None:
    """Moves specified tasks to the given status."""

    url = f"{yc.api_host}/v2/bulkchange/_transition"
    transition = utils.y_status_to_y_transition(new_status)

    task_keys = utils.extract_task_keys(tasks)
    if not task_keys:
        logging.error(f"Failed to move tasks to status {new_status}.")
        return

    body: dict[str, list[str] | str] = {
        "transition": transition,
        "issues": task_keys
    }

    response = await perform_http_request(
        'POST', url, headers=yc.headers, body=json.dumps(body)
    )

    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to move tasks to status {new_status}.")
        return None

    logging.info(f"Moved {len(task_keys)} tasks to status {new_status}.")
    return response


async def move_status_task_groups(
        grouped_tasks: dict[str, list[dict[str, str]]]
) -> list[dict[str, Any] | None]:
    """Moves each group of Yandex tasks to a new status asynchronously."""

    move_tasks = [
        move_tasks_status(tasks, new_status)
        for new_status, tasks in grouped_tasks.items()
    ]

    return await asyncio.gather(*move_tasks)


async def batch_move_tasks_status(
        g_tasks: list[dict[str, Any]],
        y_tasks: list[dict[str, Any]],
        should_filter: bool = True
) -> None:
    """Batch moves tasks to their updated statuses based on grouping."""

    grouped_tasks: dict[str, list[dict[str, Any]]] = group_tasks_by_status(
        g_tasks,
        y_tasks,
        to_filter=should_filter
    )
    await move_status_task_groups(grouped_tasks)
    logging.info("Task statuses are up-to-date!")


async def batch_edit_tasks(
        values: dict[str, Any],
        task_ids: list[str]
) -> dict[str, Any] | None:
    """Bulk update tasks in Yandex Tracker."""

    if not task_ids:
        logging.warning("No task IDs provided for bulk update.")
        return None

    if not values:
        logging.warning("No values provided for bulk update.")
        return None

    url = f"{yc.api_host}/v2/bulkchange/_update"

    body: dict[str, Any] = {
        "issues": task_ids,
        "values": values
    }
    json_body = json.dumps(body)

    response = await perform_http_request(
        "POST", url, headers=yc.headers, body=json_body
    )
    if not (response and isinstance(response, dict)):
        logging.error("Failed to perform bulk change for tasks: %s", task_ids)
        return

    logging.info("Bulk change successful for tasks: %s", task_ids)
    return response


def filter_tasks_with_gandiva_task_id(
        y_tasks: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Filters and returns only the tasks that contain the initiator_department key."""
    return [task for task in y_tasks if yc.fid_initiator_department in task]


def filter_tasks_with_unique(
    y_tasks: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Filters and returns only the tasks that contain the 'unique' key."""
    return [task for task in y_tasks if "unique" in task]


async def add_existing_tracker_tasks_to_db() -> None:
    """Fetches existing tracker tasks and adds those with Gandiva task IDs to the database."""

    tasks = await get_tasks()
    filtered_tasks = filter_tasks_with_gandiva_task_id(tasks)

    if not filtered_tasks:
        logging.info("No tasks with Gandiva task IDs found.")
        return

    db_session = db.get_db_session()
    db.add_tasks(db_session, filtered_tasks, yc.fid_gandiva_task_id)

    logging.info(f"Added {len(filtered_tasks)} tasks to the database.")


async def get_sprints_on_board(
        board_id: str
) -> list[dict[str, Any]] | None:
    """Fetches and returns sprints for the specified board."""

    url = f"{yc.api_host}/v2/boards/{board_id}/sprints"

    response = await perform_http_request('GET', url, headers=yc.headers)
    if not (response and isinstance(response, list)):
        logging.error(f"Failed to retrieve sprints for board {board_id}.")
        return

    return response


async def create_sprint(
        board_id: str,
        name: str,
        start_date: str,
        end_date: str
) -> None:
    """Creates sprint for the given board.
    date_format: YYYY-MM-DD"""

    url = f"{yc.api_host}/v2/sprints"
    body: dict[str, Any] = {
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
        return

    logging.info(
        f"Sprint '{name}' [{start_date} - {end_date}] was successfully created.")
    return


async def create_weekly_release_sprint(
        board_id: str, day_of_the_week: int = 3, sprint_length_days: int = 8
) -> dict[str, Any] | None:
    """Creates a sprint starting on weekday and ending the following weekday, 
    named ' '.

    Skips creation if a sprint for the same period already exists."""

    start_date: datetime.date = get_next_day_of_the_week(day_of_the_week)
    end_date: datetime.date = start_date + \
        datetime.timedelta(days=sprint_length_days)
    sprint_name: str = format_sprint_name(end_date)

    existing_sprints = await get_sprints_on_board(board_id)
    if existing_sprints is None:
        logging.error(
            f"Failed to retrieve existing sprints on board {board_id}.")
        return

    if sprint_already_exists(existing_sprints, start_date, end_date, sprint_name):
        logging.info(
            f"Sprint '{sprint_name}' [{start_date} - {end_date}] already exists.")
        return next(
            (sprint for sprint in existing_sprints if sprint['name'] == sprint_name),
            None
        )

    await create_sprint(board_id, sprint_name, start_date.isoformat(), end_date.isoformat())
    return


def get_next_day_of_the_week(day_of_the_week: int) -> datetime.date:
    today = datetime.date.today()
    return today + datetime.timedelta((day_of_the_week - today.weekday()) % 7)


def format_sprint_name(end_date: datetime.date) -> str:
    day = end_date.day
    month = utils.MONTHS_RUSSIAN[end_date.month]
    return f"Релиз {day} {month}"


def sprint_already_exists(
        existing_sprints: list[dict[str, Any]],
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
) -> list[dict[str, Any]] | None:
    """Fetches comments for the given Yandex task with pagination support."""

    url = f"{yc.api_host}/v2/issues/{y_task_id}/comments?perPage={per_page}"
    if after_id:
        url += f"&id={after_id}"

    response = await perform_http_request('GET', url, headers=yc.headers)
    return response if isinstance(response, list) else None


async def get_comments(
        y_task_id: str,
        per_page: int = 100
) -> list[dict[str, Any]]:
    """Retrieves all comments for the given Yandex task, handling pagination."""

    comments: list[dict[str, Any]] = []
    after_id: str | None = None

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
        y_task_id: str,
        comment: str,
        g_comment_id: str | None = None,
        author_name: str | None = None
):
    """
    Adds a comment to the given Yandex task.

    :param y_task_id: the id of the Yandex task to add the comment to
    :param comment: the comment text
    :param g_comment_id: the id of the associated Gandiva comment
    :param author_name: the name of the author of the comment
    :return: the response from the Yandex API, or None if the comment could not be added
    """

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


def format_g_comment_to_y(
        comment: str, g_comment_id: str | None, author_name: str | None
) -> str:
    """Formats a Gandiva comment to be added to Yandex
    by adding the comment id and author name if given."""

    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"
    return comment


async def edit_comment(
        y_task_id: str,
        comment: str,
        g_comment_id: str | None = None,
        y_comment_id: str | None = None,
        author_name: str | None = None
) -> dict[str, Any] | None:
    """Edits a comment in the given Yandex task.

    :param y_task_id: the id of the Yandex task to edit the comment in
    :param comment: the new comment text
    :param g_comment_id: the id of the associated Gandiva comment
    :param y_comment_id: the id of the comment to edit
    :param author_name: the name of the author of the comment
    :return: the response from the Yandex API, or None if the comment could not be edited
    """
    url = f"{yc.api_host}/v2/issues/{y_task_id}/comments/{y_comment_id}"

    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"

    body = {
        "text": comment
    }
    json_body = json.dumps(body)
    response = await perform_http_request("PATCH", url=url, headers=yc.headers, body=json_body)

    if not (response and isinstance(response, dict)):
        logging.error(f"Comment was not edited in task {y_task_id}.")
        return None

    logging.debug(f"Comment was successfully edited in task {y_task_id}!")
    return response


async def remove_followers_in_tasks(
        y_task_ids: list[str],
        followers: list[str]
) -> dict[str, Any] | None:
    """Removes followers from tasks in Yandex Tracker.

    :param y_task_ids: list of Yandex task IDs to remove followers from
    :param followers: list of Yandex user IDs to remove as followers
    :return: the response from the Yandex API, or None if the followers could not be removed
    """
    url = f"{yc.api_host}/v2/bulkchange/_update"

    body: dict[str, Any] = {
        "issues": y_task_ids,
        "values": {"followers": {"remove": followers}}
    }
    json_body = json.dumps(body)

    response = await perform_http_request('POST', url, headers=yc.headers, body=json_body)

    if not (response and isinstance(response, dict)):
        logging.error(f"Followers were not removed from tasks: {y_task_ids}.")
        return None

    logging.debug(
        f"Followers were successfully removed from tasks: {y_task_ids}!")
    return response


def group_tasks_by_status(
        g_tasks: list[dict[str, Any]],
        y_tasks: list[dict[str, Any]],
        to_filter: bool = True
) -> dict[str, list[dict[str, Any]]]:
    """Groups tasks by their current Gandiva status with optional filtering."""
    grouped_tasks: dict[str, list[dict[str, Any]]] = {}

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


def find_matching_yandex_task(
        yandex_tasks: list[dict[str, Any]],
        gandiva_task_id: str
) -> dict[str, Any] | None:
    """Finds the corresponding Yandex task based on Gandiva task ID."""
    return next(
        (
            task
            for task in yandex_tasks
            if task.get(yc.fid_initiator_department) == str(gandiva_task_id)
        ),
        None
    )


def map_task_status_steps(
        gandiva_status: str,
        yandex_task: dict[str, str | dict[str, Any]]
) -> tuple[int, int]:
    """Maps Gandiva status to Yandex task status steps."""

    y_status = yandex_task.get('status')
    if not (y_status and isinstance(y_status, dict)):
        logging.error("Invalid status structure in Yandex task.")
        return 0, 0

    y_key: Any = y_status.get('key')
    if not (y_key and isinstance(y_key, str)):
        logging.error("Yandex status key is missing.")
        return 0, 0

    yandex_status: str | None = utils.g_to_y_status(gandiva_status)
    if not yandex_status:
        return 0, 0

    g_step = utils.y_status_to_step(yandex_status)
    y_step = utils.y_status_to_step(y_key)

    return (g_step, y_step) if g_step is not None and y_step is not None else (0, 0)


def adjust_status_if_needed(
        gandiva_task: dict[str, str | dict[str, Any]],
        g_status_step: int
) -> int:
    """Adjusts the Gandiva status step based on specific conditions."""
    waiting_analyst_id = gapi.gc.waiting_analyst_id

    contractor = gandiva_task.get('Contractor')
    if not (contractor and isinstance(contractor, dict)):
        return g_status_step

    contractor_id: str | int | None = contractor.get('Id')
    if not contractor_id:
        return g_status_step

    contractor_id = str(contractor_id)
    writing_spec = 3
    if (
        g_status_step == writing_spec
        and contractor_id
        and contractor_needs_adjustment(contractor_id, waiting_analyst_id)
    ):
        g_status_step = 2
    return g_status_step


def contractor_needs_adjustment(
        contractor_id: str,
        waiting_analyst_id: str
) -> bool:
    """Checks if the contractor requires status adjustment."""

    users_to_skip: list[int] = [1001, 878, 1020, 852, 410]
    db_session = db.get_db_session()

    return contractor_id == waiting_analyst_id or (
        not db.get_user_by_gandiva_id(
            session=db_session, g_user_id=contractor_id)
        and int(contractor_id) not in users_to_skip)


def group_task_by_status(
        gandiva_task: dict[str, Any],
        yandex_task: dict[str, Any],
        grouped_tasks: dict[str, list[dict[str, Any]]]
) -> None:
    """Groups the task by its Gandiva status."""
    g_status = str(gandiva_task.get('Status'))

    y_status = utils.g_to_y_status(g_status)
    if not y_status:
        return

    if y_status not in grouped_tasks:
        grouped_tasks[y_status] = []
    grouped_tasks[y_status].append(yandex_task)


async def handle_in_work_but_waiting_for_analyst(
        g_tasks: list[dict[str, Any]],
        y_tasks: list[dict[str, Any]]
) -> bool:
    """Handles tasks in work that are waiting for an analyst."""

    needed_status: str = 'twowaitingfortheanalyst'
    anomaly_g_tasks: list[dict[str, Any]] = []
    anomaly_y_tasks: list[dict[str, Any]] = []
    users_to_skip: set[str] = {"1001", "878", "1020", "852", "410"}
    writing_spec: int = 3

    for g_task in g_tasks:
        g_status: str | None = g_task.get('Status')
        if not g_status:
            continue
        g_status = str(g_status)
        y_status = utils.g_to_y_status(g_status)
        if not (
            y_status
            and utils.y_status_to_step(y_status) == writing_spec
        ):
            continue
        contractor: int | str | None = g_task.get(
            'Contractor', {}).get('Id')
        if not contractor:
            continue
        contractor = str(contractor)

        db_session = db.get_db_session()

        if not (
            contractor == gapi.gc.waiting_analyst_id
            or (
                not db.get_user_by_gandiva_id(
                    session=db_session, g_user_id=contractor)
                and contractor not in users_to_skip
            )
        ):
            continue
        anomaly_g_tasks.append(g_task)

    cursed_g_task_ids = utils.extract_task_ids(anomaly_g_tasks)
    if not cursed_g_task_ids:
        return False

    if not anomaly_g_tasks:
        logging.info("No anomalies found!")
        return True

    for y_task in y_tasks:
        g_task_id: str | None = y_task.get(yc.fid_initiator_department)

        if not (g_task_id in cursed_g_task_ids and g_task_id):
            continue

        status: dict[str, Any] | None = y_task.get('status', {})
        if not isinstance(status, dict):
            logging.error(f"Status is not a dict, got {status}")
            continue

        if status.get('key') != needed_status:
            anomaly_y_tasks.append(y_task)
            continue
        logging.debug(
            f'Task {y_task.get("key")} already in status {needed_status}'
        )

    if not anomaly_y_tasks:
        logging.info("No anomalies found!")
        return True

    if await move_tasks_status(anomaly_y_tasks, needed_status):
        logging.info(f"Handled {len(anomaly_y_tasks)} anomaly tasks!")

    return bool(anomaly_y_tasks)


async def handle_cancelled_tasks_still_have_g_task_ids(queue: str) -> bool:
    """Removes Gandiva task IDs from cancelled Yandex tasks."""

    query = (
        f'{queue}."Номер заявки в Гандиве": notEmpty() "Status type": cancelled '
        f'Queue: {queue} "Sort by": Updated DESC'
    )

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

    response = await edit_task(y_task_id, g_task_id='')
    if not response:
        logging.error(
            f"Failed removing GandivaTaskId from cancelled task {y_task_id}.")
        return False

    logging.info(f"Removed GandivaTaskId from cancelled task {y_task_id}!")
    return True


async def download_file_from_yandex_disk(path: str) -> bytes | None:
    """
    Downloads a file from Yandex Disk by its path.

    :param path: path to the file on Yandex Disk
    :return: bytes of the file or None if failed
    """
    url = f'https://cloud-api.yandex.net/v1/disk/resources/download?path={path}'

    file_dict = await perform_http_request(method='GET', url=url, headers=yc.disk_headers)
    if not (file_dict and isinstance(file_dict, dict)):
        logging.error(f"Failed getting file url from Yandex Disk.")
        return
    file_url = str(file_dict.get('href'))
    file = await perform_http_request(method='GET', url=file_url, headers=yc.disk_headers)
    if not (file and isinstance(file, bytes)):
        logging.error(f"Failed getting file from Yandex Disk.")
        return
    return file
