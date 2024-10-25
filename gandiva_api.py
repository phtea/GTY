import asyncio
import json
import logging
import urllib.parse
from datetime import datetime, timedelta, timezone
from configparser import ConfigParser
from functools import wraps
from typing import Callable, Awaitable, Any, TypeVar

# SQLAlchemy Imports
from sqlalchemy.orm import Session

# Project-Specific Modules
import db_module as db
from utils import perform_http_request
from utils import get_config
from utils import get_next_year_datetime


class GandivaConfig:
    """Configuration class for Gandiva API."""

    def __init__(self, config: ConfigParser):
        section = 'Gandiva'

        chars = '"\''

        self.login = config.get(section, 'login').strip(chars)
        self.password = config.get(section, 'password').strip(chars)
        self.programmer_id = config.get(section, 'programmer_id').strip(chars)
        self.waiting_analyst_id = config.get(
            section, 'waiting_analyst_id').strip(chars)
        self.robot_id = config.get(section, 'robot_id').strip(chars)
        self.max_concurrent_requests = config.getint(
            section, 'max_concurrent_requests')
        self.host = "https://api-gandiva.s-stroy.ru"


class GandivaTokens:
    """Class to manage access and refresh tokens for Gandiva API."""

    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry_date = None

    def is_expired(self):
        """Check if the access token is expired."""
        if self.token_expiry_date is None:
            logging.info(
                "Token expiry date not set, assuming token is expired.")
            return True

        current_time = datetime.now(timezone.utc)
        if current_time >= self.token_expiry_date:
            logging.info("Token is expired based on the expiry date.")
            return True
        return False

    def set_tokens(
            self, access_token: str, refresh_token: str, expires_in: int | None
    ) -> None:
        """Set access and refresh tokens and their expiry."""
        self.access_token = access_token
        self.refresh_token = refresh_token
        if expires_in is not None:
            self.token_expiry_date = datetime.now(
                timezone.utc) + timedelta(seconds=expires_in)


gc = GandivaConfig(get_config())
gt = GandivaTokens()


class Statuses:
    in_progress = [3, 4, 6, 8, 13]
    in_progress_or_waiting = [3, 4, 6, 8, 10, 11, 12, 13]
    waiting = [10, 11, 12]
    waiting_or_finished = [5, 7, 9, 10, 11, 12]
    finished = [5, 7, 9]
    all_ = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]


# Retry decorator

def retry_async(
        max_retries: int = 3,
        cooldown: int = 5,
        exceptions: tuple[type[BaseException], ...] = (Exception,)
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Decorator to retry an asynchronous function after failures with a cooldown."""
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(1, max_retries + 1):
                try:
                    # Try to execute the function
                    return await func(*args, **kwargs)
                except exceptions as e:
                    logging.error(f"Attempt {attempt} failed with error: {e}")

                    if attempt < max_retries:
                        logging.info(
                            f"Retrying in {cooldown} seconds... (Attempt {attempt} of {max_retries})")
                        await asyncio.sleep(cooldown)
                    else:
                        logging.error("Max retries reached. Giving up.")
                        raise  # Reraise the exception after max retries
        return wrapper
    return decorator


_FuncType = TypeVar("_FuncType", bound=Callable[..., Awaitable[Any]])


def token_refresh_decorator(func: _FuncType) -> _FuncType:
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if gt.access_token is None or gt.is_expired():
            # If no token or it's expired, refresh it
            logging.info(
                "Token is missing or expired. Fetching new access token.")
            new_token = await get_access_token(gc.login, gc.password)
            if new_token is None:
                logging.error("Failed to retrieve access token.")
                return None

        # Await the actual function only once, after confirming token validity
        return await func(*args, **kwargs)
    return wrapper  # type: ignore


async def get_access_token(login: str, password: str) -> str | None:
    """Updates GAND_ACCESS_TOKEN and GAND_REFRESH_TOKEN from login+password."""
    endpoint = "/Token"
    url = gc.host + endpoint

    body = {
        "grant_type": "password",
        "username": login,
        "password": password
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = await perform_http_request("POST", url, headers=headers, body=body)

    if not response or not isinstance(response, dict):
        logging.error("Failed to retrieve access token.")
        return None

    access_token = response.get('access_token')
    refresh_token = response.get('refresh_token')
    expires_in = response.get('expires_in')

    if not (access_token and refresh_token and expires_in):
        logging.error("Failed to retrieve access token.")
        return None

    gt.set_tokens(access_token, refresh_token, expires_in)
    logging.info(f"Authorized user: {login} [Gandiva]")
    return access_token


async def refresh_access_token(refresh_token: str) -> str | None:
    """Gets and updates access_token + refresh_token."""
    endpoint = "/Token"
    url = gc.host + endpoint

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = await perform_http_request("POST", url, headers=headers, body=body)
    if not isinstance(response, dict):
        logging.error("Failed to refresh access token.")
        return None

    access_token = response.get('access_token')
    new_refresh_token = response.get('refresh_token')
    expires_in = response.get('expires_in')
    if not (access_token and new_refresh_token and expires_in):
        logging.error("Failed to retrieve access token.")
        return None

    gt.set_tokens(access_token, new_refresh_token, expires_in)
    logging.info('Successfully refreshed Gandiva token')
    return access_token


def get_headers(content_type: str = "application/json") -> dict[str, str]:
    return {
        "Content-type": content_type,
        "Authorization": f"Bearer {gt.access_token}"
    }


@token_refresh_decorator
async def get_task(
    g_task_id: str
) -> dict[str, Any] | None:
    """Fetch a task by ID using the Gandiva API."""
    content_type = "application/x-www-form-urlencoded"
    url = f"{gc.host}/api/Requests/{g_task_id}"

    task = await perform_http_request('GET', url, headers=get_headers(content_type=content_type))
    if not isinstance(task, dict):
        return None
    return task


# Global cache for department lookups
department_cache: dict[str, str] = {}


@token_refresh_decorator
async def get_department_by_user_id(
    g_user_id: str
) -> str | None:
    """
    Fetch the department name for a given Gandiva user ID.

    :param g_user_id: The Gandiva user ID for which to fetch the department.
    :type g_user_id: str
    :return: The department name associated with the user ID, or None if not found.
    :rtype: str | None
    """
    department = check_department_in_cache(g_user_id)
    if department:
        return department

    url = f"{gc.host}/api/Users/{g_user_id}"
    response = await perform_http_request('GET', url, headers=get_headers())

    if response and isinstance(response, dict):
        department = str(response.get("Department"))
        cache_department(g_user_id, department)
        return department

    logging.error(f"Error fetching department for user {g_user_id}")
    return None


def cache_department(g_user_id: str, department: str) -> None:
    department_cache[g_user_id] = department


def check_department_in_cache(g_user_id: str) -> str | None:
    """
    Cache the department name for a given Gandiva user ID.

    :param g_user_id: The Gandiva user ID for which to fetch the department.
    :type g_user_id: str
    :return: The department name associated with the user ID, or None if not found.
    :rtype: str | None
    """
    if g_user_id in department_cache:
        logging.debug(
            f"Cache hit for user {g_user_id}, returning cached department.")
        return department_cache[g_user_id]


@token_refresh_decorator
async def get_departments_for_users(
    user_ids: list[str]
) -> list[str | None]:
    tasks = [get_department_by_user_id(user_id)
             for user_id in user_ids]

    departments: list[str | None] = await asyncio.gather(*tasks)
    unique_departments = list(set(departments))
    return unique_departments


@token_refresh_decorator
async def get_page_of_tasks(
    page_number: int, statuses: list[int]
) -> dict[str, Any] | None:
    """Fetch a page of tasks."""
    url = f"{gc.host}/api/Requests/Filter"
    filter_data: dict[str, list[int]] = {
        "Departments": [2],
        "Categories": [32],
        "Statuses": statuses
    }

    body: dict[str, Any] = {
        "Filtering": filter_data,
        "BaseFilter": 0,
        "Page": page_number,
        "Size": 100,
        "Sorting": 0,
        "Descending": False
    }
    json_body = json.dumps(body)
    response = await perform_http_request(
        method="POST", url=url, headers=get_headers(), body=json_body
    )

    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to fetch page {page_number}")
        return None

    logging.debug(f"Page {page_number} fetched.")
    return response

# Define the function to get comments for a specific task


@token_refresh_decorator
@retry_async(max_retries=3, cooldown=5)
async def get_task_comments(g_task_id: int) -> list[dict[str, Any]] | None:
    """Fetch comments for a specific task."""

    url = f"{gc.host}/api/Requests/{g_task_id}/Comments"

    response = await perform_http_request(method="GET", url=url, headers=get_headers())
    if not isinstance(response, list):
        logging.error(f"Failed to fetch comments for task {g_task_id}")
        return None

    logging.debug(f"Found {len(response)} comments for task {g_task_id}.")
    return response


def get_task_by_id_from_list(task_list: list[dict[str, Any]], task_id: str) -> dict[str, Any] | None:
    """Retrieve a task from a list of tasks based on the given task_id."""
    return next((task for task in task_list if task.get('Id') == task_id), None)


@token_refresh_decorator
async def get_comments_for_tasks_concurrently(
    g_task_ids: list[int]
) -> dict[str, list[dict[str, Any]]]:
    """Fetch comments for a list of tasks with a limit on concurrent requests."""
    task_comments: dict[str, list[dict[str, Any]]] = {}
    max_concurrent_requests = gc.max_concurrent_requests or 5
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def fetch_comments(task_id: int) -> tuple[int, list[dict[str, Any]]]:
        async with semaphore:
            comment_data = await get_task_comments(task_id)
            return task_id, comment_data

    responses = await asyncio.gather(*(fetch_comments(task_id) for task_id in g_task_ids))

    for task_id, comments in responses:
        task_comments[str(task_id)] = comments

    return task_comments


@token_refresh_decorator
async def get_comments_for_tasks_consecutively(
    g_task_ids: list[int]
) -> dict[str, list[dict[str, Any]]]:
    """Fetch comments for a list of tasks one at a time."""
    task_comments: dict[str, list[dict[str, Any]]] = {}

    for task_id in g_task_ids:
        comments = await get_task_comments(task_id)
        task_comments[str(task_id)] = comments

    return task_comments


def get_comments_generator(
        execution_mode: str = 'async'
) -> Callable[[list[int]], Awaitable[dict[str, list[dict[str, Any]]]]]:
    if execution_mode == 'async':
        return get_comments_for_tasks_concurrently
    return get_comments_for_tasks_consecutively


@token_refresh_decorator
async def get_tasks(
    statutes: list[int] | None = None
) -> list[dict[str, Any]]:
    """
    Fetch all tasks from Gandiva with the given statuses.

    :param statutes: The statuses of the tasks to fetch. Defaults to [3, 4, 6, 8, 10, 11].
    :type statutes: list[int] or None
    :return: A list of dictionaries representing the tasks.
    :rtype: list[dict[str, Any]]
    """
    if statutes is None:
        statutes = [3, 4, 6, 8, 10, 11]
    logging.info("Fetching tasks...")
    all_requests: list[dict[str, Any]] = []

    first_page_data = await get_page_of_tasks(1, statuses=statutes)
    if not first_page_data:
        return all_requests

    total = first_page_data.get('Total')
    requests_first_page = first_page_data.get('Requests')
    if not (total and requests_first_page):
        return all_requests

    all_requests.extend(requests_first_page)
    total_pages = (total // 100) + 1
    logging.debug(f"Found {total_pages} pages of tasks.")

    tasks = [get_page_of_tasks(page_number, statuses=statutes)
             for page_number in range(2, total_pages + 1)]
    responses = await asyncio.gather(*tasks)

    for r in responses:
        requests = r.get('Requests') if r else None
        if requests:
            all_requests.extend(requests)

    logging.info(f"Fetched {len(all_requests)} tasks. [Gandiva]")
    return all_requests


def extract_tasks_by_status(
    g_tasks: list[dict[str, Any]], statuses: list[int]
) -> list[dict[str, Any]]:
    """
    Extract tasks that have a status in the provided list of statuses.

    :param g_tasks: List of task dictionaries.
    :param statuses: List of statuses to filter tasks by.
    :return: List of tasks that match the provided statuses.
    """
    return [task for task in g_tasks if task.get('Status') in statuses]

# Comments functionality


@token_refresh_decorator
async def add_comment(
    g_task_id: int,
    text: str,
    y_comment_id: str | None,
    author_name: str | None,
    g_addressees: list[str] | None = None
) -> dict[str, Any] | None:
    """Fetch comments for a list of tasks one at a time."""
    url = f"{gc.host}/api/Requests/{g_task_id}/Comments"
    # Format the comment according to the specified template
    if y_comment_id and author_name:
        text = f"[{y_comment_id}] {author_name}:<br>{text}"

    body: dict[str, Any] = {"Text": text}

    if g_addressees:
        body["Addressees"] = g_addressees

    json_body = json.dumps(body)

    response = await perform_http_request(
        method="POST",
        url=url,
        headers=get_headers(),
        body=json_body
    )

    if not (response and isinstance(response, dict)):
        logging.error(f"Comment was not added to task {g_task_id}.")
        return None

    logging.debug(f"Comment was successfully added to task {g_task_id}!")
    return response


@token_refresh_decorator
async def edit_comment(
    g_comment_id: int | str,
    y_comment_id: str | None,
    text: str,
    author_name: str | None,
    g_addressees: list[str] | None = None
) -> dict[str, Any] | None:
    """Edit a comment in Gandiva."""
    url = f"{gc.host}/api/Common/Comments/{g_comment_id}"

    if y_comment_id and author_name:
        text = f"[{y_comment_id}] {author_name}:<br>{text}"

    body: dict[str, Any] = {"Text": text}
    if g_addressees:
        body["Addressees"] = g_addressees
    json_body = json.dumps(body)
    response = await perform_http_request(
        method="PUT",
        url=url,
        headers=get_headers(),
        body=json_body
    )

    if isinstance(response, dict):
        logging.debug(f"Comment {g_comment_id} was successfully edited.")
        return response

    logging.error(f"Failed to edit comment {g_comment_id}.")
    return None


@token_refresh_decorator
async def edit_task(
    g_task_id: int,
    last_modified_date: str,
    required_start_date: str | None = None
) -> dict[str, Any] | None:
    """
    Edit task in Gandiva.

    :param g_task_id: task ID in Gandiva
    :param last_modified_date: date of last modification in ISO format
    :param required_start_date: new required start date in ISO format
    :return:
        successful response from Gandiva API or None if error
    """

    url = f"{gc.host}/api/Requests/{g_task_id}"
    body = {"LastModifiedDate": last_modified_date}
    if required_start_date:
        body["RequiredStartDate"] = required_start_date

    json_body = json.dumps(body)
    response = await perform_http_request(
        method="PUT",
        url=url,
        headers=get_headers(),
        body=json_body
    )

    if isinstance(response, dict):
        logging.debug(f"Task {g_task_id} was successfully edited!")
        return response

    logging.error(f"Task {g_task_id} was not edited.")
    return None


@token_refresh_decorator
async def edit_task_required_start_date(
    g_task_id: str,
    last_modified_date: str,
    required_start_date: str
) -> dict[str, Any] | None:
    """edit task in Gandiva"""
    url = f"{gc.host}/api/Requests/{g_task_id}/RequiredStartDate"
    body: dict[str, str] = {
        "LastModifiedDate": last_modified_date,
        "RequiredStartDate": required_start_date
    }
    json_body = json.dumps(body)

    response = await perform_http_request(
        method="PUT",
        url=url,
        headers=get_headers(),
        body=json_body
    )

    if not (response and isinstance(response, dict)):
        logging.error(f"Task's RequiredStartDate {g_task_id} was not edited.")
        return None
    logging.debug(
        f"Task's RequiredStartDate {g_task_id} was successfully edited!"
    )
    return response


@token_refresh_decorator
async def edit_task_contractor(
    g_task_id: int | str,
    last_modified_date: str,
    contractor: str,
) -> dict[str, Any] | None:
    """
    Edit task in Gandiva.

    :param g_task_id: task ID in Gandiva
    :param last_modified_date: date of last modification in ISO format
    :param contractor: ID of the contractor in Gandiva
    :return:
        successful response from Gandiva API or None if error
    """
    url = f"{gc.host}/api/Requests/{g_task_id}/Contractor"

    body: dict[str, Any] = {
        "LastModifiedDate": last_modified_date,
        "Contractor": {"Id": contractor},
    }

    json_body = json.dumps(body)
    response = await perform_http_request(
        method="PUT",
        url=url,
        headers=get_headers(),
        body=json_body,
    )

    if not (response and isinstance(response, dict)):
        logging.error(f"Task's Contractor {g_task_id} was not edited.")
        return None

    logging.debug(
        f"Task's Contractor {g_task_id} was successfully edited!")
    return response


@token_refresh_decorator
async def delete_comment(
    g_comment_id: int
) -> dict[str, Any] | None:
    """
    Delete a comment.

    :param g_comment_id: The ID of the comment to delete.
    :type g_comment_id: int
    :return: A dictionary response from the API if successful, otherwise None.
    :rtype: dict[str, Any] | None
    """
    url = f"{gc.host}/api/Common/Comments/{g_comment_id}"

    response = await perform_http_request(method="DELETE", url=url, headers=get_headers())

    if not (response and isinstance(response, dict)):
        logging.error(f"Comment [{g_comment_id}] was not deleted.")
        return None

    logging.debug(f"Comment [{g_comment_id}] was successfully deleted!")
    return response


@token_refresh_decorator
async def handle_tasks_in_work_but_waiting_for_analyst(
    needed_date: str
) -> None:
    """needed_date in format: 2025-01-01T00:00:00+03:00"""
    tasks: list[dict[str, Any]] = await get_tasks(statutes=[6, 11, 13])
    waiting_analyst_id = gc.waiting_analyst_id
    needed_tasks: list[dict[str, Any]] = [
        t for t in tasks
        if (c := t.get('Contractor'))
        and (c := c.get('Id'))
        and c == waiting_analyst_id
    ]
    for needed_task in needed_tasks:
        last_modified_date: str = str(needed_task.get('LastModifiedDate'))
        task_id: str = str(needed_task.get('Id'))
        await edit_task_required_start_date(
            task_id, last_modified_date, required_start_date=needed_date
        )


@token_refresh_decorator
async def handle_no_contractor_or_waiting_for_analyst(
    tasks: list[dict[str, Any]]
) -> bool:
    """
    Handle tasks with no contractor or waiting for analyst.

    Tasks are considered anomalies if they have no contractor 
    or the contractor is the waiting analyst and there's no 
    required start date set. This function attempts to fix these anomalies.
    """
    next_year_start_date = get_next_year_datetime()
    waiting_analyst_id = gc.waiting_analyst_id

    anomalous_tasks = filter_anomalous_tasks(tasks, waiting_analyst_id)

    if not anomalous_tasks:
        logging.info("No anomalies found!")
        return True

    db_session = db.get_db_session()

    return await process_anomalous_tasks(anomalous_tasks, db_session, next_year_start_date)


def filter_anomalous_tasks(
    tasks: list[dict[str, Any]], waiting_analyst_id: str
) -> list[dict[str, Any]]:
    """
    Filter tasks with no contractor or where the contractor is the waiting analyst 
    and no required start date is set.
    """
    return [
        task for task in tasks
        if not task.get('Contractor') or (
            task['Contractor'].get('Id') == waiting_analyst_id
            and not task.get('RequiredStartDate')
        )
    ]


async def process_anomalous_tasks(
    tasks: list[dict[str, Any]], db_session: Session, next_year_start_date: str
) -> bool:
    """
    Process anomalous tasks: set a required start date and change the contractor to the correct analyst.
    """
    fixed_task_count = 0

    for task in tasks:
        task_id = task['Id']
        last_modified_date = task['LastModifiedDate']

        initiator: dict[str, str] = task.get('Initiator', {})
        if not initiator:
            raise ValueError(
                f"Task {task_id} has no initiator!"
            )

        initiator_id: Any = initiator.get('Id')
        if not initiator_id:
            raise ValueError(
                f"Initiator for task {task_id} has no ID!"
            )
        initiator_id = str(initiator.get('Id'))
        department: Any = await get_department_by_user_id(initiator_id)

        if not isinstance(department, str):
            logging.warning(
                f"No department found for user {initiator_id}, skipping contractor update..."
            )
            continue

        analyst_id = get_analyst_id_by_department(db_session, department)
        if not (isinstance(analyst_id, str) and valid_analyst(task_id, analyst_id)):
            logging.warning(
                f"No analyst found for task {task_id}, skipping contractor update..."
            )
            continue

        task_fixed = await fix_task_anomaly(
            task_id, last_modified_date, analyst_id, next_year_start_date
        )

        if task_fixed:
            fixed_task_count += 1

    logging.info(f"Handled {fixed_task_count} anomaly tasks!")
    return fixed_task_count == len(tasks)


def valid_department(task_id: str, department: str | None) -> bool:
    """
    Check if a valid department was found for the task initiator.
    """
    if not department:
        logging.warning(
            f"No department found for task {task_id}, skipping contractor update..."
        )
        return False
    return True


def get_analyst_id_by_department(db_session: Session, department: str) -> str | None:
    """
    Fetch the analyst's ID based on the department.
    """
    y_analyst_id = db.get_user_id_by_department(
        session=db_session, department_name=department
    )
    analyst_user = db.get_user_by_yandex_id(db_session, str(y_analyst_id))

    return str(analyst_user.gandiva_user_id) if analyst_user else None


def valid_analyst(task_id: str, analyst_id: str | None) -> bool:
    """
    Check if a valid analyst was found.
    """
    if not analyst_id:
        logging.warning(
            f"No analyst found for task {task_id}, skipping contractor update..."
        )
        return False
    return True


async def fix_task_anomaly(
    task_id: str, last_modified_date: str, analyst_id: str, next_year_start_date: str
) -> bool:
    """
    Attempt to fix anomalies in the task by setting the required start date 
    and updating the contractor to the correct analyst.
    """
    task_fixed = False

    # Set the required start date if missing
    if not await set_required_start_date(task_id, last_modified_date, next_year_start_date):
        return False

    task_fixed = True

    # Refresh the task data after updating the start date
    updated_task = await get_task(task_id)
    if not updated_task:
        logging.error(f"Task {task_id} not found after start date update.")
        return False

    # Update the contractor to the correct analyst
    last_modified_date = updated_task['LastModifiedDate']
    if await update_contractor(task_id, last_modified_date, analyst_id):
        logging.info(f"Changed contractor to analyst in task {task_id}!")
    else:
        logging.error(
            f"Failed to change contractor to analyst in task {task_id}.")
        task_fixed = False

    return task_fixed


async def set_required_start_date(
    task_id: str, last_modified_date: str, next_year_start_date: str
) -> bool:
    """
    Set the required start date for the task if it is missing.
    """
    if await edit_task_required_start_date(task_id, last_modified_date, next_year_start_date):
        logging.info(
            f"Set required start date for task {task_id} to {next_year_start_date}.")
        return True
    logging.warning(f"Failed to set required start date for task {task_id}.")
    return False


async def update_contractor(
    task_id: str, last_modified_date: str, analyst_id: str
) -> bool:
    """
    Update the contractor of the task to the specified analyst.
    """
    return bool(await edit_task_contractor(task_id, last_modified_date, analyst_id))
