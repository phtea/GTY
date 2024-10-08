import asyncio
import logging
# String manipulations
import urllib.parse
import configparser
import json
from utils import make_http_request
import db_module as db
import utils
from datetime import datetime, timedelta, timezone

# Path to the .ini file
CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
GAND_ACCESS_TOKEN       = None
GAND_REFRESH_TOKEN      = None
GAND_TOKEN_EXPIRY_DATE  = None

GAND_LOGIN              = config.get('Gandiva', 'login')
GAND_PASSWORD           = config.get('Gandiva', 'password')
GAND_PROGRAMMER_ID      = config.getint('Gandiva', 'programmer_id')
GAND_ROBOT_ID           = config.getint('Gandiva', 'robot_id')
MAX_CONCURRENT_REQUESTS = config.getint('Gandiva', 'max_concurrent_requests')
WAITING_ANALYST_ID      = 1018

class GroupsOfStatuses:
    in_progress             = [3, 4, 6, 8, 13]
    in_progress_or_waiting  = [3, 4, 6, 8, 10, 11, 12, 13]
    waiting                 = [10, 11, 12]
    waiting_or_finished     = [5, 7, 9, 10, 11, 12]
    finished                = [5, 7, 9]
    _all                    = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]



from functools import wraps

# Retry decorator
def retry_async(max_retries=3, cooldown=5, exceptions=(Exception,)):
    """Decorator to retry an asynchronous function after failures with a cooldown."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    # Try to execute the function
                    return await func(*args, **kwargs)
                except exceptions as e:
                    logging.error(f"Attempt {attempt} failed with error: {e}")

                    if attempt < max_retries:
                        logging.info(f"Retrying in {cooldown} seconds... (Attempt {attempt} of {max_retries})")
                        await asyncio.sleep(cooldown)
                    else:
                        logging.error("Max retries reached. Giving up.")
                        raise  # Reraise the exception after max retries
        return wrapper
    return decorator

def token_refresh_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        global GAND_ACCESS_TOKEN

        if GAND_ACCESS_TOKEN is None or await is_token_expired():
            # If no token or it's expired, refresh it
            logging.info("Token is missing or expired. Fetching new access token.")
            new_token = await get_access_token(GAND_LOGIN, GAND_PASSWORD)
            if new_token is None:
                logging.error("Failed to retrieve access token.")
                return None
        return await func(*args, **kwargs)
    return wrapper

async def is_token_expired():
    """Check if the token is expired by comparing expiration date with the current time."""
    global GAND_TOKEN_EXPIRY_DATE

    if GAND_TOKEN_EXPIRY_DATE is None:
        logging.info("Token expiry date not set, assuming token is expired.")
        return True

    current_time = datetime.now(timezone.utc)
    if current_time >= GAND_TOKEN_EXPIRY_DATE:
        logging.info("Token is expired based on the expiry date.")
        return True
    return False

# Dictionary for caching user departments
user_department_cache = {}

# Handle not enough data in .env
if not (GAND_PASSWORD and GAND_LOGIN):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     GAND_PASSWORD
                     GAND_LOGIN""")
HOST = "https://api-gandiva.s-stroy.ru"

# Authorization block

async def get_access_token(login, password):
    """Updates GAND_ACCESS_TOKEN and GAND_REFRESH_TOKEN from login+password
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "password",
        "username": login,
        "password": password
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Use make_http_request to send the POST request
    response = await make_http_request("POST", url, headers=headers, body=body)

    if response:
        access_token = response.get('access_token')
        refresh_token = response.get('refresh_token')
        expires_in = response.get('expires_in')


        # Update global tokens
        global GAND_ACCESS_TOKEN
        GAND_ACCESS_TOKEN = access_token

        global GAND_REFRESH_TOKEN
        GAND_REFRESH_TOKEN = refresh_token

        if expires_in:
            # Calculate the token expiry date
            global GAND_TOKEN_EXPIRY_DATE
            GAND_TOKEN_EXPIRY_DATE = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        logging.info(f"Authorized user: {login} [Gandiva]")
        return access_token
    else:
        logging.error("Failed to retrieve access token.")
        return None

async def refresh_access_token(refresh_token):
    """Gets and updates access_token + refresh_token
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Use make_http_request to send the POST request
    response = await make_http_request("POST", url, headers=headers, body=body)

    if response:
        access_token = response.get('access_token')
        refresh_token = response.get('refresh_token')

        # Update global tokens and environment variables
        global GAND_ACCESS_TOKEN
        GAND_ACCESS_TOKEN = access_token

        global GAND_REFRESH_TOKEN
        GAND_REFRESH_TOKEN = refresh_token

        logging.info('Successfully refreshed Gandiva token')

        return access_token
    else:
        logging.error("Failed to refresh access token.")
        return None

def get_headers(content_type="application/json"):
    return {
        "Content-type": content_type,
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }

# Functions

@token_refresh_decorator
async def get_task(g_task_id: str):
    """Fetch a task by ID using the GAND API."""
    content_type = "application/x-www-form-urlencoded"
    url = f"{HOST}/api/Requests/{g_task_id}"

    return await make_http_request('GET', url, headers=get_headers(content_type=content_type))

@token_refresh_decorator
async def get_department_by_user_id(g_user_id):
    """Fetch department by user ID with caching."""
    if g_user_id in user_department_cache:
        logging.debug(f"Returning cached department for user {g_user_id}")
        return user_department_cache[g_user_id]

    url = f"{HOST}/api/Users/{g_user_id}"
    headers = get_headers(content_type="application/x-www-form-urlencoded")

    response_json = await make_http_request('GET', url, headers=headers)

    if response_json:
        department = response_json.get("Department")
        user_department_cache[g_user_id] = department  # Cache the department
        logging.debug(f"Successfully gathered department for user {g_user_id}")
        return department

    logging.error(f"Error fetching department for user {g_user_id}")
    return None

@token_refresh_decorator
async def get_departments_for_users(user_ids):
    tasks = [get_department_by_user_id(user_id) for user_id in user_ids]
    departments = await asyncio.gather(*tasks)
    unique_departments = list(set(departments))
    return unique_departments     

@token_refresh_decorator
async def get_page_of_tasks(page_number: int, statuses: list[int]):
    """Fetch a page of tasks."""
    url = f"{HOST}/api/Requests/Filter"
    filter_data = {
        "Departments": [2],
        "Categories": [32],
        "Statuses": statuses
    }

    body = {
        "Filtering": filter_data,
        "BaseFilter": 0,
        "Page": page_number,
        "Size": 100,
        "Sorting": 0,
        "Descending": False
    }

    response = await make_http_request(method="POST", url=url, 
        headers=get_headers(), body=json.dumps(body))

    if response:
        logging.debug(f"Page {page_number} fetched.")
        return response
    else:
        logging.error(f"Failed to fetch page {page_number}")
        return None

# Define the function to get comments for a specific task
@token_refresh_decorator
@retry_async(max_retries=3, cooldown=5)
async def get_task_comments(g_task_id):
    """Fetch comments for a specific task."""

    url = f"{HOST}/api/Requests/{g_task_id}/Comments"
    
    response = await make_http_request(method="GET", url=url, headers=get_headers())

    if response is None:
        logging.error(f"Failed to fetch comments for task {g_task_id}")
        return None
    logging.debug(f"Found {len(response)} comments for task {g_task_id}.")
    return response

def get_task_by_id_from_list(task_list, task_id):
    """
    Retrieves a task from a list of tasks based on the given task_id.

    :param task_list: List of task dictionaries.
    :param task_id: The ID of the task to retrieve.
    :return: The task dictionary if found, otherwise None.
    """
    for task in task_list:
        if task.get('Id') == task_id:
            return task
    return None  # Return None if no task with the given ID is found

# Define the function to get comments for a list of task IDs with concurrency control
@token_refresh_decorator
async def get_comments_for_tasks_concurrently(g_task_ids: list[int]):
    """Fetch comments for a list of tasks with a limit on concurrent requests."""
    task_comments = {}
    max_concurrent_requests = MAX_CONCURRENT_REQUESTS if MAX_CONCURRENT_REQUESTS else 5
    semaphore = asyncio.Semaphore(max_concurrent_requests)  # Create a semaphore with the desired limit

    async def fetch_comments(task_id):
        """Fetch comments for a single task ID while respecting the semaphore limit."""
        async with semaphore:  # Acquire the semaphore
            comments = await get_task_comments(task_id)
            return task_id, comments

    # Create a list of tasks
    tasks = [fetch_comments(task_id) for task_id in g_task_ids]

    # Run all tasks concurrently while respecting the semaphore
    responses = await asyncio.gather(*tasks)

    # Collect the comments for each task ID
    for task_id, comments in responses:
        task_comments[task_id] = comments

    return task_comments

# Define the function to get comments for a list of task IDs consecutively
@token_refresh_decorator
async def get_comments_for_tasks_consecutively(g_task_ids: list[int]):
    """Fetch comments for a list of tasks one at a time."""
    task_comments = {}

    for task_id in g_task_ids:
        comments = await get_task_comments(task_id)
        task_comments[task_id] = comments

    return task_comments

def get_comments_generator(execution_mode: str = 'async'):
    if execution_mode == 'async':
        return get_comments_for_tasks_concurrently
    return get_comments_for_tasks_consecutively

@token_refresh_decorator
async def get_tasks(statutes: list[int] = [3, 4, 6, 8, 10, 11]):
    logging.info("Fetching tasks...")
    all_requests = []
    
    # Fetch the first page to get the total count and number of pages
    first_page_data = await get_page_of_tasks(1, statuses=statutes)
    if not first_page_data:
        return all_requests  # Return an empty list if the first page request fails

    total_requests = first_page_data['Total']
    all_requests.extend(first_page_data['Requests'])

    # Calculate the total number of pages
    total_pages = (total_requests // 100) + 1
    logging.debug(f"Found {total_pages} pages of tasks.")

    # Create a list of tasks for all remaining pages
    tasks = [
        get_page_of_tasks(page_number, statuses=statutes)
        for page_number in range(2, total_pages + 1)
    ]

    # Run all tasks concurrently
    responses = await asyncio.gather(*tasks)

    # Collect the requests from all the pages
    for response in responses:
        if response:
            all_requests.extend(response['Requests'])

    logging.info(f"Fetched {len(all_requests)} tasks. [Gandiva]")

    return all_requests


def extract_tasks_by_status(g_tasks, statuses):
    """
    Extract tasks that have a status in the provided list of statuses.

    :param g_tasks: List of task dictionaries.
    :param statuses: List of statuses to filter tasks by.
    :return: List of tasks that match the provided statuses.
    """
    return [task for task in g_tasks if task.get('Status') in statuses]

# Comments functionality
@token_refresh_decorator
async def add_comment(g_task_id, text, y_comment_id, author_name, g_addressees=None):
    """Fetch comments for a list of tasks one at a time."""
    url = f"{HOST}/api/Requests/{g_task_id}/Comments"
    # Format the comment according to the specified template
    if y_comment_id and author_name:
        text = f"[{y_comment_id}] {author_name}:<br>{text}"
        
    body = {"Text": text}

    if g_addressees: body["Addressees"] = g_addressees

    response = await make_http_request(
        method="POST", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body)
    )

    if response:
        logging.debug(f"Comment was successfully added to task {g_task_id}!")
        return response
    else:
        logging.error(f"Comment was not added to task {g_task_id}.")
        return None

@token_refresh_decorator
async def edit_comment(g_comment_id, y_comment_id, text, author_name, g_addressees=None):
    """Fetch comments for a list of tasks one at a time."""
    url = f"{HOST}/api/Common/Comments/{g_comment_id}"
    # Format the comment according to the specified template
    if y_comment_id and author_name:
        text = f"[{y_comment_id}] {author_name}:<br>{text}"
        
    body = {"Text": text}

    if g_addressees: body["Addressees"] = g_addressees

    response = await make_http_request(
        method="PUT", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body)
    )

    if response:
        logging.debug(f"Comment {g_comment_id} was edited!")
        return response
    else:
        logging.error(f"Comment {g_comment_id} was not edited.")
        return None

@token_refresh_decorator
async def edit_task(g_task_id: int, last_modified_date: str, required_start_date: str = None):
    """edit task in Gandiva"""
    url = f"{HOST}/api/Requests/{g_task_id}"
    body = {
        "LastModifiedDate": last_modified_date
    }
    if required_start_date: body["RequiredStartDate"] = required_start_date
    
    response = await make_http_request(
        method="PUT", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body))
    
    if response:
        logging.debug(f"Task {g_task_id} was successfully edited!")
        return response
    else:
        logging.error(f"Task {g_task_id} was not edited.")
        return None

@token_refresh_decorator
async def edit_task_required_start_date(g_task_id: int, last_modified_date: str, required_start_date: str):
    """edit task in Gandiva"""
    url = f"{HOST}/api/Requests/{g_task_id}/RequiredStartDate"
    body = {
        "LastModifiedDate": last_modified_date,
        "RequiredStartDate": required_start_date
    }
    
    response = await make_http_request(
        method="PUT", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body))
    
    if response:
        logging.debug(f"Task's RequiredStartDate {g_task_id} was successfully edited!")
        return response
    else:
        logging.error(f"Task's RequiredStartDate {g_task_id} was not edited.")
        return None

@token_refresh_decorator
async def edit_task_contractor(g_task_id: int, last_modified_date: str, contractor: str):
    """edit task in Gandiva"""
    url = f"{HOST}/api/Requests/{g_task_id}/Contractor"
    body = {
        "LastModifiedDate": last_modified_date,
        "Contractor": {'Id': contractor}
    }

    response = await make_http_request(
        method="PUT", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body))
    
    if response:
        logging.debug(f"Task's Contractor {g_task_id} was successfully edited!")
        return response
    else:
        logging.error(f"Task's Contractor {g_task_id} was not edited.")
        return None

@token_refresh_decorator
async def delete_comment(g_comment_id):
    """Delete comment."""
    url = f"{HOST}/api/Common/Comments/{g_comment_id}"

    response = await make_http_request(method="DELETE", url=url, headers=get_headers())

    if response:
        logging.debug(f"Comment [{g_comment_id}] was successfully deleted!")
        return response
    else:
        logging.error(f"Comment [{g_comment_id}] was not deleted.")
        return None

@token_refresh_decorator
async def handle_tasks_in_work_but_waiting_for_analyst(needed_date):
    """needed_date in format: 2025-01-01T00:00:00+03:00"""
    tasks = await get_tasks(statutes=[6, 11, 13])
    waiting_for_analyst_id = WAITING_ANALYST_ID
    needed_tasks = [t for t in tasks 
                    if (c := t.get('Contractor')) and (c := c.get('Id')) and c == waiting_for_analyst_id]
    for needed_task in needed_tasks:
        last_modified_date = needed_task.get('LastModifiedDate')
        task_id = needed_task.get('Id')
        await edit_task_required_start_date(task_id, last_modified_date, required_start_date=needed_date)

@token_refresh_decorator
async def handle_waiting_for_analyst_or_no_contractor_no_required_start_date(g_tasks):
    next_year = utils.get_next_year_datetime()
    g_task_anomalies = [g for g in g_tasks if not g['Contractor'] or g['Contractor']['Id'] == WAITING_ANALYST_ID and not g['RequiredStartDate']]
    if not g_task_anomalies:
        logging.info(f"No anomalies found!")
        return True
    db_session = db.get_db_session()
    count = 0
    for g_task_anomaly in g_task_anomalies:
        g_task_id               = g_task_anomaly['Id']
        last_modified_date      = g_task_anomaly['LastModifiedDate']
        initiator_id            = g_task_anomaly.get('Initiator', {}).get('Id')
        department              = await get_department_by_user_id(initiator_id)
        y_analyst               = db.get_user_id_by_department(session=db_session, department_name=department)
        user                    = db.get_user_by_yandex_id(db_session, y_analyst)
        g_analyst_id            = None
        if user: g_analyst_id   = user.gandiva_user_id
        res_contractor          = None
        res_date                = None

        if not g_task_anomaly['RequiredStartDate']:
            res_date = await edit_task_required_start_date(g_task_id, last_modified_date, next_year)
            if res_date:
                logging.info(f"Changed required start date in task {g_task_id}!")
            else:
                logging.warning(f"Date was not changed in {g_task_id}, skipping...")
                continue

        if not g_analyst_id:
            logging.warning(f"No analyst found for task {g_task_id}, skipping [edit contractor]...")
            continue

        g_task_anomaly      = await get_task(g_task_id)
        last_modified_date  = g_task_anomaly['LastModifiedDate']
        res_contractor      = await edit_task_contractor(g_task_id, last_modified_date, g_analyst_id)
        if res_contractor:
            logging.info(f"Changed contractor to analyst in task {g_task_id}!")
        else:
            logging.error(f"Failed changing contractor to analyst in task {g_task_id}.")
        if res_contractor or res_date:
            count += 1
    logging.info(f'Handled {count} anomaly tasks!')
    # If all anomalies fixed -> return True, else False
    if len(g_task_anomalies) == count:
        return True
    else:
        return False