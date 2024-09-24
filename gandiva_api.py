import asyncio
import logging
# String manipulations
import urllib.parse
import configparser
import json
from utils import make_http_request

# Path to the .ini file
CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
GAND_ACCESS_TOKEN       = ''
GAND_REFRESH_TOKEN      = ''
GAND_LOGIN              = config.get('Gandiva', 'login')
GAND_PASSWORD           = config.get('Gandiva', 'password')
GAND_PROGRAMMER_ID      = config.getint('Gandiva', 'programmer_id')
GAND_ROBOT_ID           = config.getint('Gandiva', 'robot_id')
MAX_CONCURRENT_REQUESTS = config.getint('Gandiva', 'max_concurrent_requests')

class GroupsOfStatuses:
    in_progress = [3, 4, 6, 8, 10, 11, 12, 13]
    finished = [5, 7, 9]
    _all = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]



from functools import wraps

# Define the retry decorator
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

        # Update global tokens
        global GAND_ACCESS_TOKEN
        GAND_ACCESS_TOKEN = access_token

        global GAND_REFRESH_TOKEN
        GAND_REFRESH_TOKEN = refresh_token
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

async def get_task_by_id(g_task_id):
    """Fetch a task by ID using the GAND API."""
    content_type = "application/x-www-form-urlencoded"
    url = f"{HOST}/api/Requests/{g_task_id}"

    return await make_http_request('GET', url, headers=get_headers(content_type=content_type))

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
        
async def get_departments_for_users(user_ids):
    tasks = [get_department_by_user_id(user_id) for user_id in user_ids]
    departments = await asyncio.gather(*tasks)
    unique_departments = list(set(departments))
    return unique_departments     

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
@retry_async(max_retries=3, cooldown=5)
async def get_task_comments(g_task_id):
    """Fetch comments for a specific task."""

    url = f"{HOST}/api/Requests/{g_task_id}/Comments"
    
    response = await make_http_request(
        method="GET", 
        url=url, 
        headers=get_headers()
    )

    if response or response == []:
        logging.debug(f"Found {len(response)} comments for task {g_task_id}.")
        return response
    else:
        logging.error(f"Failed to fetch comments for task {g_task_id}")
        return None

# Define the function to get comments for a list of task IDs with concurrency control
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
async def get_comments_for_tasks_consecutively(g_task_ids: list[int]):
    """Fetch comments for a list of tasks one at a time."""
    task_comments = {}

    for task_id in g_task_ids:
        comments = await get_task_comments(task_id)
        task_comments[task_id] = comments

    return task_comments

async def get_all_tasks(statutes: list[int] = [3, 4, 6, 8, 10, 11]):
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

# Comments functionality
async def add_comment(g_task_id, text, y_comment_id, author_name, addressees=None):
    """Fetch comments for a list of tasks one at a time."""
    url = f"{HOST}/api/Requests/{g_task_id}/Comments"
    # Format the comment according to the specified template
    if y_comment_id and author_name:
        text = f"[{y_comment_id}] {author_name}:\n{text}"
        
    body = {"Text": text}

    if addressees: body["Addressees"] = addressees

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

async def edit_task_required_start_date(g_task_id: int, last_modified_date: str, required_start_date: str = None):
    """edit task in Gandiva"""
    url = f"{HOST}/api/Requests/{g_task_id}/RequiredStartDate"
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
        logging.debug(f"Task's RequiredStartDate {g_task_id} was successfully edited!")
        return response
    else:
        logging.error(f"Task's RequiredStartDate {g_task_id} was not edited.")
        return None

async def delete_comment(g_comment_id, text, addressees=None):
    """Fetch comments for a list of tasks one at a time."""
    url = f"{HOST}/api/Common/Comments/{g_comment_id}"

    body = {
    "Text": text
    }

    if addressees: body["Addressees"] = addressees

    response = await make_http_request(
        method="PUT", 
        url=url, 
        headers=get_headers(),
        body=json.dumps(body)
    )

    if response:
        logging.debug(f"Comment [{g_comment_id}] was successfully edited!")
        return response
    else:
        logging.error(f"Comment [{g_comment_id}] was not edited.")
        return None

async def delete_comment(g_comment_id):
    """Fetch comments for a list of tasks one at a time."""
    url = f"{HOST}/api/Common/Comments/{g_comment_id}"

    response = await make_http_request(method="DELETE", url=url, headers=get_headers())

    # TODO: может быть, это неправильная реализация, понадобится проверка на error в словаре
    if response:
        logging.debug(f"Comment [{g_comment_id}] was successfully deleted!")
        return response
    else:
        logging.error(f"Comment [{g_comment_id}] was not deleted.")
        return None

async def handle_tasks_in_work_but_waiting_for_analyst(needed_date):
    """needed_date in format: 2025-01-01T00:00:00+03:00"""
    tasks = await get_all_tasks(statutes=[6, 11, 13])
    waiting_for_analyst_id = 1018
    needed_tasks = []
    for task in tasks:
        if task.get('Contractor') and task.get('Contractor').get('Id') and task.get('Contractor').get('Id') == waiting_for_analyst_id:
            needed_tasks.append(task)
    for needed_task in needed_tasks:
        last_modified_date = needed_task.get('LastModifiedDate')
        task_id = needed_task.get('Id')
        await edit_task_required_start_date(task_id, last_modified_date, required_start_date=needed_date)

import time # using for timing functions
async def main():
    await get_access_token(GAND_LOGIN, GAND_PASSWORD)
    pass

if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())