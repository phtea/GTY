import asyncio
import logging
# String manipulations
import urllib.parse

import os
import dotenv
import json
from utils import make_http_request


# Load environment variables
DOTENV_PATH = ".env"
dotenv.load_dotenv()

GAND_ACCESS_TOKEN   = os.environ.get("GAND_ACCESS_TOKEN")
GAND_REFRESH_TOKEN  = os.environ.get("GAND_REFRESH_TOKEN")
GAND_PASSWORD       = os.environ.get("GAND_PASSWORD")
GAND_LOGIN          = os.environ.get("GAND_LOGIN")
GAND_PROGRAMMER_ID  = 777799898898

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

async def get_access_token(username, password):
    """Updates GAND_ACCESS_TOKEN and GAND_REFRESH_TOKEN from login+password
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Use make_http_request to send the POST request
    response_json = await make_http_request("POST", url, headers=headers, body=body)

    if response_json:
        access_token = response_json.get('access_token')
        refresh_token = response_json.get('refresh_token')

        # Update global tokens and environment variables
        global GAND_ACCESS_TOKEN
        GAND_ACCESS_TOKEN = access_token
        os.environ["GAND_ACCESS_TOKEN"] = access_token
        dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", access_token)

        global GAND_REFRESH_TOKEN
        GAND_REFRESH_TOKEN = refresh_token
        os.environ["GAND_REFRESH_TOKEN"] = refresh_token
        dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", refresh_token)
        logging.info(f"Authorized user: {username} [Gandiva]")
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
    response_json = await make_http_request("POST", url, headers=headers, body=body)

    if response_json:
        access_token = response_json.get('access_token')
        refresh_token = response_json.get('refresh_token')

        # Update global tokens and environment variables
        global GAND_ACCESS_TOKEN
        GAND_ACCESS_TOKEN = access_token
        os.environ["GAND_ACCESS_TOKEN"] = access_token
        dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", access_token)

        global GAND_REFRESH_TOKEN
        GAND_REFRESH_TOKEN = refresh_token
        os.environ["GAND_REFRESH_TOKEN"] = refresh_token
        dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", refresh_token)

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

async def get_task_by_id(request_id):
    """Fetch a task by ID using the GAND API."""
    content_type = "application/x-www-form-urlencoded"
    url = f"{HOST}/api/Requests/{request_id}"

    return await make_http_request('GET', url, headers=get_headers(content_type=content_type))

async def get_department_by_user_id(user_id):
    """Fetch department by user ID with caching."""
    if user_id in user_department_cache:
        logging.debug(f"Returning cached department for user {user_id}")
        return user_department_cache[user_id]

    url = f"{HOST}/api/Users/{user_id}"
    headers = get_headers(content_type="application/x-www-form-urlencoded")

    response_json = await make_http_request('GET', url, headers=headers)

    if response_json:
        department = response_json.get("Department")
        user_department_cache[user_id] = department  # Cache the department
        logging.debug(f"Successfully gathered department for user {user_id}")
        return department

    logging.error(f"Error fetching department for user {user_id}")
    return None
        
async def get_departments_for_users(user_ids):
    tasks = [get_department_by_user_id(user_id) for user_id in user_ids]
    departments = await asyncio.gather(*tasks)
    unique_departments = list(set(departments))
    return unique_departments     

async def get_page_of_tasks(page_number):
    """Fetch a page of tasks."""
    url = f"{HOST}/api/Requests/Filter"
    filter_data = {
        "Departments": [2],
        "Categories": [32],
        "Statuses": [4, 5, 6, 8, 10, 11, 12, 13]
    }

    body = {
        "Filtering": filter_data,
        "BaseFilter": 0,
        "Page": page_number,
        "Size": 100,
        "Sorting": 0,
        "Descending": False
    }

    response_json = await make_http_request(
        method="POST", 
        url=url, 
        headers=get_headers(content_type="application/json"), 
        body=json.dumps(body)
    )

    if response_json:
        logging.debug(f"Page {page_number} fetched.")
        return response_json
    else:
        logging.error(f"Failed to fetch page {page_number}")
        return None

# Define the function to get comments for a specific task
@retry_async(max_retries=3, cooldown=5)
async def get_task_comments(task_id):
    """Fetch comments for a specific task."""

    url = f"{HOST}/api/Requests/{task_id}/Comments"
    
    response_json = await make_http_request(
        method="GET", 
        url=url, 
        headers=get_headers(content_type="application/json")
    )

    if response_json or response_json == []:
        logging.debug(f"Found {len(response_json)} comments for task {task_id}.")
        return response_json
    else:
        logging.error(f"Failed to fetch comments for task {task_id}")
        return None

# Define the function to get comments for a list of task IDs with concurrency control
async def get_comments_for_tasks_concurrently(task_ids: list[int], max_concurrent_requests: int = 5):
    """Fetch comments for a list of tasks with a limit on concurrent requests."""
    task_comments = {}
    semaphore = asyncio.Semaphore(max_concurrent_requests)  # Create a semaphore with the desired limit

    async def fetch_comments(task_id):
        """Fetch comments for a single task ID while respecting the semaphore limit."""
        async with semaphore:  # Acquire the semaphore
            comments = await get_task_comments(task_id)
            return task_id, comments

    # Create a list of tasks
    tasks = [fetch_comments(task_id) for task_id in task_ids]

    # Run all tasks concurrently while respecting the semaphore
    responses = await asyncio.gather(*tasks)

    # Collect the comments for each task ID
    for task_id, comments in responses:
        task_comments[task_id] = comments

    return task_comments

# Define the function to get comments for a list of task IDs consecutively
async def get_comments_for_tasks_consecutively(task_ids: list[int]):
    """Fetch comments for a list of tasks one at a time."""
    task_comments = {}

    for task_id in task_ids:
        comments = await get_task_comments(task_id)
        task_comments[task_id] = comments

    return task_comments

async def get_all_tasks():
    logging.info("Fetching tasks... [Gandiva]")
    all_requests = []
    
    # Fetch the first page to get the total count and number of pages
    first_page_data = await get_page_of_tasks(1)
    if not first_page_data:
        return all_requests  # Return an empty list if the first page request fails

    total_requests = first_page_data['Total']
    all_requests.extend(first_page_data['Requests'])

    # Calculate the total number of pages
    total_pages = (total_requests // 100) + 1
    logging.info(f"Found {total_pages} pages of tasks. [Gandiva]")

    # Create a list of tasks for all remaining pages
    tasks = [
        get_page_of_tasks(page_number)
        for page_number in range(2, total_pages + 1)
    ]

    # Run all tasks concurrently
    responses = await asyncio.gather(*tasks)

    # Collect the requests from all the pages
    for response in responses:
        if response:
            all_requests.extend(response['Requests'])

    return all_requests


import time # using for timing functions
async def main():

    await get_access_token(GAND_LOGIN, GAND_PASSWORD)


if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())