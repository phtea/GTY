import asyncio
import aiohttp
import logging
# String manipulations
import urllib.parse

import os
import dotenv
import json


# Load environment variables
DOTENV_PATH = ".env"
dotenv.load_dotenv()

GAND_ACCESS_TOKEN   = os.environ.get("GAND_ACCESS_TOKEN")
GAND_REFRESH_TOKEN  = os.environ.get("GAND_REFRESH_TOKEN")
GAND_PASSWORD       = os.environ.get("GAND_PASSWORD")
GAND_LOGIN          = os.environ.get("GAND_LOGIN")
GAND_PROGRAMMER_ID  = 777799898898

# Dictionary for caching user departments
user_department_cache = {}

# TODO: before each request add token regeneration if token expired

# Handle not enough data in .env
if not (GAND_PASSWORD and GAND_LOGIN):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     GAND_PASSWORD
                     GAND_LOGIN""")
HOST = "https://api-gandiva.s-stroy.ru"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body) as response:
            response_json = await response.json()
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

            return access_token

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

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body) as response:
            response_json = await response.json()
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

# Common helper functions for HTTP requests
async def make_http_request(method, url, headers=None, body=None):
    """Generalized function to handle HTTP GET/POST requests."""

    valid_methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']
    if method.upper() not in valid_methods:
        logging.error(f"Invalid HTTP method: {method}")
        return None
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, data=body) as response:
                status_code = response.status
                response_text = await response.text()  # For logging purposes
                
                if status_code in [200, 201]:
                    return await response.json()  # Return the JSON response
                else:
                    logging.error(f"Request to {url} failed with status {status_code}: {response_text}")
                    return None
    except Exception as e:
        logging.error(f"Exception during request to {url}: {str(e)}")
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
        logging.info(f"Returning cached department for user {user_id}")
        return user_department_cache[user_id]

    url = f"{HOST}/api/Users/{user_id}"
    headers = get_headers(content_type="application/x-www-form-urlencoded")

    response_json = await make_http_request('GET', url, headers=headers)

    if response_json:
        department = response_json.get("Department")
        user_department_cache[user_id] = department  # Cache the department
        logging.info(f"Successfully gathered department for user {user_id}")
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
        logging.info(f"Page {page_number} fetched.")
        return response_json
    else:
        logging.error(f"Failed to fetch page {page_number}")
        return None

# Define the function to get comments for a specific task
async def get_task_comments(task_id):
    """Fetch comments for a specific task."""

    url = f"{HOST}/api/Requests/{task_id}/Comments"
    
    response_json = await make_http_request(
        method="GET", 
        url=url, 
        headers=get_headers(content_type="application/json")
    )

    if response_json:
        logging.info(f"Comments for task {task_id} fetched successfully.")
        return response_json
    elif response_json == []:
        logging.info(f"There are no comments for task {task_id}.")
        return []
    else:
        logging.error(f"Failed to fetch comments for task {task_id}")
        return None

# Define the function to get comments for a list of task IDs concurrently
async def get_comments_for_tasks(task_ids: list[int]):
    """Fetch comments for a list of tasks."""
    task_comments = {}

    tasks = [
        get_task_comments(task_id)
        for task_id in task_ids
    ]

    # Run all tasks concurrently
    responses = await asyncio.gather(*tasks)

    # Collect the comments for each task ID
    for task_id, comments in zip(task_ids, responses):
        task_comments[task_id] = comments

    return task_comments

async def get_all_tasks():
    all_requests = []
    
    # Fetch the first page to get the total count and number of pages
    first_page_data = await get_page_of_tasks(1)
    if not first_page_data:
        return all_requests  # Return an empty list if the first page request fails

    total_requests = first_page_data['Total']
    all_requests.extend(first_page_data['Requests'])

    # Calculate the total number of pages
    total_pages = (total_requests // 100) + 1
    logging.info(f"Found {total_pages} pages of tasks in Gandiva.")

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

        # response = await get_page_of_tasks_by_filter(page_number=1, session=session)
        # res = await get_department_by_user_id(session=session, user_id=138)


    start_time = time.time()
    # await get_all_tasks_by_filter()
    task_ids = [852, 853, 4382]
    tasks_comments = await get_comments_for_tasks(task_ids)
    

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())